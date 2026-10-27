// Read-only Chrome metadata + bounded AX roles. No JavaScript, clicks, chat or captions.
import Foundation
import AppKit
import ApplicationServices
import Carbon
import CryptoKit

func output(_ object: [String: Any]) -> Never {
    if let data = try? JSONSerialization.data(withJSONObject: object),
       let value = String(data: data, encoding: .utf8) { print(value) }
    exit(0)
}
func attribute(_ element: AXUIElement, _ name: String) -> CFTypeRef? {
    var result: CFTypeRef?
    if AXUIElementCopyAttributeValue(element, name as CFString, &result) == .success { return result }
    return nil
}
func children(_ element: AXUIElement) -> [AXUIElement] {
    return attribute(element, "AXChildren") as? [AXUIElement] ?? []
}
func text(_ element: AXUIElement, _ name: String) -> String {
    return attribute(element, name) as? String ?? ""
}
func meeting(_ raw: String) -> String? {
    guard let url = URL(string: raw), url.scheme == "https", url.host == "meet.google.com",
          url.port == nil || url.port == 443 else { return nil }
    var path = url.path.lowercased()
    // Meet now keeps joined calls at /meet/<code>; bind both forms to the same meeting.
    if path.hasPrefix("/meet/") { path = String(path.dropFirst(5)) }
    guard path.range(of: "^/[a-z]{3}-[a-z]{4}-[a-z]{3}$", options: .regularExpression) != nil else { return nil }
    return "https://meet.google.com" + path
}
let bundle = "com.google.Chrome"
let args = CommandLine.arguments
let listOnly = args.count == 2 && args[1] == "--list"
guard listOnly || (args.count == 3 && args[1] == "--tab" && Int(args[2]) != nil) else {
    output(["state": "unavailable"])
}
guard let chrome = NSRunningApplication.runningApplications(withBundleIdentifier: bundle).first else {
    output(listOnly ? ["tabs": []] : ["state": "closed"])
}
var target = AEAddressDesc()
let bytes = Array(bundle.utf8)
let descriptor = bytes.withUnsafeBytes { AECreateDesc(DescType(typeApplicationBundleID), $0.baseAddress, bytes.count, &target) }
guard descriptor == noErr else { output(["state": "unavailable"]) }
let permission = AEDeterminePermissionToAutomateTarget(&target, AEEventClass(typeWildCard), AEEventID(typeWildCard), false)
AEDisposeDesc(&target)
guard permission == noErr else { output(["state": "unavailable"]) }
// AppleScript is only a native read adapter. Permission is checked without prompting first.
let source = """
with timeout of 2 seconds
    tell application id "com.google.Chrome"
        set rows to {}
        repeat with w in windows
            set activeID to id of active tab of w
            repeat with t in tabs of w
                set end of rows to {id of t, URL of t, (id of t is activeID)}
            end repeat
        end repeat
        return rows
    end tell
end timeout
"""
var error: NSDictionary?
guard let script = NSAppleScript(source: source) else { output(["state": "unavailable"]) }
let reply = script.executeAndReturnError(&error)
guard error == nil, reply.descriptorType == typeAEList else { output(["state": "unknown"]) }
var tabs: [(id: Int, url: String, active: Bool)] = []
if reply.numberOfItems > 0 {
    for i in 1...reply.numberOfItems {
        guard let row = reply.atIndex(i), row.numberOfItems == 3,
              let id = row.atIndex(1), let url = row.atIndex(2)?.stringValue,
              let active = row.atIndex(3) else { output(["state": "unknown"]) }
        tabs.append((Int(id.int32Value), url, active.booleanValue))
    }
}
if listOnly {
    output(["tabs": tabs.filter { meeting($0.url) != nil }.map { ["tab_id": $0.id, "active": $0.active] }])
}
let selected = Int(args[2])!
let matches = tabs.filter { $0.id == selected }
guard matches.count <= 1 else { output(["state": "unknown"]) }
guard let tab = matches.first else { output(["state": "closed"]) }
guard let url = meeting(tab.url) else { output(["state": "not_meet"]) }
let fingerprint = SHA256.hash(data: Data(url.utf8)).map { String(format: "%02x", $0) }.joined()
func state(_ value: String) -> Never { output(["state": value, "binding": fingerprint]) }
guard tab.active else { state("background") }
guard AXIsProcessTrusted() else { state("unavailable") }
let app = AXUIElementCreateApplication(chrome.processIdentifier)
AXUIElementSetMessagingTimeout(app, 0.25)
var areas: [AXUIElement] = []
var budget = 12000
func findAreas(_ element: AXUIElement) {
    budget -= 1
    guard budget > 0 else { return }
    if text(element, "AXRole") == "AXWebArea" {
        let raw = (attribute(element, "AXURL") as? URL)?.absoluteString ?? text(element, "AXURL")
        if meeting(raw) == url { areas.append(element) }
        return // Never traverse another page's content.
    }
    for child in children(element) { findAreas(child) }
}
findAreas(app)
guard budget > 0, areas.count == 1 else { state("unknown") }
let leaveButtons: Set<String> = ["Leave call", "Leave meeting", "Quitter l’appel", "Quitter l'appel", "Quitter la réunion", "Покинуть звонок", "Выйти из встречи", "Покинуть встречу"]
let leftHeadings: Set<String> = ["You left the meeting", "You've left the meeting", "Vous avez quitté la réunion", "Вы покинули встречу"]
let endedHeadings: Set<String> = ["The meeting has ended", "This meeting has ended", "La réunion est terminée", "Встреча завершена"]
var joined = false
var left = false
var ended = false
// Only button names and headings are considered. Static chat/caption text is never read.
func headingName(_ element: AXUIElement) -> String {
    for key in ["AXTitle", "AXValue", "AXDescription"] {
        let value = text(element, key)
        if !value.isEmpty { return value }
    }
    return children(element).filter { text($0, "AXRole") == "AXStaticText" }
        .map { text($0, "AXValue") }.joined(separator: " ")
}
func inspect(_ element: AXUIElement) {
    budget -= 1
    guard budget > 0 else { return }
    let role = text(element, "AXRole")
    if role == "AXButton" {
        let label = text(element, "AXTitle").isEmpty ? text(element, "AXDescription") : text(element, "AXTitle")
        if leaveButtons.contains(label) { joined = true }
    } else if role == "AXHeading" {
        let label = headingName(element)
        if leftHeadings.contains(label) { left = true }
        if endedHeadings.contains(label) { ended = true }
        return
    }
    for child in children(element) { inspect(child) }
}
inspect(areas[0])
guard budget > 0 else { state("unknown") }
// Contradictory signals are not departure; no missing-button inference.
if joined && (left || ended) { state("unknown") }
if joined { state("joined") }
if left && !ended { state("left") }
if ended && !left { state("ended") }
state("unknown")
