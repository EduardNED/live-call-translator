// Read existing permissions only. Never ask for consent, send AppleEvents or read a tab.
import Foundation
import ApplicationServices
import Carbon

var target = AEAddressDesc()
let bundle = Array("com.google.Chrome".utf8)
let created = bundle.withUnsafeBytes { bytes in
    AECreateDesc(DescType(typeApplicationBundleID), bytes.baseAddress, bundle.count, &target)
}
var status: OSStatus = OSStatus(created)
if created == noErr {
    status = AEDeterminePermissionToAutomateTarget(&target, AEEventClass(typeWildCard),
                                                  AEEventID(typeWildCard), false)
    AEDisposeDesc(&target)
}
let result: [String: Any] = [
    "accessibility_current_process_trusted": AXIsProcessTrusted(),
    "chrome_appleevents_permission_status": status,
    "prompt_requested": false,
    "appleevent_sent": false,
    "screen_or_conversation_read": false
]
let data = try JSONSerialization.data(withJSONObject: result, options: [.sortedKeys])
FileHandle.standardOutput.write(data)
FileHandle.standardOutput.write(Data([10]))
