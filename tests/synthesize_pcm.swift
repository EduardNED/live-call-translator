// Diagnostic-only stock Apple TTS -> PCM16 mono 16 kHz on stdout, no audio file/device.
import AVFoundation
import Foundation

let args = CommandLine.arguments
if args.count != 3 { exit(2) }
let language = args[1]
let utterance = AVSpeechUtterance(string: args[2])
utterance.voice = AVSpeechSynthesisVoice(language: language)
utterance.rate = 0.45
let synthesizer = AVSpeechSynthesizer()
let lock = NSLock()
var finished = false
var failed = false
var pcm = Data()
var converter: AVAudioConverter?
let target = AVAudioFormat(commonFormat: .pcmFormatInt16, sampleRate: 16000,
                           channels: 1, interleaved: true)!
synthesizer.write(utterance) { audio in
    guard let buffer = audio as? AVAudioPCMBuffer else { return }
    lock.lock()
    defer { lock.unlock() }
    if buffer.frameLength == 0 { finished = true; return }
    if converter == nil { converter = AVAudioConverter(from: buffer.format, to: target) }
    guard let converter = converter else { failed = true; finished = true; return }
    let capacity = AVAudioFrameCount(ceil(Double(buffer.frameLength) * 16000 / buffer.format.sampleRate)) + 64
    let output = AVAudioPCMBuffer(pcmFormat: target, frameCapacity: capacity)!
    var supplied = false
    var error: NSError?
    let status = converter.convert(to: output, error: &error) { _, state in
        if supplied { state.pointee = .noDataNow; return nil }
        supplied = true
        state.pointee = .haveData
        return buffer
    }
    if status == .error || error != nil { failed = true; finished = true; return }
    if let samples = output.int16ChannelData {
        pcm.append(UnsafeBufferPointer(start: samples[0], count: Int(output.frameLength)))
    }
}
let deadline = Date().addingTimeInterval(20)
while Date() < deadline {
    lock.lock(); let done = finished; lock.unlock()
    if done { break }
    RunLoop.current.run(until: Date().addingTimeInterval(0.02))
}
lock.lock()
let valid = finished && !failed && !pcm.isEmpty
let result = pcm
lock.unlock()
if !valid { synthesizer.stopSpeaking(at: .immediate); exit(3) }
FileHandle.standardOutput.write(result)
