# Live Call Translator

Real-time, two-way voice translation for Google Meet on macOS, built on Google's
`gemini-3.5-live-translate-preview` model.

You speak your language into your microphone; the other participants hear the translation as
your Meet microphone. They speak theirs; you hear the translation in your headphones. Nothing
has to be installed on the other participants' side.

It was built by a solo founder to hold real partner meetings across a language barrier, and was
used for a 52-minute Google Meet (Russian ↔ French) with two remote participants on separate
devices: 12 session renewals, no dropouts, no restarts.

## How it works

```
 your mic ──► [outgoing session: your language → their language] ──► "Microsoft Teams Audio" ──► Meet microphone
 Meet speaker ──► "BlackHole 2ch" ──► [incoming session: their language → yours] ──► your headphones
```

- Two independent Gemini Live sessions, one per direction (PCM16 16 kHz in, 24 kHz out).
- The source language is auto-detected by the model; only the target language is configured.
- Each session is renewed about every 8 minutes: the next one is opened in advance and audio is
  handed over, so long calls continue without a gap of more than a few seconds.
- Transient provider errors are recovered automatically; stale audio is dropped after a
  recovery so the delay does not accumulate.
- A small native helper (Swift, read-only Accessibility/AppleScript) watches the selected Chrome
  Meet tab: the translator stops by itself when you leave the meeting.
- `call_until_left.sh` restarts the translator after any technical stop and watches the
  headphone route (currently a device whose name contains "AirPods"): if the headphones
  disconnect or change Bluetooth mode, the call is restarted so audio does not keep "playing"
  into a dead route.
- Optional private text transcript (original + translation) saved locally; audio is never
  recorded.

## Requirements

- macOS, Python 3.11+, Xcode Command Line Tools (`swiftc` builds the native helper).
- Google Chrome with Google Meet.
- Wired or Bluetooth **headphones** (loudspeakers are rejected to avoid echo loops).
- Two virtual audio devices:
  - [BlackHole 2ch](https://github.com/ExistentialAudio/BlackHole) — Meet speaker → translator;
  - "Microsoft Teams Audio" (installed with Microsoft Teams) — translator → Meet microphone.
- A Google Cloud project with access to `gemini-3.5-live-translate-preview` on Vertex AI
  (`location = "global"`), billing enabled, and `gcloud auth login` done on this Mac.
- The terminal you run it from needs **Accessibility** and **Automation → Google Chrome**
  permissions (System Settings › Privacy & Security), so the helper can see the Meet tab.

## Setup

```sh
python3 -m venv .venv
.venv/bin/pip install -e '.[test]'
.venv/bin/call-translator prepare          # builds the native Meet observer into .local/
.venv/bin/call-translator devices          # exact device names for the config
cp config.example.toml config.local.toml   # git-ignored; edit it
```

In `config.local.toml` set your Google Cloud `project`, the language pair, your microphone and
headphones, and uncomment the `[continuous]` table. Supported pairs (outgoing; incoming is the
reverse): `en→fr`, `en→ru`, `ru→fr`.

## Running a call

1. Join the meeting in Chrome. In Meet › Settings › Audio choose
   **Microphone: Microsoft Teams Audio** and **Speaker: BlackHole 2ch**.
2. Find the Meet tab id and start:

   ```sh
   .venv/bin/call-translator meet-tabs
   zsh call_until_left.sh <TAB_ID>
   ```

   The script passes `--save-transcript`; text goes to the git-ignored `.local/transcripts/`.
3. Speak after `LIVE_BOTH` appears. To finish, simply leave the meeting.

Emergency stop: Ctrl-C in the terminal.

## Costs

Pay-as-you-go Google Cloud usage. At the rates encoded in `call_budget.py` both directions cost
about **US$4.4 per hour** of call (a conservative local estimate, not an invoice; check current
Google pricing). A local safety stop ends the call before the configured allocation is reached.

## Limitations

- macOS only; the audio routing and the Meet observer are platform-specific.
- The model is a preview; recognition can mishear numbers, dates and names. Confirm key figures
  out loud, and keep sentences short when speaking through a machine translator.
- Translated speech uses a synthetic voice; overlapping speakers are translated as one stream.
- **Other call apps (Zoom, Microsoft Teams, etc.).** The audio routing itself is app-agnostic:
  any app where you can choose the microphone and the speaker can use the two virtual devices.
  The continuous `call` mode, however, is tied to a Google Meet tab in Chrome: it refuses to
  start without one and stops when you leave that meeting. Using it with another app needs a
  Meet-independent start/stop (for example, stop by Ctrl-C only); this is neither built nor
  tested. In Microsoft Teams, choosing the "Microsoft Teams Audio" device as the microphone has
  not been verified and may require another virtual device.

## Engineering notes

Lessons from live calls that shaped the code:

- **Fixed output callback size.** Adaptive PortAudio buffer sizes garbled the outgoing voice
  through the virtual microphone; fixed 100 ms callbacks fixed it.
- **Recovery limits per call kill long calls.** A cap of three recoveries for the whole call
  would end a one-hour meeting; the cap is now 50.
- **Do not stop on recoverable signals.** "Interrupted" events (overlapping speakers) and single
  audio xruns are counted, not fatal; a 10-second playback queue absorbs handovers during
  nonstop speech.
- **Watch the route, not just the stream.** When Bluetooth headphones switch mode, playback can
  continue "successfully" into nothing; only an external watchdog notices.
- **The built-in microphone was more stable** than the headphones' microphone, which forces a
  Bluetooth mode switch.

## Tests

```sh
.venv/bin/python -m pytest -q
```

The test suite runs offline with fakes; it never opens audio devices or calls the API. Scripts
named `probe_*`, `diagnose_*`, `monitor_*` and `manual_*` in `tests/` are manual live
diagnostics and are not part of the suite.

## How it was built

Designed and tested by the author on real calls, with the code written together with AI coding
agents (OpenAI Codex and Anthropic Claude Code).

## License

[MIT](LICENSE)
