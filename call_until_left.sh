#!/bin/zsh
# Keep translating until the owner leaves Meet: a technical stop (AirPods dropped, failed
# renewal, network) restarts the call after 3 s instead of ending translation for the meeting.
# Exit 0 (left Meet) and 130 (Ctrl-C) end the loop.
#
# Headphone watchdog: in a live test the AirPods silently changed Bluetooth mode mid-call and
# translated audio kept "playing" into a dead route with no error. If the headphones' output
# sample rate changes or they disappear, the call is restarted so streams reopen on the
# current route.
cd "$(dirname "$0")" || exit 1
tab=$1
headphones_rate() {
  system_profiler SPAudioDataType 2>/dev/null |
    awk '/AirPods/{f=1} f&&/Output Channels/{o=1} f&&o&&/Current SampleRate/{print $3; exit}'
}
while true; do
  if .venv/bin/call-translator check-call --meet-tab-id "$tab"; then
    .venv/bin/call-translator call --meet-tab-id "$tab" --save-transcript &
    pid=$!
    sleep 8
    start_rate=$(headphones_rate)
    while kill -0 $pid 2>/dev/null; do
      rate=$(headphones_rate)
      if [[ "$rate" != "$start_rate" ]]; then
        echo "{\"event\": \"headphone_route_changed\", \"from\": \"$start_rate\", \"to\": \"$rate\", \"at\": \"$(date -u +%H:%M:%S)\"}"
        kill -TERM $pid 2>/dev/null
        break
      fi
      sleep 3
    done
    wait $pid
    code=$?
  else
    code=1
  fi
  [[ $code -eq 0 || $code -eq 130 ]] && break
  echo "{\"event\": \"restart_after_stop\", \"exit_code\": $code, \"at\": \"$(date -u +%H:%M:%S)\"}"
  sleep 3
done
