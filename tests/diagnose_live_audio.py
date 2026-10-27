"""Owner-approved language diagnostics, no audio/transcript files.

Run only neutral test phrases with ready headphone-wearing participants. Printed live
text is transient; never redirect it to a file or copy human transcripts into evidence.
Normal production config validation and EN/FR CLI remain unchanged.
"""
import argparse
import asyncio
from dataclasses import replace
import json

import sounddevice as sd
from call_translator.bridge import check_bridge, run_bridge
from call_translator.config import Config
from call_translator.stream import translate


def limited_routes(routes, *, language_pair, direction, seconds=None, call_window=False):
    """Temporary language override. [call] timing is kept only when explicitly requested."""
    remote_language = language_pair.split('-')[1]
    limit = seconds or (60 if direction == 'both' else 30)
    return {name: (replace(item, source_language='en' if name == 'outgoing' else remote_language,
                           target_language=remote_language if name == 'outgoing' else 'en',
                           **({} if call_window else dict(max_seconds=limit, call=None))),
                   source, target)
            for name, (item, source, target) in routes.items()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', required=True)
    parser.add_argument('--direction', choices=['outgoing', 'incoming', 'both'], required=True)
    parser.add_argument('--owner-approved-diagnostics', '--owner-approved-en-es',
                        action='store_true', required=True)
    parser.add_argument('--both-controls-passed', action='store_true')
    parser.add_argument('--seconds', type=int, choices=[30, 45, 60])
    parser.add_argument('--call-window', action='store_true',
                        help='use the finite [call] window and drain from the config instead of --seconds')
    parser.add_argument('--language-pair', choices=['en-es', 'en-fr', 'en-ru'], default='en-es')
    args = parser.parse_args()
    if args.direction == 'both' and not args.both_controls_passed:
        parser.error('Verify original audio and each single direction before both')
    config = Config.load(args.config)
    if args.call_window and (args.seconds or not config.call):
        parser.error('--call-window needs a [call] table and excludes --seconds')
    routes = check_bridge(config, sd)
    routes = limited_routes(routes, language_pair=args.language_pair, direction=args.direction,
                            seconds=args.seconds, call_window=args.call_window)
    call = config.call
    limit = (f"{call['setup_seconds']}s setup + {call['conversation_seconds']}s conversation + "
             f"{call['drain_seconds']}s drain" if args.call_window
             else f"{args.seconds or (60 if args.direction == 'both' else 30)} seconds")

    async def diagnostic(item, source, target, audio, *, direction, on_metrics):
        def text(kind, fragment, elapsed):
            print(json.dumps(dict(direction=direction, kind=kind, elapsed=elapsed,
                                  text=fragment), ensure_ascii=False), flush=True)
        return await translate(item, source, target, audio, direction=direction,
                               on_metrics=on_metrics, on_text=text)

    print(f'DIAGNOSTIC_LIMIT: {limit} per selected direction; {args.language_pair}', flush=True)
    asyncio.run(run_bridge(routes, sd, args.direction, translator=diagnostic))
    print('DIAGNOSTIC_CLOSED; semantic/latency acceptance requires listener review')


if __name__ == '__main__':
    main()
