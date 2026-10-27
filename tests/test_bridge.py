import asyncio
from dataclasses import replace
from types import SimpleNamespace

import pytest

from call_translator.bridge import check_bridge, run_bridge
from test_config_audio import valid_config


def test_routes_fail_closed_and_keep_languages_isolated():
    devices = [dict(name=n, max_input_channels=i, max_output_channels=o) for n, i, o in
               [("Microphone", 1, 0), ("Headphones", 0, 2),
                ("BlackHole 2ch", 2, 2), ("Microsoft Teams Audio", 1, 1),
                ("Speakers", 0, 2), ("Aggregate Device", 2, 2)]]
    sd = SimpleNamespace(query_devices=lambda: devices,
                         check_input_settings=lambda **kw: None,
                         check_output_settings=lambda **kw: None)
    config = replace(valid_config(), bridge={"outgoing_device": "Microsoft Teams Audio",
                                            "incoming_device": "BlackHole 2ch"})
    routes = check_bridge(config, sd)
    assert routes["outgoing"][1:] == (0, 3)
    assert routes["incoming"][1:] == (2, 1)
    assert len({routes["outgoing"][1], routes["outgoing"][2],
                routes["incoming"][1], routes["incoming"][2]}) == 4
    assert routes["outgoing"][0].target_language == "fr"
    assert routes["incoming"][0].target_language == "en"
    for change in [dict(bridge=None), dict(output_device="Speakers"),
                   dict(source_language="fr", target_language="en"),
                   dict(bridge={"outgoing_device": "BlackHole 2ch", "incoming_device": "BlackHole 2ch"}),
                   dict(bridge={"outgoing_device": "BlackHole 2ch", "incoming_device": "Microsoft Teams Audio"}),
                   dict(bridge={"outgoing_device": "Aggregate Device", "incoming_device": "BlackHole 2ch"})]:
        with pytest.raises(ValueError):
            check_bridge(replace(config, **change), sd)


@pytest.mark.parametrize("reason", ["error", "deadline", "cancel"])
def test_both_sessions_close_on_any_exit(reason):
    async def scenario():
        opened, closed = set(), set()
        ready = asyncio.Event()
        async def fake(config, source, target, sd, *, direction, on_metrics):
            opened.add(direction)
            if len(opened) == 2:
                ready.set()
            try:
                await ready.wait()
                if direction == "outgoing" and reason == "error":
                    raise RuntimeError("route fault")
                if direction == "outgoing" and reason == "deadline":
                    return {}
                await asyncio.Future()
            finally:
                closed.add(direction)
                on_metrics({"direction": direction})
        task = asyncio.create_task(run_bridge(
            {n: (None, 0, 1) for n in ("outgoing", "incoming")}, None, translator=fake))
        await ready.wait()
        if reason == "cancel":
            task.cancel()
        if reason in ("error", "cancel"):
            with pytest.raises(RuntimeError if reason == "error" else asyncio.CancelledError):
                await task
        else:
            assert set(await task) == {"outgoing", "incoming"}
        assert closed == opened == {"outgoing", "incoming"}
        assert not [t for t in asyncio.all_tasks() if t is not asyncio.current_task() and not t.done()]
    asyncio.run(scenario())


CALL = dict(setup_seconds=1, conversation_seconds=300, drain_seconds=1,
            packet_remaining_seconds=600, packet_verified_on="2026-10-21")


def call_routes():
    config = SimpleNamespace(call=CALL)
    return {n: (config, 0, 1) for n in ("outgoing", "incoming")}


def test_normal_call_finish_lets_the_sibling_drain_before_closing():
    async def scenario():
        closed = {}
        async def fake(config, source, target, sd, *, direction, on_metrics):
            try:
                if direction == "incoming":
                    await asyncio.sleep(0.2)  # still draining when outgoing is done
                return {"status": "call_completed"}
            finally:
                closed[direction] = "finished"
                on_metrics({"direction": direction})
        metrics = await run_bridge(call_routes(), None, translator=fake)
        assert set(metrics) == {"outgoing", "incoming"} and closed == {
            "outgoing": "finished", "incoming": "finished"}
    asyncio.run(scenario())


def test_sibling_drain_is_bounded_and_a_fault_does_not_wait_for_it():
    async def scenario(fault):
        closed = set()
        async def fake(config, source, target, sd, *, direction, on_metrics):
            try:
                if direction == "outgoing":
                    if fault:
                        raise RuntimeError("capture_callbacks_stalled")
                    return {"status": "call_completed"}
                await asyncio.Future()
            finally:
                closed.add(direction)
        started = asyncio.get_running_loop().time()
        if fault:
            with pytest.raises(RuntimeError):
                await run_bridge(call_routes(), None, translator=fake)
        else:
            await run_bridge(call_routes(), None, translator=fake)
        assert closed == {"outgoing", "incoming"}
        return asyncio.get_running_loop().time() - started
    assert asyncio.run(scenario(fault=True)) < 0.5
    # grace is setup + drain of the route's own call window, never open-ended
    assert 2 <= asyncio.run(scenario(fault=False)) < 3


def test_ctrl_c_during_a_call_window_closes_both_without_waiting():
    async def scenario():
        closed = set()
        ready = asyncio.Event()
        async def fake(config, source, target, sd, *, direction, on_metrics):
            ready.set()
            try:
                await asyncio.Future()
            finally:
                closed.add(direction)
        task = asyncio.create_task(run_bridge(call_routes(), None, translator=fake))
        await ready.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert closed == {"outgoing", "incoming"}
    asyncio.run(scenario())
