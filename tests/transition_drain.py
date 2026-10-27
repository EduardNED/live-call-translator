"""Bounded diagnostic drain rules; exact-zero PCM is not translated speech."""


def only_zero_tail(buffers):
    with buffers.playback.mutex:
        return not any(buffers.pending) and all(not any(x) for x in buffers.playback.queue)


def drain_ready(node, now, ended_at, quiet_seconds=2.0):
    # turn_complete is optional on this translation stream. Silence is a measured
    # heuristic, never a provider guarantee that a late response cannot arrive.
    return (now - ended_at >= quiet_seconds
            and now - node.record.get('last_activity_elapsed', ended_at) >= quiet_seconds
            and only_zero_tail(node.buffers))


def discard_leading_zeros(buffers):
    discarded = 0
    if buffers.pending:
        if any(buffers.pending):
            return 0
        discarded += len(buffers.pending)
        buffers.pending = b''
    with buffers.playback.mutex:
        while buffers.playback.queue and not any(buffers.playback.queue[0]):
            discarded += len(buffers.playback.queue.popleft())
    return discarded
