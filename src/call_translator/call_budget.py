"""Planning and conservative local warnings; never a provider billing cap."""
import math

INPUT_USD_PER_SECOND = 25 * 3.50 / 1_000_000
OUTPUT_USD_PER_SECOND = 25 * 21 / 1_000_000
FULL_RATE = INPUT_USD_PER_SECOND + OUTPUT_USD_PER_SECOND


def validate_continuous(value, credit):
    keys = {'planned_minutes', 'allocation_usd', 'reserve_percent', 'auth_source'}
    if not isinstance(value, dict) or set(value) != keys:
        raise ValueError('continuous needs planned_minutes/allocation_usd/reserve_percent/auth_source')
    minutes = value['planned_minutes']
    reserve = value['reserve_percent']
    allocation = value['allocation_usd']
    if type(minutes) is not int or not 30 <= minutes <= 90:
        raise ValueError('Plan capacity for 30–90 minutes; this is not a shutdown timer')
    if type(reserve) is not int or not 20 <= reserve <= 100:
        raise ValueError('Reserve must be 20–100 percent')
    if type(allocation) not in (int, float) or not math.isfinite(allocation):
        raise ValueError('Finite owner-approved allocation required')
    if value['auth_source'] not in ('adc', 'gcloud'):
        raise ValueError('Select the existing adc or gcloud identity explicitly')
    # Both directions, full audio, setup + every overlapping renewal + bounded recoveries.
    seconds = minutes * 60
    per_route = seconds + 15 + (seconds // 480 + 1) * 23 + 3 * 8
    required = 2 * per_route * FULL_RATE * (1 + reserve / 100)
    if not required <= allocation <= float(credit['remaining_usd']):
        raise ValueError(f'Covered credit and owner allocation of at least ${required:.2f} required')
    return required


class CallBudget:
    def __init__(self, config, emit):
        self.allocation = config.continuous['allocation_usd']
        self.emit = emit
        self.warned = set()
        self.estimate = 0.0

    def check(self, nodes, now):
        # Lifetime full-rate allowance also covers silence and preopened sessions.
        # Raw received bytes include PCM rejected by a full local playback queue.
        self.estimate = estimate_nodes(nodes, now)
        for threshold in (0.8, 0.9):
            if self.estimate >= self.allocation * threshold and threshold not in self.warned:
                self.warned.add(threshold)
                self.emit('budget_warning', fraction=threshold, estimate_usd=round(self.estimate, 6))
        # Leave a small local cleanup allowance. Actual provider billing can lag/diverge.
        if self.estimate >= self.allocation - 0.05:
            raise RuntimeError('financial_safety_stop')
        return self.estimate


def estimate_nodes(nodes, now):
    return sum(max(
        (min(now, n.closed_at) - n.opened_at) * FULL_RATE,
        n.input_bytes / 32000 * INPUT_USD_PER_SECOND +
        n.received_bytes / 48000 * OUTPUT_USD_PER_SECOND) for n in nodes)
