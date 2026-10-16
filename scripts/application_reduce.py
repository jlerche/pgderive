"""Bounded deterministic transaction-deletion reduction for same-kind failures."""
import math


def reduce_history(history, fails, budget=32):
    if budget < 1:
        raise ValueError('reduction budget must be positive')
    candidate = list(history)
    attempts = 0
    def test(value):
        nonlocal attempts
        if attempts >= budget:
            return None
        attempts += 1
        return fails(value)
    # First establish reproducibility, rather than shrinking infrastructure flakiness.
    if not test(candidate):
        return {'history':candidate,'attempts':attempts,'status':'not-reproduced'}
    if test([]):
        return {'history':[],'attempts':attempts,'status':'1-minimal'}
    parts = 2
    while candidate:
        width = math.ceil(len(candidate)/parts)
        reduced = False
        for start in range(0,len(candidate),width):
            trial = candidate[:start]+candidate[start+width:]
            outcome = test(trial)
            if outcome is None:
                return {'history':candidate,'attempts':attempts,'status':'budget-limited'}
            if outcome:
                candidate = trial
                parts = max(2,parts-1)
                reduced = True
                break
        if not reduced:
            if parts >= len(candidate):
                return {'history':candidate,'attempts':attempts,'status':'1-minimal'}
            parts = min(len(candidate),parts*2)
    return {'history':candidate,'attempts':attempts,'status':'1-minimal'}
