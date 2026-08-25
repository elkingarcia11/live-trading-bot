"""One-off verification: OTM selection vs rule round(price)±2."""
import math
from datetime import date

from option_selector import select_otm_contract_from_chain, synthetic_atm_option


def build_chain(strikes, right):
    key = "2026-08-25:0"
    m = {key: {}}
    for s in strikes:
        m[key][str(s)] = [{
            "symbol": "SPY   260825%s%08d" % ("C" if right == "call" else "P", int(s * 1000)),
            "strikePrice": s,
            "mark": 1.0,
        }]
    return {"%sExpDateMap" % ("call" if right == "call" else "put"): m}


def expected(price, side):
    base = math.floor(price + 0.5)  # half-up rounding
    return base + 2 if side == "call" else base - 2


strikes = [float(x) for x in range(640, 661)]
as_of = date(2026, 8, 25)

mismatches = []
for p10 in range(6450, 6561):
    price = p10 / 10.0
    for side in ("call", "put"):
        sel = select_otm_contract_from_chain(
            build_chain(strikes, side), "SPY", price,
            side=side, target_dte=0, otm_strikes=2, as_of=as_of)
        want = expected(price, side)
        if sel.strike != want:
            mismatches.append((price, side, sel.strike, want))

print("chain-path mismatches: %d of %d" % (len(mismatches), 111 * 2))
for m in mismatches[:20]:
    print("  price=%-7s %-4s got=%-6s want=%s" % m)

syn_bad = []
for p10 in range(6450, 6561, 5):
    price = p10 / 10.0
    for side, right in (("call", "C"), ("put", "P")):
        sel = synthetic_atm_option("SPY", price, days_to_expiration=0,
                                   mark_price=2.5, option_right=right,
                                   otm_strikes=2, as_of=as_of)
        want = expected(price, side)
        if sel.strike != want:
            syn_bad.append((price, side, sel.strike, want))

print("synthetic-path mismatches: %d of %d" % (len(syn_bad), 23 * 2))
for m in syn_bad[:20]:
    print("  price=%-7s %-4s got=%-6s want=%s" % m)
