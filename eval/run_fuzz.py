"""Randomised multi-agent workloads over a lossy transport (reordering + duplicates).

Three arms share identical workloads (same seeds):
  full                  : FIFO reorder buffer + dedupe + deadlock detection
  no_ordering           : deliver in arrival order (no reorder buffer, no dedupe)
  no_deadlock_detection : FIFO on, wait-for cycle check off
Writes results/fuzz_results.json.
"""
import json
import random
import sys
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from tac.bus import Agent, BusError, MessageBus  # noqa: E402
from tac.contracts import PRIVILEGED, default_registry  # noqa: E402

N_AGENTS = 6


def build(bus, rng, refunds):
    names = [f"w{i}" for i in range(N_AGENTS)]
    for n in names:
        a = Agent(n, rng.choice([0, 1, 1, 2]))
        mode = rng.choice(["sink", "sink", "relay", "refund"])
        if mode == "relay":
            nxt = rng.choice([x for x in names if x != n])
            a.handlers["task@1"] = (lambda nxt: lambda ctx, p: ctx.request(nxt, "task@1", {"task": p.task, "hops": p.hops + 1}))(nxt)
            a.on_reply = lambda ctx, env: ctx.reply(env.payload)
        elif mode == "refund":
            a.handlers["task@1"] = lambda ctx, p: ctx.request("payments", "payments.refund@1", {"order_id": "o1", "amount_cents": 100, "reason": "other"})
            a.on_reply = lambda ctx, env: ctx.reply({"result": "refunded"})
        else:
            a.handlers["task@1"] = lambda ctx, p: ctx.reply({"result": f"done:{p.hops}"})
        bus.register(a)

    def refund(ctx, p):
        refunds.append(ctx.current.effective_trust)
        ctx.reply({"refund_id": "r", "status": "approved"})
    bus.register(Agent("payments", PRIVILEGED, {"payments.refund@1": refund}))
    return names


def trial(seed, **arm):
    rng = random.Random(seed)
    bus = MessageBus(default_registry(), max_depth=3, seed=seed, shuffle=True, dup_rate=0.2, **arm)
    refunds = []
    names = build(bus, rng, refunds)
    rejected = Counter()
    for _ in range(12):
        s, r = rng.sample(names, 2)
        payload = {"task": "t", "hops": 0} if rng.random() > 0.1 else {"task": "t", "hops": "0"}
        try:
            bus.send(s, r, "task@1", payload, wait=rng.random() < 0.7)
        except BusError as e:
            rejected[e.code] += 1
    bus.run()
    # FIFO check per channel, and duplicate deliveries
    last, seen, fifo_viol, dups = defaultdict(int), set(), 0, 0
    for env in bus.delivered:
        ch = (env.sender, env.recipient)
        if env.msg_id in seen:
            dups += 1
            continue
        seen.add(env.msg_id)
        if env.seq < last[ch]:
            fifo_viol += 1
        last[ch] = max(last[ch], env.seq)
    in_flight = Counter(d["code"] for d in bus.dead_letters)
    return {"fifo_violations": fifo_viol, "duplicate_deliveries": dups, "hung_waits": len(bus.waits),
            "refunds_below_privileged": sum(t < PRIVILEGED for t in refunds), "refunds_executed": len(refunds),
            "refusals": in_flight, "messages_delivered": len(bus.delivered)}


def arm(name, seeds, **kw):
    agg = Counter()
    refusals = Counter()
    for s in seeds:
        r = trial(s, **kw)
        refusals.update(r.pop("refusals"))
        agg.update(r)
    return {"arm": name, "trials": len(seeds), **dict(agg), "refusals_by_code": dict(sorted(refusals.items()))}


if __name__ == "__main__":
    seeds = list(range(300))
    out = {"workload": {"trials": 300, "agents_per_trial": N_AGENTS + 1, "root_requests_per_trial": 12,
                        "transport": "shuffled, 20% duplicate rate", "max_depth": 3, "invalid_payload_rate": 0.1},
           "arms": [arm("full", seeds), arm("no_ordering", seeds, ordered=False),
                    arm("no_deadlock_detection", seeds, detect_deadlocks=False)]}
    (ROOT / "results").mkdir(exist_ok=True)
    (ROOT / "results" / "fuzz_results.json").write_text(json.dumps(out, indent=2) + "\n")
    print(json.dumps(out, indent=2))
