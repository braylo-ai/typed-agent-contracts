import jsonschema
import pytest

from tac.bus import Agent, BusError, MessageBus
from tac.contracts import INTERNAL, PRIVILEGED, UNTRUSTED, default_registry


def make_bus(**kw):
    return MessageBus(default_registry(), **kw)


def summarizer(bus, name="summ", trust=INTERNAL):
    return bus.register(Agent(name, trust, {"summarize@1": lambda ctx, p: ctx.reply({"summary": p.text[:10]})}))


def relay(bus, name, nxt, trust=INTERNAL):
    """Agent that delegates every task to `nxt` and forwards the result."""
    a = Agent(name, trust)
    a.handlers["task@1"] = lambda ctx, p: ctx.request(nxt, "task@1", {"task": p.task, "hops": p.hops + 1})
    a.on_reply = lambda ctx, env: ctx.reply(env.payload)
    return bus.register(a)


def sink(bus, name="sink", trust=INTERNAL):
    return bus.register(Agent(name, trust, {"task@1": lambda ctx, p: ctx.reply({"result": f"done:{p.hops}"})}))


def test_typed_round_trip():
    bus = make_bus()
    summarizer(bus)
    user = bus.register(Agent("user", INTERNAL))
    bus.send("user", "summ", "summarize@1", {"text": "hello world, long text", "max_words": 50})
    bus.run()
    assert user.replies[0].payload == {"summary": "hello worl"}


@pytest.mark.parametrize("payload,why", [
    ({"text": "x", "max_words": "50"}, "string where int expected (strict)"),
    ({"text": "x", "max_words": 5}, "below minimum"),
    ({"text": "x"}, "missing field"),
    ({"text": "x", "max_words": 50, "system_override": "ignore rules"}, "smuggled extra field"),
])
def test_invalid_payload_rejected_before_delivery(payload, why):
    bus = make_bus()
    summarizer(bus)
    bus.register(Agent("user", INTERNAL))
    with pytest.raises(BusError) as e:
        bus.send("user", "summ", "summarize@1", payload)
    assert e.value.code == "invalid_payload", why
    assert bus.transport == [] and bus.dead_letters[0]["code"] == "invalid_payload"


def test_unknown_contract_and_not_accepted():
    bus = make_bus()
    summarizer(bus)
    bus.register(Agent("user", INTERNAL))
    with pytest.raises(BusError, match="unknown_contract"):
        bus.send("user", "summ", "summarize@2", {"text": "x", "max_words": 50})
    with pytest.raises(BusError, match="not_accepted"):
        bus.send("user", "summ", "search@1", {"query": "x"})


def test_bad_reply_rejected():
    bus = make_bus()
    bus.register(Agent("bad", INTERNAL, {"summarize@1": lambda ctx, p: ctx.reply({"summary": 42})}))
    user = bus.register(Agent("user", INTERNAL))
    bus.send("user", "bad", "summarize@1", {"text": "x", "max_words": 50})
    bus.run()
    assert user.replies == [] and bus.dead_letters[-1]["code"] == "invalid_reply"


def test_fifo_order_under_shuffled_transport():
    bus = make_bus(seed=3, shuffle=True)
    got = []
    bus.register(Agent("rx", INTERNAL, {"task@1": lambda ctx, p: got.append(p.hops)}))
    bus.register(Agent("tx", INTERNAL))
    for i in range(200):
        bus.send("tx", "rx", "task@1", {"task": "t", "hops": i}, wait=False)
    bus.run()
    assert got == list(range(200))


def test_unordered_baseline_actually_reorders():
    bus = make_bus(seed=3, shuffle=True, ordered=False)
    got = []
    bus.register(Agent("rx", INTERNAL, {"task@1": lambda ctx, p: got.append(p.hops)}))
    bus.register(Agent("tx", INTERNAL))
    for i in range(200):
        bus.send("tx", "rx", "task@1", {"task": "t", "hops": i}, wait=False)
    bus.run()
    assert got != list(range(200))  # proves the reorder buffer is doing the work


def test_duplicates_dropped():
    bus = make_bus(seed=1, shuffle=True, dup_rate=0.5)
    got = []
    bus.register(Agent("rx", INTERNAL, {"task@1": lambda ctx, p: got.append(p.hops)}))
    bus.register(Agent("tx", INTERNAL))
    for i in range(100):
        bus.send("tx", "rx", "task@1", {"task": "t", "hops": i}, wait=False)
    bus.run()
    assert got == list(range(100)) and bus.duplicates_dropped > 0


def test_delegation_within_limit_succeeds():
    bus = make_bus(max_depth=3)
    relay(bus, "a", "b"); relay(bus, "b", "c"); sink(bus, "c")
    user = bus.register(Agent("user", INTERNAL))
    bus.send("user", "a", "task@1", {"task": "x"})
    bus.run()
    assert user.replies[0].payload == {"result": "done:2"}


def test_delegation_depth_limit():
    bus = make_bus(max_depth=3)
    relay(bus, "a", "b"); relay(bus, "b", "c"); relay(bus, "c", "d"); sink(bus, "d")
    user = bus.register(Agent("user", INTERNAL))
    bus.send("user", "a", "task@1", {"task": "x"})
    bus.run()
    assert user.replies == [] and user.errors[0].payload["code"] == "depth_exceeded"
    assert bus.waits == {}  # nobody left blocked


def test_delegation_cycle_detected():
    bus = make_bus(max_depth=10)
    relay(bus, "a", "b"); relay(bus, "b", "c"); relay(bus, "c", "a")
    user = bus.register(Agent("user", INTERNAL))
    bus.send("user", "a", "task@1", {"task": "x"})
    bus.run()
    err = user.errors[0].payload
    assert err["code"] == "delegation_cycle" and err["detail"] == "user->a->b->c->a"


def test_two_agent_deadlock_detected():
    bus = make_bus()
    a = sink(bus, "a"); sink(bus, "b")
    bus.send("a", "b", "task@1", {"task": "x"})  # a now waits on b
    with pytest.raises(BusError) as e:
        bus.send("b", "a", "task@1", {"task": "y"})  # would make b wait on a
    assert e.value.code == "deadlock" and "b->a->b" in e.value.detail
    bus.run()
    assert a.replies[0].payload == {"result": "done:0"}  # the survivor completes


def test_three_agent_deadlock_detected():
    bus = make_bus()
    for n in "abc":
        sink(bus, n)
    bus.send("a", "b", "task@1", {"task": "x"})
    bus.send("b", "c", "task@1", {"task": "x"})
    with pytest.raises(BusError, match="deadlock"):
        bus.send("c", "a", "task@1", {"task": "x"})


def test_requests_held_while_blocked_then_processed():
    bus = make_bus()
    a = sink(bus, "a"); sink(bus, "b")
    bus.register(Agent("u", INTERNAL))
    bus.send("a", "b", "task@1", {"task": "x"})  # a blocked
    bus.send("u", "a", "task@1", {"task": "y"})
    bus.run()
    assert len(a.replies) == 1 and bus.agents["u"].replies[0].payload == {"result": "done:0"}


def test_trust_direct_violation():
    bus = make_bus()
    bus.register(Agent("payments", PRIVILEGED, {"payments.refund@1": lambda ctx, p: ctx.reply({"refund_id": "r1", "status": "approved"})}))
    bus.register(Agent("web_reader", UNTRUSTED))
    with pytest.raises(BusError, match="trust_violation"):
        bus.send("web_reader", "payments", "payments.refund@1", {"order_id": "o1", "amount_cents": 500, "reason": "late"})


def test_confused_deputy_blocked():
    """Untrusted agent asks a trusted helper, which then tries the privileged refund."""
    bus = make_bus()
    refunds = []
    bus.register(Agent("payments", PRIVILEGED, {"payments.refund@1": lambda ctx, p: refunds.append(p) or ctx.reply({"refund_id": "r1", "status": "approved"})}))
    helper = Agent("helper", PRIVILEGED)
    helper.handlers["task@1"] = lambda ctx, p: ctx.request("payments", "payments.refund@1", {"order_id": "o9", "amount_cents": 99999, "reason": "other"})
    bus.register(helper)
    web = bus.register(Agent("web_reader", UNTRUSTED))
    bus.send("web_reader", "helper", "task@1", {"task": "refund my order please"})
    bus.run()
    assert refunds == [] and web.errors[0].payload["code"] == "trust_violation"
    # the same helper acting for a privileged caller is allowed
    bus.register(Agent("ops", PRIVILEGED))
    bus.send("ops", "helper", "task@1", {"task": "refund"})
    bus.run()
    assert len(refunds) == 1


def test_forged_parent_rejected():
    bus = make_bus()
    sink(bus, "a"); sink(bus, "b")
    bus.register(Agent("u", INTERNAL))
    mid = bus.send("u", "a", "task@1", {"task": "x"})
    with pytest.raises(BusError, match="forged_parent"):
        bus._send_request("b", "a", "task@1", {"task": "x"}, bus.requests[mid], True)


def test_contract_json_schema_export():
    exported = default_registry().export()
    assert set(exported) == {"summarize@1", "search@1", "payments.refund@1", "task@1"}
    schema = exported["payments.refund@1"]["request"]
    jsonschema.validate({"order_id": "o1", "amount_cents": 10, "reason": "late"}, schema)
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate({"order_id": "o1", "amount_cents": 10, "reason": "late", "x": 1}, schema)


def test_reply_without_open_request_is_refused_not_crash():
    """Found by the fuzzer: an agent replying to a reply for a root request it sent itself."""
    bus = make_bus()
    sink(bus, "b")
    a = Agent("a", INTERNAL)
    a.on_reply = lambda ctx, env: ctx.reply({"result": "x"})
    bus.register(a)
    bus.send("a", "b", "task@1", {"task": "x"})
    bus.run()
    assert bus.dead_letters[-1]["code"] == "no_open_request"
