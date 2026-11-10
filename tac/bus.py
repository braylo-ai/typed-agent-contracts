"""Multi-agent message bus treated as a distributed system.

Guarantees and checks, all enforced by the bus (never trusted from the sender):
- Typed contracts: request and reply payloads are validated against Pydantic models.
- Per-channel FIFO: every (sender, recipient) channel gets sequence numbers; the receiver side
  buffers out-of-order arrivals and drops duplicates, so a lossy/reordering transport is safe.
- Delegation chain: built by the bus from the message being handled, so it cannot be forged.
  Depth limit and cycle detection (A -> B -> C -> A) run on that chain.
- Deadlock detection: a wait-for graph of blocking requests; an edge that closes a cycle is refused
  and the requester gets a `deadlock` error instead of hanging.
- Trust: the effective trust of a request is the minimum trust over its whole delegation chain,
  so an untrusted agent cannot reach a privileged contract through a trusted deputy.
"""

from __future__ import annotations

import itertools
import random
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Callable, Literal

from pydantic import BaseModel, ValidationError

from .contracts import ContractRegistry


class BusError(Exception):
    def __init__(self, code: str, detail: str = ""):
        super().__init__(f"{code}: {detail}")
        self.code = code
        self.detail = detail


class Envelope(BaseModel):
    msg_id: str
    kind: Literal["request", "reply", "error"]
    sender: str
    recipient: str
    contract: str
    seq: int
    chain: list[str]  # agents that delegated before `sender`, oldest first
    effective_trust: int
    in_reply_to: str | None = None
    payload: dict


@dataclass
class Agent:
    name: str
    trust: int
    handlers: dict[str, Callable] = field(
        default_factory=dict
    )  # contract id -> fn(ctx, payload model)
    on_reply: Callable | None = None  # fn(ctx, envelope)
    replies: list = field(default_factory=list)
    errors: list = field(default_factory=list)


class Ctx:
    """Handed to agent code. Every send is tied to the message currently being handled."""

    def __init__(self, bus: "MessageBus", agent: Agent, current: Envelope | None):
        self.bus, self.agent, self.current = bus, agent, current

    def request(self, to: str, contract: str, payload: dict, wait: bool = True) -> str:
        return self.bus._send_request(
            self.agent.name, to, contract, payload, self.current, wait
        )

    def reply(self, payload: dict) -> str:
        if self.current is None:
            self.bus._reject("no_open_request", f"{self.agent.name} has no request to reply to")
        return self.bus._send_reply(self.agent.name, self.current, payload)


class MessageBus:
    def __init__(
        self,
        registry: ContractRegistry,
        max_depth: int = 3,
        seed: int | None = None,
        shuffle: bool = False,
        dup_rate: float = 0.0,
        ordered: bool = True,
        detect_deadlocks: bool = True,
    ):
        self.registry = registry
        self.max_depth = max_depth
        self.rng = random.Random(seed)
        self.shuffle, self.dup_rate, self.ordered = shuffle, dup_rate, ordered
        self.detect_deadlocks = detect_deadlocks
        self.agents: dict[str, Agent] = {}
        self._ids = itertools.count(1)
        self._out_seq: dict[tuple[str, str], int] = defaultdict(int)
        self._next_in: dict[tuple[str, str], int] = defaultdict(lambda: 1)
        self._reorder: dict[tuple[str, str], dict[int, Envelope]] = defaultdict(dict)
        self.transport: list[Envelope] = []
        self.waits: dict[str, tuple[str, str]] = (
            {}
        )  # request msg_id -> (waiter, callee)
        self.held: dict[str, deque] = defaultdict(
            deque
        )  # requests held while an agent is blocked
        self.requests: dict[str, Envelope] = {}
        self.parent_of: dict[str, str] = (
            {}
        )  # delegated request id -> request being handled
        self.delivered: list[Envelope] = []
        self.dead_letters: list[dict] = []
        self.duplicates_dropped = 0

    def register(self, agent: Agent) -> Agent:
        self.agents[agent.name] = agent
        return agent

    # --- sending --------------------------------------------------------
    def send(
        self, sender: str, to: str, contract: str, payload: dict, wait: bool = True
    ) -> str:
        """Entry point for a root request (no parent message)."""
        return self._send_request(sender, to, contract, payload, None, wait)

    def _reject(self, code: str, detail: str, **kw):
        self.dead_letters.append({"code": code, "detail": detail, **kw})
        raise BusError(code, detail)

    def _blocked(self, name: str) -> bool:
        return any(w == name for w, _ in self.waits.values())

    def _wait_cycle(self, start: str, target: str) -> list[str] | None:
        """Would adding start -> target close a cycle? Return the cycle path if so."""
        graph = defaultdict(set)
        for w, c in self.waits.values():
            graph[w].add(c)
        stack, seen = [(target, [start, target])], set()
        while stack:
            node, path = stack.pop()
            if node == start:
                return path
            if node in seen:
                continue
            seen.add(node)
            for nxt in graph[node]:
                stack.append((nxt, path + [nxt]))
        return None

    def _send_request(
        self,
        sender: str,
        to: str,
        cid: str,
        payload: dict,
        parent: Envelope | None,
        wait: bool,
    ) -> str:
        if sender not in self.agents:
            self._reject("unknown_sender", sender)
        if parent is not None and parent.recipient != sender:
            self._reject(
                "forged_parent",
                f"{sender} cannot delegate from a message sent to {parent.recipient}",
            )
        contract = self.registry.get(cid)
        if contract is None:
            self._reject("unknown_contract", cid, sender=sender)
        target = self.agents.get(to)
        if target is None or cid not in target.handlers:
            self._reject("not_accepted", f"{to} does not accept {cid}", sender=sender)
        try:
            model = contract.request.model_validate(payload)
        except ValidationError as e:
            self._reject(
                "invalid_payload", e.errors()[0]["msg"], sender=sender, contract=cid
            )
        chain = (parent.chain + [parent.sender]) if parent else []
        path = chain + [sender]
        if len(path) > self.max_depth:
            self._reject(
                "depth_exceeded",
                f"delegation depth {len(path)} > {self.max_depth}: {'->'.join(path + [to])}",
            )
        if to in path:
            self._reject("delegation_cycle", "->".join(path + [to]))
        trust = min(self.agents[a].trust for a in path)
        if trust < contract.min_trust:
            self._reject(
                "trust_violation",
                f"effective trust {trust} via {'->'.join(path)} < {contract.min_trust} for {cid}",
            )
        msg_id = f"m{next(self._ids)}"
        if wait:
            cyc = self._wait_cycle(sender, to) if self.detect_deadlocks else None
            if cyc:
                self._reject("deadlock", "wait-for cycle " + "->".join(cyc))
            self.waits[msg_id] = (sender, to)
        env = self._envelope(
            msg_id, "request", sender, to, cid, chain, trust, None, model.model_dump()
        )
        self.requests[msg_id] = env
        if parent is not None:
            self.parent_of[msg_id] = parent.msg_id
        return msg_id

    def _send_reply(
        self, sender: str, req: Envelope, payload: dict, kind: str = "reply"
    ) -> str:
        if kind == "reply":
            contract = self.registry.get(req.contract)
            try:
                payload = contract.reply.model_validate(payload).model_dump()
            except ValidationError as e:
                self._reject(
                    "invalid_reply",
                    e.errors()[0]["msg"],
                    sender=sender,
                    contract=req.contract,
                )
        msg_id = f"m{next(self._ids)}"
        self._envelope(
            msg_id,
            kind,
            sender,
            req.sender,
            req.contract,
            [],
            req.effective_trust,
            req.msg_id,
            payload,
        )
        return msg_id

    def _envelope(
        self, msg_id, kind, sender, to, cid, chain, trust, irt, payload
    ) -> Envelope:
        self._out_seq[(sender, to)] += 1
        env = Envelope(
            msg_id=msg_id,
            kind=kind,
            sender=sender,
            recipient=to,
            contract=cid,
            seq=self._out_seq[(sender, to)],
            chain=chain,
            effective_trust=trust,
            in_reply_to=irt,
            payload=payload,
        )
        self.transport.append(env)
        if self.dup_rate and self.rng.random() < self.dup_rate:
            self.transport.append(env.model_copy())
        return env

    # --- delivery -------------------------------------------------------
    def _pop(self) -> Envelope:
        if self.shuffle:
            return self.transport.pop(self.rng.randrange(len(self.transport)))
        return self.transport.pop(0)

    def run(self, max_steps: int = 100_000) -> int:
        steps = 0
        while self.transport and steps < max_steps:
            steps += 1
            env = self._pop()
            if not self.ordered:
                self._deliver(env)
                continue
            ch = (env.sender, env.recipient)
            if env.seq < self._next_in[ch] or env.seq in self._reorder[ch]:
                self.duplicates_dropped += 1
                continue
            self._reorder[ch][env.seq] = env
            while self._next_in[ch] in self._reorder[ch]:
                self._deliver(self._reorder[ch].pop(self._next_in[ch]))
                self._next_in[ch] += 1
        return steps

    def _deliver(self, env: Envelope):
        agent = self.agents[env.recipient]
        self.delivered.append(env)
        if env.kind in ("reply", "error"):
            self.waits.pop(env.in_reply_to or "", None)
            (agent.replies if env.kind == "reply" else agent.errors).append(env)
            if env.kind == "reply" and agent.on_reply:
                self._invoke(agent, env, lambda ctx: agent.on_reply(ctx, env))
            elif env.kind == "error":
                parent = self.requests.get(
                    self.parent_of.get(env.in_reply_to or "", "")
                )
                if (
                    parent is not None
                ):  # default: propagate failures up the delegation chain
                    self._send_reply(agent.name, parent, env.payload, kind="error")
            self._drain_held(agent)
            return
        if self._blocked(agent.name):
            self.held[agent.name].append(
                env
            )  # single-threaded agent: handle after unblocking
            return
        self._handle_request(agent, env)

    def _handle_request(self, agent: Agent, env: Envelope):
        contract = self.registry.get(env.contract)
        payload = contract.request.model_validate(env.payload)
        self._invoke(agent, env, lambda ctx: agent.handlers[env.contract](ctx, payload))

    def _invoke(self, agent: Agent, env: Envelope, fn):
        ctx = Ctx(
            self,
            agent,
            (
                env
                if env.kind == "request"
                else self.requests.get(self._origin(agent, env))
            ),
        )
        try:
            fn(ctx)
        except BusError as e:
            # A delegation the agent attempted was refused: fail the request it was handling.
            target = ctx.current
            if target is not None and target.recipient == agent.name:
                self._send_reply(
                    agent.name,
                    target,
                    {"code": e.code, "detail": e.detail},
                    kind="error",
                )

    def _origin(self, agent: Agent, reply: Envelope) -> str | None:
        """For a reply, return the request this agent was handling when it delegated (if any)."""
        return self.parent_of.get(reply.in_reply_to or "")

    def _drain_held(self, agent: Agent):
        while self.held[agent.name] and not self._blocked(agent.name):
            self._handle_request(agent, self.held[agent.name].popleft())
