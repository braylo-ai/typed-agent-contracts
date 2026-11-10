"""Typed contracts between agents (Pydantic models, exportable as JSON Schema)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

UNTRUSTED, INTERNAL, PRIVILEGED = 0, 1, 2


class Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


@dataclass(frozen=True)
class Contract:
    name: str
    version: str
    request: type[BaseModel]
    reply: type[BaseModel]
    min_trust: int = INTERNAL

    @property
    def id(self) -> str:
        return f"{self.name}@{self.version}"

    def json_schema(self) -> dict:
        return {
            "contract": self.id,
            "min_trust": self.min_trust,
            "request": self.request.model_json_schema(),
            "reply": self.reply.model_json_schema(),
        }


class ContractRegistry:
    def __init__(self, contracts: list[Contract] = ()):
        self._c: dict[str, Contract] = {}
        for c in contracts:
            self.add(c)

    def add(self, c: Contract):
        if c.id in self._c:
            raise ValueError(f"duplicate contract {c.id}")
        self._c[c.id] = c

    def get(self, cid: str) -> Contract | None:
        return self._c.get(cid)

    def export(self) -> dict:
        return {cid: c.json_schema() for cid, c in sorted(self._c.items())}


# --- example contracts used by tests and the eval -------------------------
class SummarizeReq(Strict):
    text: str = Field(max_length=20_000)
    max_words: int = Field(ge=10, le=500)


class SummarizeRep(Strict):
    summary: str


class SearchReq(Strict):
    query: str = Field(max_length=500)
    top_k: int = Field(default=5, ge=1, le=20)


class SearchRep(Strict):
    hits: list[str]


class RefundReq(Strict):
    order_id: str = Field(pattern=r"^o[0-9]+$")
    amount_cents: int = Field(gt=0, le=100_000)
    reason: Literal["damaged", "late", "duplicate", "other"]


class RefundRep(Strict):
    refund_id: str
    status: Literal["approved", "rejected"]


class TaskReq(Strict):
    task: str
    hops: int = 0


class TaskRep(Strict):
    result: str


SUMMARIZE = Contract("summarize", "1", SummarizeReq, SummarizeRep, min_trust=UNTRUSTED)
SEARCH = Contract("search", "1", SearchReq, SearchRep, min_trust=UNTRUSTED)
REFUND = Contract("payments.refund", "1", RefundReq, RefundRep, min_trust=PRIVILEGED)
TASK = Contract("task", "1", TaskReq, TaskRep, min_trust=UNTRUSTED)


def default_registry() -> ContractRegistry:
    return ContractRegistry([SUMMARIZE, SEARCH, REFUND, TASK])
