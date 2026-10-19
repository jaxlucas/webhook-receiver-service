"""Handlers keyed by ``event_type``, with a default for every other type."""

from __future__ import annotations

import importlib
from collections.abc import Iterable
from dataclasses import dataclass

from webhook_receiver.processing.handler import Handler, HandlerSpec


@dataclass(frozen=True, slots=True)
class Registration:
    handler: Handler
    spec: HandlerSpec


class HandlerRegistry:
    def __init__(self, default: Handler, default_spec: HandlerSpec) -> None:
        self._default = Registration(default, default_spec)
        self._by_type: dict[str, Registration] = {}
        self._by_name: dict[str, Registration] = {default_spec.name: self._default}

    def register(self, event_types: Iterable[str] | str, handler: Handler, spec: HandlerSpec) -> None:
        types = [event_types] if isinstance(event_types, str) else list(event_types)
        known = self._by_name.get(spec.name)
        if known is not None and (known.handler is not handler or known.spec != spec):
            raise ValueError(f"handler name {spec.name!r} is already registered differently")
        registration = known or Registration(handler, spec)
        self._by_name[spec.name] = registration
        for event_type in types:
            self._by_type[event_type] = registration

    def set_default(self, handler: Handler, spec: HandlerSpec) -> None:
        self._by_name.pop(self._default.spec.name, None)
        self._default = Registration(handler, spec)
        self._by_name[spec.name] = self._default

    @property
    def default(self) -> Registration:
        return self._default

    def resolve(self, event_type: str) -> Registration:
        return self._by_type.get(event_type, self._default)

    def registrations(self) -> list[Registration]:
        return list(self._by_name.values())

    def types_for(self, name: str) -> list[str]:
        return sorted(t for t, reg in self._by_type.items() if reg.spec.name == name)

    def routed_types(self) -> list[str]:
        """Event types routed to a non-default handler."""
        return sorted(t for t, reg in self._by_type.items() if reg is not self._default)


def load_plugins(registry: HandlerRegistry, spec: str) -> None:
    """Call each ``module:function`` in the comma-separated ``spec`` with the registry."""
    for item in filter(None, (part.strip() for part in spec.split(","))):
        module_name, _, func_name = item.partition(":")
        if not func_name:
            raise ValueError(f"handler plugin {item!r} must look like 'module:function'")
        getattr(importlib.import_module(module_name), func_name)(registry)
