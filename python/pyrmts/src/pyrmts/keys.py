"""Key templates: `{name}` placeholders plus the content-hash token
(`specs/content-addressed-shards.md`).

`{hash}` expands to the shard payload's full md5 (32 hex chars); `{hash:N}` to
its first `N` (1..32) — the hashed-asset-filename convention (webpack
`[contenthash:8]`, Vite `[hash:8]`: `:N` truncates), *not* Python's format
spec. A template with a hash token yields **immutable keys**: a rewrite of a
slot writes a new blob and swaps the registry row; the previous blob becomes an
orphan for GC. A template without one keeps mutable, template-derived keys.

Vocabulary:
- a **slot** is what a shard *is*: `(tier, shard_dur, period[, filter dims])`;
- the **slot key** (`slot_key`) is the template with every placeholder but the
  hash substituted — the stable, human-readable identity of a slot, equal to
  the storage key for a hashless template;
- the **key** (`substitute_key` with `hash`) is where a specific version of the
  slot's bytes lives.
"""
from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Protocol

_PLACEHOLDER = re.compile(r'\{(\w+)(?::(\d+))?\}')
HASH = 'hash'
MD5_HEX_LEN = 32


def validate_key_template(template: str) -> None:
    """Config-time check: `:N` is only defined for `{hash}`, and `N` is 1..32."""
    for m in _PLACEHOLDER.finditer(template):
        name, width = m.group(1), m.group(2)
        if width is None:
            continue
        if name != HASH:
            raise ValueError(f"keyTemplate: `:{width}` is only defined for {{hash}}, not {{{name}}} ({template!r})")
        n = int(width)
        if not 1 <= n <= MD5_HEX_LEN:
            raise ValueError(f"keyTemplate: {{hash:{width}}} must be 1..{MD5_HEX_LEN} ({template!r})")


def template_has_hash(template: str) -> bool:
    return any(m.group(1) == HASH for m in _PLACEHOLDER.finditer(template))


def hash_width(template: str) -> int | None:
    """Hex chars the template's hash token keeps (32 for bare `{hash}`), or
    None when the template has no hash token."""
    for m in _PLACEHOLDER.finditer(template):
        if m.group(1) == HASH:
            return int(m.group(2)) if m.group(2) else MD5_HEX_LEN
    return None


def content_hash(payload: bytes) -> str:
    """The content hash a `{hash}` token expands from: md5 hex of the bytes."""
    return hashlib.md5(payload).hexdigest()


def substitute_key(template: str, values: Mapping[str, str | int]) -> str:
    """Expand every placeholder. `values['hash']` must be the payload's md5
    hex when the template has a hash token (`{hash:N}` keeps its first N)."""
    validate_key_template(template)

    def repl(m: re.Match[str]) -> str:
        name, width = m.group(1), m.group(2)
        if name not in values:
            raise KeyError(f"substitute_key: missing value for {{{name}}}")
        value = str(values[name])
        if name == HASH:
            if len(value) != MD5_HEX_LEN or not re.fullmatch(r'[0-9a-f]+', value):
                raise ValueError(f"substitute_key: {{hash}} wants a 32-char md5 hex, got {value!r}")
            return value[: int(width)] if width else value
        return value

    return _PLACEHOLDER.sub(repl, template)


def slot_key(template: str, values: Mapping[str, str | int]) -> str:
    """The template with every placeholder but `{hash}` substituted: a slot's
    stable identity (and, for a hashless template, its storage key)."""
    validate_key_template(template)

    def repl(m: re.Match[str]) -> str:
        name = m.group(1)
        if name == HASH:
            return m.group(0)
        if name not in values:
            raise KeyError(f"slot_key: missing value for {{{name}}}")
        return str(values[name])

    return _PLACEHOLDER.sub(repl, template)


def key_pattern(template: str) -> re.Pattern[str]:
    """A regex matching keys the template can produce, one named group per
    placeholder (`hash` is fixed-width `[0-9a-f]{N}`, others one path
    segment). The inverse of `substitute_key` for listings (GC, adopt, fsck)."""
    validate_key_template(template)
    parts: list[str] = []
    pos = 0
    seen: set[str] = set()
    for m in _PLACEHOLDER.finditer(template):
        parts.append(re.escape(template[pos:m.start()]))
        name, width = m.group(1), m.group(2)
        if name in seen:
            parts.append(f'(?P={name})')
        else:
            seen.add(name)
            if name == HASH:
                parts.append(f'(?P<{name}>[0-9a-f]{{{int(width) if width else MD5_HEX_LEN}}})')
            else:
                parts.append(f'(?P<{name}>[^/]+)')
        pos = m.end()
    parts.append(re.escape(template[pos:]))
    return re.compile('^' + ''.join(parts) + '$')


def parse_key(template: str, key: str) -> dict[str, str] | None:
    """Placeholder values a key encodes, or None if it doesn't match the template."""
    m = key_pattern(template).match(key)
    return None if m is None else m.groupdict()


def slot_of(template: str, key: str) -> str | None:
    """The slot key a storage key belongs to (the key with its hash replaced
    by the placeholder), or None if the key doesn't match the template."""
    values = parse_key(template, key)
    if values is None:
        return None
    return slot_key(template, {k: v for k, v in values.items() if k != HASH})


@dataclass(frozen=True)
class ShardWrite:
    key: str
    md5: str
    n_bytes: int
    #: False when the key already existed (content-addressed: identical bytes
    #: by construction), so nothing was uploaded.
    put: bool


def put_shard(storage, template: str, values: Mapping[str, str | int], payload: bytes) -> ShardWrite:
    """The write protocol every shard writer uses. Hashed template: derive the
    key from the payload's md5, `put` only if absent (rebuilds are idempotent
    for free), never overwrite — registering the returned key is the caller's
    (atomic) swap. Hashless template: put in place, as before."""
    md5 = content_hash(payload)
    if template_has_hash(template):
        key = substitute_key(template, {**values, HASH: md5})
        if storage.head(key) is not None:
            return ShardWrite(key=key, md5=md5, n_bytes=len(payload), put=False)
        storage.put(key, payload)
        return ShardWrite(key=key, md5=md5, n_bytes=len(payload), put=True)
    key = substitute_key(template, values)
    storage.put(key, payload)
    return ShardWrite(key=key, md5=md5, n_bytes=len(payload), put=True)


class KeyResolver(Protocol):
    """Where a slot's *current* bytes live. For a hashless template that is the
    template itself; for a hashed one it is the registry row, which is the
    only truth for "what's built"."""

    def resolve(self, tier: str, shard_dur: str, period_start_ms: int, period_label: str, filter: Mapping[str, str | int]) -> str | None: ...


@dataclass(frozen=True)
class TemplateResolver:
    template: str

    def resolve(self, tier: str, shard_dur: str, period_start_ms: int, period_label: str, filter: Mapping[str, str | int]) -> str | None:
        if template_has_hash(self.template):
            raise ValueError(
                f"keyTemplate {self.template!r} has a {{hash}} token: a slot's current key lives in the "
                f"registry, not the template — pass a registry-backed resolver (`RegistryResolver`)"
            )
        return substitute_key(self.template, {**filter, 'tier': tier, 'shard': shard_dur, 'period': period_label})
