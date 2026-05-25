from __future__ import annotations

import re


_PLACEHOLDER = re.compile(r'\{(\w+)\}')


def substitute_key(template: str, values: dict[str, str | int]) -> str:
    def repl(m: re.Match[str]) -> str:
        name = m.group(1)
        if name not in values:
            raise KeyError(f"substitute_key: missing value for {{{name}}}")
        return str(values[name])
    return _PLACEHOLDER.sub(repl, template)
