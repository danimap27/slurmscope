"""Lector de un subconjunto de YAML, sin dependencias externas.

SlurmScope debe funcionar en clústeres donde no se puede instalar nada, así que
incluye su propio lector de la parte de YAML que basta para describir un barrido:
mapas, listas (en bloque y en línea ``[a, b]``), escalares entre comillas,
comentarios y escalares multilínea (``>``/``|``).

Si PyYAML está disponible se usa PyYAML (YAML completo); si no, este módulo.
"""

from __future__ import annotations

from typing import Any, Dict, List, Tuple

__all__ = ["loads", "MiniYamlError"]


class MiniYamlError(ValueError):
    """Error de sintaxis en el subconjunto soportado."""


_BLOCK_SCALARS = (">", ">-", ">+", "|", "|-", "|+")


def _strip_comment(text: str) -> str:
    """Elimina un comentario final, respetando comillas."""
    quote = None
    for index, char in enumerate(text):
        if quote:
            if char == quote:
                quote = None
            continue
        if char in ("'", '"'):
            quote = char
        elif char == "#" and (index == 0 or text[index - 1] in " \t"):
            return text[:index]
    return text


def _parse_scalar(raw: str) -> Any:
    text = raw.strip()
    if text == "":
        return None
    if text[0] == "[" and text[-1] == "]":
        return [_parse_scalar(item) for item in _split_flow(text[1:-1])]
    if text[0] == "{" and text[-1] == "}":
        result: Dict[str, Any] = {}
        for item in _split_flow(text[1:-1]):
            if ":" not in item:
                raise MiniYamlError(f"mapa en línea inválido: {item!r}")
            key, _, value = item.partition(":")
            result[str(_parse_scalar(key))] = _parse_scalar(value)
        return result
    if len(text) >= 2 and text[0] == text[-1] and text[0] in ("'", '"'):
        inner = text[1:-1]
        if text[0] == '"':
            inner = inner.encode("utf-8").decode("unicode_escape")
        return inner
    lowered = text.lower()
    if lowered in ("null", "~"):
        return None
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    try:
        return int(text)
    except ValueError:
        pass
    try:
        return float(text)
    except ValueError:
        pass
    return text


def _split_flow(text: str) -> List[str]:
    """Divide ``a, b, [c, d]`` respetando comillas y anidamiento."""
    parts: List[str] = []
    depth = 0
    quote = None
    current = []
    for char in text:
        if quote:
            current.append(char)
            if char == quote:
                quote = None
            continue
        if char in ("'", '"'):
            quote = char
            current.append(char)
        elif char in "[{":
            depth += 1
            current.append(char)
        elif char in "]}":
            depth -= 1
            current.append(char)
        elif char == "," and depth == 0:
            parts.append("".join(current))
            current = []
        else:
            current.append(char)
    tail = "".join(current).strip()
    if tail:
        parts.append(tail)
    return [p.strip() for p in parts if p.strip() != ""]


def _indent_of(line: str) -> int:
    return len(line) - len(line.lstrip(" "))


def _preprocess(text: str) -> List[Tuple[int, str]]:
    """Devuelve ``(indentación, contenido)`` resolviendo escalares en bloque."""
    raw_lines = text.replace("\r\n", "\n").replace("\t", "  ").split("\n")
    result: List[Tuple[int, str]] = []
    index = 0

    while index < len(raw_lines):
        line = raw_lines[index]
        stripped = line.strip()
        index += 1
        if not stripped or stripped.startswith("#") or stripped == "---":
            continue
        indent = _indent_of(line)
        content = _strip_comment(stripped).rstrip()
        if not content:
            continue

        marker = None
        head = None
        if ":" in content:
            candidate_head, _, tail = content.partition(":")
            if tail.strip() in _BLOCK_SCALARS:
                marker = tail.strip()
                head = candidate_head.rstrip()

        if marker and head is not None:
            block: List[str] = []
            while index < len(raw_lines):
                candidate = raw_lines[index]
                if candidate.strip() == "":
                    block.append("")
                    index += 1
                    continue
                if _indent_of(candidate) <= indent:
                    break
                block.append(candidate)
                index += 1
            while block and block[-1].strip() == "":
                block.pop()
            if block:
                pad = min(_indent_of(b) for b in block if b.strip())
                block = [b[pad:] if len(b) >= pad else "" for b in block]
            folded = " ".join(part.strip() for part in block) if marker.startswith(">") else "\n".join(block)
            content = f"{head}: {folded.strip()}"

        result.append((indent, content))

    return result


def _parse_block(lines: List[Tuple[int, str]], start: int, indent: int) -> Tuple[Any, int]:
    if start >= len(lines):
        return None, start

    if lines[start][1].startswith("- "):
        items: List[Any] = []
        while start < len(lines):
            current_indent, content = lines[start]
            if current_indent != indent or not content.startswith("- "):
                break
            value = content[2:].strip()
            if value:
                items.append(_parse_scalar(value))
                start += 1
            else:
                start += 1
                nested, start = _parse_block(lines, start, _next_indent(lines, start, indent))
                items.append(nested)
        return items, start

    mapping: Dict[str, Any] = {}
    while start < len(lines):
        current_indent, content = lines[start]
        if current_indent < indent:
            break
        if current_indent > indent:
            raise MiniYamlError(f"indentación inesperada en la línea: {content!r}")
        if ":" not in content:
            raise MiniYamlError(f"se esperaba 'clave: valor' y llegó: {content!r}")
        key, _, remainder = content.partition(":")
        key = key.strip().strip("'\"")
        remainder = remainder.strip()
        start += 1
        if remainder:
            mapping[key] = _parse_scalar(remainder)
        else:
            nested_indent = _next_indent(lines, start, indent)
            if start < len(lines) and lines[start][0] > indent:
                value, start = _parse_block(lines, start, nested_indent)
                mapping[key] = value
            else:
                mapping[key] = None
    return mapping, start


def _next_indent(lines: List[Tuple[int, str]], start: int, parent: int) -> int:
    if start < len(lines) and lines[start][0] > parent:
        return lines[start][0]
    return parent + 2


def loads(text: str) -> Any:
    """Interpreta el subconjunto de YAML soportado."""
    lines = _preprocess(text)
    if not lines:
        return {}
    value, _ = _parse_block(lines, 0, lines[0][0])
    return value


def load(path) -> Any:
    return loads(open(path, encoding="utf-8").read())
