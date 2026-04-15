"""Deterministic solver for hCaptcha accessibility text prompts."""

from __future__ import annotations

import re
import unicodedata


class HcaptchaTextSolveError(RuntimeError):
    pass


def _norm(text: str) -> str:
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return text.strip().lower()


def solve_accessibility_prompt(prompt: str) -> str:
    p = _norm(prompt)
    nums = re.findall(r"\d+", p)

    # Word prompts seen in the wild.
    if "secarte despues de nadar" in p:
        return "toalla"

    # Replace every occurrence of X with/to Y in N.
    m = re.search(
        r"(?:reemplaza|cambia|convierte|sustituye|replace|change)(?:[^\d]+)"
        r"(?:every occurrence of|all occurrences of|aparicion(?:es)?(?: de)?|todas las apariciones de|cada aparicion de)\s*(\d)"
        r"(?:[^\d]+)(?:with|to|por|en)\s*(\d)(?:[^\d]+)(\d+)",
        p,
    )
    if m:
        src, dst, number = m.groups()
        return "".join(dst if ch == src else ch for ch in number)

    # First occurrence of X -> Y in N.
    m = re.search(
        r"(?:primera aparicion de|first occurrence of)\s*(\d)(?:[^\d]+)(\d)(?:[^\d]+)(\d+)",
        p,
    )
    if m:
        src, dst, number = m.groups()
        return number.replace(src, dst, 1)

    # Second occurrence of X -> Y in N.
    m = re.search(
        r"(?:segunda aparicion de|second occurrence of)\s*(\d)(?:[^\d]+)(\d)(?:[^\d]+)(\d+)",
        p,
    )
    if m:
        src, dst, number = m.groups()
        seen = 0
        out = []
        for ch in number:
            if ch == src:
                seen += 1
                out.append(dst if seen == 2 else ch)
            else:
                out.append(ch)
        return "".join(out)

    # Remove/delete/erase occurrences of a digit.
    m = re.search(
        r"(?:elimina|quita|remove|delete|erase)(?:[^\d]+)"
        r"(?:(?:all|every|each) occurrences? of\s*)?(\d)(?:[^\d]+)(\d+)",
        p,
    )
    if m:
        src, number = m.groups()
        return "".join(ch for ch in number if ch != src)

    # Duplicate each occurrence of a digit.
    m = re.search(
        r"(?:for\s+)?every occurrence of\s*(\d)(?:[^\d]+)(\d+)(?:[^\d]+)write (?:it|them) twice|"
        r"write twice each occurrence of\s*(\d)(?:[^\d]+)(\d+)",
        p,
    )
    if m:
        src = m.group(1) or m.group(3)
        number = m.group(2) or m.group(4)
        out = []
        for ch in number:
            out.append(ch)
            if ch == src:
                out.append(ch)
        return "".join(out)

    # Fallback for simpler prompts with 3 numeric groups.
    if len(nums) >= 3:
        src, dst, number = nums[0], nums[1], nums[-1]
        if len(src) == len(dst) == 1:
            return "".join(dst if ch == src else ch for ch in number)

    raise HcaptchaTextSolveError(f"Unrecognized hCaptcha accessibility prompt: {prompt}")
