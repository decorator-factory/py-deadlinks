from collections.abc import Collection
from dataclasses import dataclass
import re
from typing import override

from html.parser import HTMLParser


@dataclass(frozen=True)
class ParseResult:
    base_url: str | None
    http_equiv_redirect: str | None
    warnings: Collection[str]
    links: Collection[str]
    ids: Collection[str]


def analyze_page(source: str) -> ParseResult:
    parser = _Parser()
    parser.feed(source)
    parser.close()
    # HTMLParser never raises an exception
    return parser.result()


class _Parser(HTMLParser):
    def __init__(self, *, convert_charrefs: bool = True) -> None:
        super().__init__(convert_charrefs=convert_charrefs)

        self._seen_base = False
        self._base_url = None

        self._http_equiv_redirect = None

        self._links: set[str] = set()
        self._ids: set[str] = set()
        self._warnings: list[str] = []

    def result(self) -> ParseResult:
        return ParseResult(
            base_url=self._base_url,
            links=self._links,
            ids=self._ids,
            http_equiv_redirect=self._http_equiv_redirect,
            warnings=self._warnings,
        )

    @override
    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        for name, value in attrs:
            if name == "id" and value is not None:
                self._ids.add(value)
                break

        if tag == "base":
            if not self._seen_base:
                self._seen_base = True
                for name, value in attrs:
                    if name == "href" and value is not None:
                        self._base_url = value
                        break

        elif tag == "a":
            # https://developer.mozilla.org/en-US/docs/Web/HTML/Element/a#href
            for name, value in attrs:
                if name == "href" and value is not None:
                    if not any(value.startswith(p) for p in ("tel:", "mailto:", "sms:", "javascript:")):
                        self._links.add(value)
                    break

        elif tag == "meta":
            # https://developer.mozilla.org/en-US/docs/Web/HTML/Element/meta#http-equiv
            if ("http-equiv", "refresh") in attrs:
                for name, value in attrs:
                    if name == "content" and value:
                        if match := re.fullmatch(r"[^;]+;.*url=(.+)", value):
                            self._http_equiv_redirect = match[1]
                        else:
                            self._warnings.append(
                                f"Invalid content for http-equiv=refresh around pos={len(self.rawdata)}")
                        break
                else:
                    self._warnings.append(
                        f"Invalid http-equiv=refresh meta tag around pos={len(self.rawdata)}")

