from __future__ import annotations

import asyncio
import json
import re
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Literal

import aiohttp
from yarl import URL

from deadlinks.concurrency import LevelQueue, Countdown
from deadlinks.parser import ParseResult, analyze_page


async def crawl(urls: Iterable[str], config: Config) -> CrawlState:
    queue = LevelQueue[_QueueItem]()
    for url in urls:
        queue.add(0, ("crawl", URL(url)))

    # the `counter` is needed to detect when we have no more queue items
    # and no more workers processing HTTP requests
    counter = Countdown(queue.length())

    state = CrawlState(
        results={},
        warnings={},
        expected_ids=defaultdict(set),
        seen_urls={},
    )

    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=config.request_timeout),
        cookie_jar=aiohttp.DummyCookieJar(),
    ) as session:
        async with asyncio.TaskGroup() as tg:
            for worker_id in range(config.worker_count):
                tg.create_task(worker(
                    worker_id,
                    session,
                    queue,
                    config,
                    counter,
                    state,
                ))

            await counter.wait()
            queue.close()

    return state


@dataclass(frozen=True, kw_only=True)
class Config:
    crawlable_pattern: re.Pattern[str]  #
    max_size_bytes: int = 3*2**20   # 3 MiB
    crawl_depth: int = 10
    max_redirects: int = 4
    worker_count: int = 4
    request_timeout: float = 5.0


@dataclass(repr=False)
class CrawlState:
    results: dict[URL, FetchResult]
    warnings: dict[URL, list[str]]
    expected_ids: defaultdict[URL, set[str]]

    seen_urls: dict[URL, int]  # url -> max level at which the URL was encountered
    # consider this:
    #  a0 -> a1 -> a2 -> a3 -> a4 (stop)  -- a4 stopped first
    #  b0 ->        ...slow -> a4 -> a5
    #                             ^^^^ we still need to
    # NOTE: for redirect depth, we don't do this for the sake of simplicity
    #       (if you hit that problem, fix your redirects being too deep!)

    def __repr__(self) -> str:
        return (
            "<CrawlState"
            + f" results:{len(self.results)}"
            + f" frags:{len(self.expected_ids)}"
            + f" seen_urls:{len(self.seen_urls)}"
            + ">"
        )

    def serialize(self) -> dict[str, object]:
        return {
            "results":
                {str(url): _serialize_fr(fr) for url, fr in self.results.items()},
            "warnings":
                {str(url): ws for url, ws in self.warnings.items()},
            "expected_fragments":
                {str(url): list(fs) for url, fs in self.expected_ids.items()}
        }


type FetchResult = (
    tuple[Literal["ok"], ParseResult]
    | tuple[Literal["redirect"], URL]
    | tuple[Literal["error"], str, dict[str, object]]  # message, detail
)


def _serialize_fr(fr: FetchResult) -> dict[str, object]:
    match fr:
        case "ok", html:
            return {
                "kind": "ok",
                "base_url": html.base_url,
                "http_equiv_redirect": html.http_equiv_redirect,
                "warnings": list(html.warnings),
                "links": list(html.links),
                "ids": list(html.ids),
            }

        case "redirect", url:
            return {"kind": "redirect", "url": str(url)}

        case "error", message, detail:
            return {"kind": "error", "message": message, "detail": detail}


type _QueueItem = (
    tuple[Literal["follow_redirect"], int, URL]  # redirect depth, location
    | tuple[Literal["crawl"], URL]
)


async def worker(
    worker_id: int,
    cs: aiohttp.ClientSession,
    queue: LevelQueue[_QueueItem],
    config: Config,
    counter: Countdown,
    state: CrawlState,
) -> None:
    while item := await queue.fetch():
        level, item = item
        match item:
            case "follow_redirect", redirect_depth, url:
                pass
            case "crawl", url:
                redirect_depth = 0

        if not (fr := state.results.get(url)):
            fr = await _fetch(worker_id, cs, url, config.max_size_bytes)

        print(f"{worker_id}: Result for {url} is: {_repr_fetchresult(fr)}")

        new_items = _include_fetch_result(  # mutates `state`
            state,
            config,
            fr,
            url,
            level=level,
            redirect_depth=redirect_depth,
        )
        if new_items:
            print(f"{worker_id} New items:")
            for item in new_items:
                print(f"  - {item}")
        else:
            print(f"{worker_id}: Nothing to do for {url}")

        for level, item in new_items:
            queue.add(level, item)
        # we processed one item and added `len(new_items)` new items:
        counter.add(len(new_items) - 1)

        print(f"{worker_id}: {state} {counter}")


def _repr_fetchresult(fr: FetchResult) -> str:
    match fr:
        case "ok", ParseResult(links=links, ids=ids):
            return f"<ok: {len(links)} links, {len(ids)} ids>"
        case "redirect", loc:
            return f"<redirect: {loc}>"
        case "error", msg, _:
            return f"<error: {msg}>"


def _include_fetch_result(
    state: CrawlState,
    config: Config,
    fr: FetchResult,
    base_url: URL,
    *,
    level: int,
    redirect_depth: int,
) -> list[tuple[int, _QueueItem]]:
    base_url = base_url.with_fragment(None)

    max_seen = state.seen_urls.get(base_url)
    # See comment for `CrawlState.seen_urls`
    if max_seen is not None and max_seen <= level:
        return []
    state.seen_urls[base_url] = level
    state.results[base_url] = fr

    match fr:
        case "ok", html:
            urls: set[URL] = set()

            if html.http_equiv_redirect:
                if level + 1 < config.crawl_depth:
                    # treat http-equiv=refresh as an extra crawl step instead of
                    # a proper redirect for simplicity
                    url = base_url.join(URL(html.http_equiv_redirect))
                    urls.add(url)
            elif _should_crawl(base_url, config):
                for link in html.links:
                    link = base_url.join(URL(link))
                    if (fragment := link.fragment) is not None:
                        link = link.with_fragment(None)
                        if fragment != "":
                            state.expected_ids[link].add(fragment)

                    if level + 1 < config.crawl_depth:
                        urls.add(link)

            return [(level + 1, ("crawl", url)) for url in urls]

        case "redirect", loc:
            if (fragment := loc.fragment) is not None:
                loc = loc.with_fragment(None)
                if fragment != "":
                    state.expected_ids[loc].add(fragment)

            if redirect_depth + 1 < config.max_redirects:
                return [(level, ("follow_redirect", redirect_depth + 1, loc))]
            else:
                state.results[loc] =\
                    ("error", "too many redirects", {"redirects": redirect_depth})
                return []


        case "error", _, _:
            return []


def _should_crawl(url: URL, config: Config) -> bool:
    # We only want to crawl pages that are "ours".
    # `crawlable_pattern` defines the scope for our search of dead links.
    return (
        url.host is not None
        and config.crawlable_pattern.fullmatch(str(url)) is not None
    )


async def _fetch(
    num: int,
    session: aiohttp.ClientSession,
    url: URL, max_body_size: int,
) -> FetchResult:
    # this function shouldn't raise
    print(f"{num}: Fetching {url}...")
    try:
        async with session.get(url, allow_redirects=False) as resp:
            return await _handle_response(resp, url, max_body_size)
    except asyncio.TimeoutError:
        return "error", "timeout", {}

    except aiohttp.ClientError as exc:
        return "error", "network_error", {"exc": str(exc)}


async def _handle_response(
    resp: aiohttp.ClientResponse,
    url: URL,
    max_body_size: int,
) -> FetchResult:
    if 200 <= resp.status < 300:
        body = await _safely_read_body(resp, max_body_size)
        if body is None:
            return "error", "response too large", {}

        try:
            body = body.decode()  # TODO: handle non-utf8 encodings?
        except UnicodeDecodeError as exc:
            return (
                "error",
                "body is not valid UTF-8",
                {"start": exc.start, "end": exc.end, "reason": exc.reason}
            )

        return "ok", analyze_page(body)

    elif 300 <= resp.status < 400:
        match resp.headers.getall("location", []):
            case [loc]:
                # The `Location` header can contain relative paths
                return "redirect", url.join(URL(loc))

            case locs:
                return (
                    "error",
                    "invalid Location header",
                    {"status": resp.status, "locations": locs},
                )

    else:
        return "error", "bad status", {"status": resp.status}


async def _safely_read_body(resp: aiohttp.ClientResponse, limit: int) -> bytes | None:
    chunks: list[bytes] = []
    total = 0
    while chunk := await resp.content.read(2**20):
        if total + len(chunk) > limit:
            return None
        chunks.append(chunk)
        total += len(chunk)
    return b"".join(chunks)


if __name__ == "__main__":
    urls = [
        "https://decorator-factory.github.io/typing-tips",
    ]
    crawl_state = asyncio.run(
        crawl(urls, Config(
            crawlable_pattern = re.compile(r"https?://decorator-factory\.github\.io($|/.*)")
        ))
    )
    with open(".crawl.json", "w") as file:
        json.dump(crawl_state.serialize(), file, indent=2, ensure_ascii=False)
