from __future__ import annotations

import asyncio
import json
import re
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any, Literal

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
        expected_ids=defaultdict(lambda: defaultdict(set)),
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
    max_size_bytes: int = 5*2**20   # 5 MiB
    crawl_depth: int = 3
    max_redirects: int = 4
    worker_count: int = 8
    request_timeout: float = 5.0


@dataclass(repr=False, kw_only=True)
class CrawlState:
    results: dict[URL, FetchResult]
    warnings: dict[URL, list[str]]
    expected_ids: defaultdict[URL, defaultdict[str, set[URL]]]  # {url -> {id -> expecters}}

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

    @classmethod
    def deserialize(cls, obj: Any) -> CrawlState:
        if obj["v"] != [1, 0]:
            raise ValueError("Unsupported version")

        # TODO: better validation?

        return CrawlState(
            results={URL(url): _deserialize_fr(fr) for url, fr in obj["results"].items()},
            warnings={URL(url): ws for url, ws in obj["warnings"].items()},
            expected_ids=defaultdict(
                lambda: defaultdict(set),
                {
                    URL(url): defaultdict(set, {
                        frag: set(map(URL, expecters)) for frag, expecters in fs.items()
                    }) for url, fs in obj["expected_ids"].items()
                }
            ),
            seen_urls={}, #  TODO?
        )

    def serialize(self) -> dict[str, object]:
        return {
            "v": [1, 0],
            "results": {str(url): _serialize_fr(fr) for url, fr in self.results.items()},
            "warnings": {str(url): ws for url, ws in self.warnings.items()},
            "expected_ids": {
                str(url): {frag: list(map(str, expecters)) for frag, expecters in fs.items()}
                for url, fs in self.expected_ids.items()
            }
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


def _deserialize_fr(obj: Any) -> FetchResult:
    match obj["kind"]:
        case "ok":
            return (
                "ok",
                ParseResult(
                    base_url=obj["base_url"],
                    http_equiv_redirect=obj["http_equiv_redirect"],
                    warnings=obj["warnings"],
                    links=obj["links"],
                    ids=obj["ids"],
                )
            )

        case "redirect":
            return "redirect", URL("url")

        case "error":
            return "error", obj["message"], obj["detail"]

        case other:
            raise ValueError(other)


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
                    if fragment := link.fragment:
                        link = link.with_fragment(None)
                        state.expected_ids[link][fragment].add(base_url)

                    if level + 1 < config.crawl_depth:
                        urls.add(link)

            return [(level + 1, ("crawl", url)) for url in urls]

        case "redirect", loc:
            if fragment := loc.fragment:
                loc = loc.with_fragment(None)
                state.expected_ids[loc][fragment].add(base_url)

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
        if "html" not in resp.content_type:
            return "ok", analyze_page("")

        length = resp.headers.get("content-length")
        if length is not None:
            try:
                n = int(length)
            except ValueError:
                return "error", "content-length is not an integer??", {"content-length": length}
            if n > max_body_size:
                return "error", "response too large", {}

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



# Analysis (TODO: move to a separate module)


def find_anomalies(state: CrawlState) -> None:
    errors: dict[URL, tuple[set[URL], object]] = {}  # to -> ({from}, error)

    def _add_error(from_: URL, to: URL, err: object):
        if to not in errors:
            errors[to] = (set(), err)
        errors[to][0].add(from_)

    for base_url, result in state.results.items():
        match result:
            case "ok", html:
                for link in html.links:
                    abs_url = base_url.join(URL(link)).with_fragment(None)
                    res = _resolve_page(state, abs_url)
                    if res[0] not in {"ok", "out_of_scope"}:
                        _add_error(base_url, abs_url, res)

    for url, (refs, err) in errors.items():
        print(f"{url} referenced from {len(refs)} pages: {err}")

    for base_url, result in state.results.items():
        match result:
            case "ok", html:
                expected_ids = state.expected_ids.get(base_url, defaultdict(set))
                if diff := expected_ids.keys() - html.ids:
                    print(f"Ids missing from {base_url}:")
                    for id_ in diff:
                        print(f"- {id_} from: ")
                        for ref in expected_ids[id_]:
                            print(f"  .  {ref}")
                    print()


def _resolve_page(state: CrawlState, url: URL) -> (
    tuple[Literal["ok"], ParseResult]
    | tuple[Literal["out_of_scope"]]
    | tuple[Literal["error"], str, dict[str, object]]
):
    seen: set[URL] = set()
    while True:
        url = url.with_fragment(None)
        seen.add(url)
        match state.results.get(url):
            case None:
                return "out_of_scope",  # didn't get that far when crawling

            case "ok", html:
                return "ok", html

            case "redirect", url:
                url = url.with_fragment(None)
                if url in seen:
                    return "error", "too many redirects", {"chain": list(map(str, seen))}

            case "error", err, detail:
                return "error", err, detail


if __name__ == "__main__":
    stage = "crawl"

    match stage:
        case "crawl":
            urls = [
                "https://decorator-factory.github.io/typing-tips",
            ]
            crawl_state = asyncio.run(
                crawl(urls, Config(
                    crawlable_pattern = re.compile(r"https?://decorator-factory\.github\.io($|/.*)")
                ))
            )
            with open(".crawl2.json", "w") as file:
                json.dump(crawl_state.serialize(), file, indent=2, ensure_ascii=False)

        case "analyze":
            with open(".crawl2.json") as file:
                j = json.load(file)
            state = CrawlState.deserialize(j)
            print(state)
            find_anomalies(state)

