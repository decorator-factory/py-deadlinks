import asyncio
from contextlib import asynccontextmanager
import random

from hypothesis import given, strategies as st

from deadlinks.concurrency import Countdown, LevelQueue


@asynccontextmanager
async def _atimeout(t: float):
    assert (task := asyncio.current_task())
    th = asyncio.get_running_loop().call_later(t, lambda: task.cancel("Timed out!"))

    try:
        yield
    finally:
        th.cancel()


# LevelQueue tests


@given(st.lists(st.tuples(st.integers(0, 10),st.text()), min_size=0, max_size=10000))
async def test_queue_ordering(inputs: list[tuple[int, str]]):
    q: LevelQueue[str] = LevelQueue()
    for level, item in inputs:
        q.add(level, item)

    async with _atimeout(0.1):
        outputs = [await q.fetch() for _ in range(len(inputs))]

    assert outputs == sorted(inputs, key = lambda lx: lx[0])


async def test_queue_blocks_on_empty():
    q: LevelQueue[str] = LevelQueue()

    async with asyncio.TaskGroup() as tg:
        fetch = tg.create_task(q.fetch())

        await asyncio.sleep(0.01)
        q.add(0, "hello")

        assert (await fetch) == (0, "hello")


@given(st.lists(st.tuples(st.integers(0, 10),st.text()), min_size=1, max_size=100))
async def test_when_queue_unblocks_lowest_level_is_picked(inputs: list[tuple[int, str]]):
    q: LevelQueue[str] = LevelQueue()
    expected_first = sorted(inputs, key = lambda lx: lx[0])[0]

    async with _atimeout(0.1):
        async with asyncio.TaskGroup() as tg:
            fetch = tg.create_task(q.fetch())

            await asyncio.sleep(0.001)
            for lvl, item in inputs:
                q.add(lvl, item)

            assert (await fetch) == expected_first


# Countdown tests


async def test_cdown_zero():
    cdown = Countdown(0)

    async with _atimeout(0.1):
        await cdown.wait()

    assert cdown.count() == 0


async def test_cdown_add_then_sub():
    cdown = Countdown(5)
    async with asyncio.TaskGroup() as tg:
        task = tg.create_task(cdown.wait())
        await asyncio.sleep(0.001)
        assert not task.done()

        assert cdown.count() == 5
        cdown.add(-2)
        assert cdown.count() == 3
        cdown.add(-3)
        assert cdown.count() == 0

        async with _atimeout(0.1):
            await task

    assert cdown.count() == 0
