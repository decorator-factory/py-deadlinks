import asyncio
from collections import defaultdict, deque


class LevelQueue[T]:
    """
    Queue optimized for concurrent breadth-first search.


    Items with a lower level are fetched before items with a higher level.
    LevelQueue operates under the assumption that items will eventually
    start moving at a higher level: after you consume an item of level L,
    items of level L or L+1 should be produced.

    ```plain
    level=0   level=1   level=2

    url1  -----> url4 --> url6
    url2 __.              url7
           |              url8
        redirect
           |
    url3 <-/
       +-------> url5 --> url9
                          url10
    ```
    """

    def __init__(self) -> None:
        self._queues: dict[int, deque[T]] = defaultdict(deque)
        self._length = 0
        self._smallest = 0
        self._largest = 0
        self._event = asyncio.Event()
        self._closed = False

    def length(self) -> int:
        return self._length

    async def fetch(self) -> tuple[int, T] | None:
        """
        Wait for the next item to be available, return a tuple of (level, item).

        If the queue is closed and no more items are available, returns None.
        """
        while True:
            while self._length == 0:
                # should be fine if the number of readers is pretty small
                if self._closed:
                    return None
                await self._event.wait()
            self._length -= 1

            for lvl in range(self._smallest, self._largest + 1):
                q = self._queues[lvl]
                match len(q):
                    case 0:
                        if lvl == self._smallest:
                            self._smallest += 1
                    case 1:
                        if lvl == self._smallest:
                            self._smallest += 1
                        return lvl, q.pop()

                    case _:
                        return lvl, q.pop()

    def close(self) -> None:
        self._closed = True
        self._event.set()

    def add(self, level: int, item: T, /) -> None:
        if self._closed:
            raise AssertionError("cannot add items to a closed queue")
        assert level >= 0
        self._length += 1
        self._largest = max(level, self._largest)
        self._smallest = min(level, self._smallest)
        self._queues[level].appendleft(item)
        self._event.set()
        self._event = asyncio.Event()


class Countdown:
    """
    Starts at a non-negative value. When the value goes to 0,
    the `wait()` method returns.
    """
    def __init__(self, initial: int) -> None:
        assert initial >= 0

        self._count = initial
        self._event = asyncio.Event()
        if initial == 0:
            self._event.set()

    def count(self) -> int:
        return self._count

    async def wait(self) -> None:
        await self._event.wait()

    def add(self, n: int) -> None:
        if self._count <= 0:
            raise RuntimeError("Cannot add to terminated Countdown")

        new_count = self._count + n
        assert new_count >= 0
        self._count = new_count
        if new_count == 0:
            self._event.set()

    def __repr__(self) -> str:
        return f"<Countdown {self._count}{' triggered' * self._event.is_set()}>"
