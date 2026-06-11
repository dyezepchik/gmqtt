import asyncio
from collections import OrderedDict
from typing import Callable, Set, Tuple


class PersistentStorage:
    """In-memory store for inflight QoS 1/2 outbound PUBLISH packets.

    Keyed by packet identifier (mid) for O(1) push and removal on PUBACK/PUBCOMP.
    Insertion order is preserved so _resend_qos_messages replays packets in
    send order on reconnect.

    Instance variables
    ------------------
    _messages : OrderedDict[int, bytes]
        Maps mid → raw packet bytes for every in-flight outbound publish.

    _empty_waiters : Set[asyncio.Future]
        Futures created by wait_empty() callers that are blocking until the
        store drains to zero.  Resolved (set_result(None)) by _check_empty()
        after the last message is removed, and by clear() when the store is
        wiped in one shot.
        """

    def __init__(self):
        self._messages: OrderedDict[int, bytes] = OrderedDict()
        self._empty_waiters: Set[asyncio.Future] = set()

    def _notify_waiters(self, notify: Callable[[asyncio.Future], None]) -> None:
        while self._empty_waiters:
            notify(self._empty_waiters.pop())

    def _check_empty(self):
        if not self._messages:
            self._notify_waiters(lambda waiter: waiter.set_result(None))

    def push_message(self, mid, raw_package):
        self._messages[mid] = raw_package

    async def remove_message_by_mid(self, mid):
        if self._messages.pop(mid, None) is not None:
            self._check_empty()

    @property
    async def is_empty(self):
        return not self._messages

    async def wait_empty(self) -> None:
        if self._messages:
            waiter = asyncio.get_running_loop().create_future()
            self._empty_waiters.add(waiter)
            await waiter

    async def clear(self):
        self._messages.clear()
        self._notify_waiters(lambda waiter: waiter.set_result(None))

    async def get_all(self):
        # Returns a snapshot list of (mid, package) pairs in insertion order.
        return list(self._messages.items())
