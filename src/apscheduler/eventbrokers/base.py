from __future__ import annotations

from base64 import b64decode, b64encode
from collections.abc import Callable, Iterable
from contextlib import AsyncExitStack
from enum import Enum
from inspect import iscoroutine
from logging import Logger
from threading import Lock
from typing import Any

import attrs
from anyio import (
    CapacityLimiter,
    create_task_group,
    get_cancelled_exc_class,
    to_thread,
)
from anyio.abc import TaskGroup

from .. import _events
from .._events import Event
from .._exceptions import DeserializationError
from .._retry import RetryMixin
from ..abc import EventBroker, Serializer, Subscription
from ..serializers.json import JSONSerializer


class OneShotState(Enum):
    pending = "pending"
    claimed = "claimed"
    completed = "completed"
    failed = "failed"
    cancelled = "cancelled"


@attrs.define(eq=False)
class OneShotClaim:
    state: OneShotState = OneShotState.pending

    def claim(self) -> bool:
        if self.state is not OneShotState.pending:
            return False
        self.state = OneShotState.claimed
        return True

    def finish(self, state: OneShotState) -> None:
        if self.state is OneShotState.claimed:
            self.state = state


@attrs.define(eq=False, frozen=True)
class LocalSubscription(Subscription):
    callback: Callable[[Event], Any]
    event_types: set[type[Event]] | None
    one_shot: bool
    is_async: bool
    token: object
    _source: BaseEventBroker
    _claim: OneShotClaim | None = None

    def unsubscribe(self) -> None:
        self._source.unsubscribe(self.token)


@attrs.define(kw_only=True)
class BaseEventBroker(EventBroker):
    _logger: Logger = attrs.field(init=False)
    _subscriptions: dict[object, LocalSubscription] = attrs.field(
        init=False, factory=dict
    )
    _task_group: TaskGroup = attrs.field(init=False)
    _thread_limiter: CapacityLimiter = attrs.field(init=False)
    _subscription_lock: Lock = attrs.field(init=False, factory=Lock)

    async def start(self, exit_stack: AsyncExitStack, logger: Logger) -> None:
        self._logger = logger
        self._task_group = await exit_stack.enter_async_context(create_task_group())
        self._thread_limiter = CapacityLimiter(1)

    def subscribe(
        self,
        callback: Callable[[Event], Any],
        event_types: Iterable[type[Event]] | None = None,
        *,
        is_async: bool = True,
        one_shot: bool = False,
    ) -> Subscription:
        types = set(event_types) if event_types else None
        token = object()
        claim = OneShotClaim() if one_shot else None
        subscription = LocalSubscription(
            callback, types, one_shot, is_async, token, self, claim
        )
        with self._subscription_lock:
            self._subscriptions[token] = subscription

        return subscription

    def unsubscribe(self, token: object) -> None:
        with self._subscription_lock:
            self._subscriptions.pop(token, None)

    async def publish_local(self, event: Event) -> None:
        event_type = type(event)
        matched_subscriptions: list[LocalSubscription] = []
        with self._subscription_lock:
            for subscription in tuple(self._subscriptions.values()):
                if (
                    subscription.event_types is None
                    or event_type in subscription.event_types
                ):
                    if not subscription.one_shot or (
                        subscription._claim is not None and subscription._claim.claim()
                    ):
                        matched_subscriptions.append(subscription)
                        if subscription.one_shot:
                            self._subscriptions.pop(subscription.token)

        for subscription in matched_subscriptions:
            self._task_group.start_soon(self._deliver_event, subscription, event)

    async def _deliver_event(
        self, subscription: LocalSubscription, event: Event
    ) -> None:
        try:
            try:
                if subscription.is_async:
                    retval = subscription.callback(event)
                    if iscoroutine(retval):
                        await retval
                else:
                    await to_thread.run_sync(
                        subscription.callback, event, limiter=self._thread_limiter
                    )
            except BaseException as exc:
                self._finish_one_shot(
                    subscription,
                    OneShotState.cancelled
                    if isinstance(exc, get_cancelled_exc_class())
                    else OneShotState.failed,
                )
                raise
            else:
                self._finish_one_shot(subscription, OneShotState.completed)
        except Exception:
            self._logger.exception(
                "Error delivering %s event", event.__class__.__name__
            )

    def _finish_one_shot(
        self,
        subscription: LocalSubscription,
        state: OneShotState,
    ) -> None:
        claim = subscription._claim
        if claim is None:
            return

        with self._subscription_lock:
            claim.finish(state)


@attrs.define(kw_only=True)
class BaseExternalEventBroker(BaseEventBroker, RetryMixin):
    """
    Base class for event brokers that use an external service.

    :param serializer: the serializer used to (de)serialize events for transport
    """

    serializer: Serializer = attrs.field(factory=JSONSerializer)

    def generate_notification(self, event: Event) -> bytes:
        serialized = self.serializer.serialize(event.marshal())
        return event.__class__.__name__.encode("ascii") + b" " + serialized

    def generate_notification_str(self, event: Event) -> str:
        serialized = self.serializer.serialize(event.marshal())
        return event.__class__.__name__ + " " + b64encode(serialized).decode("ascii")

    def _reconstitute_event(self, event_type: str, serialized: bytes) -> Event | None:
        try:
            kwargs = self.serializer.deserialize(serialized)
        except DeserializationError:
            self._logger.exception(
                "Failed to deserialize an event of type %s",
                event_type,
                extra={"serialized": serialized},
            )
            return None

        try:
            event_class = getattr(_events, event_type)
        except AttributeError:
            self._logger.error(
                "Receive notification for a nonexistent event type: %s",
                event_type,
                extra={"serialized": serialized},
            )
            return None

        try:
            return event_class.unmarshal(kwargs)
        except Exception:
            self._logger.exception("Error reconstituting event of type %s", event_type)
            return None

    def reconstitute_event(self, payload: bytes) -> Event | None:
        try:
            event_type_bytes, serialized = payload.split(b" ", 1)
        except ValueError:
            self._logger.error(
                "Received malformatted notification", extra={"payload": payload}
            )
            return None

        event_type = event_type_bytes.decode("ascii", errors="replace")
        return self._reconstitute_event(event_type, serialized)

    def reconstitute_event_str(self, payload: str) -> Event | None:
        try:
            event_type, b64_serialized = payload.split(" ", 1)
        except ValueError:
            self._logger.error(
                "Received malformatted notification", extra={"payload": payload}
            )
            return None

        return self._reconstitute_event(event_type, b64decode(b64_serialized))
