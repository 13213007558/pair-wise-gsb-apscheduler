from __future__ import annotations

from base64 import b64decode, b64encode
from collections.abc import Callable, Iterable
from contextlib import AsyncExitStack
from inspect import iscoroutine
from logging import Logger, getLogger
from typing import Any

import attrs
from anyio import CapacityLimiter, CancelScope, create_task_group, sleep, to_thread
from anyio.abc import TaskGroup
from attr.validators import ge, gt, instance_of

from .. import _events
from .._events import Event
from .._exceptions import DeserializationError, SerializationError
from .._retry import RetryMixin
from ..abc import EventBroker, Serializer, Subscription
from ..serializers.json import JSONSerializer
from ._fragments import (
    DEFAULT_FRAGMENT_TIMEOUT,
    DEFAULT_MAX_EVENT_SIZE,
    DEFAULT_MAX_FRAGMENT_BUFFER_SIZE,
    DEFAULT_MAX_PENDING_EVENTS,
    LEGACY_FORMAT,
    FragmentAssembler,
    split_payload,
)


@attrs.define(eq=False, frozen=True)
class LocalSubscription(Subscription):
    callback: Callable[[Event], Any]
    event_types: set[type[Event]] | None
    one_shot: bool
    is_async: bool
    token: object
    _source: BaseEventBroker

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
        subscription = LocalSubscription(
            callback, types, one_shot, is_async, token, self
        )
        self._subscriptions[token] = subscription
        return subscription

    def unsubscribe(self, token: object) -> None:
        self._subscriptions.pop(token, None)

    async def publish_local(self, event: Event) -> None:
        event_type = type(event)
        one_shot_tokens: list[object] = []
        for subscription in self._subscriptions.values():
            if (
                subscription.event_types is None
                or event_type in subscription.event_types
            ):
                self._task_group.start_soon(self._deliver_event, subscription, event)
                if subscription.one_shot:
                    one_shot_tokens.append(subscription.token)

        for token in one_shot_tokens:
            self.unsubscribe(token)

    async def _deliver_event(
        self, subscription: LocalSubscription, event: Event
    ) -> None:
        try:
            if subscription.is_async:
                retval = subscription.callback(event)
                if iscoroutine(retval):
                    await retval
            else:
                await to_thread.run_sync(
                    subscription.callback, event, limiter=self._thread_limiter
                )
        except Exception:
            self._logger.exception(
                "Error delivering %s event", event.__class__.__name__
            )


@attrs.define(kw_only=True)
class BaseExternalEventBroker(BaseEventBroker, RetryMixin):
    """
    Base class for event brokers that use an external service.

    :param serializer: the serializer used to (de)serialize events for transport
    :param max_notification_size: maximum size of an unversioned notification before
        the broker switches to fragmented envelopes
    :param max_event_size: maximum accepted size of one complete event
    :param fragment_timeout: how long incomplete or delivered fragments are retained
    :param max_pending_events: maximum number of fragment and delivery cache entries
    :param max_fragment_buffer_size: maximum bytes preallocated for fragmented events
    :param fragment_cleanup_interval: interval for expiring fragment cache entries
    """

    serializer: Serializer = attrs.field(factory=JSONSerializer)
    max_notification_size: int = attrs.field(
        kw_only=True,
        default=7999,
        validator=[instance_of(int), ge(1)],
    )
    max_event_size: int = attrs.field(
        kw_only=True,
        default=DEFAULT_MAX_EVENT_SIZE,
        validator=[instance_of(int), ge(1)],
    )
    fragment_timeout: float = attrs.field(
        kw_only=True,
        default=DEFAULT_FRAGMENT_TIMEOUT,
        validator=[instance_of((int, float)), gt(0)],
    )
    max_pending_events: int = attrs.field(
        kw_only=True,
        default=DEFAULT_MAX_PENDING_EVENTS,
        validator=[instance_of(int), ge(1)],
    )
    max_fragment_buffer_size: int = attrs.field(
        kw_only=True,
        default=DEFAULT_MAX_FRAGMENT_BUFFER_SIZE,
        validator=[instance_of(int), ge(1)],
    )
    fragment_cleanup_interval: float = attrs.field(
        kw_only=True,
        default=1,
        validator=[instance_of((int, float)), gt(0)],
    )

    _fragment_assembler: FragmentAssembler | None = attrs.field(
        init=False, default=None
    )
    _fragment_cleanup_scope: CancelScope = attrs.field(init=False)

    async def start(self, exit_stack: AsyncExitStack, logger: Logger) -> None:
        await super().start(exit_stack, logger)
        self._fragment_cleanup_scope = CancelScope()
        exit_stack.callback(self._fragment_cleanup_scope.cancel)
        self._task_group.start_soon(
            self._sweep_fragment_cache, name="Fragment cache sweeper"
        )

    async def _sweep_fragment_cache(self) -> None:
        with self._fragment_cleanup_scope:
            while True:
                await sleep(self.fragment_cleanup_interval)
                assembler = self._get_fragment_assembler()
                assembler.sweep_expired()

    def _get_fragment_assembler(self) -> FragmentAssembler:
        if self._fragment_assembler is None:
            self._fragment_assembler = FragmentAssembler(
                logger=getattr(self, "_logger", getLogger(__name__)),
                max_event_size=self.max_event_size,
                fragment_timeout=self.fragment_timeout,
                max_pending_events=self.max_pending_events,
                max_fragment_buffer_size=self.max_fragment_buffer_size,
            )

        return self._fragment_assembler

    def _validate_event_size(self, payload: bytes) -> None:
        if len(payload) > self.max_event_size:
            raise SerializationError(
                f"Serialized event object exceeds {self.max_event_size} bytes in size"
            )

    def generate_notification(self, event: Event) -> bytes:
        serialized = self.serializer.serialize(event.marshal())
        return event.__class__.__name__.encode("ascii") + b" " + serialized

    def generate_notifications(self, event: Event) -> tuple[bytes, ...]:
        notification = self.generate_notification(event)
        if len(notification) <= self.max_notification_size:
            return (notification,)

        self._validate_event_size(notification)
        return split_payload(notification)

    def generate_notification_str(self, event: Event) -> str:
        serialized = self.serializer.serialize(event.marshal())
        return event.__class__.__name__ + " " + b64encode(serialized).decode("ascii")

    def generate_notifications_str(self, event: Event) -> tuple[str, ...]:
        notification = self.generate_notification_str(event)
        encoded_notification = notification.encode("ascii")
        if len(encoded_notification) <= self.max_notification_size:
            return (notification,)

        self._validate_event_size(encoded_notification)
        return split_payload(encoded_notification, text=True)

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
        assembled = self._get_fragment_assembler().assemble(payload)
        if assembled is None:
            return None

        if isinstance(assembled, bytes):
            payload = assembled
        elif not self._get_fragment_assembler().mark_delivered(payload):
            return None

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
        assembled = self._get_fragment_assembler().assemble(payload, text=True)
        if assembled is None:
            return None

        if isinstance(assembled, bytes):
            payload = assembled.decode("ascii")
        elif not self._get_fragment_assembler().mark_delivered(
            payload.encode("ascii")
        ):
            return None

        try:
            event_type, b64_serialized = payload.split(" ", 1)
        except ValueError:
            self._logger.error(
                "Received malformatted notification", extra={"payload": payload}
            )
            return None

        return self._reconstitute_event(event_type, b64decode(b64_serialized))
