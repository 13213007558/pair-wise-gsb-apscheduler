from __future__ import annotations

from base64 import b64decode, b64encode
from collections.abc import Callable, Iterable
from contextlib import AsyncExitStack
from inspect import iscoroutine
from logging import Logger
from typing import Any

import attrs
from anyio import CapacityLimiter, create_task_group, to_thread
from anyio.abc import TaskGroup

from .. import _events
from .._events import Event
from .._exceptions import DeserializationError
from .._retry import RetryMixin
from ..abc import EventBroker, Serializer, Subscription
from ..serializers.json import JSONSerializer
from ._envelope import (
    EnvelopeError,
    FragmentReassembler,
    fragment_payload,
    is_envelope,
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
    :param max_notification_size: maximum size, in bytes/characters, of a single
        transport notification. Notifications larger than this are transparently
        split into versioned envelope fragments. ``None`` (the default) disables
        fragmentation and keeps the legacy single-packet wire format forever.
        The PostgreSQL-based brokers default this to 7999 to stay within the
        ``NOTIFY`` payload limit.
    :param max_payload_size: maximum accepted size, in bytes, of a serialized
        event payload, on both the sending and the receiving side
    :param max_fragments: maximum number of fragments a single event may consist
        of
    :param fragment_reassembly_timeout: maximum time, in seconds, the receiver
        buffers fragments of an incomplete event before discarding them
    :param max_buffered_events: maximum number of incomplete events the receiver
        buffers concurrently, bounding reassembly memory use
    """

    serializer: Serializer = attrs.field(factory=JSONSerializer)
    max_notification_size: int | None = attrs.field(default=None)
    max_payload_size: int = attrs.field(default=1024 * 1024)
    max_fragments: int = attrs.field(default=1024)
    fragment_reassembly_timeout: float = attrs.field(default=10.0)
    max_buffered_events: int = attrs.field(default=1000)

    _reassembler: FragmentReassembler = attrs.field(init=False, eq=False)

    @_reassembler.default
    def _create_reassembler(self) -> FragmentReassembler:
        return FragmentReassembler(
            max_events=self.max_buffered_events,
            max_payload_size=self.max_payload_size,
            max_fragments=self.max_fragments,
            timeout=self.fragment_reassembly_timeout,
        )

    def _serialize_event(self, event: Event) -> tuple[str, bytes]:
        event_type = event.__class__.__name__
        return event_type, self.serializer.serialize(event.marshal())

    def generate_notification(self, event: Event) -> bytes:
        serialized = self.serializer.serialize(event.marshal())
        return event.__class__.__name__.encode("ascii") + b" " + serialized

    def generate_notification_str(self, event: Event) -> str:
        serialized = self.serializer.serialize(event.marshal())
        return event.__class__.__name__ + " " + b64encode(serialized).decode("ascii")

    def generate_notifications(self, event: Event) -> list[bytes]:
        """
        Generate one or more transport notifications for the event.

        Small events are encoded as a single legacy format notification
        (``EventType <serialized>``) which old clients understand. Events that
        would not fit in a single packet are split into versioned envelope
        fragments instead.
        """
        event_type, serialized = self._serialize_event(event)
        legacy = event_type.encode("ascii") + b" " + serialized
        if (
            self.max_notification_size is None
            or len(legacy) <= self.max_notification_size
        ):
            return [legacy]

        return [
            line.encode("ascii")
            for line in fragment_payload(
                event_type,
                serialized,
                self.max_notification_size,
                self.max_payload_size,
                self.max_fragments,
            )
        ]

    def generate_notifications_str(self, event: Event) -> list[str]:
        """String variant of :meth:`generate_notifications`."""
        event_type, serialized = self._serialize_event(event)
        legacy = event_type + " " + b64encode(serialized).decode("ascii")
        if (
            self.max_notification_size is None
            or len(legacy) <= self.max_notification_size
        ):
            return [legacy]

        return fragment_payload(
            event_type,
            serialized,
            self.max_notification_size,
            self.max_payload_size,
            self.max_fragments,
        )

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
        if is_envelope(payload):
            return self._reconstitute_enveloped(
                payload.decode("ascii", errors="replace")
            )

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
        if is_envelope(payload):
            return self._reconstitute_enveloped(payload)

        try:
            event_type, b64_serialized = payload.split(" ", 1)
        except ValueError:
            self._logger.error(
                "Received malformatted notification", extra={"payload": payload}
            )
            return None

        return self._reconstitute_event(event_type, b64decode(b64_serialized))

    def _reconstitute_enveloped(self, payload: str) -> Event | None:
        try:
            result = self._reassembler.receive(payload)
        except EnvelopeError:
            self._logger.exception(
                "Received malformed event envelope",
                extra={"payload": payload},
            )
            return None

        if result is None:
            return None

        event_type, serialized = result
        return self._reconstitute_event(event_type, serialized)
