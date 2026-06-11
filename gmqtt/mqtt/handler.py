import asyncio
import logging
import struct
import time
from collections import defaultdict
from functools import partial
from typing import Awaitable, Callable, List, Optional

from .constants import MQTTCommands, MQTTv50, MQTTv311, PubRecReasonCode
from .package import Package
from .property import Property
from .utils import (
    IdGenerator,
    ConnectionState,
    run_coroutine_or_function,
    unpack_variable_byte_integer,
)


def _empty_callback(*args, **kwargs):
    pass


class MQTTError(Exception):
    pass


class MQTTConnectError(MQTTError):
    __messages__ = {
        1: "Connection Refused: unacceptable protocol version",
        2: "Connection Refused: identifier rejected",
        3: "Connection Refused: broker unavailable",
        4: "Connection Refused: bad user name or password",
        5: "Connection Refused: not authorised",
        10: "Cannot handle CONNACK package",
        128: "Connection Refused: Unspecified error",
        129: "Connection Refused: Malformed Packet",
        130: "Connection Refused: Protocol Error",
        131: "Connection Refused: Implementation specific error",
        132: "Connection Refused: Unsupported Protocol Version",
        133: "Connection Refused: Client Identifier not valid",
        134: "Connection Refused: Bad User Name or Password",
        135: "Connection Refused: Not authorized",
        136: "Connection Refused: Server unavailable",
        137: "Connection Refused: Server busy",
        138: "Connection Refused: Banned",
        140: "Connection Refused: Bad authentication method",
        144: "Connection Refused: Topic Name invalid",
        149: "Connection Refused: Packet too large",
        151: "Connection Refused: Quota exceeded",
        153: "Connection Refused: Payload format invalid",
        154: "Connection Refused: Retain not supported",
        155: "Connection Refused: QoS not supported",
        156: "Connection Refused: Use another server",
        157: "Connection Refused: Server moved",
        159: "Connection Refused: Connection rate exceeded",
    }

    def __init__(self, code):
        self._code = code
        self.message = self.__messages__.get(code, "Unknown error")

    def __str__(self):
        return "code {} ({})".format(self._code, self.message)


class EventCallbackMixin:

    def __init__(self, *args, **kwargs):
        # provided by a main class
        self._connection_state: ConnectionState

        self._on_connected_callback = _empty_callback
        self._on_disconnected_callback = _empty_callback
        self._on_message_callback = _empty_callback
        self._on_subscribe_callback = _empty_callback
        self._on_unsubscribe_callback = _empty_callback

    def stop_reconnect(self):
        self._connection_state.config["reconnect_retries"] = 0

    @property
    def reconnect_delay(self):
        return self._connection_state.config["reconnect_delay"]

    @reconnect_delay.setter
    def reconnect_delay(self, value):
        self._connection_state.config["reconnect_delay"] = value

    @property
    def reconnect_retries(self):
        return self._connection_state.config["reconnect_retries"]

    @reconnect_retries.setter
    def reconnect_retries(self, value):
        self._connection_state.config["reconnect_retries"] = value

    @property
    def on_subscribe(self):
        return self._on_subscribe_callback

    @on_subscribe.setter
    def on_subscribe(self, cb):
        if not callable(cb):
            raise ValueError
        self._on_subscribe_callback = cb

    @property
    def on_connect(self):
        return self._on_connected_callback

    @on_connect.setter
    def on_connect(self, cb):
        if not callable(cb):
            raise ValueError
        self._on_connected_callback = cb

    @property
    def on_message(self):
        return self._on_message_callback

    @on_message.setter
    def on_message(self, cb):
        if not callable(cb):
            raise ValueError
        self._on_message_callback = cb

    @property
    def on_disconnect(self):
        return self._on_disconnected_callback

    @on_disconnect.setter
    def on_disconnect(self, cb):
        if not callable(cb):
            raise ValueError
        self._on_disconnected_callback = cb

    @property
    def on_unsubscribe(self):
        return self._on_unsubscribe_callback

    @on_unsubscribe.setter
    def on_unsubscribe(self, cb):
        if not callable(cb):
            raise ValueError
        self._on_unsubscribe_callback = cb


class MqttPackageHandler(EventCallbackMixin):
    def __init__(
        self,
        *args,
        connack_event: asyncio.Event,
        connection_state: ConnectionState,
        reconnect_callback: Callable[..., Awaitable],
        disconnect_callback: Callable[..., Awaitable],
        resend_qos_callback: Callable[[], Awaitable],
        clear_qos_callback: Callable[[], Awaitable],
        connect_properties: dict,
        subscriptions_getter: Callable[[], List],
        remove_message_callback: Callable[[int], None],
        send_command_with_mid_callback: Callable[..., None],
        receive_maximum: int = 65535,
        optimistic_acknowledgement: bool = True,
        logger: Optional[logging.Logger] = None,
        **kwargs
    ):

        super().__init__(*args, **kwargs)

        self._connack_received = connack_event
        self._connection_state = connection_state
        self._reconnect_callback = reconnect_callback
        self._disconnect_callback = disconnect_callback
        self._resend_qos_callback = resend_qos_callback
        self._clear_qos_callback = clear_qos_callback
        self._connect_properties = connect_properties
        self._subscriptions_getter = subscriptions_getter
        self._remove_message_callback = remove_message_callback
        self._send_command_with_mid_callback = send_command_with_mid_callback
        self._connack_properties: dict = {}
        self._messages_in = {}
        self._handler_cache = {}
        self._error = None
        self._connection = None
        self._server_topics_aliases = {}

        if connection_state.protocol_version < MQTTv50:
            optimistic_acknowledgement = True

        self._optimistic_acknowledgement = optimistic_acknowledgement
        self.id_generator = IdGenerator(max=receive_maximum)

        self._logger = logger or logging.getLogger(__name__)

    @property
    def has_error(self):
        return self._error is not None

    @property
    def properties(self) -> dict:
        """Merged connect + connack properties, passed to the on_connect callback."""
        return {**self._connack_properties, **self._connect_properties}

    def propagate_error(self):
        if self._error:
            raise self._error

    def get_subscriptions_by_mid(self, mid: int) -> List:
        return [sub for sub in self._subscriptions_getter() if sub.mid == mid]

    def clear_topics_aliases(self):
        self._server_topics_aliases = {}

    def _send_command_with_mid(self, cmd, mid, dup, reason_code=0):
        self._send_command_with_mid_callback(cmd, mid, dup, reason_code=reason_code)

    def _remove_message_from_queue(self, mid: int) -> None:
        self._remove_message_callback(mid)

    def _send_puback(self, mid, reason_code=0):
        self._send_command_with_mid(
            MQTTCommands.PUBACK, mid, False, reason_code=reason_code
        )

    def _send_pubrec(self, mid, reason_code=0):
        self._send_command_with_mid(
            MQTTCommands.PUBREC, mid, False, reason_code=reason_code
        )

    def _send_pubrel(self, mid, dup, reason_code=0):
        self._send_command_with_mid(
            MQTTCommands.PUBREL | 2, mid, dup, reason_code=reason_code
        )

    def _send_pubcomp(self, mid, dup, reason_code=0):
        self._send_command_with_mid(
            MQTTCommands.PUBCOMP, mid, dup, reason_code=reason_code
        )

    def _default_cmd_handler(self, cmd, packet):
        self._logger.warning("[MqttPackageHandler] %s %s", hex(cmd), packet)

    def __get_handler__(self, cmd):
        cmd_type = cmd & 0xF0
        if cmd_type not in self._handler_cache:
            handler_name = "_handle_{}_packet".format(
                MQTTCommands(cmd_type).name.lower()
            )
            self._handler_cache[cmd_type] = getattr(
                self, handler_name, self._default_cmd_handler
            )
        return self._handler_cache[cmd_type]

    def _handle_packet(self, cmd, packet):
        self._logger.debug("[MqttPackageHandler] cmd: %s, packet: %s", hex(cmd), packet)
        handler = self.__get_handler__(cmd)
        handler(cmd, packet)
        self._last_msg_in = time.monotonic()

    def _handle_exception_in_future(self, future, msg=None):
        """Done-callback for fire-and-forget futures.

        msg: string to be added to the log message with the exception itself
        """
        exc = future.exception()
        log_string = "[MqttPackageHandler]: %s"
        if msg:
            log_string = "[MqttPackageHandler] " + msg + ": %s"

        if exc:
            self._logger.warning(log_string, exc)

    def _handle_disconnect_packet(self, cmd, packet):
        # reset server topics on disconnect
        self.clear_topics_aliases()

        future = asyncio.ensure_future(self._reconnect_callback(delay=True))
        future.add_done_callback(
            partial(self._handle_exception_in_future, msg="reconnect failed")
        )
        self.on_disconnect(packet)

    def _parse_properties(self, packet):
        if self._connection_state.protocol_version < MQTTv50:
            # If protocol is version is less than 5.0, there is no properties in packet
            return {}, packet
        properties_len, left_packet = unpack_variable_byte_integer(packet)
        packet = left_packet[:properties_len]
        left_packet = left_packet[properties_len:]
        properties_dict = defaultdict(list)
        while packet:
            (property_identifier,) = struct.unpack("!B", packet[:1])
            property_obj = Property.factory(id_=property_identifier)
            if property_obj is None:
                self._logger.critical(
                    "[MqttPackageHandler] received invalid property id %s, disconnecting",
                    property_identifier
                )
                return None, None
            result, packet = property_obj.loads(packet[1:])
            for k, v in result.items():
                properties_dict[k].append(v)
        properties_dict = dict(properties_dict)
        return properties_dict, left_packet

    def _update_keepalive_if_needed(self):
        if not self._connack_properties.get("server_keep_alive"):
            return
        self._keepalive = self._connack_properties["server_keep_alive"][0]
        self._connection.keepalive = self._keepalive

    def _handle_connack_packet(self, cmd, packet):
        (session_present, result) = struct.unpack("!BB", packet[:2])

        # --- Step 1: check result before any side-effects ---
        if result != 0:
            self._logger.warning("[MqttPackageHandler] CONNACK: %s", hex(result))
            self._connection_state.failed_connections += 1

            if result == 1 and self._connection_state.protocol_version == MQTTv50:
                # Broker rejected MQTT v5; downgrade to v3.1.1 and retry.
                # Do NOT set _connack_received here — Client.connect() must keep
                # waiting until the subsequent successful CONNACK arrives.
                self._logger.info("[MqttPackageHandler] Downgrading to MQTT 3.1 protocol version")
                self._connection_state.protocol_version = MQTTv311
                future = asyncio.ensure_future(self._reconnect_callback(delay=True))
                future.add_done_callback(
                    partial(self._handle_exception_in_future,
                            msg="reconnect failed on protocol downgrade")
                )
            else:
                # Permanent or transient failure: record error and unblock
                # connect() so it can raise via propagate_error().
                self._error = MQTTConnectError(result)
                self._connack_received.set()
                future = asyncio.ensure_future(self._reconnect_callback(delay=True))
                future.add_done_callback(
                    partial(self._handle_exception_in_future,
                            msg="reconnect failed after refused CONNACK")
                )

            return

        # --- Step 2: successful CONNACK ---
        self._connection_state.failed_connections = 0

        # --- Step 3: parse MQTT 5.0 properties (if present) ---
        if len(packet) > 2:
            properties, _ = self._parse_properties(packet[2:])
            if properties is None:
                # Malformed properties — set error and return before touching
                # any mutable state (fixes the None-assignment crash).
                self._error = MQTTConnectError(10)
                self._connack_received.set()
                self._logger.warning(
                    "[MqttPackageHandler] malformed properties: disconnecting (code 10)"
                )
                self.on_disconnect(self._error)
                asyncio.ensure_future(self._disconnect_callback())
                return

            self._connack_properties = properties
            self._update_keepalive_if_needed()

            # --- Step 4: apply broker's flow-control window ---
            if "receive_maximum" in self._connack_properties:
                self.id_generator._max = self._connack_properties["receive_maximum"][0]

        # --- Step 5: all state is consistent — unblock connect() ---
        self._connack_received.set()

        # --- Step 6: schedule QoS session-restore only on success ---
        if session_present:
            asyncio.ensure_future(self._resend_qos_callback())
        else:
            asyncio.ensure_future(self._clear_qos_callback())

        self._logger.debug(
            "[MqttPackageHandler] session_present: %s, result: %s",
            hex(session_present), hex(result),
        )
        self.on_connect(session_present, result, self.properties)

    def _handle_publish_packet(self, cmd, raw_packet):
        header = cmd

        dup = (header & 0x08) >> 3
        qos = (header & 0x06) >> 1
        retain = header & 0x01

        pack_format = "!H" + str(len(raw_packet) - 2) + "s"
        (slen, packet) = struct.unpack(pack_format, raw_packet)

        pack_format = "!" + str(slen) + "s" + str(len(packet) - slen) + "s"
        (topic, packet) = struct.unpack(pack_format, packet)

        # we will change the packet ref, let's save origin
        payload = packet

        if qos > 0:
            pack_format = "!H" + str(len(packet) - 2) + "s"
            (mid, packet) = struct.unpack(pack_format, packet)
        else:
            mid = None

        properties, packet = self._parse_properties(packet)
        properties["dup"] = dup
        properties["retain"] = retain

        if packet is None:
            self._logger.critical("[MqttPackageHandler] invalid message. Skipping: {}".format(raw_packet))
            return

        if "topic_alias" in properties:
            # TODO: need to add validation (topic alias must be greater than 0 and less than topic_alias_maximum)
            topic_alias = properties["topic_alias"][0]
            if topic:
                self._server_topics_aliases[topic_alias] = topic
            else:
                topic = self._server_topics_aliases.get(topic_alias, None)

        if not topic:
            self._logger.warning(
                "[MqttPackageHandler] topic name is empty (or server has send invalid topic alias)"
            )
            return

        try:
            print_topic = topic.decode("utf-8")
        except UnicodeDecodeError as exc:
            self._logger.warning("[MqttPackageHandler] invalid character in topic: %s", topic, exc_info=exc)
            print_topic = topic

        self._logger.debug("[MqttPackageHandler] RECV %s, QoS: %s, payload: %s", print_topic, qos, payload)

        if qos == 0:
            run_coroutine_or_function(
                self.on_message, print_topic, packet, qos, properties
            )
        elif qos == 1:
            self._handle_qos_1_publish_packet(mid, packet, print_topic, properties)
        elif qos == 2:
            self._handle_qos_2_publish_packet(mid, packet, print_topic, properties)
        # NOTE: do NOT free `mid` here. It is the BROKER's packet identifier for
        # an inbound PUBLISH and lives in a namespace independent of our
        # outbound allocator. Freeing it could remove an inflight outbound mid
        # from the pool and cause MQTT-2.2.1-3 violations.

    def _handle_qos_2_publish_packet(self, mid, packet, print_topic, properties):
        if self._optimistic_acknowledgement:
            self._send_pubrec(mid)
            run_coroutine_or_function(
                self.on_message, print_topic, packet, 2, properties
            )
        else:
            run_coroutine_or_function(
                self.on_message,
                print_topic,
                packet,
                2,
                properties,
                callback=partial(self.__handle_publish_callback, qos=2, mid=mid),
            )

    def __handle_publish_callback(self, f, qos=None, mid=None):
        reason_code = f.result()
        if reason_code not in (c.value for c in PubRecReasonCode):
            raise ValueError("Invalid PUBREC reason code {}".format(reason_code))
        if qos == 2:
            self._send_pubrec(mid, reason_code=reason_code)
        else:
            self._send_puback(mid, reason_code=reason_code)
        # NOTE: do NOT free `mid` here — it is the broker's inbound mid, not the
        # one we allocated. See _handle_publish_packet for the full rationale.

    def _handle_qos_1_publish_packet(self, mid, packet, print_topic, properties):
        if self._optimistic_acknowledgement:
            self._send_puback(mid)
            run_coroutine_or_function(
                self.on_message, print_topic, packet, 1, properties
            )
        else:
            run_coroutine_or_function(
                self.on_message,
                print_topic,
                packet,
                1,
                properties,
                callback=partial(self.__handle_publish_callback, qos=1, mid=mid),
            )

    def __call__(self, package: Package):
        try:
            self._handle_packet(package.cmd, package.data)
        except Exception:
            self._logger.exception("[MqttPackageHandler] error handling package")

    def _handle_suback_packet(self, cmd, raw_packet):
        pack_format = "!H" + str(len(raw_packet) - 2) + "s"
        (mid, packet) = struct.unpack(pack_format, raw_packet)
        properties, packet = self._parse_properties(packet)

        pack_format = "!" + "B" * len(packet)
        granted_qoses = struct.unpack(pack_format, packet)

        subs = self.get_subscriptions_by_mid(mid)
        for granted_qos, sub in zip(granted_qoses, subs):
            if granted_qos >= 128:
                # subscription was not acknowledged
                sub.acknowledged = False
            else:
                sub.acknowledged = True
                sub.qos = granted_qos

        self._logger.info("[MqttPackageHandler] SUBACK mid: %s, QoS: %s", mid, granted_qoses)
        self.on_subscribe(mid, granted_qoses, properties)

        for sub in self._subscriptions_getter():
            if sub.mid == mid:
                sub.mid = None

        self.id_generator.free_id(mid)

    def _handle_unsuback_packet(self, cmd, raw_packet):
        pack_format = "!H" + str(len(raw_packet) - 2) + "s"
        (mid, packet) = struct.unpack(pack_format, raw_packet)
        pack_format = "!" + "B" * len(packet)
        granted_qos = struct.unpack(pack_format, packet)

        self._logger.info("[MqttPackageHandler] UNSUBACK mid: %s, QoS: %s", mid, granted_qos)

        self.on_unsubscribe(mid, granted_qos)
        self.id_generator.free_id(mid)

    def _handle_pingreq_packet(self, cmd, packet):
        self._logger.debug("[MqttPackageHandler] PINGREQ cmd: %s, packet: %s", hex(cmd), packet)
        pass

    def _handle_pingresp_packet(self, cmd, packet):
        self._logger.debug("[MqttPackageHandler] PINGRESP cmd: %s, packet: %s", hex(cmd), packet)

    def _handle_puback_packet(self, cmd, packet):
        (mid,) = struct.unpack("!H", packet[:2])

        # TODO: For MQTT 5.0 parse reason code and properties

        self._logger.debug("[MqttPackageHandler] PUBACK mid: %s", mid)

        self.id_generator.free_id(mid)
        self._remove_message_from_queue(mid)

    def _handle_pubcomp_packet(self, cmd, packet):
        pass

    def _handle_pubrec_packet(self, cmd, packet):
        (mid,) = struct.unpack("!H", packet[:2])
        self._logger.debug("[MqttPackageHandler] PUBREC mid: %s", mid)
        self.id_generator.free_id(mid)
        self._remove_message_from_queue(mid)
        self._send_pubrel(mid, 0)

    def _handle_pubrel_packet(self, cmd, packet):
        (mid,) = struct.unpack("!H", packet[:2])
        self._logger.debug("[MqttPackageHandler] PUBREL mid: %s", mid)
        self._send_pubcomp(mid, 0)

        self.id_generator.free_id(mid)
