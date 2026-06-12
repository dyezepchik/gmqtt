"""Offline coverage for the MQTT 5.0 Session Expiry Interval (§3.1.2.11.2).

The property is identifier 17 / Four Byte Integer.  It must be:
  * accepted on `Client(...)` and serialised into the CONNECT properties.
  * accepted on `client.disconnect(...)` and serialised into DISCONNECT.
  * validated against the 0..0xFFFFFFFF range.
  * readable back via `client.session_expiry_interval` (server CONNACK value
    wins over the client's request per §3.2.2.3.2).
"""
import struct

import pytest

import gmqtt
from gmqtt.mqtt.constants import MQTTv50, MQTTv311
from gmqtt.mqtt.package import DisconnectPacket, LoginPackageFactor
from gmqtt.mqtt.property import PROPERTIES_BY_ID, PROPERTIES_BY_NAME


class _StubProto:
    """Minimal stand-in for MQTTProtocol used by package builders."""
    proto_name = b"MQTT"

    def __init__(self, ver=MQTTv50):
        self.proto_ver = ver


# ---------------------------------------------------------------------------
# Property table sanity
# ---------------------------------------------------------------------------
def test_session_expiry_interval_property_definition():
    prop = PROPERTIES_BY_NAME["session_expiry_interval"]
    assert prop.id == 17
    assert prop.bytes_struct == "!L"
    assert PROPERTIES_BY_ID[17] is prop
    # §3.1.2.11.2 (CONNECT), §3.2.2.3.2 (CONNACK), §3.14.2.2.2 (DISCONNECT)
    assert set(prop.allowed_packages) == {"CONNECT", "CONNACK", "DISCONNECT"}


# ---------------------------------------------------------------------------
# Construction-time validation
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("value", [-1, 0x1_0000_0000, 0x1_0000_0001])
def test_session_expiry_interval_out_of_range_rejected(value):
    with pytest.raises(ValueError):
        gmqtt.Client("c", session_expiry_interval=value)


@pytest.mark.parametrize("value", ["60", 1.5, True])
def test_session_expiry_interval_wrong_type_rejected(value):
    with pytest.raises(TypeError):
        gmqtt.Client("c", session_expiry_interval=value)


@pytest.mark.parametrize("value", [0, 1, 60, 0xFFFFFFFE, 0xFFFFFFFF])
def test_session_expiry_interval_in_range_accepted(value):
    c = gmqtt.Client("c", session_expiry_interval=value)
    assert c._connect_properties["session_expiry_interval"] == value


def test_session_expiry_interval_absent_defaults_to_zero():
    c = gmqtt.Client("c")
    assert c.session_expiry_interval == 0


# ---------------------------------------------------------------------------
# Encoding on the wire
# ---------------------------------------------------------------------------
def _find_property_value(packet: bytes, prop_id: int, fmt: str):
    """Search for property identifier byte followed by its packed value
    anywhere in the packet.  Good enough for offline assertion."""
    size = struct.calcsize(fmt)
    needle_id = bytes([prop_id])
    for i in range(len(packet) - size):
        if packet[i:i + 1] == needle_id:
            try:
                return struct.unpack(fmt, packet[i + 1:i + 1 + size])[0]
            except struct.error:
                continue
    return None


def test_session_expiry_interval_serialised_in_connect_v5():
    packet = LoginPackageFactor.build_package(
        client_id="cid",
        username=None,
        password=None,
        clean_session=True,
        keepalive=60,
        protocol=_StubProto(MQTTv50),
        session_expiry_interval=600,
    )
    assert _find_property_value(bytes(packet), 17, "!L") == 600


def test_session_expiry_interval_max_value_round_trips_in_connect():
    packet = LoginPackageFactor.build_package(
        client_id="cid",
        username=None,
        password=None,
        clean_session=True,
        keepalive=60,
        protocol=_StubProto(MQTTv50),
        session_expiry_interval=0xFFFFFFFF,
    )
    assert _find_property_value(bytes(packet), 17, "!L") == 0xFFFFFFFF


def test_session_expiry_interval_omitted_when_v311():
    # MQTT 3.1.1 has no properties; the kwarg must be silently dropped, not raise.
    packet = LoginPackageFactor.build_package(
        client_id="cid",
        username=None,
        password=None,
        clean_session=True,
        keepalive=60,
        protocol=_StubProto(MQTTv311),
        session_expiry_interval=600,
    )
    # No property bytes at all in v3.1.1.
    assert _find_property_value(bytes(packet), 17, "!L") is None


def test_session_expiry_interval_serialised_in_disconnect_v5():
    packet = DisconnectPacket.build_package(
        protocol=_StubProto(MQTTv50),
        reason_code=0,
        session_expiry_interval=0,
    )
    assert _find_property_value(bytes(packet), 17, "!L") == 0


# ---------------------------------------------------------------------------
# Server CONNACK override (§3.2.2.3.2)
# ---------------------------------------------------------------------------
def test_session_expiry_interval_property_uses_server_value_when_present():
    c = gmqtt.Client("c", session_expiry_interval=600)
    # Simulate broker-assigned override; _parse_properties returns a list,
    # so the value is wrapped in a list matching what a real CONNACK produces.
    # _parse_properties stores values as lists (defaultdict(list) + .append).
    c._package_handler._connack_properties["session_expiry_interval"] = [120]
    assert c.session_expiry_interval == 120


def test_session_expiry_interval_property_falls_back_to_request():
    c = gmqtt.Client("c", session_expiry_interval=600)
    assert c.session_expiry_interval == 600
