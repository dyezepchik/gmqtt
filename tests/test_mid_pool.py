import struct

import gmqtt
from gmqtt.mqtt.utils import IdGenerator


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _id_gen(client: gmqtt.Client) -> IdGenerator:
    """Convenience accessor: the client's outbound packet-id allocator."""
    return client._package_handler.id_generator


def _make_offline_client(name: str) -> gmqtt.Client:
    """Construct a Client whose handler can run _handle_publish_packet without
    a real connection.  _send_command_with_mid is stubbed so PUBACK/PUBREC
    sends become no-ops."""
    client = gmqtt.Client(name)
    client._package_handler.on_message = lambda *args, **kwargs: None
    client._package_handler._send_command_with_mid = lambda *args, **kwargs: None
    return client


def _build_v5_publish(topic: bytes, mid: int, payload: bytes, qos: int = 1):
    """Build an MQTT v5 PUBLISH packet's fixed-header byte and variable part.

    Returns (cmd_byte, raw_packet_after_fixed_header_byte_and_remaining_length).
    The caller passes these straight into MqttPackageHandler._handle_publish_packet.
    """
    raw = struct.pack("!H", len(topic)) + topic
    if qos > 0:
        raw += struct.pack("!H", mid)
    raw += b"\x00"  # property-length VBI = 0 (no properties)
    raw += payload
    cmd = 0x30 | (qos << 1)  # PUBLISH command | qos bits
    return cmd, raw


# ---------------------------------------------------------------------------
# IdGenerator must not be a process-wide singleton
# ---------------------------------------------------------------------------
def test_idgenerator_instances_are_independent():
    """Two IdGenerator() calls must return distinct objects."""
    a = IdGenerator()
    b = IdGenerator()
    assert a is not b, (
        "IdGenerator is a Singleton — every gmqtt.Client in the process "
        "shares one 16-bit packet-id pool."
    )


def test_two_clients_have_independent_id_generators():
    """Each Client must own its own IdGenerator instance (via its handler)."""
    c1 = gmqtt.Client("client-1")
    c2 = gmqtt.Client("client-2")
    assert _id_gen(c1) is not _id_gen(c2), (
        "Clients share an IdGenerator across instances."
    )


def test_one_client_id_allocation_does_not_leak_into_another():
    """An allocation on client 1 must not change client 2's internal state,
    and both clients must independently start their allocation sequence at 1."""
    c1 = gmqtt.Client("client-3")
    c2 = gmqtt.Client("client-4")

    mid1 = _id_gen(c1).next_id()

    assert _id_gen(c2)._used_ids == set(), (
        f"Client 2's pool already saw client 1's allocation: "
        f"{_id_gen(c2)._used_ids}."
    )
    assert _id_gen(c2)._last_used_id == 0

    mid2 = _id_gen(c2).next_id()

    assert mid1 == 1
    assert mid2 == 1, (
        f"Client 2 returned mid {mid2}; expected 1 from a fresh independent pool."
    )


def test_new_client_starts_with_empty_used_ids():
    """A freshly constructed Client must not inherit used_ids from any
    previously constructed Client."""
    c1 = gmqtt.Client("client-5")
    for _ in range(5):
        _id_gen(c1).next_id()

    c2 = gmqtt.Client("client-6")
    assert _id_gen(c2)._used_ids == set(), (
        f"New Client started with non-empty used_ids: "
        f"{_id_gen(c2)._used_ids}."
    )
    assert _id_gen(c2)._last_used_id == 0


# ---------------------------------------------------------------------------
# Inbound PUBLISH must not feed broker mids into the outbound id pool
# ---------------------------------------------------------------------------
def test_inbound_qos1_publish_must_not_free_outbound_mid():
    """Handling an inbound QoS 1 PUBLISH must NOT remove an outbound mid
    from our id generator's used-id set."""
    client = _make_offline_client("client-qos1")
    gen = _id_gen(client)

    OUTBOUND_MID = 42
    gen._used_ids.add(OUTBOUND_MID)
    gen._last_used_id = OUTBOUND_MID
    assert OUTBOUND_MID in gen._used_ids

    cmd, raw = _build_v5_publish(
        topic=b"TEST/from-broker",
        mid=OUTBOUND_MID,
        payload=b"broker payload",
        qos=1,
    )
    client._package_handler._handle_publish_packet(cmd, raw)

    assert OUTBOUND_MID in gen._used_ids, (
        "Handling an inbound PUBLISH freed an outbound mid from the pool."
    )


def test_inbound_qos2_publish_must_not_free_outbound_mid():
    """Same as above but for inbound QoS 2."""
    client = _make_offline_client("client-qos2")
    gen = _id_gen(client)

    OUTBOUND_MID = 1234
    gen._used_ids.add(OUTBOUND_MID)
    gen._last_used_id = OUTBOUND_MID

    cmd, raw = _build_v5_publish(
        topic=b"TEST/qos2",
        mid=OUTBOUND_MID,
        payload=b"qos2 payload",
        qos=2,
    )
    client._package_handler._handle_publish_packet(cmd, raw)

    assert OUTBOUND_MID in gen._used_ids, (
        "Handling an inbound QoS 2 PUBLISH freed an outbound mid from the pool."
    )


def test_inbound_publish_does_not_cause_outbound_mid_collision():
    """Practical consequence: after an inbound PUBLISH whose mid matches
    one of our inflight outbound mids, next_id() must NOT hand the same mid
    out to a new outbound publish."""
    client = _make_offline_client("client-collision")
    gen = _id_gen(client)

    OUTBOUND_MID = 7
    gen._used_ids.add(OUTBOUND_MID)
    gen._last_used_id = OUTBOUND_MID - 1

    cmd, raw = _build_v5_publish(
        topic=b"TEST/collision",
        mid=OUTBOUND_MID,
        payload=b"x",
        qos=1,
    )
    client._package_handler._handle_publish_packet(cmd, raw)

    new_mid = gen.next_id()
    assert new_mid != OUTBOUND_MID, (
        f"next_id() returned mid={new_mid} which is still inflight on an "
        f"outbound publish — the inbound PUBLISH handler corrupted the pool."
    )
