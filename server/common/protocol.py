# Package common defines the protocol for the client-server communication.
# It is used to serialize and deserialize the messages exchanged between the client and the server.
# It is used to define the actions that can be performed by the client and the server.
# It is used to define the types of the messages exchanged between the client and the server.
# Message structure: [ Header ][ Body ]
# Header: [ Length ][ Action ] (uint32, uint8)
# Body: [ Document ][ Number ] (string, string)

from dataclasses import dataclass
import json
import socket
import struct
import io  # Import io for easier byte stream handling

# Custom Exception for clean connection closure
class ConnectionClosedCleanlyError(Exception):
    """Indicates that the socket connection was closed cleanly by the peer."""
    pass

# Fixed size header: Length (4 bytes) + Action (1 byte)
HEADER_SIZE = 5

# Action types
ACTION_BATCH_BET = 4
ACTION_ERROR = 3
ACTION_BATCH_CONFIRM = 5
ACTION_BATCH_ERROR = 6
ACTION_LOTTERY_NOTIFY = 7   # Client notifies server that all bets are sent
ACTION_LOTTERY_QUERY = 8    # Client queries server for winners from its agency
ACTION_LOTTERY_RESULT = 9   # Server responds with winners from an agency
ACTION_LOTTERY_PENDING = 10  # Lottery hasn't been drawn yet, still waiting for other agencies

@dataclass
class Header:
    length: int
    action: int

@dataclass
class Body:
    agency: str = ""  # Ensure default values for all expected fields
    firstname: str = ""
    lastname: str = ""
    document: str = ""
    birthdate: str = ""
    number: str = ""
    error: str = ""
    batch_id: str = ""
    count: int = 0
    bets: list = None  # Representing list of Bet objects potentially
    winners: list = None # Representing list of winner document strings
    message: str = ""

    def __post_init__(self):
        if self.bets is None:
            self.bets = []
        if self.winners is None:
            self.winners = []

@dataclass
class Message: # This class might not be strictly needed anymore but kept for structure
    header: Header
    body: Body

# Helper functions for binary serialization/deserialization

def serialize_string(s: str) -> bytes:
    """Serializes a string with a uint32 length prefix."""
    s_bytes = s.encode('utf-8')
    return struct.pack('>I', len(s_bytes)) + s_bytes

def deserialize_string(buffer: io.BytesIO) -> str:
    """Deserializes a string with a uint32 length prefix from a buffer."""
    length = struct.unpack('>I', buffer.read(4))[0]
    return buffer.read(length).decode('utf-8')

def serialize(header: Header, body: Body) -> bytes:
    """Serializes Header and Body into a binary message."""
    body_buffer = io.BytesIO()

    # Serialize body based on action type
    if header.action == ACTION_BATCH_BET:
        # [Agency Len][Agency Bytes][BatchID Len][BatchID Bytes][Num Bets (uint32)][Bet 1][Bet 2]...
        # Bet: [Doc Len][Doc Bytes][Num Len][Num Bytes][First Len][First Bytes][Last Len][Last Bytes][Birth Len][Birth Bytes]
        body_buffer.write(serialize_string(body.agency))
        body_buffer.write(serialize_string(body.batch_id))
        body_buffer.write(struct.pack('>I', len(body.bets)))
        for bet in body.bets: # Assuming body.bets contains Bet objects (or similar structure)
            body_buffer.write(serialize_string(getattr(bet, 'document', '')))
            body_buffer.write(serialize_string(getattr(bet, 'number', '')))
            body_buffer.write(serialize_string(getattr(bet, 'first_name', '')))
            body_buffer.write(serialize_string(getattr(bet, 'last_name', '')))
            body_buffer.write(serialize_string(getattr(bet, 'birthdate', '')))

    elif header.action == ACTION_ERROR:
        # [Agency Len][Agency Bytes][Error Len][Error Bytes]
        body_buffer.write(serialize_string(body.agency))
        body_buffer.write(serialize_string(body.error))

    elif header.action == ACTION_BATCH_CONFIRM:
        # [Agency Len][Agency Bytes][BatchID Len][BatchID Bytes][Count (uint32)]
        body_buffer.write(serialize_string(body.agency))
        body_buffer.write(serialize_string(body.batch_id))
        body_buffer.write(struct.pack('>I', body.count))

    elif header.action == ACTION_BATCH_ERROR:
        # [Agency Len][Agency Bytes][BatchID Len][BatchID Bytes][Count (uint32)][Error Len][Error Bytes]
        body_buffer.write(serialize_string(body.agency))
        body_buffer.write(serialize_string(body.batch_id))
        body_buffer.write(struct.pack('>I', body.count))
        body_buffer.write(serialize_string(body.error))

    elif header.action == ACTION_LOTTERY_NOTIFY:
        # [Agency Len][Agency Bytes]
        body_buffer.write(serialize_string(body.agency))

    elif header.action == ACTION_LOTTERY_QUERY:
        # [Agency Len][Agency Bytes]
        body_buffer.write(serialize_string(body.agency))

    elif header.action == ACTION_LOTTERY_RESULT:
        # [Agency Len][Agency Bytes][Num Winners (uint32)][Winner 1 Len][Winner 1 Bytes]...
        body_buffer.write(serialize_string(body.agency))
        body_buffer.write(struct.pack('>I', len(body.winners)))
        for winner_doc in body.winners:
            body_buffer.write(serialize_string(winner_doc))

    elif header.action == ACTION_LOTTERY_PENDING:
        # [Agency Len][Agency Bytes][Count (uint32)][Message Len][Message Bytes]
        body_buffer.write(serialize_string(body.agency))
        body_buffer.write(struct.pack('>I', body.count))
        body_buffer.write(serialize_string(body.message))

    # Get the serialized body bytes
    body_data = body_buffer.getvalue()
    header.length = len(body_data) # Update header length

    # Pack the header
    header_bytes = struct.pack('>IB', header.length, header.action)

    return header_bytes + body_data

def deserialize_header(header_data: bytes) -> Header:
    """Deserializes the binary header."""
    length, action = struct.unpack(">IB", header_data)
    return Header(length=length, action=action)

# Removed deserialize_body as its logic is now within receive

def send(conn: socket.socket, message: bytes):
    """Sends a full message over the socket connection."""
    conn.sendall(message)

def receive(conn: socket.socket) -> tuple:
    """
    Receives a binary message using two-phase approach:
    1. Read fixed-length header
    2. Use Length field from header to read body
    3. Deserialize body based on Action field from header

    Returns a tuple of (Header, Body).
    Raises ConnectionClosedCleanlyError if the connection is closed cleanly by the peer.
    Raises ConnectionAbortedError if there's a socket error during read.
    Raises ValueError if the message format is invalid (e.g., too large, bad action, parsing error).
    """
    header_data = bytearray(HEADER_SIZE)
    bytes_received = 0
    while bytes_received < HEADER_SIZE:
        try:
            chunk = conn.recv(HEADER_SIZE - bytes_received)
            if not chunk:
                # Connection closed cleanly -> Raise custom Exception
                raise ConnectionClosedCleanlyError("Connection closed cleanly while receiving header")
            header_data[bytes_received:bytes_received+len(chunk)] = chunk
            bytes_received += len(chunk)
        except OSError as e:
             # Handle other potential socket errors during recv -> Raise Exception
             # logging.error(f"Socket error receiving header: {e}") # Consider adding logging if needed
             raise ConnectionAbortedError(f"Socket error receiving header: {e}")


    header = deserialize_header(header_data)

    # Basic sanity check for body length
    if header.length > 10_000_000: # Example limit: 10MB
        # logging.error(f"Message body too large: {header.length} bytes. Action: {header.action}")
        # Raise ValueError for invalid message format
        raise ValueError(f"Message body too large: {header.length} bytes. Action: {header.action}")

    body_data = bytearray(header.length)
    bytes_received = 0
    while bytes_received < header.length:
        try:
            chunk = conn.recv(header.length - bytes_received)
            if not chunk:
                # Connection closed before full body received -> Raise custom Exception
                # logging.warning(f"Connection closed prematurely receiving body. Expected {header.length}, got {bytes_received}. Action: {header.action}")
                raise ConnectionClosedCleanlyError(f"Connection closed prematurely receiving body. Expected {header.length}, got {bytes_received}. Action: {header.action}")
            body_data[bytes_received:bytes_received+len(chunk)] = chunk
            bytes_received += len(chunk)
        except OSError as e:
             # Handle other potential socket errors during recv -> Raise Exception
             # logging.error(f"Socket error receiving body: {e}")
             raise ConnectionAbortedError(f"Socket error receiving body: {e}")

    # Deserialize body based on action
    body_buffer = io.BytesIO(body_data)
    body = Body() # Create an empty Body object to populate

    try:
        if header.action == ACTION_BATCH_BET:
            body.agency = deserialize_string(body_buffer)
            body.batch_id = deserialize_string(body_buffer)
            num_bets = struct.unpack('>I', body_buffer.read(4))[0]
            body.bets = [] # Initialize bets list
            for _ in range(num_bets):
                # Create a temporary structure or dict for each bet's data
                # Note: The Body class doesn't store individual bet fields directly,
                # but the Bets list. We might need a Bet class here or use dicts.
                # Using a simple dict for now for deserialization storage:
                bet_data = {
                    'document': deserialize_string(body_buffer),
                    'number': deserialize_string(body_buffer),
                    'first_name': deserialize_string(body_buffer),
                    'last_name': deserialize_string(body_buffer),
                    'birthdate': deserialize_string(body_buffer)
                }
                body.bets.append(bet_data) # Append dict to list

        elif header.action == ACTION_ERROR:
            body.agency = deserialize_string(body_buffer)
            body.error = deserialize_string(body_buffer)

        elif header.action == ACTION_BATCH_CONFIRM:
            body.agency = deserialize_string(body_buffer)
            body.batch_id = deserialize_string(body_buffer)
            body.count = struct.unpack('>I', body_buffer.read(4))[0]

        elif header.action == ACTION_BATCH_ERROR:
            body.agency = deserialize_string(body_buffer)
            body.batch_id = deserialize_string(body_buffer)
            body.count = struct.unpack('>I', body_buffer.read(4))[0]
            body.error = deserialize_string(body_buffer)

        elif header.action == ACTION_LOTTERY_NOTIFY:
            body.agency = deserialize_string(body_buffer)

        elif header.action == ACTION_LOTTERY_QUERY:
            body.agency = deserialize_string(body_buffer)

        elif header.action == ACTION_LOTTERY_RESULT:
            body.agency = deserialize_string(body_buffer)
            num_winners = struct.unpack('>I', body_buffer.read(4))[0]
            body.winners = [deserialize_string(body_buffer) for _ in range(num_winners)]

        elif header.action == ACTION_LOTTERY_PENDING:
            body.agency = deserialize_string(body_buffer)
            body.count = struct.unpack('>I', body_buffer.read(4))[0]
            body.message = deserialize_string(body_buffer)

        else:
            # Unknown action type -> Raise Exception
            # logging.warning(f"Received message with unknown action type: {header.action}")
            raise ValueError(f"Received message with unknown action type: {header.action}")

        # Check if we consumed the exact number of bytes expected
        if body_buffer.tell() != header.length:
            # Data length mismatch -> Raise Exception
            # logging.error(f"Body parsing error: consumed {body_buffer.tell()} bytes, expected {header.length}. Action: {header.action}")
            raise ValueError(f"Body parsing error: consumed {body_buffer.tell()} bytes, expected {header.length}. Action: {header.action}")

    except (struct.error, EOFError, UnicodeDecodeError) as e:
        # Handle potential errors during unpacking or decoding -> Raise Exception
        # logging.error(f"Error deserializing body: {e}. Action: {header.action}")
        raise ValueError(f"Error deserializing body: {e}. Action: {header.action}")

    return (header, body)
