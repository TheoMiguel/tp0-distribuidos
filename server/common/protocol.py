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

# Fixed size header: Length (4 bytes) + Action (1 byte)
HEADER_SIZE = 5

# Action types
ACTION_BATCH_BET = 4
ACTION_ERROR = 3
ACTION_BATCH_CONFIRM = 5
ACTION_BATCH_ERROR = 6

@dataclass
class Header:
    length: int
    action: int

@dataclass
class Body:
    agency: str
    firstname: str = ""
    lastname: str = ""
    document: str = ""
    birthdate: str = ""
    number: str = ""
    error: str = ""  # Added error field with default empty string
    batch_id: str = ""  # Added batch_id field for batch identification
    count: int = 0  # Added count field for batch processing
    bets: list = None  # Added bets field for batch processing

    def __post_init__(self):
        if self.bets is None:
            self.bets = []

@dataclass
class Message:
    header: Header
    body: Body

def serialize(header: Header, body: Body) -> bytes:
    # First, serialize the body
    body_dict = {
        "Agency": body.agency,
    }
    
    # Add optional fields only if they're not empty
    if body.firstname:
        body_dict["Firstname"] = body.firstname
    if body.lastname:
        body_dict["Lastname"] = body.lastname
    if body.document:
        body_dict["Document"] = body.document
    if body.birthdate:
        body_dict["Birthdate"] = body.birthdate
    if body.number:
        body_dict["Number"] = body.number
    if body.error:
        body_dict["Error"] = body.error
    if body.batch_id:
        body_dict["BatchID"] = body.batch_id
    if body.count > 0:
        body_dict["Count"] = body.count
    if body.bets:
        # Convert each bet to a dictionary
        bets_dict = []
        for bet in body.bets:
            bet_dict = {
                "Document": bet.document,
                "Number": bet.number,
                "Firstname": bet.first_name,
                "Lastname": bet.last_name,
                "Birthdate": bet.birthdate
            }
            bets_dict.append(bet_dict)
        body_dict["Bets"] = bets_dict
        
    body_data = json.dumps(body_dict).encode("utf-8")
    
    # Calculate total message length and update header
    body_len = len(body_data)
    header.length = body_len
    
    # Create header bytes
    header_bytes = struct.pack(">IB", header.length, header.action)
    
    # Concatenate header and body bytes
    return header_bytes + body_data

def deserialize_header(header_data: bytes) -> Header:
    # Parse the binary header
    length, action = struct.unpack(">IB", header_data)
    return Header(length=length, action=action)

def deserialize_body(body_data: bytes) -> dict:
    # Parse the JSON body
    return json.loads(body_data.decode("utf-8"))

def send(conn: socket.socket, message: bytes):
    # Use sendall to ensure all data is sent
    conn.sendall(message)

def receive(conn: socket.socket) -> tuple:
    """
    Receive a message using two-phase approach:
    1. Read fixed-length header
    2. Use Length field from header to read body
    
    Returns a tuple of (Header, Body dict) or None if connection is closed
    """
    # Read the fixed-size header first
    header_data = bytearray(HEADER_SIZE)
    bytes_received = 0
    
    # Keep reading until we have the full header
    while bytes_received < HEADER_SIZE:
        try:
            chunk = conn.recv(HEADER_SIZE - bytes_received)
            if not chunk:  # Connection closed
                return None
            header_data[bytes_received:bytes_received+len(chunk)] = chunk
            bytes_received += len(chunk)
        except socket.timeout:
            # If we time out and got no data at all, treat as closed connection
            if bytes_received == 0:
                return None
            # Otherwise, continue trying to receive
            continue
    
    # Parse the header
    header = deserialize_header(header_data)
    
    # Sanity check for unreasonably large bodies
    if header.length > 10_000_000:  # 10MB
        raise ValueError(f"Message body too large: {header.length} bytes")
    
    # Now read exactly header.length bytes for the body
    body_data = bytearray(header.length)
    bytes_received = 0
    
    # Keep reading until we have the full body
    while bytes_received < header.length:
        try:
            chunk = conn.recv(header.length - bytes_received)
            if not chunk:  # Connection closed
                return None
            body_data[bytes_received:bytes_received+len(chunk)] = chunk
            bytes_received += len(chunk)
        except socket.timeout:
            # If we timeout during body, continue trying
            continue
    
    # Parse the body
    body_dict = deserialize_body(body_data)
    
    return (header, body_dict)
