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

@dataclass
class Header:
    length: int
    action: int

@dataclass
class Body:
    agency: str
    firstname: str
    lastname: str
    document: str
    birthdate: str
    number: str
    error: str = ""  # Added error field with default empty string

@dataclass
class Message:
    header: Header
    body: Body

def serialize(header: Header, body: Body) -> bytes:
    # First, serialize the body
    body_dict = {
        "Agency": body.agency,
        "Firstname": body.firstname,
        "Lastname": body.lastname,
        "Document": body.document,
        "Birthdate": body.birthdate,
        "Number": body.number
    }
    
    # Add error field only if it's not empty
    if body.error:
        body_dict["Error"] = body.error
        
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
    
    Returns a tuple of (Header, Body dict)
    """
    # Read the fixed-size header first
    header_data = bytearray(HEADER_SIZE)
    bytes_received = 0
    
    # Keep reading until we have the full header
    while bytes_received < HEADER_SIZE:
        chunk = conn.recv(HEADER_SIZE - bytes_received)
        if not chunk:  # Connection closed
            return None
        header_data[bytes_received:bytes_received+len(chunk)] = chunk
        bytes_received += len(chunk)
    
    # Parse the header
    header = deserialize_header(header_data)
    
    # Now read exactly header.length bytes for the body
    body_data = bytearray(header.length)
    bytes_received = 0
    
    # Keep reading until we have the full body
    while bytes_received < header.length:
        chunk = conn.recv(header.length - bytes_received)
        if not chunk:  # Connection closed
            return None
        body_data[bytes_received:bytes_received+len(chunk)] = chunk
        bytes_received += len(chunk)
    
    # Parse the body
    body_dict = deserialize_body(body_data)
    
    return (header, body_dict)
