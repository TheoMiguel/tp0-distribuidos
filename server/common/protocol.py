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

@dataclass
class Message:
    header: Header
    body: Body

def serialize(header: Header, body: Body) -> bytes:
    # Create a structure similar to what the Go client expects
    message = {
        "Header": {
            "Length": header.length,
            "Action": header.action
        },
        "Body": {
            "Agency": body.agency,
            "Firstname": body.firstname,
            "Lastname": body.lastname,
            "Document": body.document,
            "Birthdate": body.birthdate,
            "Number": body.number
        }
    }
    return json.dumps(message).encode("utf-8")

def deserialize(data: bytes) -> dict:
    # Parse the message from the client
    message = json.loads(data.decode("utf-8"))
    return message

def send(conn: socket.socket, message: bytes):
    # Use sendall to ensure all data is sent
    conn.sendall(message)

def receive(conn: socket.socket) -> bytes:
    # Read data from the socket (could be improved with length-prefixed messages)
    buffer = bytearray(1024)
    bytes_received = conn.recv_into(buffer)
    if bytes_received == 0:
        return None
    return bytes(buffer[:bytes_received])
