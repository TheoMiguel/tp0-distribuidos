// Package common defines the protocol for the client-server communication.
// It is used to serialize and deserialize the messages exchanged between the client and the server.
// It is used to define the actions that can be performed by the client and the server.
// It is used to define the types of the messages exchanged between the client and the server.
// Message structure: [ Header ][ Body ]
// Header: [ Length ][ Action ] (uint32, uint8)
// Binary Body: Fields serialized sequentially based on Action. Strings are [Length (uint32)][UTF-8 Bytes].

package common

import (
	"bytes" // Used for buffer operations
	"encoding/binary"

	// "encoding/json" // No longer needed
	"errors"
	"fmt" // For error formatting
	"io"
	"net"
	// "strconv" // No longer needed
)

// Fixed size header: Length (4 bytes) + Action (1 byte)
const HEADER_SIZE = 5

// Action types (ensure these match server/common/protocol.py)
const (
	ACTION_BATCH_BET     = 4
	ACTION_ERROR         = 3
	ACTION_BATCH_CONFIRM = 5
	ACTION_BATCH_ERROR   = 6
	ACTION_LOTTERY_NOTIFY = 7   // Client notifies server that all bets are sent
	ACTION_LOTTERY_QUERY  = 8   // Client queries server for winners from its agency
	ACTION_LOTTERY_RESULT = 9   // Server responds with winners from an agency
	ACTION_LOTTERY_PENDING = 10  // Lottery hasn't been drawn yet, still waiting for other agencies
)

type Message struct {
	Header Header
	Body   Body
}

type Header struct {
	Length uint32
	Action uint8
}

type Body struct {
	Agency    string
	Firstname string // Primarily used within Bets for ACTION_BATCH_BET
	Lastname  string // Primarily used within Bets for ACTION_BATCH_BET
	Document  string // Primarily used within Bets for ACTION_BATCH_BET
	Birthdate string // Primarily used within Bets for ACTION_BATCH_BET
	Number    string // Primarily used within Bets for ACTION_BATCH_BET
	Error     string // Used in ACTION_ERROR, ACTION_BATCH_ERROR
	BatchID   string // Used in ACTION_BATCH_BET, ACTION_BATCH_CONFIRM, ACTION_BATCH_ERROR
	Bets      []BetInfo // Used in ACTION_BATCH_BET (Client -> Server)
	Count     int    // Used in ACTION_BATCH_CONFIRM, ACTION_BATCH_ERROR, ACTION_LOTTERY_PENDING (Server -> Client)
	Winners   []string // Used in ACTION_LOTTERY_RESULT (Server -> Client)
	Message   string // Used in ACTION_LOTTERY_PENDING (Server -> Client)
}

// BetInfo needs to contain fields sent in ACTION_BATCH_BET
// Ensure this struct is defined appropriately elsewhere or here
// Example definition:
type BetInfo struct {
	Document  string
	Number    string
	Firstname string
	Lastname  string
	Birthdate string
}

// --- Helper functions for binary serialization --- 

// writeString serializes a string with a uint32 length prefix.
func writeString(buf *bytes.Buffer, s string) error {
	sBytes := []byte(s)
	lenBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBytes, uint32(len(sBytes)))
	_, err := buf.Write(lenBytes)
	if err != nil {
		return fmt.Errorf("failed to write string length: %w", err)
	}
	_, err = buf.Write(sBytes)
	if err != nil {
		return fmt.Errorf("failed to write string data: %w", err)
	}
	return nil
}

// readString deserializes a string with a uint32 length prefix from a reader.
func readString(r io.Reader) (string, error) {
	lenBytes := make([]byte, 4)
	_, err := io.ReadFull(r, lenBytes)
	if err != nil {
		return "", fmt.Errorf("failed to read string length: %w", err)
	}
	length := binary.BigEndian.Uint32(lenBytes)

	// Basic sanity check for potentially huge strings
	if length > 1024*1024 { // 1MB limit for a single string
		return "", fmt.Errorf("string length %d exceeds limit", length)
	}
	if length == 0 {
		return "", nil
	}

	sBytes := make([]byte, length)
	_, err = io.ReadFull(r, sBytes)
	if err != nil {
		return "", fmt.Errorf("failed to read string data (expected %d bytes): %w", length, err)
	}
	return string(sBytes), nil
}

// writeInt32 serializes an int as uint32.
func writeInt32(buf *bytes.Buffer, val int) error {
	bytes := make([]byte, 4)
	binary.BigEndian.PutUint32(bytes, uint32(val))
	_, err := buf.Write(bytes)
	return err
}

// readInt32 deserializes a uint32 into an int.
func readInt32(r io.Reader) (int, error) {
	bytes := make([]byte, 4)
	_, err := io.ReadFull(r, bytes)
	if err != nil {
		return 0, err
	}
	val := binary.BigEndian.Uint32(bytes)
	return int(val), nil
}

// --- Serialize method --- 

func (m *Message) Serialize() ([]byte, error) {
	bodyBuf := new(bytes.Buffer)
	var err error

	// Serialize body based on action type
	switch m.Header.Action {
	case ACTION_BATCH_BET:
		// Order: Agency, BatchID, NumBets, [Bets...]
		// Bet Order: Document, Number, Firstname, Lastname, Birthdate
		err = writeString(bodyBuf, m.Body.Agency)
		if err == nil { err = writeString(bodyBuf, m.Body.BatchID) }
		if err == nil { err = writeInt32(bodyBuf, len(m.Body.Bets)) }
		if err == nil {
			for _, bet := range m.Body.Bets {
				err = writeString(bodyBuf, bet.Document)
				if err == nil { err = writeString(bodyBuf, bet.Number) }
				if err == nil { err = writeString(bodyBuf, bet.Firstname) }
				if err == nil { err = writeString(bodyBuf, bet.Lastname) }
				if err == nil { err = writeString(bodyBuf, bet.Birthdate) }
				if err != nil { break } // Stop on first bet serialization error
			}
		}

	case ACTION_ERROR: // Server -> Client, but include for completeness/testing
		// Order: Agency, Error
		err = writeString(bodyBuf, m.Body.Agency)
		if err == nil { err = writeString(bodyBuf, m.Body.Error) }

	case ACTION_BATCH_CONFIRM: // Server -> Client
		// Order: Agency, BatchID, Count
		err = writeString(bodyBuf, m.Body.Agency)
		if err == nil { err = writeString(bodyBuf, m.Body.BatchID) }
		if err == nil { err = writeInt32(bodyBuf, m.Body.Count) }

	case ACTION_BATCH_ERROR: // Server -> Client
		// Order: Agency, BatchID, Count, Error
		err = writeString(bodyBuf, m.Body.Agency)
		if err == nil { err = writeString(bodyBuf, m.Body.BatchID) }
		if err == nil { err = writeInt32(bodyBuf, m.Body.Count) }
		if err == nil { err = writeString(bodyBuf, m.Body.Error) }

	case ACTION_LOTTERY_NOTIFY:
		// Order: Agency
		err = writeString(bodyBuf, m.Body.Agency)

	case ACTION_LOTTERY_QUERY:
		// Order: Agency
		err = writeString(bodyBuf, m.Body.Agency)

	case ACTION_LOTTERY_RESULT: // Server -> Client
		// Order: Agency, NumWinners, [Winners...]
		err = writeString(bodyBuf, m.Body.Agency)
		if err == nil { err = writeInt32(bodyBuf, len(m.Body.Winners)) }
		if err == nil {
			for _, winner := range m.Body.Winners {
				err = writeString(bodyBuf, winner)
				if err != nil { break } // Stop on first winner serialization error
			}
		}
	
	case ACTION_LOTTERY_PENDING: // Server -> Client
		// Order: Agency, Count, Message
		err = writeString(bodyBuf, m.Body.Agency)
		if err == nil { err = writeInt32(bodyBuf, m.Body.Count) }
		if err == nil { err = writeString(bodyBuf, m.Body.Message) }
	
	default:
		err = fmt.Errorf("unsupported action type for serialization: %d", m.Header.Action)
	}

	if err != nil {
		return nil, fmt.Errorf("error serializing body for action %d: %w", m.Header.Action, err)
	}

	// Get body bytes and length
	bodyData := bodyBuf.Bytes()
	bodyLen := len(bodyData)
	m.Header.Length = uint32(bodyLen)

	// Create the result buffer with header + body
	result := make([]byte, HEADER_SIZE+bodyLen)

	// Write header
	binary.BigEndian.PutUint32(result[0:4], m.Header.Length)
	result[4] = m.Header.Action

	// Copy body data
	copy(result[HEADER_SIZE:], bodyData)

	return result, nil
}

// --- Deserialize method --- 

func (m *Message) Deserialize(headerData []byte, bodyData []byte) error {
	// Parse header
	m.Header.Length = binary.BigEndian.Uint32(headerData[0:4])
	m.Header.Action = headerData[4]

	// Check if body length matches header
	if m.Header.Length != uint32(len(bodyData)) {
		return fmt.Errorf("header length (%d) does not match actual body length (%d)", m.Header.Length, len(bodyData))
	}

	bodyReader := bytes.NewReader(bodyData)
	var err error
	body := Body{} // Create new Body to populate

	// Deserialize body based on action type (primarily for messages RECEIVED from server)
	switch m.Header.Action {
	case ACTION_BATCH_BET: // Client -> Server, usually not deserialized by client, but added for completeness
		body.Agency, err = readString(bodyReader)
		if err == nil { body.BatchID, err = readString(bodyReader) }
		var numBets int
		if err == nil { numBets, err = readInt32(bodyReader) }
		if err == nil && numBets > 0 {
			body.Bets = make([]BetInfo, 0, numBets)
			for i := 0; i < numBets; i++ {
				var bet BetInfo
				bet.Document, err = readString(bodyReader)
				if err == nil { bet.Number, err = readString(bodyReader) }
				if err == nil { bet.Firstname, err = readString(bodyReader) }
				if err == nil { bet.Lastname, err = readString(bodyReader) }
				if err == nil { bet.Birthdate, err = readString(bodyReader) }
				if err != nil { break }
				body.Bets = append(body.Bets, bet)
			}
		}

	case ACTION_ERROR:
		body.Agency, err = readString(bodyReader)
		if err == nil { body.Error, err = readString(bodyReader) }

	case ACTION_BATCH_CONFIRM:
		body.Agency, err = readString(bodyReader)
		if err == nil { body.BatchID, err = readString(bodyReader) }
		if err == nil { body.Count, err = readInt32(bodyReader) }

	case ACTION_BATCH_ERROR:
		body.Agency, err = readString(bodyReader)
		if err == nil { body.BatchID, err = readString(bodyReader) }
		if err == nil { body.Count, err = readInt32(bodyReader) }
		if err == nil { body.Error, err = readString(bodyReader) }

	case ACTION_LOTTERY_NOTIFY: // Client -> Server, not usually deserialized by client
		body.Agency, err = readString(bodyReader)

	case ACTION_LOTTERY_QUERY: // Client -> Server, not usually deserialized by client
		body.Agency, err = readString(bodyReader)

	case ACTION_LOTTERY_RESULT:
		body.Agency, err = readString(bodyReader)
		var numWinners int
		if err == nil { numWinners, err = readInt32(bodyReader) }
		if err == nil && numWinners > 0 {
			body.Winners = make([]string, 0, numWinners)
			for i := 0; i < numWinners; i++ {
				var winner string
				winner, err = readString(bodyReader)
				if err != nil { break }
				body.Winners = append(body.Winners, winner)
			}
		}

	case ACTION_LOTTERY_PENDING:
		body.Agency, err = readString(bodyReader)
		if err == nil { body.Count, err = readInt32(bodyReader) }
		if err == nil { body.Message, err = readString(bodyReader) }

	default:
		err = fmt.Errorf("unsupported action type for deserialization: %d", m.Header.Action)
	}

	if err != nil {
		return fmt.Errorf("error deserializing body for action %d: %w", m.Header.Action, err)
	}

	// Check if all bytes were consumed
	if bodyReader.Len() > 0 {
		return fmt.Errorf("extra %d bytes remaining in body buffer after deserialization for action %d", bodyReader.Len(), m.Header.Action)
	}

	m.Body = body
	return nil
}

// --- Send and Receive methods --- (Largely unchanged, rely on Serialize/Deserialize)

func (m *Message) Send(conn net.Conn) error {
	if conn == nil {
		return errors.New("connection is nil")
	}
	
	// Serialize the message using the new binary format
	data, err := m.Serialize()
	if err != nil {
		return fmt.Errorf("failed to serialize message: %w", err)
	}

	// Send the entire message
	totalWritten := 0
	dataLen := len(data)
	
	for totalWritten < dataLen {
		written, err := conn.Write(data[totalWritten:])
		if err != nil {
			return fmt.Errorf("failed to write message data: %w", err)
		}
		totalWritten += written
	}

	return nil
}

func (m *Message) Receive(conn net.Conn) error {
	if conn == nil {
		return errors.New("connection is nil")
	}
	
	// Read header first (fixed size)
	headerBuf := make([]byte, HEADER_SIZE)
	_, err := io.ReadFull(conn, headerBuf)
	if err != nil {
		if err == io.EOF {
			return io.EOF // Propagate EOF cleanly
		}
		return fmt.Errorf("failed to read message header: %w", err)
	}
	
	// Extract the length from the header
	bodyLength := binary.BigEndian.Uint32(headerBuf[0:4])

	// Basic sanity check for body length before allocation
	if bodyLength > 10*1024*1024 { // 10MB limit example
		return fmt.Errorf("message body too large: %d bytes", bodyLength)
	}
	
	// Read exactly bodyLength bytes for the body
	bodyBuf := make([]byte, bodyLength)
	_, err = io.ReadFull(conn, bodyBuf)
	if err != nil {
		if err == io.EOF {
			// This means connection closed before full body was received
			return fmt.Errorf("connection closed prematurely while reading body (expected %d bytes): %w", bodyLength, io.ErrUnexpectedEOF)
		}
		return fmt.Errorf("failed to read message body (expected %d bytes): %w", bodyLength, err)
	}
	
	// Deserialize the message using the new binary format
	err = m.Deserialize(headerBuf, bodyBuf)
	if err != nil {
		return fmt.Errorf("failed to deserialize message: %w", err)
	}
	return nil
}

