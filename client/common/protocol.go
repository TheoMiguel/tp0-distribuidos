// Package common defines the protocol for the client-server communication.
// It is used to serialize and deserialize the messages exchanged between the client and the server.
// It is used to define the actions that can be performed by the client and the server.
// It is used to define the types of the messages exchanged between the client and the server.
// Message structure: [ Header ][ Body ]
// Header: [ Length ][ Action ] (uint32, uint8)
// Body: [ Document ][ Number ] (string, string)

package common

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net"
	"strconv"
)

// Fixed size header: Length (4 bytes) + Action (1 byte)
const HEADER_SIZE = 5

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
	Firstname string
	Lastname  string
	Document  string
	Birthdate string
	Number    string
	Error     string
}

func (m *Message) Serialize() ([]byte, error) {
	// First, serialize the body
	bodyMap := map[string]interface{}{
		"Agency":    m.Body.Agency,
		"Firstname": m.Body.Firstname,
		"Lastname":  m.Body.Lastname,
		"Document":  m.Body.Document,
		"Birthdate": m.Body.Birthdate,
		"Number":    m.Body.Number,
	}
	
	// Add error field only if it's not empty
	if m.Body.Error != "" {
		bodyMap["Error"] = m.Body.Error
	}
	
	bodyData, err := json.Marshal(bodyMap)
	if err != nil {
		return nil, err
	}
	
	// Calculate total message length
	bodyLen := len(bodyData)
	m.Header.Length = uint32(bodyLen)
	
	// Create the result buffer with header + body
	result := make([]byte, HEADER_SIZE+bodyLen)
	
	// Write Length (uint32) in the first 4 bytes
	binary.BigEndian.PutUint32(result[0:4], m.Header.Length)
	
	// Write Action (uint8) in the 5th byte
	result[4] = m.Header.Action
	
	// Copy the body data after the header
	copy(result[HEADER_SIZE:], bodyData)
	
	return result, nil
}

func (m *Message) Deserialize(headerData []byte, bodyData []byte) error {
	// Parse header
	m.Header.Length = binary.BigEndian.Uint32(headerData[0:4])
	m.Header.Action = headerData[4]
	
	// Parse body
	var bodyMap map[string]interface{}
	err := json.Unmarshal(bodyData, &bodyMap)
	if err != nil {
		return err
	}
	
	// Manually build the Body struct with proper type conversions
	var body Body
	
	// Handle Agency - could be number or string
	if agency, ok := bodyMap["Agency"]; ok {
		switch v := agency.(type) {
		case string:
			body.Agency = v
		case float64: // JSON numbers are decoded as float64
			body.Agency = strconv.Itoa(int(v))
		case int:
			body.Agency = strconv.Itoa(v)
		}
	}
	
	// Handle other string fields
	if firstname, ok := bodyMap["Firstname"].(string); ok {
		body.Firstname = firstname
	}
	if lastname, ok := bodyMap["Lastname"].(string); ok {
		body.Lastname = lastname
	}
	if document, ok := bodyMap["Document"].(string); ok {
		body.Document = document
	}
	if birthdate, ok := bodyMap["Birthdate"].(string); ok {
		body.Birthdate = birthdate
	}
	if number, ok := bodyMap["Number"].(string); ok {
		body.Number = number
	}
	// Handle Error field for error messages
	if errorMsg, ok := bodyMap["Error"].(string); ok {
		body.Error = errorMsg
	}
	
	m.Body = body
	return nil
}

func (m *Message) Send(conn net.Conn) error {
	if conn == nil {
		return errors.New("connection is nil")
	}
	
	// Serialize the message
	data, err := m.Serialize()
	if err != nil {
		return err
	}

	// Send the entire message
	totalWritten := 0
	dataLen := len(data)
	
	for totalWritten < dataLen {
		written, err := conn.Write(data[totalWritten:])
		if err != nil {
			return err
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
		return err
	}
	
	// Extract the length from the header
	bodyLength := binary.BigEndian.Uint32(headerBuf[0:4])
	
	// Read exactly bodyLength bytes for the body
	bodyBuf := make([]byte, bodyLength)
	_, err = io.ReadFull(conn, bodyBuf)
	if err != nil {
		return err
	}
	
	// Deserialize the message
	return m.Deserialize(headerBuf, bodyBuf)
}

