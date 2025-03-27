// Package common defines the protocol for the client-server communication.
// It is used to serialize and deserialize the messages exchanged between the client and the server.
// It is used to define the actions that can be performed by the client and the server.
// It is used to define the types of the messages exchanged between the client and the server.
// Message structure: [ Header ][ Body ]
// Header: [ Length ][ Action ] (uint32, uint8)
// Body: [ Document ][ Number ] (string, string)

package common

import (
	"encoding/json"
	"errors"
	"net"
	"strconv"
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
	Firstname string
	Lastname  string
	Document  string
	Birthdate string
	Number    string
}

func (m *Message) Serialize() ([]byte, error) {
	// Create a JSON object with the Header and Body fields
	messageJson := map[string]interface{}{
		"Header": m.Header,
		"Body":   m.Body,
	}
	
	// Marshal the entire message as a single JSON object
	serialized, err := json.Marshal(messageJson)
	if err != nil {
		return nil, err
	}
	
	return serialized, nil
}

func (m *Message) Deserialize(data []byte) error {
	// Create a temporary structure with a more flexible intermediate representation
	var rawMessage map[string]json.RawMessage
	err := json.Unmarshal(data, &rawMessage)
	if err != nil {
		return err
	}
	
	// Unmarshal the Header
	var header Header
	if headerData, ok := rawMessage["Header"]; ok {
		err = json.Unmarshal(headerData, &header)
		if err != nil {
			return err
		}
		m.Header = header
	}
	
	// Unmarshal the Body with special handling for Agency field
	if bodyData, ok := rawMessage["Body"]; ok {
		// First parse into a map to handle type conversions
		var bodyMap map[string]interface{}
		err = json.Unmarshal(bodyData, &bodyMap)
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
		
		m.Body = body
	}
	
	return nil
}

func (m *Message) Send(conn net.Conn) error {
	if conn == nil {
		return errors.New("connection is nil")
	}
	
	serialized, err := m.Serialize()
	if err != nil {
		return err
	}

	_, err = conn.Write(serialized)
	if err != nil {
		return err
	}

	return nil
}

func (m *Message) Receive(conn net.Conn) error {
	if conn == nil {
		return errors.New("connection is nil")
	}
	
	buffer := make([]byte, 1024)
	n, err := conn.Read(buffer)
	if err != nil {
		return err
	}

	return m.Deserialize(buffer[:n])
}

