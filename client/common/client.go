package common

import (
	"errors"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

// Bet info
type BetInfo struct {
	Document  string
	Number    string
	Firstname string
	Lastname  string
	Birthdate string
}

// ClientConfig Configuration used by the client
type ClientConfig struct {
	ID            string
	ServerAddress string
	LoopAmount    int
	LoopPeriod    time.Duration
	BetInfo       BetInfo
}

// Client Entity that encapsulates how
type Client struct {
	config ClientConfig
	conn   net.Conn
}

// NewClient Initializes a new client receiving the configuration
// as a parameter
func NewClient(config ClientConfig) *Client {
	client := &Client{
		config: config,
	}
	return client
}

// CreateClientSocket Initializes client socket. In case of
// failure, error is printed in stdout/stderr and exit 1
// is returned
func (c *Client) createClientSocket() error {
	tries := 0
	maxTries := 9 // Maximum number of tries to connect to the server. 1 second between each try

	for tries < maxTries {
		conn, err := net.Dial("tcp", c.config.ServerAddress)
		if err != nil {
			// log.Criticalf("action: connect | result: fail | client_id: %v | error: %v", c.config.ID, err)
			// log.Infof("action: connect | result: fail | client_id: %v | error: %v", c.config.ID, err)
			tries++
			time.Sleep(time.Second * 3)
			continue // Continue to the next iteration
		}
		c.conn = conn
		return nil
	}
	err := errors.New("failed to connect to the server")
	log.Criticalf("action: connect | result: fail | client_id: %v | error: %v", c.config.ID, err)
	return err
}

// StartClientLoop Send messages to the client until some time threshold is met
func (c *Client) StartClientLoop() {
	// channel to handle OS signals
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT)

	// There is an autoincremental msgID to identify every message sent
	// Messages if the message amount threshold has not been surpassed
	for msgID := 1; msgID <= c.config.LoopAmount; msgID++ {
		select {
		case <-sigs:
			if c.conn != nil {
				c.conn.Close()
			}
			log.Infof("action: shutdown_signal | result: success | client_id: %v", c.config.ID)
			return
		default:
			// Create the connection the server in every loop iteration
			err := c.createClientSocket()
			if err != nil {
				log.Errorf("action: create_connection | result: fail | client_id: %v | error: %v",
					c.config.ID,
					err,
				)
				// Wait before trying again
				time.Sleep(c.config.LoopPeriod)
				continue
			}

			// Create a new message using the protocol structure
			message := &Message{
				Header: Header{
					Length: 0, // Length will be set by Serialize
					Action: 1, // Action 1 for bet submission
				},
				Body: Body{
					Agency:    c.config.ID,
					Firstname: c.config.BetInfo.Firstname,
					Lastname:  c.config.BetInfo.Lastname,
					Document:  c.config.BetInfo.Document,
					Birthdate: c.config.BetInfo.Birthdate,
					Number:    c.config.BetInfo.Number,
				},
			}

			// Log the bet being sent
			log.Infof("action: send_apuesta | result: in_progress | dni: %s | numero: %s | nombre: %s | apellido: %s | fecha_nacimiento: %s",
				c.config.BetInfo.Document,
				c.config.BetInfo.Number,
				c.config.BetInfo.Firstname,
				c.config.BetInfo.Lastname,
				c.config.BetInfo.Birthdate,
			)

			// Send the message using the protocol's Send method to avoid short-write
			err = message.Send(c.conn)
			if err != nil {
				log.Errorf("action: send_message | result: fail | client_id: %v | error: %v",
					c.config.ID,
					err,
				)
				c.conn.Close()
				// Wait before trying again
				time.Sleep(c.config.LoopPeriod)
				continue
			}

			// Create a new message to receive the response
			response := &Message{}
			err = response.Receive(c.conn)
			c.conn.Close()

			if err != nil {
				log.Errorf("action: receive_message | result: fail | client_id: %v | error: %v",
					c.config.ID,
					err,
				)
				// Wait before trying again
				time.Sleep(c.config.LoopPeriod)
				continue
			}

			// Handle response based on action type
			switch response.Header.Action {
			case 2: // Confirmation message
				// Verify that the agency (client ID) matches
				if response.Body.Agency == c.config.ID {
					// Log the confirmation with the specified format
					log.Infof("action: apuesta_enviada | result: success | dni: %s | numero: %s",
						c.config.BetInfo.Document,
						c.config.BetInfo.Number,
					)
				} else {
					log.Errorf("action: confirm_bet | result: fail | client_id: %v | received_client_id: %v",
						c.config.ID,
						response.Body.Agency,
					)
					// Wait before trying again
					time.Sleep(c.config.LoopPeriod)
					continue
				}
			case 3: // Error message
				log.Errorf("action: confirm_bet | result: fail | client_id: %v | error: %s",
					c.config.ID,
					response.Body.Error,
				)
				// Wait before trying again
				time.Sleep(c.config.LoopPeriod)
				continue
			default:
				log.Errorf("action: confirm_bet | result: fail | client_id: %v | unexpected response type: %v",
					c.config.ID,
					response.Header.Action,
				)
				// Wait before trying again
				time.Sleep(c.config.LoopPeriod)
				continue
			}

			// Also log the full message for debugging
			log.Debugf("action: receive_message | result: success | client_id: %v | msg: %+v",
				c.config.ID,
				response,
			)

			// Wait a time between sending one message and the next one
			time.Sleep(c.config.LoopPeriod)
		}
	}
	log.Infof("action: loop_finished | result: success | client_id: %v", c.config.ID)
}
