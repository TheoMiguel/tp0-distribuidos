package common

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

// BetInfo Represents a single bet
type BetInfo struct {
	Document  string
	Number    string
	Firstname string
	Lastname  string
	Birthdate string
}

// Serialize converts a BetInfo into a byte slice
func (b *BetInfo) Serialize() []byte {
	// Using JSON for simplicity and compatibility with existing code
	betMap := map[string]string{
		"Document":  b.Document,
		"Number":    b.Number,
		"Firstname": b.Firstname,
		"Lastname":  b.Lastname,
		"Birthdate": b.Birthdate,
	}
	
	data, err := json.Marshal(betMap)
	if err != nil {
		// In case of error, return empty byte slice
		return []byte{}
	}
	
	return data
}

// ClientConfig Configuration used by the client
type ClientConfig struct {
	ID            string
	ServerAddress string
	LoopAmount    int
	LoopPeriod    time.Duration
	BatchAmount   int
	Bets          []BetInfo
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
	
	// Ensure BatchAmount is at least 1
	if client.config.BatchAmount < 1 {
		client.config.BatchAmount = 1
	}


	//log config bets
	log.Infof("action: client_init | result: success | client_id: %v | bet_count: %v", 
		client.config.ID, len(client.config.Bets))
	
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

	// Create the connection to the server
	err := c.createClientSocket()
	if err != nil {
		log.Errorf("action: create_connection | result: fail | client_id: %v | error: %v",
			c.config.ID,
			err,
		)
		return
	}
	
	// If there are no bets, we don't need to send anything
	if len(c.config.Bets) == 0 {
		log.Infof("action: no_bets | result: success | client_id: %v", c.config.ID)
		// Still notify the server we are done (with 0 bets)
		err = c.notifyAllBetsSent()
		if err != nil {
			log.Errorf("action: notify_completion | result: fail | client_id: %v | error: %v",
				c.config.ID, err)
		}
		
		// Query for winners
		err = c.queryWinners()
		if err != nil {
			log.Errorf("action: query_winners | result: fail | client_id: %v | error: %v",
				c.config.ID, err)
		}
		
		if c.conn != nil {
			c.conn.Close()
			c.conn = nil
		}
		return
	}

	// New approach: iterate through bets based on both count and byte size
	i := 0
	batchCounter := 0
	batchID := fmt.Sprintf("%s-batch-%d", c.config.ID, batchCounter)
	
	for i < len(c.config.Bets) {
		select {
		case <-sigs:
			if c.conn != nil {
				c.conn.Close()
			}
			log.Infof("action: shutdown_signal | result: success | client_id: %v", c.config.ID)
			return
		default:
			bytes := []byte{}
			totalBytes := 0
			batchBets := []BetInfo{}
			
			for j := 0; j < c.config.BatchAmount && i < len(c.config.Bets) && totalBytes < 8192; j++ {
				bet := c.config.Bets[i]
				betBytes := bet.Serialize()
				bytes = append(bytes, betBytes...)
				totalBytes += len(betBytes)
				batchBets = append(batchBets, bet)
				i++
			}
			
			// Create a batch ID using the client ID and current batch number
			batchID = fmt.Sprintf("%s-batch-%d", c.config.ID, batchCounter)
			batchCounter++
			
			// Send the batch if we have any bets
			if len(batchBets) > 0 {
				err := c.sendBets(batchBets, batchID)
				if err != nil {
					log.Errorf("action: send_batch | result: fail | client_id: %v | batch_id: %v | error: %v",
						c.config.ID, batchID, err)
					break
				}
			}
			
			// Wait between batches if we haven't processed all bets
			if i < len(c.config.Bets) {
				time.Sleep(c.config.LoopPeriod)
			}
		}
	}
	
	// After all bets have been sent, notify the server
	err = c.notifyAllBetsSent()
	if err != nil {
		log.Errorf("action: notify_completion | result: fail | client_id: %v | error: %v",
			c.config.ID, err)
	} else {
		log.Infof("action: notify_completion | result: success | client_id: %v", c.config.ID)
	}
	
	// Query winners after notifying
	err = c.queryWinners()
	if err != nil {
		log.Errorf("action: query_winners | result: fail | client_id: %v | error: %v",
			c.config.ID, err)
	}
	
	// Close the connection after sending all batches
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
	
	log.Infof("action: processing_finished | result: success | client_id: %v", c.config.ID)
}

// sendBets sends a batch of bets to the server and handles the response
func (c *Client) sendBets(bets []BetInfo, batchID string) error {
	// Ensure we have at least one bet
	if len(bets) == 0 {
		return fmt.Errorf("no bets to send")
	}

	message := &Message{
		Header: Header{
			Length: 0, // Length will be calculated in Serialize
			Action: ACTION_BATCH_BET, // Action for batch bet submission
		},
		Body: Body{
			Agency:  c.config.ID,
			BatchID: batchID,
			Bets:    bets,
		},
	}

	// Log the batch being sent
	log.Infof("action: send_batch | result: in_progress | client_id: %v | batch_id: %v | cantidad: %d",
		c.config.ID,
		batchID,
		len(bets),
	)

	// Send the message using the protocol's Send method
	err := message.Send(c.conn)
	if err != nil {
		log.Errorf("action: send_batch | result: fail | client_id: %v | batch_id: %v | error: %v",
			c.config.ID,
			batchID,
			err,
		)
		return err
	}

	// Set a timeout for receiving response
	err = c.conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	if err != nil {
		log.Errorf("action: set_timeout | result: fail | client_id: %v | batch_id: %v | error: %v",
			c.config.ID,
			batchID,
			err,
		)
		return err
	}

	// Create a new message to receive the response
	response := &Message{}
	err = response.Receive(c.conn)
	if err != nil {
		log.Errorf("action: receive_batch_response | result: fail | client_id: %v | batch_id: %v | error: %v",
			c.config.ID,
			batchID,
			err,
		)
		return err
	}

	// Clear the timeout
	err = c.conn.SetReadDeadline(time.Time{})
	if err != nil {
		log.Errorf("action: clear_timeout | result: fail | client_id: %v | batch_id: %v | error: %v",
			c.config.ID,
			batchID,
			err,
		)
		// Not returning error here as we already got our response
	}

	// Handle response based on action type
	switch response.Header.Action {
	case ACTION_BATCH_CONFIRM: // Batch confirmation message
		// Verify that the agency (client ID) matches
		if response.Body.Agency == c.config.ID && response.Body.BatchID == batchID {
			// Log the confirmation with the specified format
			log.Infof("action: batch_enviado | result: success | client_id: %v | batch_id: %v | cantidad: %d",
				c.config.ID,
				batchID,
				response.Body.Count,
			)
			return nil
		} else {
			err := fmt.Errorf("client_id or batch_id mismatch: got %s-%s, expected %s-%s",
				response.Body.Agency, response.Body.BatchID, c.config.ID, batchID)
			log.Errorf("action: confirm_batch | result: fail | client_id: %v | batch_id: %v | error: %v",
				c.config.ID,
				batchID,
				err,
			)
			return err
		}
	case ACTION_BATCH_ERROR: // Batch error message
		err := fmt.Errorf("server reported error: %s", response.Body.Error)
		log.Errorf("action: confirm_batch | result: fail | client_id: %v | batch_id: %v | error: %s",
			c.config.ID,
			batchID,
			response.Body.Error,
		)
		return err
	default:
		err := fmt.Errorf("unexpected response type: %v", response.Header.Action)
		log.Errorf("action: confirm_batch | result: fail | client_id: %v | batch_id: %v | error: %v",
			c.config.ID,
			batchID,
			err,
		)
		return err
	}
}

// notifyAllBetsSent sends a notification to the server that all bets have been sent
func (c *Client) notifyAllBetsSent() error {
	// Ensure connection is established
	if c.conn == nil {
		err := c.createClientSocket()
		if err != nil {
			return err
		}
	}

	// Create notification message
	message := &Message{
		Header: Header{
			Length: 0, // Length will be calculated in Serialize
			Action: ACTION_LOTTERY_NOTIFY,
		},
		Body: Body{
			Agency: c.config.ID,
		},
	}

	// Send the notification
	log.Infof("action: notify_completion | result: in_progress | client_id: %v", c.config.ID)
	err := message.Send(c.conn)
	if err != nil {
		return err
	}

	// Wait for acknowledgement
	response := &Message{}
	err = response.Receive(c.conn)
	if err != nil {
		return err
	}

	// Check if response indicates success
	if response.Header.Action == ACTION_ERROR || response.Header.Action == ACTION_BATCH_ERROR {
		return fmt.Errorf("notification failed: %s", response.Body.Error)
	}

	return nil
}

// queryWinners asks the server for the winners from this agency and processes the response
func (c *Client) queryWinners() error {
	// Maximum number of retries when lottery is pending
	maxRetries := 30
	retryDelay := time.Second * 2 // 2 seconds between retries
	
	for retry := 0; retry < maxRetries; retry++ {
		// Ensure connection is established
		if c.conn == nil {
			err := c.createClientSocket()
			if err != nil {
				return err
			}
		}
	
		// Create query message
		message := &Message{
			Header: Header{
				Length: 0, // Length will be calculated in Serialize
				Action: ACTION_LOTTERY_QUERY,
			},
			Body: Body{
				Agency: c.config.ID,
			},
		}
	
		// Send the query
		if retry == 0 {
			log.Infof("action: query_winners | result: in_progress | client_id: %v", c.config.ID)
		} else {
			log.Infof("action: query_winners | result: in_progress | client_id: %v | retry: %d", c.config.ID, retry)
		}
		
		err := message.Send(c.conn)
		if err != nil {
			return err
		}
	
		// Set a timeout for receiving response
		err = c.conn.SetReadDeadline(time.Now().Add(10 * time.Second))
		if err != nil {
			return err
		}
	
		// Wait for response
		response := &Message{}
		err = response.Receive(c.conn)
		if err != nil {
			return err
		}
	
		// Clear the timeout
		err = c.conn.SetReadDeadline(time.Time{})
		if err != nil {
			// Not returning error here as we already got our response
			log.Errorf("action: clear_timeout | result: fail | client_id: %v | error: %v", c.config.ID, err)
		}
	
		// Check response action type
		switch response.Header.Action {
		case ACTION_LOTTERY_RESULT:
			// Success, process winners
			winnerCount := len(response.Body.Winners)
			log.Infof("action: consulta_ganadores | result: success | cant_ganadores: %d", winnerCount)
			return nil
			
		case ACTION_LOTTERY_PENDING:
			// Lottery not drawn yet, log pending status
			waitingFor := response.Body.Count
			log.Infof("action: query_winners | result: in_progress | client_id: %v | waiting_for: %d agencies", 
				c.config.ID, waitingFor)
			
			// Wait before retrying
			time.Sleep(retryDelay)
			continue
			
		case ACTION_ERROR, ACTION_BATCH_ERROR:
			// Real error
			return fmt.Errorf("query failed: %s", response.Body.Error)
			
		default:
			return fmt.Errorf("unexpected response action: %d", response.Header.Action)
		}
	}
	
	// If we get here, we've exceeded our retry limit
	return fmt.Errorf("exceeded maximum retry attempts waiting for lottery to be drawn")
}