package common

import (
	"bufio"
	// "encoding/json" // No longer needed
	"errors"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

// BetInfo struct is now defined in protocol.go
// Removed BetInfo struct definition and its Serialize method from here

// ClientConfig Configuration used by the client
type ClientConfig struct {
	ID            string
	ServerAddress string
	LoopAmount    int // Removed LoopAmount
	LoopPeriod    time.Duration
	BatchAmount   int
	// Bets          []BetInfo // deprecated
}

// Client Entity that encapsulates how
type Client struct {
	config  ClientConfig
	conn    net.Conn
	scanner *bufio.Scanner // scanner to read bets file
}

// NewClient Initializes a new client receiving the configuration
// and the bets file handle as parameters
func NewClient(config ClientConfig, betsFile *os.File) *Client {
	scanner := bufio.NewScanner(betsFile)
	client := &Client{
		config:  config,
		scanner: scanner, // scanner to read bets file
	}
	
	// Ensure BatchAmount is at least 1
	if client.config.BatchAmount < 1 {
		client.config.BatchAmount = 1
	}

	log.Infof("action: client_init | result: success | client_id: %v",
		client.config.ID) 
	
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

// Helper function to parse a line from the CSV into a BetInfo struct
func parseBetLine(line string) (BetInfo, error) {
	fields := strings.Split(line, ",")
	if len(fields) != 5 {
		return BetInfo{}, fmt.Errorf("invalid line format: expected 5 fields, got %d", len(fields))
	}
	// Expected format: Name, Surname, Document, Birthdate, Number
	bet := BetInfo{
		Firstname: fields[0],
		Lastname:  fields[1],
		Document:  fields[2],
		Birthdate: fields[3],
		Number:    fields[4],
	}
	return bet, nil
}

// StartClientLoop Reads bets from the scanner, sends them in batches,
// notifies completion, and queries winners.
func (c *Client) StartClientLoop() {
	// channel to handle OS signals
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT)

	// Close connection when the function returns
	defer func() {
		if c.conn != nil {
			c.conn.Close()
			c.conn = nil
			log.Infof("action: connection_closed | result: success | client_id: %v", c.config.ID)
		}
	}()

	// Create the connection to the server
	err := c.createClientSocket()
	if err != nil {
		log.Errorf("action: create_connection | result: fail | client_id: %v | error: %v",
			c.config.ID,
			err,
		)
		return
	}

	batchCounter := 0
	keepReading := true

	for keepReading {
		select {
		case <-sigs:
			log.Infof("action: shutdown_signal | result: success | client_id: %v", c.config.ID)
			return // Connection closing is handled by defer
		default:
			batchBets := []BetInfo{}
			betsInBatch := 0

			// Read lines for the current batch
			for betsInBatch < c.config.BatchAmount {
				if !c.scanner.Scan() { // Check for EOF or error
					keepReading = false // Stop outer loop
					break             // Stop inner loop
				}

				line := c.scanner.Text()
				bet, err := parseBetLine(line)
				if err != nil {
					log.Warningf("action: parse_bet_line | result: fail | client_id: %v | error: %v | line: %q",
						c.config.ID, err, line)
					continue // Skip invalid line
				}

				batchBets = append(batchBets, bet)
				betsInBatch++
			}

			// Check for scanner errors after trying to read a batch
			if err := c.scanner.Err(); err != nil {
				log.Errorf("action: read_bets_file | result: fail | client_id: %v | error: %v", c.config.ID, err)
				keepReading = false // Stop loop on read error
			}

			// Send the batch if it contains any bets
			if len(batchBets) > 0 {
				batchID := fmt.Sprintf("%s-batch-%d", c.config.ID, batchCounter)
				err := c.sendBets(batchBets, batchID)
				if err != nil {
					log.Errorf("action: send_batch | result: fail | client_id: %v | batch_id: %v | error: %v",
						c.config.ID, batchID, err)
					// Decide if we should stop completely on send error. Let's stop for now.
					keepReading = false
				} else {
					batchCounter++ // increment only on successful send
				}
			}

			// If we are still reading (not EOF or error) and there might be more data, wait.
			if keepReading && len(batchBets) == c.config.BatchAmount {
				time.Sleep(c.config.LoopPeriod)
			}
		}
	} // End of main loop (keepReading == false)

	// If loop finished due to signal, the defer handles cleanup.
	// If loop finished normally (EOF or error), proceed to notify and query.

	// Check if shutdown was initiated by signal before proceeding
	select {
	case <-sigs:
		log.Infof("action: shutdown_signal_post_loop | result: success | client_id: %v", c.config.ID)
		return // Already logged shutdown, defer handles connection close
	default:
		// Continue with notification and query
	}

	log.Infof("action: finished_reading_bets | result: success | client_id: %v | batches_sent: %d", c.config.ID, batchCounter)

	// After all bets have been sent (or attempted), notify the server
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

	// Connection closing is handled by defer

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
	
		// Wait for response
		response := &Message{}
		err = response.Receive(c.conn)
		if err != nil {
			return err
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