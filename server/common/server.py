import signal
import socket
import logging
import json
import time  # Add this import for sleep
from common.protocol import receive, serialize, send
from common.protocol import Header, Body
from common.protocol import ACTION_BATCH_BET, ACTION_ERROR, ACTION_BATCH_CONFIRM, ACTION_BATCH_ERROR
from common.utils import Bet, store_bets


class Server:
    def __init__(self, port, listen_backlog):
        # Initialize server socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)
        self._running = True

    def run(self):
        """
        Dummy Server loop

        Server that accept a new connections and establishes a
        communication with a client. After client with communucation
        finishes, servers starts to accept new connections again
        """

        signal.signal(signal.SIGTERM, self.__handle_sigterm)


        while self._running:
            self._server_socket.settimeout(1)
            try:
                client_sock = self.__accept_new_connection()
                self.__handle_client_connection(client_sock)
                # Close the client socket after handling all messages
                client_sock.close()
            except socket.timeout:
                pass

        if self._server_socket:
            self._server_socket.close()
            logging.info('action: close_server_socket | result: success')


    def __handle_sigterm(self, signum, frame):
        self._running = False

    def __handle_client_connection(self, client_sock):
        """
        Read messages from a specific client socket until the client closes the connection
        
        Processes multiple messages on the same connection until the client closes it
        or until an error occurs.
        """
        # Set a timeout to avoid infinite loops if the client doesn't close properly
        client_sock.settimeout(30)  # 30 second timeout
        
        while True:
            try:
                # Use protocol's receive function to avoid short-reads
                message_data = receive(client_sock)
                if not message_data:
                    # Client closed the connection or no data received
                    logging.info("action: client_disconnected | result: success")
                    break
                    
                # message_data is now a tuple of (header, body_data)
                header, body_data = message_data
                
                # Check the action type
                action = header.action
                
                if action == ACTION_BATCH_BET:
                    # Handle batch of bets
                    self.__handle_batch_bet(client_sock, header, body_data)
                else:
                    logging.warning(f"action: receive_message | result: fail | unexpected action type: {action}")
                    # Send error response for invalid action
                    response_header = Header(length=0, action=ACTION_ERROR)
                    response_body = Body(
                        agency=body_data.get("Agency", ""),
                        error=f"Unexpected action type: {action}"
                    )
                    response_data = serialize(response_header, response_body)
                    send(client_sock, response_data)
                
            except socket.timeout as e:
                logging.warning(f"action: handle_client_connection | result: closed | error: Connection timed out: {e}")
                break
            except OSError as e:
                logging.error(f"action: handle_client_connection | result: fail | error: {e}")
                break
            except json.JSONDecodeError as e:
                logging.error(f"action: handle_client_connection | result: fail | error: Invalid JSON format: {e}")
                break
            except Exception as e:
                logging.error(f"action: handle_client_connection | result: fail | error: Unexpected error: {e}")
                break
    
    def __handle_batch_bet(self, client_sock, header, body_data):
        """Handle a batch of bets message"""
        try:
            agency = body_data.get("Agency", "0")
            batch_id = body_data.get("BatchID", "unknown-batch")
            bets_data = body_data.get("Bets", [])
            
            logging.info(f"action: receive_batch | result: success | client_id: {agency} | batch_id: {batch_id} | cantidad: {len(bets_data)}")
            
            # Process each bet in the batch
            processed_bets = []
            error_found = False
            
            for bet_data in bets_data:
                try:
                    # Extract bet data
                    first_name = bet_data.get("Firstname", "")
                    last_name = bet_data.get("Lastname", "")
                    document = bet_data.get("Document", "")
                    birthdate = bet_data.get("Birthdate", "")
                    number = bet_data.get("Number", "0")
                    
                    # Remove quotation marks if present
                    if isinstance(birthdate, str) and birthdate.startswith('"') and birthdate.endswith('"'):
                        birthdate = birthdate[1:-1]
                    
                    if isinstance(number, str) and number.startswith('"') and number.endswith('"'):
                        number = number[1:-1]
                    
                    # Create bet object
                    bet = Bet(
                        agency=agency,
                        first_name=first_name,
                        last_name=last_name,
                        document=document,
                        birthdate=birthdate,
                        number=number
                    )
                    
                    processed_bets.append(bet)
                    
                except Exception as e:
                    logging.error(f"action: process_bet_in_batch | result: fail | client_id: {agency} | batch_id: {batch_id} | error: {str(e)}")
                    error_found = True
                    break
            
            # If all bets were processed successfully and no errors were found, store them
            if not error_found and processed_bets:
                store_bets(processed_bets)
                
                # Log successful processing as required
                logging.info(f"action: apuesta_recibida | result: success | cantidad: {len(processed_bets)}")
                
                # Create success response
                response_header = Header(length=0, action=ACTION_BATCH_CONFIRM)
                response_body = Body(
                    agency=agency,
                    batch_id=batch_id,
                    count=len(processed_bets)
                )
                
                logging.info(f"action: send_batch_response | result: in_progress | message_type: confirmation | client_id: {agency} | batch_id: {batch_id}")
            else:
                # Log error processing as required
                if processed_bets:
                    logging.info(f"action: apuesta_recibida | result: fail | cantidad: {len(processed_bets)}")
                else:
                    logging.info(f"action: apuesta_recibida | result: fail | cantidad: 0")
                
                # Create error response
                response_header = Header(length=0, action=ACTION_BATCH_ERROR)
                response_body = Body(
                    agency=agency,
                    batch_id=batch_id,
                    count=len(processed_bets),
                    error="Error processing one or more bets in the batch"
                )
                
                logging.info(f"action: send_batch_response | result: in_progress | message_type: error | client_id: {agency} | batch_id: {batch_id}")
        
        except Exception as e:
            # Handle general errors
            logging.error(f"action: process_batch | result: fail | error: {str(e)}")
            response_header = Header(length=0, action=ACTION_BATCH_ERROR)
            response_body = Body(
                agency=body_data.get("Agency", ""),
                batch_id=body_data.get("BatchID", "unknown-batch"),
                error=str(e)
            )
            logging.info(f"action: send_batch_response | result: in_progress | message_type: error")
        
        # Serialize and send response
        response_data = serialize(response_header, response_body)
        send(client_sock, response_data)
        
        if response_header.action == ACTION_BATCH_CONFIRM:
            logging.info(f"action: send_batch_response | result: success | message_type: confirmation | client_id: {response_body.agency} | batch_id: {response_body.batch_id}")
        else:
            logging.info(f"action: send_batch_response | result: success | message_type: error | client_id: {response_body.agency} | batch_id: {response_body.batch_id}")

    def __accept_new_connection(self):
        """
        Accept new connections

        Function blocks until a connection to a client is made.
        Then connection created is printed and returned
        """

        # Connection arrived
        logging.info('action: accept_connections | result: in_progress')
        c, addr = self._server_socket.accept()
        logging.info(f'action: accept_connections | result: success | ip: {addr[0]}')
        return c
