import signal
import socket
import logging
import json
import time  # Add this import for sleep
import select  # Import select module for non-blocking socket operations
from common.protocol import receive, serialize, send
from common.protocol import Header, Body
from common.protocol import ACTION_BATCH_BET, ACTION_ERROR, ACTION_BATCH_CONFIRM, ACTION_BATCH_ERROR, ACTION_LOTTERY_NOTIFY, ACTION_LOTTERY_QUERY, ACTION_LOTTERY_RESULT, ACTION_LOTTERY_PENDING
from common.utils import Bet, store_bets, load_bets, has_won


class Server:
    def __init__(self, port, listen_backlog, expected_agencies):
        # Initialize server socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)
        self._server_socket.setblocking(False)  # Set server socket to non-blocking mode
        self._running = True
        
        # Track agencies that have completed sending bets
        self._completed_agencies = set()
        self._lottery_drawn = False
        self._total_expected_agencies = expected_agencies
        
        # List to maintain active client connections
        self._active_clients = []
        self._active_client_info = {}  # Store client info (addr, last_active)

    def run(self):
        """
        Server loop that implements round-robin handling of client connections
        
        Accepts new connections non-blockingly and processes one message from
        each connected client in rotation to ensure fair processing.
        """
        signal.signal(signal.SIGTERM, self.__handle_sigterm)

        while self._running:
            # Try to accept new connections without blocking
            self.__try_accept_new_connection()
            
            # Process one message from each active client in rotation
            if self._active_clients:
                # Make a copy of the list as we may remove items during iteration
                current_clients = self._active_clients.copy()
                
                for client_sock in current_clients:
                    # Skip closed sockets
                    if client_sock.fileno() == -1:
                        self._active_clients.remove(client_sock)
                        continue
                        
                    # Process one message from this client
                    client_addr = self._active_client_info.get(client_sock, {}).get('addr', 'unknown')
                    result = self.__process_one_message(client_sock)
                    
                    # If client is done or errored, remove it from active list
                    if result == False:
                        logging.info(f"action: client_done | result: success | addr: {client_addr}")
                        try:
                            client_sock.close()
                        except:
                            pass
                        self._active_clients.remove(client_sock)
                        self._active_client_info.pop(client_sock, None)
            
            # Short sleep to prevent CPU spinning
            time.sleep(0.01)

        # Close all active client connections
        for client_sock in self._active_clients:
            try:
                client_sock.close()
            except:
                pass
                
        if self._server_socket:
            self._server_socket.close()
            logging.info('action: close_server_socket | result: success')

    def __try_accept_new_connection(self):
        """
        Try to accept a new connection without blocking
        """
        try:
            client_sock, addr = self._server_socket.accept()
            logging.info(f'action: accept_connections | result: success | ip: {addr[0]}')
            
            # Set a timeout to maintain responsiveness even if client stalls
            client_sock.settimeout(10)  # 10 second timeout 
            
            # Add to active clients list
            self._active_clients.append(client_sock)
            self._active_client_info[client_sock] = {
                'addr': addr,
                'last_active': time.time()
            }
            
        except BlockingIOError:
            # No connection waiting, continue with existing clients
            pass
        except Exception as e:
            logging.error(f"action: accept_connection | result: fail | error: {str(e)}")

    def __process_one_message(self, client_sock):
        """
        Process one message from the client socket
        
        Returns:
        - True if message was processed successfully
        - False if client has no more messages or errors occurred
        """
        try:
            # Use non-blocking socket mode for checking data availability
            ready = select.select([client_sock], [], [], 0)
            if not ready[0]:
                # No data available yet, return True to keep client in active list
                return True
                
            # Data available, receive and process one message
            message_data = receive(client_sock)
            if not message_data:
                # Client closed the connection or no data received
                logging.info("action: client_disconnected | result: success")
                return False
            
            # message_data is now a tuple of (header, body_data)
            header, body_data = message_data
            
            # Update last active time
            if client_sock in self._active_client_info:
                self._active_client_info[client_sock]['last_active'] = time.time()
            
            # Check the action type
            action = header.action
            
            if action == ACTION_BATCH_BET:
                # Handle batch of bets
                self.__handle_batch_bet(client_sock, header, body_data)
            elif action == ACTION_LOTTERY_NOTIFY:
                # Handle notification that client has sent all bets
                self.__handle_lottery_notification(client_sock, header, body_data)
            elif action == ACTION_LOTTERY_QUERY:
                # Handle query for winners from client's agency
                self.__handle_winners_query(client_sock, header, body_data)
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
            
            return True
                
        except socket.timeout as e:
            logging.warning(f"action: process_one_message | result: timeout | error: {e}")
            return False
        except ConnectionResetError as e:
            logging.warning(f"action: process_one_message | result: connection_reset | error: {e}")
            return False
        except OSError as e:
            logging.error(f"action: process_one_message | result: fail | error: {e}")
            return False
        except json.JSONDecodeError as e:
            logging.error(f"action: process_one_message | result: fail | error: Invalid JSON format: {e}")
            return False
        except Exception as e:
            logging.error(f"action: process_one_message | result: fail | error: Unexpected error: {e}")
            return False
    
    def __handle_sigterm(self, signum, frame):
        self._running = False

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

    def __handle_lottery_notification(self, client_sock, header, body_data):
        """Handle notification that an agency has finished sending all bets"""
        try:
            agency = body_data.get("Agency", "0")
            
            logging.info(f"action: agency_completion | result: success | agency: {agency}")
            
            # Add this agency to the completed set
            self._completed_agencies.add(agency)
            
            # Check if all expected agencies have completed
            if len(self._completed_agencies) >= self._total_expected_agencies and not self._lottery_drawn:
                # All agencies have reported completion, draw the lottery
                self._lottery_drawn = True
                logging.info("action: sorteo | result: success")
            
            # Send confirmation back to client
            response_header = Header(length=0, action=ACTION_BATCH_CONFIRM)
            response_body = Body(
                agency=agency,
                message="Notification received"
            )
            
            # Serialize and send response
            response_data = serialize(response_header, response_body)
            send(client_sock, response_data)
            
            logging.info(f"action: notify_completion_response | result: success | agency: {agency}")
            
        except Exception as e:
            logging.error(f"action: handle_lottery_notification | result: fail | error: {str(e)}")
            response_header = Header(length=0, action=ACTION_ERROR)
            response_body = Body(
                agency=body_data.get("Agency", ""),
                error=str(e)
            )
            response_data = serialize(response_header, response_body)
            send(client_sock, response_data)

    def __handle_winners_query(self, client_sock, header, body_data):
        """Handle query for winners from a specific agency"""
        try:
            agency = body_data.get("Agency", "0")
            
            logging.info(f"action: winners_query | result: in_progress | agency: {agency}")
            
            # Check if lottery has been drawn
            if not self._lottery_drawn:
                # Lottery not drawn yet, send pending status
                response_header = Header(length=0, action=ACTION_LOTTERY_PENDING)
                
                # Calculate how many agencies we're still waiting for
                agencies_waiting = self._total_expected_agencies - len(self._completed_agencies)
                
                response_body = Body(
                    agency=agency,
                    error=f"Lottery not drawn yet, waiting for {agencies_waiting} more agencies to complete",
                    message=f"Waiting for {agencies_waiting} more agencies to complete",
                    count=agencies_waiting
                )
                response_data = serialize(response_header, response_body)
                send(client_sock, response_data)
                
                logging.info(f"action: winners_query | result: in_progress | agency: {agency} | waiting_for: {agencies_waiting}")
                return
            
            # Find winners for this agency
            winners = []
            try:
                # Convert agency to int for comparison with bet.agency
                agency_id = int(agency)
                
                # Load all bets and check which ones are winners from this agency
                for bet in load_bets():
                    if bet.agency == agency_id and has_won(bet):
                        # Add the document (DNI) to the winners list
                        winners.append(bet.document)
            except Exception as e:
                logging.error(f"action: find_winners | result: fail | agency: {agency} | error: {str(e)}")
                # Continue processing even if there's an error, just with an empty winners list
            
            # Send winners back to client
            response_header = Header(length=0, action=ACTION_LOTTERY_RESULT)
            response_body = Body(
                agency=agency,
                winners=winners
            )
            
            # Serialize and send response
            response_data = serialize(response_header, response_body)
            send(client_sock, response_data)
            
            logging.info(f"action: winners_query | result: success | agency: {agency} | winner_count: {len(winners)}")
            
        except Exception as e:
            logging.error(f"action: handle_winners_query | result: fail | error: {str(e)}")
            response_header = Header(length=0, action=ACTION_ERROR)
            response_body = Body(
                agency=body_data.get("Agency", ""),
                error=str(e)
            )
            response_data = serialize(response_header, response_body)
            send(client_sock, response_data)
