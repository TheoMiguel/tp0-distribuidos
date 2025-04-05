import signal
import socket
import logging
import json
import time
import threading
import queue
from threading import Lock, Thread
from common.protocol import receive, serialize, send
from common.protocol import Header, Body, ConnectionClosedCleanlyError
from common.protocol import ACTION_BATCH_BET, ACTION_ERROR, ACTION_BATCH_CONFIRM, ACTION_BATCH_ERROR, ACTION_LOTTERY_NOTIFY, ACTION_LOTTERY_QUERY, ACTION_LOTTERY_RESULT, ACTION_LOTTERY_PENDING
from common.utils import Bet, store_bets, load_bets, has_won


class Server:
    def __init__(self, port, listen_backlog, expected_agencies):
        # Initialize server socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)
        self._running = True

        # Track agencies that have completed sending bets
        self._completed_agencies = set()
        self._lottery_drawn = False
        self._total_expected_agencies = expected_agencies

        # Thread synchronization
        self._lock = Lock()  # For synchronizing access to shared data
        self._threads = []   # List to keep track of client threads

        # Queue for storing bets to be processed in a thread-safe manner
        self._bet_queue = queue.Queue()

        # Start the bet persistence thread
        self._persistence_thread = Thread(target=self.__persistence_worker)
        self._persistence_thread.daemon = True
        self._persistence_thread.start()

    def run(self):
        """
        Server main loop that accepts connections and spawns threads for each client
        """
        signal.signal(signal.SIGTERM, self.__handle_sigterm)

        try:
            logging.info('action: server_start | result: success')

            # Main server loop
            while self._running:
                try:
                    # Accept new connections (blocking)
                    client_sock, addr = self._server_socket.accept()
                    logging.info(f'action: accept_connection | result: success | ip: {addr[0]}')

                    # Create and start a new thread for this client
                    client_thread = Thread(target=self.__handle_client, args=(client_sock, addr))
                    client_thread.daemon = True
                    client_thread.start()

                    # Keep track of the thread
                    self._threads.append(client_thread)

                except Exception as e:
                    # Check if the exception occurred because the socket was closed during shutdown
                    if not self._running:
                        logging.info(f"action: server_shutdown | result: success | reason: Server socket closed.")
                        break  # Exit the loop gracefully on shutdown
                    else:
                        # Log other unexpected errors during accept
                        logging.error(f"action: accept_connection | result: fail | error: {str(e)}")

        finally:
            # Clean up when server stops
            if self._server_socket:
                self._server_socket.close()
                logging.info('action: close_server_socket | result: success')

            # Wait for all threads to complete (with timeout)
            for thread in self._threads:
                thread.join(timeout=1.0)

    def __persistence_worker(self):
        """
        Worker thread that handles persisting bets to storage in a thread-safe way
        """
        while self._running:
            try:
                # Get batch of bets from queue (with timeout to check _running periodically)
                try:
                    bets_batch = self._bet_queue.get(timeout=1.0)
                except queue.Empty:
                    continue

                # Process and store the bets
                if bets_batch:
                    store_bets(bets_batch)
                    logging.info(f'action: persist_bets | result: success | batch_size: {len(bets_batch)}')

                # Mark task as done
                self._bet_queue.task_done()

            except Exception as e:
                logging.error(f"action: persistence_worker | result: fail | error: {str(e)}")

    def __handle_client(self, client_sock, addr):
        """
        Handle client connection in a separate thread
        """
        try:
            while self._running:
                try:
                    # Wrap receive() in a try-except block to handle potential exceptions
                    try:
                        message_data = receive(client_sock)
                    except ConnectionClosedCleanlyError as e:
                        # Handle clean connection closure
                        logging.info(f"action: handle_client | result: success | addr: {addr} | reason: Client closed connection cleanly.")
                        break # Exit the loop for this client
                    except (ConnectionAbortedError, ValueError) as e:
                        # Handle actual connection errors or invalid message errors from receive()
                        logging.warning(f"action: handle_client | result: fail | addr: {addr} | error: {e}")
                        break # Exit the loop for this client

                    # No need to check if message_data is None anymore
                    # Process the message
                    header, body = message_data 
                    action = header.action

                    if action == ACTION_BATCH_BET:
                        # Handle batch of bets
                        self.__handle_batch_bet(client_sock, header, body)
                    elif action == ACTION_LOTTERY_NOTIFY:
                        # Handle notification that client has sent all bets
                        self.__handle_lottery_notification(client_sock, header, body)
                    elif action == ACTION_LOTTERY_QUERY:
                        # Handle query for winners from client's agency
                        self.__handle_winners_query(client_sock, header, body)
                    else:
                        logging.warning(f"action: receive_message | result: fail | unexpected action type: {action} | addr: {addr}")
                        # Send error response for invalid action
                        response_header = Header(length=0, action=ACTION_ERROR)
                        response_body = Body(
                            agency=body.agency if body else "unknown", # Use body object's agency if available
                            error=f"Unexpected action type: {action}"
                        )
                        response_data = serialize(response_header, response_body)
                        send(client_sock, response_data)

                except ConnectionResetError:
                    logging.warning(f"action: handle_client | result: connection_reset | addr: {addr}")
                    break
                except Exception as e:
                    # Catch other potential exceptions during message processing
                    logging.error(f"action: handle_client | result: fail | error: {str(e)}")
                    break

        finally:
            # Clean up
            try:
                client_sock.close()
            except:
                pass # Ignore errors on close, might already be closed
            logging.info(f"action: client_done | result: success | addr: {addr}")

    def __handle_sigterm(self, signum, frame):
        logging.info("action: sigterm_received | result: in_progress")
        self._running = False
        # Force the server socket to close to unblock the accept() call
        try:
            # Check if the socket exists and is not already closed
            if self._server_socket and self._server_socket.fileno() != -1:
                 self._server_socket.close()
                 logging.info("action: sigterm_handler | result: success | message: Server socket closed to interrupt accept.")
        except socket.error as e:
            # Socket might already be closed, which is fine
            # 9 is EBADF (Bad file descriptor)
            if hasattr(e, 'errno') and e.errno != 9:
                logging.error(f"action: sigterm_handler | result: fail | error: Error closing server socket: {e}")
            # else: ignore EBADF, socket was already closed
        except Exception as e:
            logging.error(f"action: sigterm_handler | result: fail | error: Unexpected error closing socket: {e}")

    def __handle_batch_bet(self, client_sock, header, body: Body):
        """Handle a batch of bets message"""
        agency = "unknown"
        batch_id = "unknown-batch"
        try:
            agency = body.agency
            batch_id = body.batch_id
            bets_data = body.bets # Access the list of bet objects

            logging.info(f"action: receive_batch | result: success | client_id: {agency} | batch_id: {batch_id} | cantidad: {len(bets_data)}")

            processed_bets = [] # List to hold common.utils.Bet objects
            error_found_in_batch = False

            for bet_info in bets_data: # Iterate over BetInfo objects received
                try:
                    # Create common.utils.Bet object for storage/logic
                    # Assuming agency needs to be int for Bet class
                    bet = Bet(
                        agency=int(agency),
                        first_name=bet_info['first_name'],
                        last_name=bet_info['last_name'],
                        document=bet_info['document'],
                        birthdate=bet_info['birthdate'],
                        number=bet_info['number']
                     )
                    processed_bets.append(bet)

                except ValueError as e: # Handle specific conversion errors like int(agency)
                    logging.error(f"action: process_bet_in_batch | result: fail | client_id: {agency} | batch_id: {batch_id} | bet_doc: {bet_info.get('document', 'unknown')} | error: Invalid data format: {e}")
                    error_found_in_batch = True
                except Exception as e:
                    # Log error processing *this specific bet* in the batch
                    logging.error(f"action: process_bet_in_batch | result: fail | client_id: {agency} | batch_id: {batch_id} | bet_doc: {bet_info.get('document', 'unknown')} | error: {str(e)}")
                    error_found_in_batch = True
                    # Continue processing other bets in the batch

            # Decide response based on whether any errors occurred during bet processing
            if not error_found_in_batch:
                # Add successfully processed bets to persistent queue
                if processed_bets:
                    self._bet_queue.put(processed_bets)
                    logging.info(f"action: apuesta_recibida | result: success | client_id: {agency} | batch_id: {batch_id} | cantidad: {len(processed_bets)}")
                else:
                     # This case (no errors but no bets processed) might indicate an empty batch received
                     logging.info(f"action: apuesta_recibida | result: success | client_id: {agency} | batch_id: {batch_id} | cantidad: 0")


                # Create success response
                response_header = Header(length=0, action=ACTION_BATCH_CONFIRM)
                response_body = Body( # Create Body object for response
                    agency=agency,
                    batch_id=batch_id,
                    count=len(processed_bets) # Report count of successfully processed bets
                )
                log_msg = f"action: send_batch_response | result: in_progress | message_type: confirmation | client_id: {agency} | batch_id: {batch_id}"
                response_action = ACTION_BATCH_CONFIRM

            else:
                 # Log error summary for the batch
                logging.warning(f"action: apuesta_recibida | result: fail | client_id: {agency} | batch_id: {batch_id} | cantidad_procesada: {len(processed_bets)} | errors_encontrados: {len(bets_data) - len(processed_bets)}")

                # Create error response
                response_header = Header(length=0, action=ACTION_BATCH_ERROR)
                response_body = Body(
                    agency=agency,
                    batch_id=batch_id,
                    count=len(processed_bets), # Report how many were processed before error
                    error="Error processing one or more bets in the batch"
                )
                log_msg = f"action: send_batch_response | result: in_progress | message_type: error | client_id: {agency} | batch_id: {batch_id} | reason: {response_body.error}"
                response_action = ACTION_BATCH_ERROR

            # Serialize and send response outside the loop
            logging.info(log_msg)
            response_data = serialize(response_header, response_body)
            send(client_sock, response_data)

            # Log final status of sending the response
            if response_action == ACTION_BATCH_CONFIRM:
                logging.info(f"action: send_batch_response | result: success | message_type: confirmation | client_id: {response_body.agency} | batch_id: {response_body.batch_id}")
            else:
                logging.info(f"action: send_batch_response | result: success | message_type: error | client_id: {response_body.agency} | batch_id: {response_body.batch_id}")

        except Exception as e:
            # Handle general errors during batch handling (e.g., reading fields from body, queue errors)
            logging.exception(f"action: process_batch | result: fail | client_id: {agency} | batch_id: {batch_id} | error: {str(e)}")
            try:
                # Try to send a generic error response
                response_header = Header(length=0, action=ACTION_BATCH_ERROR)
                # Use agency/batch_id from original body if available and assignment succeeded
                response_body = Body(
                    agency=agency,
                    batch_id=batch_id,
                    error=f"General error processing batch: {str(e)}"
                )
                logging.info(f"action: send_batch_response | result: in_progress | message_type: error | client_id: {agency} | batch_id: {batch_id}")
                response_data = serialize(response_header, response_body)
                send(client_sock, response_data)
                logging.info(f"action: send_batch_response | result: success | message_type: error | client_id: {agency} | batch_id: {batch_id}")
            except Exception as send_err:
                 logging.error(f"action: send_batch_response | result: fail | client_id: {agency} | batch_id: {batch_id} | error: Failed to send error response: {send_err}")

    def __handle_lottery_notification(self, client_sock, header, body: Body):
        """Handle notification that an agency has finished sending all bets"""
        agency = "unknown"
        try:
            agency = body.agency # Get agency from Body object

            logging.info(f"action: agency_completion | result: in_progress | agency: {agency}")

            # Add this agency to the completed set (with thread synchronization)
            with self._lock:
                initial_completed_count = len(self._completed_agencies)
                self._completed_agencies.add(agency)
                current_completed_count = len(self._completed_agencies)
                log_suffix = f" | current_completed: {current_completed_count}/{self._total_expected_agencies}"

                logging.info(f"action: agency_completion | result: success | agency: {agency}{log_suffix}")

                # Check if all expected agencies have completed AND lottery hasn't been drawn yet
                if current_completed_count >= self._total_expected_agencies and not self._lottery_drawn:
                    # All agencies have reported completion, draw the lottery
                    logging.info(f"action: sorteo | result: success | completed_agencies: {current_completed_count}")
                    # --- Trigger Lottery Draw Logic Here ---
                    # Example: You might call another method or set an event
                    # For simplicity, just setting the flag for now.
                    self._lottery_drawn = True
                    logging.info("action: sorteo | result: success") # Or 'finished' if it's synchronous

            # Send confirmation back to client
            response_header = Header(length=0, action=ACTION_BATCH_CONFIRM)
            response_body = Body( # Create Body object for response
                agency=agency,
                message="Notification received"
            )

            # Serialize and send response
            response_data = serialize(response_header, response_body)
            send(client_sock, response_data)

            logging.info(f"action: notify_completion_response | result: success | agency: {agency}")

        except Exception as e:
            logging.exception(f"action: handle_lottery_notification | result: fail | agency: {agency} | error: {str(e)}")
            try:
                # Try to send an error response
                response_header = Header(length=0, action=ACTION_ERROR)
                response_body = Body(
                    agency=agency, # Use agency from original body if available
                    error=f"Error handling notification: {str(e)}"
                )
                response_data = serialize(response_header, response_body)
                send(client_sock, response_data)
            except Exception as send_err:
                 logging.error(f"action: notify_completion_response | result: fail | agency: {agency} | error: Failed to send error response: {send_err}")

    def __handle_winners_query(self, client_sock, header, body: Body):
        """Handle query for winners from a specific agency"""
        agency = "unknown"
        try:
            agency = body.agency # Get agency from Body object

            logging.info(f"action: winners_query | result: in_progress | agency: {agency}")

            # Check lottery status and find winners (protected by lock)
            winners = []
            response_header = None
            response_body = None

            with self._lock:
                lottery_drawn = self._lottery_drawn
                if not lottery_drawn:
                    agencies_waiting = self._total_expected_agencies - len(self._completed_agencies)
                    agencies_waiting = max(0, agencies_waiting) # Ensure non-negative

                    # Lottery not drawn yet, send pending status
                    response_header = Header(length=0, action=ACTION_LOTTERY_PENDING)
                    response_body = Body( # Create Body object for response
                        agency=agency,
                        # 'error' field might be misleading, using 'message'
                        error="", # Keep error empty for PENDING status
                        message=f"Waiting for {agencies_waiting} more agencies",
                        count=agencies_waiting # Pass count (agencies waiting) as int
                    )
                    logging.info(f"action: winners_query | result: in_progress | agency: {agency} | waiting_for: {agencies_waiting}")

                else:
                     # Lottery has been drawn, find winners for this agency
                    try:
                        # Convert agency string from protocol to int for comparison with bet.agency
                        agency_id = int(agency)

                        # Load all bets and check which ones are winners from this agency
                        # Note: load_bets() should ideally handle potential file errors
                        for bet in load_bets(): # Assumes load_bets yields common.utils.Bet objects
                            if bet.agency == agency_id and has_won(bet):
                                # Add the document (DNI) to the winners list
                                winners.append(bet.document)

                        logging.info(f"action: find_winners | result: success | agency: {agency} | winner_count: {len(winners)}")

                        # Send winners back to client
                        response_header = Header(length=0, action=ACTION_LOTTERY_RESULT)
                        response_body = Body( # Create Body object for response
                            agency=agency,
                            winners=winners # Pass list of winner DNI strings
                        )
                        logging.info(f"action: winners_query | result: success | agency: {agency} | winner_count: {len(winners)}")

                    except ValueError: # Error converting agency to int
                         logging.error(f"action: find_winners | result: fail | agency: {agency} | error: Invalid agency ID format")
                         response_header = Header(length=0, action=ACTION_ERROR)
                         response_body = Body(agency=agency, error="Invalid agency ID format received")
                    except Exception as e:
                        logging.exception(f"action: find_winners | result: fail | agency: {agency} | error: {str(e)}")
                        # Send generic error if finding winners failed
                        response_header = Header(length=0, action=ACTION_ERROR)
                        response_body = Body(agency=agency, error=f"Error finding winners: {str(e)}")

            # Serialize and send the determined response (PENDING, RESULT, or ERROR)
            if response_header and response_body:
                response_data = serialize(response_header, response_body)
                send(client_sock, response_data)
                logging.info(f"action: winners_query_response | result: success | agency: {agency} | type: {response_header.action}")
            else:
                 # Should not happen if logic above is correct
                 logging.error(f"action: winners_query_response | result: fail | agency: {agency} | error: No response generated")


        except Exception as e:
            logging.exception(f"action: handle_winners_query | result: fail | agency: {agency} | error: {str(e)}")
            try:
                # Try to send a generic error response if something outside the lock failed
                response_header = Header(length=0, action=ACTION_ERROR)
                response_body = Body(
                    agency=agency, # Use agency from original body if available
                    error=f"General error handling winners query: {str(e)}"
                )
                response_data = serialize(response_header, response_body)
                send(client_sock, response_data)
            except Exception as send_err:
                 logging.error(f"action: winners_query_response | result: fail | agency: {agency} | error: Failed to send error response: {send_err}")
