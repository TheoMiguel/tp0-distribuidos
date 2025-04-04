import signal
import socket
import logging
import json
import time
import threading
import queue
from threading import Lock, Thread
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
                    # Receive message from client
                    message_data = receive(client_sock)
                    if not message_data:
                        # Client closed the connection or no data received
                        logging.info(f"action: client_disconnected | result: success | addr: {addr}")
                        break

                    # Process the message
                    header, body_data = message_data
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

                except ConnectionResetError:
                    logging.warning(f"action: handle_client | result: connection_reset | addr: {addr}")
                    break
                except Exception as e:
                    logging.error(f"action: handle_client | result: fail | error: {str(e)}")
                    break

        finally:
            # Clean up
            try:
                client_sock.close()
            except:
                pass
            logging.info(f"action: client_done | result: success | addr: {addr}")

    def __handle_sigterm(self, signum, frame):
        self._running = False
        # Force the server socket to close to unblock the accept() call
        try:
            # Check if the socket exists and is not already closed
            if self._server_socket and self._server_socket.fileno() != -1:
                 self._server_socket.close()
                 logging.info("action: sigterm_handler | result: success | message: Server socket closed to interrupt accept.")
        except Exception as e:
            logging.error(f"action: sigterm_handler | result: fail | error: Error closing server socket: {e}")

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

            # If all bets were processed successfully and no errors were found, queue them for storage
            if not error_found and processed_bets:
                # Add bets to persistent queue instead of directly storing them
                self._bet_queue.put(processed_bets)

                # Log successful processing
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

            # Add this agency to the completed set (with thread synchronization)
            with self._lock:
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

            # Check if lottery has been drawn (with thread synchronization)
            with self._lock:
                lottery_drawn = self._lottery_drawn
                agencies_waiting = self._total_expected_agencies - len(self._completed_agencies)

            if not lottery_drawn:
                # Lottery not drawn yet, send pending status
                response_header = Header(length=0, action=ACTION_LOTTERY_PENDING)

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
                # Note: load_bets already provides an iterator, so we process one bet at a time
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
