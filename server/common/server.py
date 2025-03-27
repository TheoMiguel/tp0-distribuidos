import signal
import socket
import logging
import json
import time  # Add this import for sleep
from common.protocol import receive, deserialize, serialize, send
from common.protocol import Header, Body
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
            except socket.timeout:
                pass

        if self._server_socket:
            self._server_socket.close()
            logging.info('action: close_server_socket | result: success')


    def __handle_sigterm(self, signum, frame):
        self._running = False

    def __handle_client_connection(self, client_sock):
        """
        Read message from a specific client socket and closes the socket

        If a problem arises in the communication with the client, the
        client socket will also be closed
        """
        try:
            # Use protocol's receive function to avoid short-reads
            data = receive(client_sock)
            if not data:
                return
                
            # Deserialize the received data
            message = deserialize(data)
            
            
            # Extract header and body from the message
            header_data = message.get("Header", {})
            body_data = message.get("Body", {})
            
            # Check if this is a bet message (action type 1)
            action = header_data.get("Action", 0)
            if action != 1:
                logging.warning(f"action: receive_message | result: fail | unexpected action type: {action}")
                return
                
            logging.info(f"action: receive_message | result: success | message_type: bet")
            
            
            bet = Bet(
                agency=body_data.get("Agency", ""),
                first_name=body_data.get("Firstname", ""),
                last_name=body_data.get("Lastname", ""),
                document=body_data.get("Document", ""),
                birthdate=json.loads(body_data.get("Birthdate", "")),
                number=json.loads(body_data.get("Number", ""))
            )
            
            store_bets([bet])
            
            logging.info(f'action: apuesta_almacenada | result: success | dni: {bet.document} | numero: {bet.number}')
            
            # Create response message OK
            response_header = Header(length=0, action=2)  # Action 2 is the confirmation message
            response_body = Body(
                agency=bet.agency,
                firstname=bet.first_name,
                lastname=bet.last_name,
                document=bet.document,
                birthdate=str(bet.birthdate),
                number=bet.number
            )
            
            logging.info(f"action: send_message | result: in_progress | message_type: confirmation")
            
            # Serialize and send using protocol's send function to avoid short-writes
            response_data = serialize(response_header, response_body)
            send(client_sock, response_data)
            
            logging.info(f"action: send_message | result: success | message_type: confirmation | dni: {bet.document} | numero: {bet.number}")
            
            # Add a small delay to ensure the client has time to receive the data before closing
            time.sleep(5)
            
        except OSError as e:
            logging.error(f"action: handle_client_connection | result: fail | error: {e}")
        except json.JSONDecodeError as e:
            logging.error(f"action: handle_client_connection | result: fail | error: Invalid JSON format: {e}")
        finally:
            client_sock.close()

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
