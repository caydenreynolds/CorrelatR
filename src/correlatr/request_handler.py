import socketserver

from google.protobuf.reflection import ParseMessage

from protos import client_pb2
from .proto_handler import ProtoHandler

def request_handler_factory(url):
    """Returns a RequestHandler class that uses the database
    at the given url

    Args:
        url (str): url of the database to connect to
    """
    class ClientRequestHandler(socketserver.BaseRequestHandler):
        def setup(self):
            """Inhereted from base class"""
            print(f"Receiving a new connection from {self.client_address[0]}!")

        def handle(self):
            """Inhereted from base class. Handles the TCP connection"""
            data_len = self._get_data_len()
            data = self._read_data(data_len)

            proto = ParseMessage(client_pb2.ClientMessage.DESCRIPTOR, data)
            response = ProtoHandler(url).handle_proto(proto).SerializeToString()
            self.request.send(len(response).to_bytes(4, byteorder="big"))
            self.request.send(response)

        def finish(self):
            """Inhereted from base class"""
            print(f"Closing connection with {self.client_address[0]}!")

        def _read_data(self, data_len):
            """Reads a specified number of bytes from the TCP stream.

            Args:
                data_len (int): Number of bytes to read from the stream

            Returns: The read data as a bytearray
            """
            return self.request.recv(data_len)

        def _get_data_len(self):
            """Gets the length of the message the client is sending to us

            Returns: The number of bytes in the proceeding message
            """
            data = self._read_data(4)

            return int.from_bytes(data, "big")

    return ClientRequestHandler
