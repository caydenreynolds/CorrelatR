from unittest import TestCase, mock
import socket

from CorrelatR import request_handler
from protos import client_pb2

class TestRequestHandler(TestCase):

    @mock.patch.object(request_handler.proto_handler, "handle_proto")
    @mock.patch.object(request_handler.ClientRequestHandler, "_get_data_len")
    @mock.patch.object(request_handler.ClientRequestHandler, "_read_data")
    def test_handle(self, mock_read_data, mock_get_data_len, mock_handle_proto):
        proto = client_pb2.ClientMessage()
        proto.changeColumn.addRemove = False

        mock_read_data.return_value = proto.SerializeToString()

        request_handler.ClientRequestHandler([0], [0], [0])

        mock_handle_proto.assert_called_once_with(proto)

    @mock.patch.object(request_handler, "ParseMessage")
    @mock.patch.object(request_handler.proto_handler, "handle_proto")
    @mock.patch.object(request_handler.ClientRequestHandler, "_get_data_len")
    def test_read_data(self, mock_get_data_len, mock_handle_proto, mock_parse_message):
        mock_get_data_len.return_value = 3

        mock_socket = mock.MagicMock()
        mock_socket.recv.return_value = b'foo'
        request_handler.ClientRequestHandler(mock_socket, [0], [0])
        mock_parse_message.assert_called_once_with(client_pb2.ClientMessage.DESCRIPTOR, b'foo')

    @mock.patch.object(request_handler, "ParseMessage")
    @mock.patch.object(request_handler.proto_handler, "handle_proto")
    @mock.patch.object(request_handler.ClientRequestHandler, "_read_data")
    def test_get_data_len(self, mock_read_data, mock_handle_proto, mock_parse_message):
        mock_read_data.return_value = (1337).to_bytes(4, byteorder="big")

        request_handler.ClientRequestHandler([0], [0], [0])
        mock_read_data.assert_called_with(1337)