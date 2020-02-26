from unittest import TestCase, mock
import socket

from correlatr import request_handler
from protos import client_pb2

class TestRequestHandler(TestCase):
    def test_handle(self):
        proto = client_pb2.ClientMessage()
        proto.changeColumn.newColumnName = "foo"

        mock_proto_handler = mock.MagicMock()
        def mock_proto_handler_class(_):
            return mock_proto_handler

        with mock.patch.object(request_handler, "ProtoHandler", mock_proto_handler_class):
            req_handler_class = request_handler.request_handler_factory("bar")
            req_handler_class._get_data_len = mock.MagicMock()
            req_handler_class._read_data = mock.MagicMock()

            req_handler_class._read_data.return_value = proto.SerializeToString()
            req_handler_class(mock.MagicMock(), [0], [0])

            mock_proto_handler.handle_proto.assert_called_once_with(proto)

    @mock.patch.object(request_handler, "ProtoHandler")
    @mock.patch.object(request_handler, "ParseMessage")
    def test_read_data(self, mock_parse_message, _):
        req_handler_class = request_handler.request_handler_factory("bar")
        req_handler_class.handle_proto = mock.MagicMock()
        req_handler_class._get_data_len = mock.MagicMock()
        req_handler_class._get_data_len.return_value = 3

        mock_socket = mock.MagicMock()
        mock_socket.recv.return_value = b'foo'
        req_handler_class(mock_socket, [0], [0])
        mock_parse_message.assert_called_once_with(client_pb2.ClientMessage.DESCRIPTOR, b'foo')

    @mock.patch.object(request_handler, "ProtoHandler")
    @mock.patch.object(request_handler, "ParseMessage")
    def test_get_data_len(self, mock_parse_message, _):
        req_handler_class = request_handler.request_handler_factory("bar")
        req_handler_class.handle_proto = mock.MagicMock()
        req_handler_class._read_data = mock.MagicMock()
        req_handler_class._read_data.return_value = (1337).to_bytes(4, byteorder="big")

        req_handler = req_handler_class(mock.MagicMock(), [0], [0])
        req_handler._read_data.assert_called_with(1337)