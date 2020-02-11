from unittest import TestCase, mock

from correlatr import proto_handler
from protos import client_pb2

class TestProtoHandler(TestCase):
    @mock.patch.object(proto_handler, "DBConnection")
    def test_handle_proto(self, _):
        protobuf_message_handler = proto_handler.ProtoHandler("foo")
        protobuf_message_handler._column_change = mock.MagicMock()
        protobuf_message_handler._update_data = mock.MagicMock()

        change_column_proto = client_pb2.ClientMessage()
        change_column_proto.changeColumn.oldColumnName = "bar"
        protobuf_message_handler.handle_proto(change_column_proto)

        protobuf_message_handler._column_change.assert_called_once()
        protobuf_message_handler._update_data.assert_not_called()

        update_data_proto = client_pb2.ClientMessage()
        update_data_proto.updateData.newData.add()
        protobuf_message_handler.handle_proto(update_data_proto)

        protobuf_message_handler._column_change.assert_called_once()
        protobuf_message_handler._update_data.assert_called_once()
