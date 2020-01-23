from unittest import TestCase, mock

from CorrelatR import proto_handler
from protos import client_pb2

class TestProtoHandler(TestCase):

    @mock.patch.object(proto_handler, "_update_data")
    @mock.patch.object(proto_handler, "_change_column")
    def test_test_framework(self, mock_change_column, mock_update_data):
        change_column_proto = client_pb2.ClientMessage()
        change_column_proto.changeColumn.columnName = "foo"
        proto_handler.handle_proto(change_column_proto)

        mock_change_column.assert_called_once()
        mock_update_data.assert_not_called()

        update_data_proto = client_pb2.ClientMessage()
        update_data_proto.updateData.newData.add()
        proto_handler.handle_proto(update_data_proto)

        mock_change_column.assert_called_once()
        mock_update_data.assert_called_once()
