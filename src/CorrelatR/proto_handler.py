from protos import client_pb2


def handle_proto(proto):
    """Handles a protobuf message from the client

    Args:
        proto (client_pb2.ClientMessage)
    """

    if proto.WhichOneof("message") == "changeColumn":
        _change_column(proto.changeColumn)
    elif proto.WhichOneof("message") == "updateData":
        _update_data(proto.updateData)


def _change_column(change_column):
    """Handles a change_column message"""
    print(f"A change has been requested to column {change_column.columnName}!")


def _update_data(update_data):
    """Handles an update_data message"""
    print(
        f"we need to make {len(update_data.newData)} changes to the database!")
