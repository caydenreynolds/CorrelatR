from datetime import date
from io import BytesIO
import random

from PIL import Image

from protos import client_pb2, server_pb2

#TODO: Put these in a config file
IMAGE_SIZE = (200, 200) 
IMAGE_MODE = "RGB"

def handle_proto(proto):
    """Handles a protobuf message from the client

    Args:
        proto (client_pb2.ClientMessage)

    Returns: A protobuf response message to send to the client
    """
    if proto.WhichOneof("message") == "changeColumn":
        return _change_column(proto.changeColumn)
    elif proto.WhichOneof("message") == "updateData":
        return _update_data(proto.updateData)
    elif proto.WhichOneof("message") == "ping":
        return _ping(proto.ping)
    elif proto.WhichOneof("message") == "graphRequest":
        return _image_reqeust(proto.graphRequest)

def _ping(_ping):
    """Handles a ping message
    
    Args:
        ping (client_pb2.Ping): The message to handle
        
    Returns: The response message to send to the client
    """
    print(f"Received a ping!")

    response = server_pb2.ServerMessage()
    response.statusMessage.text = "Pong"
    response.statusMessage.error = False

    return response

def _change_column(change_column):
    """Handles an change_column message
    
    Args:
        change_column (client_pb2.ChangeColumnMessage): The message to handle
        
    Returns: The response message to send to the client
    """
    response = server_pb2.ServerMessage()
    if len(change_column.columnName[0]) == 0:
        response.statusMessage.text = "Cannot add an empty column!"
        response.statusMessage.error = True
        print("A column change has been requested, but the list of changes was empty!")
    else:
        response.statusMessage.text = "Success!"
        response.statusMessage.error = False
        print(f"A change has been requested to column {change_column.columnName[0]}!")

    return response


def _update_data(update_data):
    """Handles an update_data message
    
    Args:
        update_data (client_pb2.UpdateDataMessage): The message to handle
        
    Returns: The response message to send to the client
    """
    response = server_pb2.ServerMessage()
    if len(update_data.newData) == 0:
        response.statusMessage.text = "No data updates to perform!"
        response.statusMessage.error = True
        print("A data update has been requested, but the list of changes was empty!")
    else:
        response.statusMessage.text = "Success!"
        response.statusMessage.error = False
        givenDate = date.fromtimestamp(update_data.date // 1000)
        print(f"An update of ({update_data.newData[0].columnName},{update_data.newData[0].value}) has been requested on date {givenDate}")
    return response

def _image_reqeust(graph_request):
    """Handles an graph_request message
    
    Args:
        graph_request (client_pb2.GraphRequest): The message to handle
        
    Returns: The response message to send to the client
    """
    random.seed()
    image = Image.new(IMAGE_MODE, IMAGE_SIZE, (random.randint(0, 255), random.randint(0, 255), random.randint(0, 255)))
    response = server_pb2.ServerMessage()
    response.statusMessage.text = "Success!"
    response.statusMessage.error = False
    image_bytes = BytesIO()
    image.save(image_bytes, "jpeg")
    response.graphImage = image_bytes.getvalue()
    print("A graph has been requested!")
    return response

