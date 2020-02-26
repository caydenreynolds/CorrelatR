from protos import server_pb2

def create_response(message, error):
    """Creates a server response with the message and error fields set

    Args: 
        message (str): The message to send with the response
        error (bool): Whether or not the message represents an error
    """
    response = server_pb2.ServerMessage()
    response.statusMessage.text = message
    response.statusMessage.error = error
    return response
