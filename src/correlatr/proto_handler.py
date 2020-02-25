from datetime import date
from io import BytesIO
import random
import traceback

from PIL import Image

from protos import client_pb2, server_pb2, shared_pb2
from .db_connection import DBConnection, get_safe_column_name
from .response import create_response

#TODO: Put these in a config file
IMAGE_SIZE = (200, 200) 
IMAGE_MODE = "RGB"

class ProtoHandler:
    def __init__(self, db_url):
        """A class that handles the requested action from a client proto

        Args:
            db_url (str): url of the database to connect to
        """
        self.db_conn = DBConnection(db_url, DBConnection.TABLE_NAME)

    def handle_proto(self, proto):
        """Handles a protobuf message from the client

        Args:
            proto (client_pb2.ClientMessage)

        Returns: A protobuf response message to send to the client
        """
        try:
            if proto.WhichOneof("message") == "changeColumn":
                return self._column_change(proto.changeColumn)
            elif proto.WhichOneof("message") == "updateData":
                return self._update_data(proto.updateData)
            elif proto.WhichOneof("message") == "ping":
                return self._ping(proto.ping)
            elif proto.WhichOneof("message") == "graphRequest":
                return self._image_request(proto.graphRequest)
            elif proto.WhichOneof("message") == "columnsRequest":
                return self._columnsRequest(proto.columnsRequest)
            elif proto.WhichOneof("message") == "dataRequest":
                return self._data_request(proto.dataRequest)
        except Exception:
            print(traceback.format_exc())
            return create_response("Unkown server error", True)


    def _columnsRequest(self, columns_request):
        print("Columns requested!")
        columns = self.db_conn.get_all_columns()
        response = create_response("columns fetched", False)
        for column in columns:
            data_point = shared_pb2.DataPoint()
            data_point.columnName = column
            response.dataPoints.append(data_point)

        return response

    def _ping(self, _ping):
        """Handles a ping message
        
        Args:
            ping (client_pb2.Ping): The message to handle
            
        Returns: The response message to send to the client
        """
        print("Received a ping!")
        return create_response("Connected", False)

    def _update_data(self, update_data):
        """Handles an update_data message
        
        Args:
            update_data (client_pb2.UpdateDataMessage): The message to handle
            
        Returns: The response message to send to the client
        """
        if len(update_data.newData) == 0:
            print("A data update has been requested, but the list of changes was empty!")
            return create_response("No data updates to perform", True)
        else:
            update_data.date = update_data.date // 1000
            print(f"An update of ({update_data.newData[0].columnName},{update_data.newData[0].value}) "
                  f"has been requested on date {date.fromtimestamp(update_data.date)}")
            return self.db_conn.set_data(update_data.date, self._dictify_datapoints(update_data.newData))

    def _image_request(self, graph_request):
        """Handles an graph_request message
        
        Args:
            graph_request (client_pb2.GraphRequest): The message to handle
            
        Returns: The response message to send to the client
        """
        random.seed()
        image = Image.new(IMAGE_MODE, IMAGE_SIZE, (random.randint(0, 255), random.randint(0, 255), random.randint(0, 255)))
        response = create_response("Success", False)
        image_bytes = BytesIO()
        image.save(image_bytes, "jpeg")
        response.graphImage = image_bytes.getvalue()
        print("A graph has been requested!")
        return response

    def _column_change(self, change_column_message):
        """Handles a change_column message
        
        Args:
            change_column_message (client_pb2.ChangeColumnMessage): The message to handle
            
        Returns: The response message to send to the client
        """
        if not change_column_message.newColumnName and not change_column_message.oldColumnName:
            print("Bad column change message")
            return create_response("Bad changeColumn message. No column names set", True)
        elif not change_column_message.oldColumnName:
            print("Column add requested!")
            return self.db_conn.add_column(change_column_message.newColumnName)
        elif not change_column_message.newColumnName:
            print("Column remove requested")
            return self.db_conn.remove_column(change_column_message.oldColumnName)
        else:
            return self.db_conn.rename_column(change_column_message.oldColumnName, change_column_message.newColumnName)

    def _data_request(self, data_request_message):
        """Handles a change_column message
        
        Args:
            data_request_message (client_pb2.DataRequestMessage): The message to handle
            
        Returns: The response message to send to the client
        """
        data_request_message.date = data_request_message.date // 1000
        print(f'Data requested for date {date.fromtimestamp(data_request_message.date)}!')
        return self.db_conn.get_data(data_request_message.date)

    def _dictify_datapoints(self, data_points):
        """Converts a list of datapoints to a dict

        Args: 
            dataponts ([shared_pb2.DataPoint]): Datapoints to dictify

        Returns: Dict containing datapoints as key-value pairs
        """
        result = {}
        for point in data_points:
            if not point.null:
                result[point.columnName] = point.value
                print(result)
        return result