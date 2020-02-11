import atexit
import socketserver
import socket
import subprocess

from ..request_handler import request_handler_factory

def main():
    print('Starting postgresql server!')
    subprocess.run('sudo service postgresql start', shell=True)

    print("Starting CorrelatR server!")
    host, port = "", 42069
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as my_address_socket:
        my_address_socket.connect(("1.1.1.1", 1))
        print(f"Hosting on {my_address_socket.getsockname()[0]}:{port}")

    database_url = 'postgresql://correlatr_server:correlatr_password@localhost/correlatr'
    with socketserver.TCPServer((host, port), request_handler_factory(database_url)) as server:
        server.serve_forever()

@atexit.register
def stop_mysql():
    print('Stopping mysql server!')
    subprocess.run('sudo service postgresql stop', shell=True)