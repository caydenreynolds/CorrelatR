import socketserver
import socket

from ..request_handler import ClientRequestHandler


def main():
    print("Starting CorrelatR server!")
    host, port = "", 42069
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as my_address_socket:
        my_address_socket.connect(("1.1.1.1", 1))
        print(f"Hosting on {my_address_socket.getsockname()[0]}:{port}")

    with socketserver.TCPServer((host, port), ClientRequestHandler) as server:
        server.serve_forever()


if __name__ == "__main__":
    main()
