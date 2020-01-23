import socketserver

from ..request_handler import ClientRequestHandler


def main():
    print("Starting CorrelatR server!")
    host, port = "localhost", 42069
    with socketserver.TCPServer((host, port), ClientRequestHandler) as server:
        server.serve_forever()


if __name__ == "__main__":
    main()
