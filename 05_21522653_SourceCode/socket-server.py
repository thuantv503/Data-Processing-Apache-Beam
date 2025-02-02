import socket
import time
import json

def socket_server():
    host = 'localhost'
    port = 9990
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    # Allow the socket to be reused immediately after the program exits
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    
    server_socket.bind((host, port))
    server_socket.listen(1)
    print(f"Server listening on {host}:{port}")

    try:
        while True:
            conn, addr = server_socket.accept()
            print(f"Connection from {addr}")
            handle_client(conn)
    except KeyboardInterrupt:
        print("Shutting down server.")
    finally:
        server_socket.close()

def handle_client(conn):
    plants = [
        {'name': 'Apple'},
        {'name': 'Carrot'},
        {'name': 'Carrot'},
        {'name': 'Apple'},
        {'name': 'Carrot'},

        {'name': 'Apple'},
        {'name': 'Apple'},
        {'name': 'Apple'},
        {'name': 'Banana'},
        {'name': 'Banana'},

        {'name': 'Carrot'},
        {'name': 'Carrot'},
    ]

    try:
        for plant in plants:
            # Assign the current Unix timestamp to the 'timestamp' field
            plant['season'] = time.time()
            # Convert dictionary to JSON string
            json_data = json.dumps(plant)
            # Send the JSON data followed by a newline character
            conn.sendall((json_data + '\n').encode('utf-8'))
            print(f"Sent: {json_data}")
            time.sleep(1)  # Send one event per second
    except BrokenPipeError:
        print("Client disconnected.")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    socket_server()


