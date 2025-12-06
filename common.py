# common.py
"""
Common Utilities - Network Communication Helpers
This module provides helper functions for JSON-based network communication over TCP sockets.
"""
import json
import socket

BUFFER_SIZE = 65536 # Maximum size for received data chunks

def send_json_to_addr(host, port, obj, timeout=1):
    """
    Send JSON object to specific address and wait for response.

    Creates a new TCP connection for each call (fire-and-forget).
    Suitable for one-off messages like heartbeats or discovery.

    Args:
        host (str): Destination hostname/IP
        port (int): Destination port
        obj (dict): JSON-serializable object to send
        timeout (float): Socket timeout in seconds

    Returns:
        dict: Response object or None if no response or error
    """

    try:
        # Create TCP socket
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(timeout)
        s.connect((host, port))

        # Serialize and send JSON with newline delimiter
        data = json.dumps(obj).encode("utf-8") + b"\n"
        s.sendall(data)

        # Try to receive response
        resp = b""
        try:
            chunk = s.recv(BUFFER_SIZE)
            if chunk:
                resp = chunk
        except socket.timeout:
            # Timeout is expected for fire-and-forget messages
            pass
        except Exception as e:
            print(f"[NETWORK_DEBUG] Error receiving from {host}:{port}: {e}")

        # Cleanup
        try:
            s.close()
        except:
            pass

        # Parse response if received
        if resp:
            try:
                # Split by newline in case multiple messages received
                response_obj = json.loads(resp.decode("utf-8").split("\n")[0])
                return response_obj
            except Exception as e:
                print(f"[NETWORK_DEBUG] Failed to parse response: {e}")
                return None
        return None
    except Exception as e:
        print(f"[NETWORK_DEBUG] Failed to send {obj.get('type')} to {host}:{port}: {e}")
        return None

def send_json_on_sock(sock, obj):
    """
    Send JSON object on existing socket connection.

    Used for persistent connections managed by ConnectionManager.

    Args:
        sock (socket): Existing socket connection
        obj (dict): JSON-serializable object to send
    """

    data = json.dumps(obj).encode("utf-8") + b"\n"
    sock.sendall(data)

def recv_json_from_sock(sock):
    """
    Receive JSON object from socket with newline delimiter.

    Reads until newline character or socket closure.

    Args:
        sock (socket): Socket to receive from

    Returns:
        dict: Received JSON object or None on error or closure
    """

    buf = b""
    while True:
        try:
            chunk = sock.recv(BUFFER_SIZE)
            if not chunk:  # Connection closed
                return None
            buf += chunk
            if b"\n" in buf: # Message complete
                raw, rest = buf.split(b"\n", 1)
                try:
                    obj = json.loads(raw.decode("utf-8"))
                    return obj
                except Exception as e:
                    print(f"[NETWORK_DEBUG] Failed to parse message: {e}")
                    return None
        except Exception as e:
            print(f"[NETWORK_DEBUG] Error receiving data: {e}")
            return None