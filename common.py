# common.py
import json
import socket

BUFFER_SIZE = 65536

def send_json_to_addr(host, port, obj, timeout=3.0):
    # print(f"[NETWORK_DEBUG] SENDING {obj.get('type')} to {host}:{port}")
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(timeout)
        s.connect((host, port))
        data = json.dumps(obj).encode("utf-8") + b"\n"
        s.sendall(data)
        # print(f"[NETWORK_DEBUG] Successfully sent {obj.get('type')} to {host}:{port}")

        # try read optional response
        resp = b""
        try:
            chunk = s.recv(BUFFER_SIZE)
            if chunk:
                resp = chunk
                # print(f"[NETWORK_DEBUG] Received response of {len(chunk)} bytes from {host}:{port}")
        except socket.timeout:
            print(f"[NETWORK_DEBUG] Timeout waiting for response from {host}:{port}")
        except Exception as e:
            print(f"[NETWORK_DEBUG] Error receiving from {host}:{port}: {e}")

        try:
            s.close()
        except:
            pass

        if resp:
            try:
                response_obj = json.loads(resp.decode("utf-8").split("\n")[0])
                # print(f"[NETWORK_DEBUG] Parsed response type: {response_obj.get('type')}")
                return response_obj
            except Exception as e:
                print(f"[NETWORK_DEBUG] Failed to parse response: {e}")
                return None
        return None
    except Exception as e:
        print(f"[NETWORK_DEBUG] Failed to send {obj.get('type')} to {host}:{port}: {e}")
        return None

def send_json_on_sock(sock, obj):
    data = json.dumps(obj).encode("utf-8") + b"\n"
    sock.sendall(data)
    # print(f"[NETWORK_DEBUG] Sent {obj.get('type')} via existing socket")

def recv_json_from_sock(sock):
    buf = b""
    while True:
        try:
            chunk = sock.recv(BUFFER_SIZE)
            if not chunk:
                return None
            buf += chunk
            if b"\n" in buf:
                raw, rest = buf.split(b"\n", 1)
                try:
                    obj = json.loads(raw.decode("utf-8"))
                    # print(f"[NETWORK_DEBUG] Received {obj.get('type')} via socket")
                    return obj
                except Exception as e:
                    print(f"[NETWORK_DEBUG] Failed to parse message: {e}")
                    return None
        except Exception as e:
            print(f"[NETWORK_DEBUG] Error receiving data: {e}")
            return None