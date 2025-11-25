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
           