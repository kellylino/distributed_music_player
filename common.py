import json
import socket

BUFFER_SIZE = 65536

def send_json_to_addr(host, port, obj, timeout=3.0):
    """
    通过短连接发送 JSON 到指定 host:port（用于广播命令）。
    """
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(timeout)
        s.connect((host, port))
        data = json.dumps(obj).encode("utf-8") + b"\n"
        s.sendall(data)
        # try read optional response
        resp = b""
        try:
            chunk = s.recv(BUFFER_SIZE)
            if chunk:
                resp = chunk
        except:
            pass
        try:
            s.close()
        except:
            pass
        if resp:
            try:
                return json.loads(resp.decode("utf-8").split("\n")[0])
            except:
                return None
        return None
    except Exception as e:
        # silently return None on failure
        return None

def send_json_on_sock(sock, obj):
    data = json.dumps(obj).encode("utf-8") + b"\n"
    sock.sendall(data)

def recv_json_from_sock(sock):
    buf = b""
    while True:
        chunk = sock.recv(BUFFER_SIZE)
        if not chunk:
            return None
        buf += chunk
        if b"\n" in buf:
            raw, rest = buf.split(b"\n", 1)
            try:
                return json.loads(raw.decode("utf-8"))
            except:
                return None
