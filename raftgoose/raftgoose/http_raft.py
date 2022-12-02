import json
import logging
import threading

from http.server import BaseHTTPRequestHandler, HTTPServer

from urllib.parse import urlencode
from urllib.request import Request, urlopen
from collections import defaultdict
from functools import partial


from raft_base import RaftBase
from database import Database


class HttpRaft(RaftBase):
    """HTTP powered raft implementation"""

    class MemoryDb(Database):
        def __init__(self):
            self.db = {}

        def write_all_state(self, state):
            self.db = state

        def read_all_state(self):
            return self.db

    class HttpRaftHandler(BaseHTTPRequestHandler):
        """Need to run two servers, one for raft communicatino and one for control

        Otherwise, waiting on /goose will block raft communication
        """

        def __init__(self, raft_node, is_control_server, *args, **kwargs):
            self.raft_node = raft_node
            self.is_control_server = is_control_server
            super().__init__(*args, **kwargs)

        def log_message(self, format, *args):
            """redirect request logging"""
            # self.raft_node.logger.debug(format, *args)

        def do_POST(self):
            """Called by HTTPServer"""
            if not self.is_control_server:
                if self.path == "/msg":
                    length = int(self.headers["Content-Length"])
                    msg = self.rfile.read(length)
                    # Deserialize message as { peer: "string", msg: {}}
                    msg = json.loads(msg.decode("utf-8"))
                    with self.raft_node.traffic_lock:
                        if self.raft_node.dropped_peers[msg["peer"]]:
                            # Drop message
                            self.send_response(400)
                            self.end_headers()
                            self.raft_node.logger.info(
                                "dropping message from %s", msg["peer"]
                            )
                            return
                        self.raft_node.logger.debug(
                            "http received message from %s: %s", msg["peer"], msg["msg"]
                        )
                        self.raft_node.recv_message((msg["peer"], msg["msg"]))
                    self.send_response(200)
                    self.end_headers()
                    return
                else:
                    self.send_response(404)
                    self.end_headers()
                    return

            if self.path == "/stop":
                self.raft_node.stop()
                self.raft_node.logger.warning(
                    "node: %s stopping", self.raft_node.node_id
                )
                self.send_response(200)
                self.end_headers()
            elif self.path == "/start":
                self.raft_node.start()
                self.raft_node.logger.warning(
                    "node: %s started", self.raft_node.node_id
                )
                self.send_response(200)
                self.end_headers()
            elif self.path == "/partition":
                length = int(self.headers["Content-Length"])
                msg = self.rfile.read(length)
                msg = json.loads(msg.decode("utf-8"))
                with self.raft_node.traffic_lock:
                    # Drop all messages from peers in msg['peers']
                    self.raft_node.dropped_peers = defaultdict(lambda: False)
                    for peer in msg["peers"]:
                        self.raft_node.dropped_peers[peer] = True
                        self.raft_node.logger.warning("dropping messages from %s", peer)
                self.send_response(200)
                self.end_headers()
                return
            else:
                self.send_response(404)
                self.end_headers()

        def do_GET(self):
            """Called by HTTPServer"""
            if not self.is_control_server:
                self.send_response(400)
                self.end_headers()
                return

            if self.path == "/goose":
                self.send_response(200)
                self.send_header("Content-type", "text/html")
                self.end_headers()
                if self.raft_node.pub_is_leader():
                    self.wfile.write(bytes("goose", "utf-8"))
                else:
                    self.wfile.write(bytes("duck", "utf-8"))
            elif self.path == "/state":
                self.send_response(200)
                self.send_header("Content-type", "application/json")
                self.end_headers()
                self.wfile.write(
                    bytes(json.dumps(self.raft_node.db.read_all_state()), "utf-8")
                )
            else:
                self.send_response(404)
                self.end_headers()

    def __init__(self, node_id, peers, **kwargs):
        super().__init__(node_id, peers, HttpRaft.MemoryDb(), **kwargs)
        self.db = self.MemoryDb()
        # Raft communication
        handler = partial(HttpRaft.HttpRaftHandler, self, False)
        self.server = HTTPServer(("127.0.0.1", int(self.node_id)), handler)
        # Control server
        control_handler = partial(HttpRaft.HttpRaftHandler, self, True)
        # Add 1000 to port number to avoid conflict
        self.control_server = HTTPServer(
            ("127.0.0.1", int(self.node_id) + 1000), control_handler
        )

        # Stuff to manager dropped traffic
        self.traffic_lock = threading.Lock()
        self.dropped_peers = defaultdict(lambda: False)

    def start_server(self):
        self.server_thread = threading.Thread(target=self.server.serve_forever)
        self.server_thread.start()
        self.control_server_thread = threading.Thread(
            target=self.control_server.serve_forever
        )
        self.control_server_thread.start()
        # Start Raft
        self.start()

    def auth_rpc(self, peer_id, msg):
        """No-op"""
        return True

    def send_message(self, peer_id, msg):
        """Send an HTTP message (POST /msg) with JSON to a peer on localhost, port = peer_id"""
        self.logger.debug("send_message {} {}".format(peer_id, msg))
        try:
            data = {"peer": self.node_id, "msg": msg}
            url = "http://localhost:{}/msg".format(peer_id)

            postdata = json.dumps(data).encode()

            headers = {"Content-Type": "application/json; charset=UTF-8"}

            httprequest = Request(url, data=postdata, method="POST", headers=headers)

            with urlopen(httprequest) as response:
                response.read()
        except Exception as e:
            print("Error sending message to peer", peer_id, e)


if __name__ == "__main__":
    # Bring up a 5 node raft cluster with ports 8900-8904
    # Raft communication servers are 8900-8904, control servers are 9900-9904
    nodes = []
    node_ports = [str(i) for i in range(8900, 8905)]
    logging.basicConfig(level=logging.INFO)
    for port in node_ports:
        logger = logging.getLogger("raft_{}".format(port))
        nodes.append(
            HttpRaft(
                port,
                node_ports,
                logger=logger,
                timeout=0.8,
                heartbeat=0.15,
                client_timeout=0.6,
            )
        )
        nodes[-1].start_server()
