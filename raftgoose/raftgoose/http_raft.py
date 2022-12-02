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
    '''HTTP powered raft implementation
    '''
    class MemoryDb(Database):
        def __init__(self):
            self.db = {}

        def write_all_state(self, state):
            self.db = state

        def read_all_state(self):
            return self.db

    class HttpRaftHandler(BaseHTTPRequestHandler):

        def __init__(self, raft_node, *args, **kwargs):
            self.raft_node = raft_node
            self.lock = threading.Lock()
            # Dropped peers
            self.dropped_peers = defaultdict(lambda: False)
            super().__init__(*args, **kwargs)

        def log_message(self, format, *args):
            '''redirect request logging'''
            # self.raft_node.logger.debug(format, *args)

        def do_POST(self):
            '''Called by HTTPServer'''
            if self.path == '/msg':
                length = int(self.headers['Content-Length'])
                msg = self.rfile.read(length)
                # Deserialize message as { peer: "string", msg: {}}
                msg = json.loads(msg.decode('utf-8'))
                with self.lock:
                    if self.dropped_peers[msg['peer']]:
                        # Drop message
                        self.send_response(400)
                        self.end_headers()
                        return
                    self.raft_node.recv_message((msg['peer'], msg['msg']))
                self.send_response(200)
                self.end_headers()
            elif self.path == '/stop':
                self.raft_node.stop()
                self.send_response(200)
                self.end_headers()
            elif self.path == '/start':
                self.raft_node.start()
                self.send_response(200)
                self.end_headers()
            elif self.path == '/partition':
                length = int(self.headers['Content-Length'])
                msg = self.rfile.read(length)
                msg = json.loads(msg.decode('utf-8'))
                with self.lock:
                    # Drop all messages from peers in msg['peers']
                    self.dropped_peers = defaultdict(lambda: False)
                    for peer in msg['peers']:
                        self.dropped_peers[peer] = True
                self.send_response(200)
                self.end_headers()
            else:
                self.send_response(404)
                self.end_headers()


        def do_GET(self):
            '''Called by HTTPServer'''
            if self.path == '/goose':
                self.send_response(200)
                self.send_header('Content-type', 'text/html')
                self.end_headers()
                if self.raft_node.pub_is_leader():
                    self.wfile.write(bytes('goose', 'utf-8'))
                else:
                    self.wfile.write(bytes('duck', 'utf-8'))
            elif self.path == '/state':
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(bytes(json.dumps(self.raft_node.db.read_all_state()), 'utf-8'))
            else:
                self.send_response(404)
                self.end_headers()

    def __init__(self, node_id, peers, **kwargs):
        super().__init__(node_id, peers, HttpRaft.MemoryDb(), **kwargs)
        self.db = self.MemoryDb()
        handler = partial(HttpRaft.HttpRaftHandler, self)
        self.server = HTTPServer(('127.0.0.1', int(self.node_id)), handler)

    def start_server(self):
        self.server_thread = threading.Thread(target=self.server.serve_forever)
        self.server_thread.start()
        self.start()

    def auth_rpc(self, peer_id, msg):
        '''No-op'''
        return True

    def send_message(self, peer_id, msg):
        '''Send an HTTP message (POST /msg) with JSON to a peer on localhost, port = peer_id'''
        self.logger.debug('send_message {} {}'.format(peer_id, msg))
        try:
            data = {
                'peer': self.node_id,
                'msg': msg
            }
            url = "http://localhost:{}/msg".format(peer_id)
            
            postdata = json.dumps(data).encode()
            
            headers = {"Content-Type": "application/json; charset=UTF-8"}
            
            httprequest = Request(url, data=postdata, method="POST", headers=headers)

            with urlopen(httprequest) as response:
                response.read()
        except Exception as e:
            print('Error sending message to peer', peer_id, e)


if __name__ == '__main__':
    # Bring up a 5 node raft cluster with ports 8900-8904
    # Do it five times with multiple threads
    nodes = []
    node_ports = [str(i) for i in range(8900, 8905)]
    logging.basicConfig(level=logging.DEBUG)
    for port in node_ports:
        logger = logging.getLogger('raft_{}'.format(port))
        nodes.append(HttpRaft(port, node_ports, logger=logger, timeout=5.8, heartbeat=0.50, client_timeout=3.5))
        nodes[-1].start_server()
