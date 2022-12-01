import threading

from raft_base import RaftBase
from database import Database

class SendHub:

    def __init__(self):
        self.peer_dict = {}
        self.lock = threading.Lock()


    def register(self, peer_id, peer):
        with self.lock:
            self.peer_dict[peer_id] = peer


    def send(self, sender, peer, message):
        with self.lock:
            if peer not in self.peer_dict:
                # Drop it like it's hot
                return
            print('Sending message from {} to {}: {}'.format(sender, peer, message))
            self.peer_dict[peer].recv_message((sender, message))

class MemoryRaft(RaftBase):
    '''Simple in-memory raft implementation for testing
    '''
    class MemoryDb(Database):
        def __init__(self):
            self.db = {}

        def write_all_state(self, state):
            self.db = state

        def read_all_state(self):
            return self.db

    def __init__(self, node_id, peers, sendhub, timer=None, logger=None):
        super().__init__(node_id, peers, MemoryRaft.MemoryDb(), timer=None, logger=None)
        self.db = self.MemoryDb()
        self.sendhub = sendhub
        self.sendhub.register(node_id, self)

    def auth_rpc(self, peer_id, msg):
        '''No-op'''
        return True

    def send_message(self, peer_id, msg):
        self.sendhub.send(self.node_id, peer_id, msg)

    def receive_message(self, peer_id, msg):
        '''Called by sendhub'''
        self.recv_message((peer_id, msg))

if __name__ == '__main__':
    import sys
    import time
    import logging

    logging.basicConfig(level=logging.DEBUG)

    sendhub = SendHub()
    node1 = MemoryRaft('1', ['1', '2', '3'], sendhub, timer=None)
    node2 = MemoryRaft('2', ['1', '2', '3'], sendhub, timer=None)
    node3 = MemoryRaft('3', ['1', '2', '3'], sendhub, timer=None)

    node1.start()
    node2.start()
    node3.start()

    time.sleep(2)
    node1.stop()
    node2.stop()
    node3.stop()

    # Print the state of the databases
    print(node1.db.read_all_state())
    print(node2.db.read_all_state())
    print(node3.db.read_all_state())

    sys.exit(0)
