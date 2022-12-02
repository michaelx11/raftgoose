import logging
import threading

from collections import defaultdict

from raft_base import RaftBase
from database import Database

class MessageHub:

    def __init__(self, logger=None):
        self.peer_dict = {}
        self.peer_locks = {}
        self.lock = threading.Lock()
        # All nodes in 0 partition
        self.node_partition = defaultdict(int)
        if logger:
            self.logger = logger
        else:
            self.logger = logging.getLogger('MessageHub')


    def register(self, peer_id, peer):
        with self.lock:
            self.peer_dict[peer_id] = peer
            self.peer_locks[peer_id] = threading.Lock()


    def send(self, sender, peer, message):
        with self.lock:
            if peer not in self.peer_dict:
                # Drop it like it's hot
                return
            if self.node_partition[sender] != self.node_partition[peer]:
                # Prevent communication between partitions
                return
        with self.peer_locks[peer]:
            self.logger.debug('Sending message from {} to {}: {}'.format(sender, peer, message))
            self.peer_dict[peer].receive_message(sender, message)

    def partition(self, nodesA, nodesB):
        with self.lock:
            for node in nodesA:
                self.node_partition[node] = 0
            for node in nodesB:
                self.node_partition[node] = 1

    def clear_partition(self):
        with self.lock:
            self.node_partition = defaultdict(int)


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

    def __init__(self, node_id, peers, messagehub, timer=None, logger=None):
        super().__init__(node_id, peers, MemoryRaft.MemoryDb(), timer=timer, logger=logger)
        self.db = self.MemoryDb()
        self.messagehub = messagehub
        self.messagehub.register(node_id, self)

    def auth_rpc(self, peer_id, msg):
        '''No-op'''
        return True

    def send_message(self, peer_id, msg):
        self.messagehub.send(self.node_id, peer_id, msg)

    def receive_message(self, peer_id, msg):
        '''Called by messagehub'''
        self.recv_message((peer_id, msg))

if __name__ == '__main__':
    import sys
    import time
    import logging

    logging.basicConfig(level=logging.DEBUG)

    messagehub = MessageHub()
    node1 = MemoryRaft('1', ['1', '2', '3'], messagehub, timer=None)
    node2 = MemoryRaft('2', ['1', '2', '3'], messagehub, timer=None)
    node3 = MemoryRaft('3', ['1', '2', '3'], messagehub, timer=None)

    node1.start()
    node2.start()
    node3.start()

    time.sleep(0.5)
    node1.stop()
    node2.stop()
    node3.stop()

    # Print the state of the databases
    print(node1.db.read_all_state())
    print(node2.db.read_all_state())
    print(node3.db.read_all_state())

    sys.exit(0)
