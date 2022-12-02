import logging
import time

from memory_raft import MemoryRaft, MessageHub


class MemoryTestHarness:
    '''Initializes MemoryRaft nodes and runs tests with some invariants
    '''

    def __init__(self, num_nodes, quiet=False):
        self.nodes = {}
        logging.basicConfig()
        self.logger = logging.getLogger('MemoryTestHarness')
        self.logger.setLevel(logging.DEBUG if not quiet else logging.WARNING)
        self.messagehub = MessageHub(logger=self.logger)
        for i in range(num_nodes):
            self.nodes[str(i)] = MemoryRaft(str(i), list(map(str, range(num_nodes))), self.messagehub, timer=None, logger=self.logger)

    def run(self, test_steps):
        '''Handle test commands like:
        - (start, [node_ids])
        - (stop, [node_ids])
        - (stop_leader, None)
        - (delay_ms, ms)
        - (partition, [node_ids], [node_ids])
        - (clear_partition, None)
        - (run_assert, func(nodes) {})
        '''
        try:
            for index, cmd in enumerate(test_steps):
                if cmd[0] == 'start':
                    for node_id in cmd[1]:
                        self.nodes[node_id].start()
                elif cmd[0] == 'stop':
                    for node_id in cmd[1]:
                        self.nodes[node_id].stop()
                elif cmd[0] == 'stop_leader':
                    for nodes in self.nodes.values():
                        if nodes.pub_is_leader():
                            self.logger.info('Stopping leader: %s', nodes.node_id)
                            nodes.stop()
                elif cmd[0] == 'delay_ms':
                    time.sleep(cmd[1] / 1000)
                elif cmd[0] == 'partition':
                    self.messagehub.partition(cmd[1], cmd[2])
                elif cmd[0] == 'clear_partition':
                    self.messagehub.clear_partition()
                elif cmd[0] == 'run_assert':
                    if not cmd[1](self.nodes.values()):
                        self.logger.warning('Assertion failed on command index [{}]: {}'.format(index, cmd))
                        # Print the state of the databases + command list
                        for node in self.nodes.values():
                            self.logger.warning('{} {}'.format(node.node_id, node.db.read_all_state()))
                        return False
    
            # Print the state of the databases + command list
            for node in self.nodes.values():
                self.logger.info('{} {}'.format(node.node_id, node.db.read_all_state()))
            return True
        finally:
            for node in self.nodes.values():
                node.stop()
