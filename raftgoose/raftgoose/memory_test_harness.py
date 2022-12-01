import logging
import time

from memory_raft import MemoryRaft, MessageHub


class MemoryTestHarness:
    '''Initializes MemoryRaft nodes and runs tests with some invariants
    '''

    def __init__(self, num_nodes):
        self.messagehub = MessageHub()
        self.nodes = {}
        for i in range(num_nodes):
            self.nodes[str(i)] = MemoryRaft(str(i), list(map(str, range(num_nodes))), self.messagehub)

    def run(self, test_steps):
        '''Handle test commands like:
        - (start, [node_ids])
        - (stop, [node_ids])
        - (delay_ms, ms)
        - (partition, [node_ids], [node_ids])
        - (clear_partition, None)
        - (run_assert, func(nodes) {})
        '''
        for index, cmd in enumerate(test_steps):
            if cmd[0] == 'start':
                for node_id in cmd[1]:
                    self.nodes[node_id].start()
            elif cmd[0] == 'stop':
                for node_id in cmd[1]:
                    self.nodes[node_id].stop()
            elif cmd[0] == 'delay_ms':
                time.sleep(cmd[1] / 1000)
            elif cmd[0] == 'partition':
                self.messagehub.partition(cmd[1], cmd[2])
            elif cmd[0] == 'clear_partition':
                self.messagehub.clear_partition()
            elif cmd[0] == 'run_assert':
                if not cmd[1](self.nodes.values()):
                    print('Assertion failed on command index [{}]: {}'.format(index, cmd))
                    # Print the state of the databases + command list
                    for node in self.nodes.values():
                        print(node.node_id, node.db.read_all_state())
                    return False
        return True
