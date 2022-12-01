from memory_test_harness import MemoryTestHarness

class TestElectionPartition():

    def test_election_after_partition(self, quiet=False):
        test_harness = MemoryTestHarness(5, quiet)
        node_ids = list(map(str, range(5)))

        def assert_one_leader(nodes):
            leaders = [node for node in nodes if node.pub_is_leader()]
            if len(leaders) != 1:
                print("Leader results: {}".format(leaders))
                return False
            return True

        test_harness.run([
            ('start', node_ids),
            ('delay_ms', 250),
            ('run_assert', assert_one_leader),
            ('partition', ['0', '1'], ['2', '3', '4']),
            ('delay_ms', 250),
            ('run_assert', assert_one_leader),
            ('delay_ms', 250),
            ('stop', node_ids),
        ])

if __name__ == '__main__':
    for i in range(100):
        print('Test run: {}'.format(i))
        TestElectionPartition().test_election_after_partition(quiet=True)
