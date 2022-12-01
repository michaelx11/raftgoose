from memory_test_harness import MemoryTestHarness

class TestBasicElection():

    def test_basic_election(self):
        test_harness = MemoryTestHarness(5)
        node_ids = list(map(str, range(5)))

        def assert_one_leader(nodes):
            leaders = [node for node in nodes if node.pub_is_leader()]
            assert len(leaders) == 1

        test_harness.run([
            ('start', node_ids),
            ('delay_ms', 500),
            ('stop', node_ids),
            ('run_assert', assert_one_leader),
        ])

if __name__ == '__main__':
    TestBasicElection().test_basic_election()
