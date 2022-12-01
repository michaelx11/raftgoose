from memory_test_harness import MemoryTestHarness

class TestBasicElection():

    def test_basic_election(self):
        test_harness = MemoryTestHarness(5)
        node_ids = list(map(str, range(5)))

        def assert_one_leader(nodes):
            leaders = [node for node in nodes if node.pub_is_leader()]
            if len(leaders) != 1:
                print("Leader results: {}".format(leaders))
                return False
            print('Got leader: {}'.format(leaders[0].node_id))
            return True

        test_harness.run([
            ('start', node_ids),
            ('delay_ms', 500),
            ('stop', node_ids),
            ('run_assert', assert_one_leader),
            ('delay_ms', 500),
            ('run_assert', assert_one_leader),
            ('delay_ms', 500),
            ('stop', node_ids),
        ])

if __name__ == '__main__':
    TestBasicElection().test_basic_election()
