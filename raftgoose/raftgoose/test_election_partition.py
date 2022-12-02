from memory_test_harness import MemoryTestHarness

class TestElectionPartition():

    def test_election_after_partition(self, quiet=False):
        test_harness = MemoryTestHarness(5, quiet=quiet)
        node_ids = list(map(str, range(5)))

        def assert_one_leader(nodes):
            leaders = [node for node in nodes if node.pub_is_leader()]
            if len(leaders) != 1:
                print("Leader results: {}".format(leaders))
                return False
            return True

        return test_harness.run([
            ('start', node_ids),
            ('delay_ms', 500),
            ('run_assert', assert_one_leader),
            ('partition', ['0', '1'], ['2', '3', '4']),
            ('delay_ms', 500),
            ('run_assert', assert_one_leader),
            ('stop', node_ids),
        ])

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    # Check for --verbose flag
    parser.add_argument('-v', '--verbose', action='store_true')
    for i in range(100):
        print('Test run: {}'.format(i))
        assert TestElectionPartition().test_election_after_partition(quiet=not parser.parse_args().verbose)
