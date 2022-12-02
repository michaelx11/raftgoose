from memory_test_harness import MemoryTestHarness


class TestBasicElection:

    last_leader = None

    def test_basic_election(self, quiet=False):
        test_harness = MemoryTestHarness(5, quiet=quiet)
        node_ids = list(map(str, range(5)))

        def assert_one_leader(nodes):
            leaders = [node for node in nodes if node.pub_is_leader()]
            if len(leaders) != 1:
                print("Leader results: {}".format(leaders))
                return False
            # Make sure we don't get the same leader
            if self.last_leader is not None:
                if self.last_leader == leaders[0].node_id:
                    print("Got same leader as last time {}".format(self.last_leader))
                    return False
            self.last_leader = leaders[0].node_id
            return True

        return test_harness.run(
            [
                ("start", node_ids),
                ("delay_ms", 500),
                ("run_assert", assert_one_leader),
                ("stop_leader", None),
                ("delay_ms", 500),
                ("run_assert", assert_one_leader),
                ("stop", node_ids),
            ]
        )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    # Check for --verbose flag
    parser.add_argument("-v", "--verbose", action="store_true")
    for i in range(100):
        print("Test run {}".format(i))
        assert TestBasicElection().test_basic_election(
            quiet=not parser.parse_args().verbose
        )
