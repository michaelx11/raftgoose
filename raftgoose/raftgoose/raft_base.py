import time
import random
import logging

import threading
import select
from collections import deque

from abc import ABC, abstractmethod


class RaftBase(ABC):
    """A basic Raft implementation which accesses everything through expected methods for abstraction"""

    def __init__(
        self,
        node_id,
        peers,
        db,
        timer=None,
        logger=None,
        timeout=0.20,
        heartbeat=0.04,
        client_timeout=0.25,
    ):
        """Initialize the RaftBase object

        Inject the timer to allow for easier testing
        """
        self.node_id = node_id
        self.peers = peers
        self.db = db
        self.timeout = timeout
        self.heartbeat = heartbeat
        self.client_timeout = client_timeout

        self.running = True

        if timer is None:
            # Wild but use the module LOL
            self.timer = time
        else:
            self.timer = timer

        if logger is None:
            self.logger = logging.getLogger("raft {}".format(self.node_id))
        else:
            # Create a new logger wrapping old logger with node id
            class CustomAdapter(logging.LoggerAdapter):
                def process(self, msg, kwargs):
                    return "[node {}] {}".format(self.extra["node_id"], msg), kwargs

            self.logger = CustomAdapter(logger, {"node_id": self.node_id})

        # Try to load existing persistent state
        if not self.db.load_persistent():
            self.logger.info("No persistent state found, initializing new state")
            # Initialize to starting state
            self.db.set_term(0)
            self.db.set_status("follower")
            self.db.set_voted_for(None)
            self.db.set_log([])
            self.db.set_commit_index(0)
            self.db.set_last_applied(0)
            self.db.set_next_indexes_bulk({peer: 0 for peer in self.peers})
            self.db.set_match_indexes_bulk({peer: 0 for peer in self.peers})
            self.db.reset_votes()

        # On initialization we always start as a follower
        self.db.set_status("follower")

        # Lock
        self.lock = threading.Lock()
        self.client_request_lock = threading.Lock()

        self.reset_election_timeout()

        # message inboxes (must be locked, tried queue.Queue but behavior was strange)
        self.inbox = deque()
        self.outbox_flag = threading.Event()
        self.outbox = deque()

    def reset_election_timeout(self):
        """Reset the election timeout"""
        self.election_timeout = self.timer.time() + random.uniform(
            self.timeout, 2 * self.timeout
        )
        self.logger.debug(
            "Resetting election timeout to: {} from current {}".format(
                self.election_timeout, self.timer.time()
            )
        )
        self.last_heartbeat = self.timer.time()

    # Abstract methods that subclasses must implement
    @abstractmethod
    def send_message(self, peer, message):
        """Send a message to a peer"""
        pass

    @abstractmethod
    def auth_rpc(self, peer, rpc):
        """Entirely optional method to authenticate RPCs, could imagine using a signature or something"""
        return True

    def _step_down(self, rpc):
        """Let go of leader position and reset state
        NOTE: must be locked
        """
        self.logger.debug("Stepping down")
        self.db.set_status("follower")
        self.db.set_voted_for(None)
        self.db.set_term(rpc["term"])

        # Reset next indexes and match indexes
        self.db.set_next_indexes_bulk({peer: 0 for peer in self.peers})
        self.db.set_match_indexes_bulk({peer: 0 for peer in self.peers})
        # NOTE: don't think we need to respond to this
        return

    def _process_rpc(self, peer, rpc):
        """RPC is a pure Python object (up to subclass to deserialize)

        RPC types are:
            - request_vote
            - request_vote_reply
            - append_entry
            - append_entry_reply
            - add_peer (more experimental)
            - remove_peer
        """
        if not self.auth_rpc(peer, rpc):
            # TODO: log auth failure
            return
        if self.db.get_status() == "stopped":
            return

        # Check if term is up to date
        if rpc["term"] < self.db.get_term():
            # Ignore the RPC
            return

        rpc_type = rpc["type"]
        # Switch/case through all RPC types
        if rpc_type == "request_vote":
            self._process_request_vote(peer, rpc)
        elif rpc_type == "request_vote_reply":
            self._process_request_vote_reply(peer, rpc)
        elif rpc_type == "append_entry":
            self._process_append_entry(peer, rpc)
        elif rpc_type == "append_entry_reply":
            self._process_append_entry_reply(peer, rpc)
        elif rpc_type == "add_peer":
            # TODO
            pass
        elif rpc_type == "remove_peer":
            # TODO
            pass
        else:
            raise Exception("Unknown RPC type")

    def recv_message(self, message):
        """Receive a message from a peer"""
        self.logger.debug("Received message: {}".format(message))
        with self.lock:
            self.inbox.append(message)

    def queue_outbox(self, message):
        """Queue a message to be sent to a peer

        MUST BE LOCKED

        """
        self.logger.debug("Queuing message: {}".format(message))
        self.outbox.append(message)
        self.outbox_flag.set()

    def _process_request_vote(self, peer, rpc):
        """Process a request_vote RPC and schedule a reply (if necessary) in outbox

        from raft paper:

        Receiver implementation:
        1. Reply false if term < currentTerm (??5.1)
        2. If votedFor is null or candidateId, and candidate???s log is at
        least as up-to-date as receiver???s log, grant vote (??5.2, ??5.4)
        """
        # Always step down to follower if we see a higher term
        if rpc["term"] > self.db.get_term():
            self._step_down(rpc)

        # If term is less than current term, ignore

        vote_granted = True
        if rpc["term"] < self.db.get_term():
            vote_granted = False
        # Check if we've already voted for someone for this current term and it's not ourselves
        if (
            self.db.get_voted_for() is not None
            and self.db.get_voted_for() != self.node_id
        ):
            vote_granted = False
        # Check if the candidate's log is up to date
        curr_log = self.db.get_log()
        if self.db.get_log_length() > 0:
            if rpc["last_log_term"] < curr_log[-1]["term"]:
                vote_granted = False
            # If it's the same term, but we have a longer log, we can't vote for them
            if (
                rpc["last_log_term"] == curr_log[-1]["term"]
                and rpc["last_log_index"] < self.db.get_log_length() - 1
            ):
                vote_granted = False
        # Send a reply, all replies contained the original rpc
        msg = {
            "type": "request_vote_reply",
            "term": self.db.get_term(),
            "vote_granted": vote_granted,
            "node_id": self.node_id,
            "original_rpc": rpc,
        }
        if vote_granted:
            # Vote for the candidate
            self.db.set_voted_for(rpc["candidate_id"])
            # Reset election timeout
            self.reset_election_timeout()

        # Once it goes into the outbox it's as good as done from this node's perspective
        self.queue_outbox((peer, msg))

    def _process_request_vote_reply(self, peer, rpc):
        """Process a request_vote_reply RPC"""
        # Always step down to follower if we see a higher term
        if rpc["term"] > self.db.get_term():
            self._step_down(rpc)

        # Check if we're still a candidate
        if self.db.get_status() != "candidate":
            self.logger.debug(
                "Received a request_vote_reply but we are not a candidate"
            )
            return

        # Check if the reply is for the current term
        if rpc["term"] != self.db.get_term():
            self.logger.debug("Ignoring request_vote_reply for old term")
            return

        # Add to list of current votes + statuses
        self.db.add_vote(rpc["node_id"], rpc["vote_granted"])
        self.logger.debug(
            "Received a request_vote_reply from node {} with vote_granted={}".format(
                rpc["node_id"], rpc["vote_granted"]
            )
        )

        # Check if we've won the election
        if sum(self.db.get_votes().values()) > len(self.peers) / 2:
            # We've won the election!
            self.logger.info(
                "We won the election at term: {}!".format(self.db.get_term())
            )
            self.db.set_status("leader")
            # Reset the heartbeat timer
            self.reset_election_timeout()
            # Reset the votes
            self.db.reset_votes()
            # Reset the next index for each peer and match index
            log_len = self.db.get_log_length()
            self.db.set_next_indexes_bulk({peer: log_len for peer in self.peers})
            self.db.set_match_indexes_bulk({peer: 0 for peer in self.peers})

            # Send an empty append_entry to all peers
            msg = {
                "type": "append_entry",
                "term": self.db.get_term(),
                "leader_id": self.node_id,
                "prev_log_index": 0,
                "prev_log_term": 0,
                "entries": [],
                "leader_commit": self.db.get_commit_index(),
            }
            for t_peer in self.peers:
                self.queue_outbox((t_peer, msg))

    def _process_append_entry(self, peer, rpc):
        """Process an append_entry RPC and schedule a reply (if necessary) in outbox

        Taken from Raft paper:

        Receiver implementation:
        1. Reply false if term < currentTerm (??5.1)
        2. Reply false if log doesn???t contain an entry at prevLogIndex whose term matches prevLogTerm (??5.3)
        3. If an existing entry conflicts with a new one (same index
        but different terms), delete the existing entry and all that
        follow it (??5.3)
        4. Append any new entries not already in the log
        5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
        """
        # If it's from myself, ignore
        if rpc["leader_id"] == self.node_id:
            return
        # Always step down to follower if we see a higher term
        if rpc["term"] > self.db.get_term():
            self._step_down(rpc)
        # Or if we're a candidate and we see an append_entry from a leader with the right term
        if self.db.get_status() == "candidate" and rpc["term"] == self.db.get_term():
            self._step_down(rpc)

        success = True
        current_leader = True

        curr_log = self.db.get_log()
        curr_term = self.db.get_term()

        # 1. Reply false if term < currentTerm
        if rpc["term"] < curr_term:
            success = False
            current_leader = False

        # If curr_log is empty we take anything (scientific terms)
        log_len = self.db.get_log_length()
        if log_len > 0:
            # 2. Reply false if log doesn???t contain an entry at prevLogIndex whose term matches prevLogTerm
            if rpc["prev_log_index"] > log_len - 1:
                success = False
            # 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
            if (
                rpc["prev_log_index"] <= log_len - 1
                and curr_log[rpc["prev_log_index"]]["term"] != rpc["prev_log_term"]
            ):
                success = False
                # Need to delete all entries after prev_log_index
                self.db.set_log(curr_log[1 : rpc["prev_log_index"]])
                curr_log = self.db.get_log()

        if current_leader:
            # Reset the election timeout if the message is from the current leader
            self.reset_election_timeout()

        # 4. Append any new entries not already in the log
        if success:
            for entry in rpc["entries"]:
                if entry["index"] > self.db.get_log_length():
                    self.logger.debug("Appending entry from msg: {}".format(rpc))
                    self.logger.debug("Appending entry: {}".format(entry))
                    self.db.append_log(entry)

        # 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
        if rpc["leader_commit"] > self.db.get_commit_index():
            self.db.set_commit_index(
                min(rpc["leader_commit"], self.db.get_log()[-1]["index"])
            )

        # Send a reply
        msg = {
            "type": "append_entry_reply",
            "term": curr_term,
            "success": success,
            "node_id": self.node_id,
            "original_rpc": rpc,
        }
        self.queue_outbox((peer, msg))

    def _check_commit(self):
        """Check if we can commit any entries

        Taken from Raft paper:

        ??? If there exists an N such that N > commitIndex, a majority
        of matchIndex[i] ??? N, and log[N].term == currentTerm:
        set commitIndex = N (??5.3, ??5.4).
        """
        # Check if we can commit by computing max match index with majority
        match_index = self.db.get_match_indexes_bulk()
        match_index = sorted(match_index.values())
        if len(match_index) > 0:
            # Compute majority because if even (4 -> 2 = index 2 in sorted out of 5) if odd (3 -> 1 = index 0)
            majority = match_index[len(match_index) // 2]
            self.logger.debug("match index: {}".format(match_index))
            self.logger.debug(
                "Majority match index: {}, current commit index: {} and log: {}".format(
                    majority, self.db.get_commit_index(), self.db.get_log()
                )
            )
            if (
                majority > self.db.get_commit_index()
                and self.db.get_log()[majority]["term"] == self.db.get_term()
            ):
                self.logger.info("Majority match index is {}".format(majority))
                # Set commit index to majority
                self.logger.info("Committing index {}".format(majority))
                self.db.set_commit_index(majority)
                # Apply immediately
                # Check if commit index > last_applied
                if self.db.get_commit_index() > self.db.get_last_applied():
                    # Increment last_applied
                    self.db.set_last_applied(self.db.get_commit_index())
                    # TODO: here we would apply the log entry to the state machine
                    # but in practice the only thing that would change would be the peer list
                    # TODO: update peer list with add/remove operation

    def _process_append_entry_reply(self, peer, rpc):
        """Process an append entry reply

        Taken from Raft paper:

        - If successful: update nextIndex and matchIndex for follower (??5.3)
        - If AppendEntries fails because of log inconsistency: decrement nextIndex and retry (??5.3)
        """
        # Always step down to follower if we see a higher term
        if rpc["term"] > self.db.get_term():
            self._step_down(rpc)

        # Check if we're still a leader
        if self.db.get_status() != "leader":
            self.logger.debug("Received an append_entry_reply but we are not a leader")
            return

        # Check if the reply is for the current term
        if rpc["term"] != self.db.get_term():
            self.logger.debug("Ignoring append_entry_reply for old term")
            return

        # Check if the reply was successful
        if rpc["success"]:
            # Update nextIndex and matchIndex for follower
            self.db.set_next_index(
                peer,
                rpc["original_rpc"]["prev_log_index"]
                + len(rpc["original_rpc"]["entries"])
                + 1,
            )
            self.db.set_match_index(
                peer,
                rpc["original_rpc"]["prev_log_index"]
                + len(rpc["original_rpc"]["entries"]),
            )
            self.logger.debug(
                "Updating nextIndex and matchIndex for follower {} to {} and {}".format(
                    peer,
                    rpc["original_rpc"]["prev_log_index"]
                    + len(rpc["original_rpc"]["entries"])
                    + 1,
                    rpc["original_rpc"]["prev_log_index"]
                    + len(rpc["original_rpc"]["entries"]),
                )
            )
            # Check if we can commit any new entries
            self._check_commit()
        else:
            # Decrement nextIndex and retry
            self.db.set_next_index(peer, self.db.get_next_index(peer) - 1)
            # Send an append_entry to the peer
            curr_log = self.db.get_log()
            prev_log_index = max(0, self.db.get_next_index(peer) - 1)
            entries = curr_log[prev_log_index + 1 :]
            self.logger.debug(
                "Sending append_entry to {} with prev_log_index: {}, prev_log_term: {}, entries: {}, leader_commit: {}".format(
                    peer,
                    prev_log_index,
                    curr_log[prev_log_index]["term"],
                    entries,
                    self.db.get_commit_index(),
                )
            )
            assert prev_log_index + len(entries) == self.db.get_log_length()
            msg = {
                "type": "append_entry",
                "term": self.db.get_term(),
                "leader_id": self.node_id,
                "prev_log_index": prev_log_index,
                "prev_log_term": curr_log[prev_log_index]["term"],
                "entries": entries,
                "leader_commit": self.db.get_commit_index(),
            }
            self.queue_outbox((peer, msg))

    def start(self):
        """Start the internal loop"""
        with self.lock:
            self.running = True
        self.logger.info("Starting internal loop")
        self.internal_loop_thread = threading.Thread(target=self._internal_loop)
        self.internal_loop_thread.start()
        self.outbox_thread = threading.Thread(target=self._outbox_loop)
        self.outbox_thread.start()

    def stop(self):
        """Kill the internal loop"""
        with self.lock:
            self.running = False
            self.outbox_flag.set()
        self.internal_loop_thread.join()
        self.outbox_thread.join()

    def pub_is_leader(self):
        """Return True if this node is the leader

        It is not sufficient to check the state of the node, because it may be isolated in a non-quorum
        partition. This node will believe it is a leader but the true test is whether
        it can achieve majority consensus on a log entry.

        So we need to send an append_entry RPC to all peers and see if we get a majority of replies.
        At which point we return True.
        """
        # need to acquire a global client request lock to prevent concurrent requests for simplicity
        with self.client_request_lock:
            # Run two threads
            def process_client_request():
                """Send a client request to all peers and wait for a majority of replies"""
                with self.lock:
                    # If status is not leader return False immediately
                    if self.db.get_status() != "leader":
                        return False
                    # NOTE: Uncomment below to break leader election
                    # this will return multiple leaders and the event of network partition
                    # ========================
                    # else:
                    #     return True
                    # ========================

                    # The rest of the work is for network partitions and multiple leaders
                    self.logger.debug("Appending empty entry to log to test for leader")
                    # First prune local log to match commit index
                    self.db.set_log(
                        self.db.get_log()[1 : self.db.get_commit_index() + 1]
                    )
                    # Append a check_leader entry to the log
                    self.db.append_log(
                        {
                            "term": self.db.get_term(),
                            "index": self.db.get_log_length() + 1,
                            "command": "check_leader",
                        }
                    )
                    self.logger.debug(
                        "latest log is now {}".format(self.db.get_log()[-1])
                    )

            def check_leader():
                # Now create a wait condition for commit of the entry, and spin a separate thread to check it every 10ms
                # after 250ms we give up and return False
                start_time = time.time()
                while True:
                    with self.lock:
                        self.logger.debug(
                            "Comparing last applied index {} to log length {}".format(
                                self.db.get_last_applied(), self.db.get_log()
                            )
                        )
                        if self.db.get_last_applied() >= self.db.get_log_length():
                            self.logger.info("Leader check succeeded")
                            return
                    time.sleep(self.client_timeout / 10.0)
                    if time.time() - start_time > self.client_timeout:
                        self.logger.info("Leader check timed out")
                        return

            with self.lock:
                # if not leader bail immediately
                if self.db.get_status() != "leader":
                    return False
            # Start process_client_request thread and join it before starting wait thread
            process_client_request_thread = threading.Thread(
                target=process_client_request
            )
            process_client_request_thread.start()
            process_client_request_thread.join()
            # Now start the wait thread and join it
            check_leader_thread = threading.Thread(target=check_leader)
            check_leader_thread.start()
            check_leader_thread.join()
            # Now check if we have a majority of replies
            with self.lock:
                return (
                    self.db.get_status() == "leader"
                    and self.db.get_last_applied() >= self.db.get_log_length()
                )

    def _start_election(self):
        """Start an election process. Only side-effects are to set state and increment term
        and schedule messages in outbox
        """
        # Increment term
        self.db.set_term(self.db.get_term() + 1)
        self.logger.debug("Starting election with term {}".format(self.db.get_term()))
        self.logger.info("Setting status to candidate")
        # Set state to candidate
        self.db.set_status("candidate")
        # Reset last_heartbeat and election timeout
        self.reset_election_timeout()
        # Vote for self
        self.db.reset_votes()
        self.db.set_voted_for(self.node_id)
        # Send request_vote to all peers
        curr_log = self.db.get_log()
        message = {
            "type": "request_vote",
            "term": self.db.get_term(),
            "candidate_id": self.node_id,
            "last_log_index": max(0, len(curr_log) - 1),
            "last_log_term": curr_log[len(curr_log) - 1]["term"]
            if self.db.get_log_length() > 0
            else 0,
        }
        # Already locked
        for peer in self.peers:
            self.queue_outbox((peer, message))

    def _internal_loop(self):
        """This is an internal maintenance loop, for election timeouts etc

        Invariants: all lock acquisition is done in this method

        Checks (in terms of priority):
        1. Are we in an active election?
        1. If not leader, check if election timeout has passed. If so, start a new election.
        """
        # Select from the inbox or a timeout
        while True:
            if not self.running:
                return

            # Check for election timeout or need to heartbeat
            with self.lock:
                is_leader = self.db.get_status() == "leader"
                election_elapsed = self.timer.time() > self.election_timeout
                if not is_leader and election_elapsed:
                    self._start_election()

            # Pull all messages from inbox non-blocking until empty (non-blocking)
            with self.lock:
                for peer, rpc in self.inbox:
                    self._process_rpc(peer, rpc)
                self.inbox.clear()

            # If you're the leader, send some heartbeat messages ONLY IF LEADER
            # Send heartbeat
            with self.lock:
                if self.db.get_status() == "leader":
                    # Tricky bit, need to do some bookkeeping logic from paper
                    # ========================================================
                    # Either send a heartbeat if nextIndex is <= log length or send an actual set of entries
                    # If last log index ??? nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
                    # ========================================================

                    # Check all next_index values against last log index
                    for peer, next_index in self.db.get_next_indexes_bulk().items():
                        if peer == self.node_id:
                            # Just update the next index to be the log length+1
                            self.logger.debug(
                                "Updating next index for self {} to {}".format(
                                    peer, self.db.get_log_length() + 1
                                )
                            )
                            self.db.set_next_index(
                                self.node_id, self.db.get_log_length() + 1
                            )
                            self.db.set_match_index(
                                self.node_id, self.db.get_log_length()
                            )
                            continue
                        curr_log = self.db.get_log()
                        # Send append_entry RPC, but will effectively be a heartbeat
                        next_index = max(1, next_index)
                        # If next_index is 2, then we send [sentinal, 1,(now start) 2] or [2:]
                        entries = curr_log[next_index:]
                        # Log next_index, entries and log length
                        self.logger.debug(
                            "IMPORTANT: next_index is {} and entries is {} and log length is {}".format(
                                next_index, entries, self.db.get_log_length()
                            )
                        )
                        assert next_index - 1 + len(entries) == self.db.get_log_length()
                        message = {
                            "type": "append_entry",
                            "term": self.db.get_term(),
                            "leader_id": self.node_id,
                            "prev_log_index": next_index - 1,
                            "prev_log_term": curr_log[next_index - 1]["term"]
                            if next_index > 1
                            else 0,
                            "entries": entries,
                            "leader_commit": self.db.get_commit_index(),
                        }
                        self.queue_outbox((peer, message))

            # Finally sleep until either next election timeout or next heartbeat if leader
            sleep_time = (
                min(self.election_timeout - self.timer.time(), self.heartbeat)
                if not is_leader
                else self.heartbeat
            )
            time.sleep(max(0, sleep_time))

    def _outbox_loop(self):
        """This is a loop that just sends messages in the outbox

        Separated into a different thread because for memory-based network send_message might be effectively blocking
        """
        while True:
            # Wait for outbox flag, race condition unless you lock self.lock unfortunately so we have a timeout
            self.outbox_flag.wait(timeout=0.050)
            if not self.running:
                return
            messages = []
            try:
                self.lock.acquire()
                messages = self.outbox.copy()
                self.outbox.clear()
                # clear outbox flag
                self.outbox_flag.clear()
            finally:
                self.lock.release()
            # Do this without lock
            for peer, message in messages:
                self.send_message(peer, message)
