import time
import random
import logging

import threading
import queue
import select

from abc import ABC, abstractmethod

class RaftBase(ABC):
    '''A basic Raft implementation which accesses everything through expected methods for abstraction'''

    def __init__(self, node_id, peers, db, timer=None, logger=None):
        '''Initialize the RaftBase object
        
        Inject the timer to allow for easier testing
        '''
        self.node_id = node_id
        self.peers = peers
        self.db = db

        self.running = True

        if timer is None:
            # Wild but use the module LOL
            self.timer = time
        else:
            self.timer = timer
        
        if logger is None:
            self.logger = logging.getLogger('raft {}'.format(self.node_id))
        else:
            self.logger = logger

        # Try to load existing persistent state
        if not self.db.load_persistent():
            self.logger.info('No persistent state found, initializing new state')
            # Initialize to starting state
            self.db.set_term(0)
            self.db.set_status('follower')
            self.db.set_voted_for(None)
            self.db.set_log([])
            self.db.set_commit_index(0)
            self.db.set_last_applied(0)
            self.db.set_next_indexes_bulk({peer: 0 for peer in self.peers})
            self.db.set_match_indexes_bulk({peer: 0 for peer in self.peers})
            self.db.reset_votes()

        # On initialization we always start as a follower
        self.db.set_status('follower')

        # Lock
        self.lock = threading.Lock()

        self.election_timeout = 0.15 # 150ms
        self.last_heartbeat = self.timer.time()

        # Threadsafe message inbox
        self.inbox = queue.Queue()
        self.outbox = queue.Queue()


    # Abstract methods that subclasses must implement
    @abstractmethod
    def send_message(self, peer, message):
        '''Send a message to a peer'''
        pass


    @abstractmethod
    def auth_rpc(self, peer, rpc):
        '''Entirely optional method to authenticate RPCs, could imagine using a signature or something'''
        return True


    def _step_down(self, rpc):
        '''Let go of leader position and reset state
        NOTE: must be locked
        '''
        self.logger.debug('Stepping down')
        self.db.set_term(rpc['term'])
        self.db.set_status('follower')
        self.db.set_voted_for(None)
        # NOTE: don't think we need to respond to this
        return


    def _process_rpc(self, peer, rpc):
        '''RPC is a pure Python object (up to subclass to deserialize)

        RPC types are:
            - request_vote
            - request_vote_reply
            - append_entry
            - append_entry_reply
            - add_peer (more experimental)
            - remove_peer
        '''
        if not self.auth_rpc(peer, rpc):
            # TODO: log auth failure
            return
        if self.db.get_status() == 'stopped':
            return

        # Check if term is up to date
        if rpc['term'] < self.db.get_term():
            # Ignore the RPC
            return

        rpc_type = rpc['type']
        # Switch/case through all RPC types
        if rpc_type == 'request_vote':
            self._process_request_vote(peer, rpc)
        elif rpc_type == 'request_vote_reply':
            self._process_request_vote_reply(peer, rpc)
        elif rpc_type == 'append_entry':
            self._process_append_entry(peer, rpc)
        elif rpc_type == 'append_entry_reply':
            self._process_append_entry_reply(peer, rpc)
        elif rpc_type == 'add_peer':
            # TODO
            pass
        elif rpc_type == 'remove_peer':
            # TODO
            pass
        else:
            raise Exception('Unknown RPC type')


    def recv_message(self, message):
        '''Receive a message from a peer'''
        self.inbox.put(message)


    def _process_request_vote(self, peer, rpc):
        '''Process a request_vote RPC and schedule a reply (if necessary) in outbox

        from raft paper:

        Receiver implementation:
        1. Reply false if term < currentTerm (§5.1)
        2. If votedFor is null or candidateId, and candidate’s log is at
        least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
        '''
        # Always step down to follower if we see a higher term
        if rpc['term'] > self.db.get_term():
            self._step_down(rpc)

        # If term is less than current term, ignore

        vote_granted = True
        if rpc['term'] < self.db.get_term():
            vote_granted = False
        # Check if we've already voted for someone for this current term and it's not ourselves
        if self.db.get_voted_for() is not None and self.db.get_voted_for() != self.node_id:
            vote_granted = False
        # Check if the candidate's log is up to date
        curr_log = self.db.get_log()
        if self.db.get_log_length() > 0:
            if rpc['last_log_term'] < curr_log[-1]['term']:
                vote_granted = False
            # If it's the same term, but we have a longer log, we can't vote for them
            if rpc['last_log_term'] == curr_log[-1]['term'] and rpc['last_log_index'] < self.db.get_log_length() - 1:
                vote_granted = False
        # Send a reply, all replies contained the original rpc
        msg = {
            'type': 'request_vote_reply',
            'term': self.db.get_term(),
            'vote_granted': vote_granted,
            'node_id': self.node_id,
            'original_rpc': rpc
        }
        if vote_granted:
            # Vote for the candidate
            self.db.set_voted_for(rpc['candidate_id'])
            # Reset election timeout
            self.last_heartbeat = self.timer.time()
            self.election_timeout = random.uniform(0.15, 0.3)

        # Once it goes into the outbox it's as good as done from this node's perspective
        self.outbox.put((peer, msg))


    def _process_request_vote_reply(self, peer, rpc):
        '''Process a request_vote_reply RPC'''
        # Always step down to follower if we see a higher term
        if rpc['term'] > self.db.get_term():
            self._step_down(rpc)

        # Check if we're still a candidate
        if self.db.get_status() != 'candidate':
            self.logger.debug('Received a request_vote_reply but we are not a candidate')
            return

        # Check if the reply is for the current term
        if rpc['term'] != self.db.get_term():
            self.logger.debug('Ignoring request_vote_reply for old term')
            return

        # Add to list of current votes + statuses
        self.db.add_vote(rpc['node_id'], rpc['vote_granted'])
        self.logger.debug('Received a request_vote_reply from node {} with vote_granted={}'.format(
            rpc['node_id'], rpc['vote_granted']))

        # Check if we've won the election
        if sum(self.db.get_votes().values()) > len(self.peers) / 2:
            # We've won the election!
            self.logger.debug('We won the election!')
            self.db.set_status('leader')
            # Reset the heartbeat timer
            self.last_heartbeat = self.timer.time()
            self.election_timeout = random.uniform(0.15, 0.3)
            # Reset the votes
            self.db.reset_votes()
            # Reset the next index for each peer and match index
            log_len = self.db.get_log_length()
            self.db.set_next_indexes_bulk({peer: log_len for peer in self.peers})
            self.db.set_match_indexes_bulk({peer: 0 for peer in self.peers})

            # Send an empty append_entry to all peers
            msg = {
                'type': 'append_entry',
                'term': self.db.get_term(),
                'leader_id': self.node_id,
                'prev_log_index': 0,
                'prev_log_term': 0,
                'entries': [],
                'leader_commit': self.db.get_commit_index(),
            }
            for t_peer in self.peers:
                self.outbox.put((t_peer, msg))


    def _process_append_entry(self, peer, rpc):
        '''Process an append_entry RPC and schedule a reply (if necessary) in outbox

        Taken from Raft paper:

        Receiver implementation:
        1. Reply false if term < currentTerm (§5.1)
        2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
        3. If an existing entry conflicts with a new one (same index
        but different terms), delete the existing entry and all that
        follow it (§5.3)
        4. Append any new entries not already in the log
        5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
        '''
        # If it's from myself, ignore
        if rpc['leader_id'] == self.node_id:
            return
        # Always step down to follower if we see a higher term
        if rpc['term'] > self.db.get_term():
            self._step_down(rpc)
        # Or if we're a candidate and we see an append_entry from a leader with the right term
        if self.db.get_status() == 'candidate' and rpc['term'] == self.db.get_term():
            self._step_down(rpc)
        success = True

        curr_log = self.db.get_log()
        curr_term = self.db.get_term()

        # 1. Reply false if term < currentTerm
        if rpc['term'] < curr_term:
            success = False

        # If curr_log is empty we take anything (scientific terms)
        log_len = self.db.get_log_length()
        if log_len > 0:
            # 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
            if rpc['prev_log_index'] > log_len - 1:
                success = False
            # 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
            if rpc['prev_log_index'] <= log_len - 1 and curr_log[rpc['prev_log_index']]['term'] != rpc['prev_log_term']:
                success = False
                # Need to delete all entries after prev_log_index
                self.db.set_log(curr_log[:rpc['prev_log_index']])
                curr_log = self.db.get_log()

        # 4. Append any new entries not already in the log
        if success:
            for entry in rpc['entries']:
                if entry['index'] > log_len - 1:
                    self.db.append_log(entry)
            # Reset the election timeout
            self.last_heartbeat = self.timer.time()
            self.election_timeout = random.uniform(0.15, 0.3)
        # 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
        if rpc['leader_commit'] > self.db.get_commit_index():
            self.db.set_commit_index(min(rpc['leader_commit'], self.db.get_log_length() - 1))

        # Send a reply
        msg = {
            'type': 'append_entry_reply',
            'term': curr_term,
            'success': success,
            'node_id': self.node_id,
            'original_rpc': rpc
        }
        self.outbox.put((peer, msg))


    def _check_commit(self):
        '''Check if we can commit any entries

        Taken from Raft paper:

        • If there exists an N such that N > commitIndex, a majority
        of matchIndex[i] ≥ N, and log[N].term == currentTerm:
        set commitIndex = N (§5.3, §5.4).
        '''
        # Check if we can commit by computing max match index with majority
        match_index = self.db.get_match_indexes_bulk()
        match_index = sorted(match_index.values())
        if len(match_index) > 0:
            # Compute majority
            majority = match_index[len(match_index) // 2]
            if majority > self.db.get_commit_index() and self.db.get_log()[majority]['term'] == self.db.get_term():
                self.logger.debug('Majority match index is {}'.format(majority))
                # Set commit index to majority
                self.db.set_commit_index(majority)

    def _process_append_entry_reply(self, peer, rpc):
        '''Process an append entry reply 

        Taken from Raft paper:

        - If successful: update nextIndex and matchIndex for follower (§5.3)
        - If AppendEntries fails because of log inconsistency: decrement nextIndex and retry (§5.3)
        '''
        # Always step down to follower if we see a higher term
        if rpc['term'] > self.db.get_term():
            self._step_down(rpc)

        # Check if we're still a leader
        if self.db.get_status() != 'leader':
            self.logger.debug('Received an append_entry_reply but we are not a leader')
            return

        # Check if the reply is for the current term
        if rpc['term'] != self.db.get_term():
            self.logger.debug('Ignoring append_entry_reply for old term')
            return

        # Check if the reply was successful
        if rpc['success']:
            # Update nextIndex and matchIndex for follower
            self.db.set_next_index(peer, rpc['original_rpc']['prev_log_index'] + len(rpc['original_rpc']['entries']) + 1)
            self.db.set_match_index(peer, rpc['original_rpc']['prev_log_index'] + len(rpc['original_rpc']['entries']))
            # Check if we can commit any new entries
            self._check_commit()
        else:
            # Decrement nextIndex and retry
            self.db.set_next_index(peer, self.db.get_next_index(peer) - 1)
            # Send an append_entry to the peer
            curr_log = self.db.get_log()
            msg = {
                'type': 'append_entry',
                'term': self.db.get_term(),
                'leader_id': self.node_id,
                'prev_log_index': self.db.get_next_index(peer) - 1,
                'prev_log_term': curr_log[self.db.get_next_index(peer) - 1]['term'] if self.db.get_log_length() > 0 else 0,
                'entries': curr_log[self.db.get_next_index(peer):],
                'leader_commit': self.db.get_commit_index(),
            }
            self.outbox.put((peer, msg))


    def start(self):
        '''Start the internal loop'''
        with self.lock:
            self.running = True
        self.internal_loop_thread = threading.Thread(target=self._internal_loop)
        self.internal_loop_thread.start()


    def _send_heartbeats(self):
        '''Send empty append_entry RPCs to all peers'''
        msg = {
            'type': 'append_entry',
            'term': self.db.get_term(),
            'leader_id': self.node_id,
            'prev_log_index': 0,
            'prev_log_term': 0,
            'entries': [],
            'leader_commit': self.db.get_commit_index(),
        }
        for t_peer in self.peers:
            self.outbox.put((t_peer, msg))


    def stop(self):
        '''Kill the internal loop'''
        with self.lock:
            self.running = False
        self.internal_loop_thread.join()


    def pub_is_leader(self):
        '''Return True if this node is the leader

        It is not sufficient to check the state of the node, because it may be isolated in a non-quorum
        partition. This node will believe it is a leader but the true test is whether
        it can achieve majority consensus on a log entry.

        So we need to send an append_entry RPC to all peers and see if we get a majority of replies.
        At which point we return True.
        '''
        # Acquire the lock to get consistent state
        # NOTE: might be possible to do this without the lock, but it's safer and costs little
        try:
            self.lock.acquire()
            return self.db.get_status() == 'leader'
        finally:
            self.lock.release()

    def _start_election(self):
        '''Start an election process. Only side-effects are to set state and increment term
        and schedule messages in outbox
        '''
        # Increment term
        self.db.set_term(self.db.get_term() + 1)
        self.logger.debug('Starting election with term {}'.format(self.db.get_term()))
        # Set state to candidate
        self.db.set_status('candidate')
        # Reset last_heartbeat and election timeout
        self.last_heartbeat = self.timer.time()
        self.election_timeout = random.uniform(0.15, 0.3)
        # Vote for self
        self.db.reset_votes()
        self.db.set_voted_for(self.node_id)
        # Send request_vote to all peers
        curr_log = self.db.get_log()
        message = {
            'type': 'request_vote',
            'term': self.db.get_term(),
            'candidate_id': self.node_id,
            'last_log_index': max(0, len(curr_log) - 1),
            'last_log_term': curr_log[len(curr_log) - 1]['term'] if self.db.get_log_length() > 0 else 0,
        }
        for peer in self.peers:
            self.outbox.put((peer, message))

    def _internal_loop(self):
        '''This is an internal maintenance loop, for election timeouts etc

        Invariants: all lock acquisition is done in this method

        Checks (in terms of priority):
        1. Are we in an active election?
        1. If not leader, check if election timeout has passed. If so, start a new election.
        '''
        # Select from the inbox or a timeout
        while True:
            if not self.running:
                return

            # Check for election timeout or need to heartbeat
            is_leader = self.db.get_status() == 'leader'
            election_elapsed = self.timer.time() - self.last_heartbeat > self.election_timeout
            if not is_leader and election_elapsed:
                with self.lock:
                    self._start_election()

            # Check if commit index > last_applied
            if self.db.get_commit_index() > self.db.get_last_applied():
                # Increment last_applied
                self.db.increment_last_applied()
                # TODO: here we would apply the log entry to the state machine
                # but in practice the only thing that would change would be the peer list
                # TODO: update peer list with add/remove operation

            # Pull all messages from inbox non-blocking until empty (non-blocking)
            limit = 10
            while not self.inbox.empty() and limit > 0:
                try:
                    peer, rpc = self.inbox.get_nowait()
                    # Only need to lock processing cause queue is thread-safe
                    with self.lock:
                        self._process_rpc(peer, rpc)
                    limit -= 1
                except queue.Empty:
                    break

            # If you're the leader, send some heartbeat messages ONLY IF LEADER
            if is_leader:
                # Send heartbeat
                with self.lock:
                    self._send_heartbeats()
                    # Tricky bit, need to do some bookkeeping logic from paper
                    # ========================================================
                    # If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
                    # ========================================================

                    # Check all next_index values against last log index
                    for peer, next_index in self.db.get_next_indexes_bulk().items():
                        curr_log = self.db.get_log()
                        if next_index <= len(curr_log) - 1:
                            # Send append_entry RPC
                            message = {
                                'type': 'append_entry',
                                'term': self.db.get_term(),
                                'leader_id': self.node_id,
                                'prev_log_index': next_index - 1,
                                'prev_log_term': curr_log[next_index - 1]['term'] if next_index > 0 else 0,
                                'entries': curr_log[next_index:],
                                'leader_commit': self.db.get_commit_index(),
                            }
                            self.outbox.put((peer, message))

            # Send all messages in outbox, no need to lock here
            while not self.outbox.empty():
                peer, message = self.outbox.get()
                # Shouldn't need to check for outdated because protocol is robust
                # to out of order messages (or should be)
                self.send_message(peer, message)

            # Finally sleep until either next election timeout or next heartbeat if leader
            next_heartbeat_delay = 0.03
            sleep_time = min(self.election_timeout, next_heartbeat_delay)
            time.sleep(sleep_time)
