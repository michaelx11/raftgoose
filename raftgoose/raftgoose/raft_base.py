import time
import random
import logging

import threading
import queue
import select

from abc import ABC, abstractmethod

class RaftBase(ABC):
    '''A basic Raft implementation which accesses everything through expected methods for abstraction'''

    def __init__(self, node_id, peers, db, timer, logger=None):
        '''Initialize the RaftBase object
        
        Inject the timer to allow for easier testing
        '''
        self.node_id = node_id
        self.peers = peers
        self.db = db

        # Try to load existing persistent state
        if not self.db.load_persistent():
            # Initialize to starting state
            self.db.set_term(0)
            self.db.set_state('follower')
            self.db.set_voted_for(None)
            self.db.set_log([])
            self.db.set_commit_index(0)
            self.db.set_last_applied(0)
            self.db.set_next_index({peer: 0 for peer in self.peers})
            self.db.set_match_index({peer: 0 for peer in self.peers})
            self.db.reset_votes()

        # On initialization we always start as a follower
        self.db.set_state('follower')

        # Lock
        self.lock = threading.Lock()

        self.election_timeout = 0.15 # 150ms
        self.last_heartbeat = timer.time()

        # Threadsafe message inbox
        self.inbox = queue.Queue()
        self.outbox = queue.Queue()
        
        if logger is None:
            self.logger = logging.getLogger('raft {}'.format(self.node_id))
        else:
            self.logger = logger


    # Abstract methods that subclasses must implement
    @abstractmethod
    def send_message(self, peer, message):
        '''Send a message to a peer'''
        pass


    @abstractmethod
    def auth_rpc(self, rpc):
        '''Entirely optional method to authenticate RPCs, could imagine using a signature or something'''
        return True


    def _step_down(self):
        '''Let go of leader position and reset state
        NOTE: must be locked
        '''
        self.logger.debug('Stepping down')
        self.db.set_term(rpc['term'])
        self.db.set_state('follower')
        self.db.set_voted_for(None)
        # NOTE: don't think we need to respond to this
        return


    def _process_rpc(self, rpc):
        '''RPC is a pure Python object (up to subclass to deserialize)

        RPC types are:
            - request_vote
            - request_vote_reply
            - append_entry (singular, to keep things simple)
            - append_entry_reply
            - add_peer (more experimental)
            - remove_peer
        '''
        if not self.auth_rpc(rpc):
            # TODO: log auth failure
            return
        if self.db.get_state() == 'stopped':
            return

        # Check if term is up to date
        if rpc['term'] < self.db.get_term():
            # Ignore the RPC
            return

        rpc_type = rpc['type']
        # Switch/case through all RPC types
        if rpc_type == 'request_vote':
            self._process_request_vote(rpc)
        elif rpc_type == 'request_vote_reply':
            self._process_request_vote_reply(rpc)
        elif rpc_type == 'append_entry':
            self._process_append_entry(rpc)
        elif rpc_type == 'append_entry_reply':
            self._process_append_entry_reply(rpc)
        elif rpc_type == 'add_peer':
            # TODO
        elif rpc_type == 'remove_peer':
            # TODO
        else:
            raise Exception('Unknown RPC type')


    def recv_message(self, message):
        '''Receive a message from a peer'''
        self.inbox.put(message)


    def _process_request_vote(self, rpc):
        '''Process a request_vote RPC and schedule a reply (if necessary) in outbox
        '''
        # If we're the leader and we see a higher term, we need to step down but continue to reply to message
        if self.db.get_state() == 'leader' and rpc['term'] > self.db.get_term():
            _step_down()

        vote_granted = True
        # Check if we've already voted for someone for this current term
        if self.db.get_voted_for() is not None:
            vote_granted = False
        # Check if the candidate's log is up to date
        if rpc['last_log_term'] < self.db.get_log()[-1]['term']:
            vote_granted = False
        # If it's the same term, but we have a longer log, we can't vote for them
        if rpc['last_log_term'] == self.db.get_log()[-1]['term'] and rpc['last_log_index'] < len(self.db.get_log()) - 1:
            vote_granted = False
        # Send a reply
        msg = {
            'type': 'request_vote_reply',
            'term': self.db.get_term(),
            'vote_granted': vote_granted,
        }
        if vote_granted:
            # Vote for the candidate
            self.db.set_voted_for(rpc['candidate_id'])
        # Once it goes into the outbox it's as good as done from this node's perspective
        self.outbox.put(msg)


    def _process_request_vote_reply(self, rpc):
        '''Process a request_vote_reply RPC'''
        # If we're the leader and we see a higher term, we need to step down but continue rest of processing
        if self.db.get_state() == 'leader' and rpc['term'] > self.db.get_term():
            _step_down()

        # Check if we're still a candidate
        if self.db.get_state() != 'candidate':
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
        if self.db.get_votes() > len(self.peers) / 2:
            # We've won the election!
            self.logger.debug('We won the election!')
            self.db.set_state('leader')
            # Reset the heartbeat timer
            self.last_heartbeat = self.timer.time()
            self.election_timeout = random.uniform(0.15, 0.3)
            # Reset the votes
            self.db.reset_votes()
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
            self.outbox.put(msg)


    def _process_append_entry(self, rpc):
        '''Process an append_entry RPC and schedule a reply (if necessary) in outbox
        '''
        # If we're the leader and we see a higher term, we need to step down but continue to reply to message
        if self.db.get_state() == 'leader' and rpc['term'] > self.db.get_term():
            _step_down()

        # Check if the RPC is for the current term
        if rpc['term'] != self.db.get_term():
            return
        # Check if the log is up to date
        if rpc['prev_log_index'] > len(self.db.get_log()) - 1:
            pass

    def _process_append_entry_reply(self, rpc):
        '''Process an append entry reply 
        '''
        # If we're the leader and we see a higher term, we need to step down but continue rest of processing
        if self.db.get_state() == 'leader' and rpc['term'] > self.db.get_term():
            _step_down()



    def start_internal_loop(self):
        '''Start the internal loop'''
        self.internal_loop_thread = threading.Thread(target=self._internal_loop)
        self.internal_loop_thread.start()


    def kill(self):
        '''Kill the internal loop'''
        self.internal_loop_thread.join()
        self.db.set_state('stopped')


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
            return self.db.get_state() == 'leader'
        finally:
            self.lock.release()

    def _start_election(self):
        '''Start an election process. Only side-effects are to set state and increment term
        and schedule messages in outbox
        '''
        # Increment term
        self.db.increment_term()
        # Set state to candidate
        self.db.set_state('candidate')
        # Reset last_heartbeat and election timeout
        self.last_heartbeat = self.timer.time()
        self.election_timeout = random.uniform(0.15, 0.3)
        # Vote for self
        self.db.reset_votes()
        self.db.add_vote(rpc['node_id'], rpc['vote_granted'])
        self.db.set_voted_for(self.node_id)
        # Send request_vote to all peers
        message = {
            'type': 'request_vote',
            'term': self.db.get_term(),
            'candidate_id': self.node_id,
            'last_log_index': len(self.db.get_log()) - 1,
            'last_log_term': self.db.get_log()[-1]['term']
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
            # Pull all messages from inbox non-blocking until empty (non-blocking)
            limit = 100
            while not self.inbox.empty() and limit > 0:
                try:
                    rpc = self.inbox.get_nowait()
                    # Only need to lock processing cause queue is thread-safe
                    with self.lock:
                        self._process_rpc(rpc)
                    limit -= 1
                except queue.Empty:
                    break

            # Check for election timeout or need to heartbeat
            is_leader = self.db.get_state() == 'leader'
            election_elapsed = self.timer.time() - self.last_heartbeat > self.election_timeout
            if not is_leader and election_elapsed:
                with self.lock:
                    self._start_election()

            # If you're the leader, send some heartbeat messages ONLY IF LEADER
            if is_leader:
                # Send heartbeat
                with self.lock:
                    self._send_heartbeats()

            # Send all messages in outbox, no need to lock here
            while not self.outbox.empty():
                peer, message = self.outbox.get()
                # Shouldn't need to check for outdated because protocol is robust
                # to out of order messages (or should be)
                self.send_message(peer, message)

            # Finally sleep until either next election timeout or next heartbeat if leader
            next_heartbeat_delay = 0.05
            sleep_time = min(self.election_timeout, next_heartbeat_delay)
            time.sleep(sleep_time)
