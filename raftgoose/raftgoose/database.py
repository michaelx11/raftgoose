from abc import ABC, abstractmethod


class Database(ABC):
    '''An abstract database interface for Raft clients'''

    def __init__(self):
        pass

    @abstractmethod
    def write_all_state(self):
        '''for simplicity sake just write all state at once'''
        pass

    @abstractmethod
    def read_all_state(self):
        '''similarly for simplicity's sake just read all state at once'''
        pass

    def load_persistent(self):
        '''Try to loads some kind of state, if empty then return false'''
        if self.read_all_state():
            return True
        return False

    def set_term(self, term):
        '''highest term server has seen'''
        state = self.read_all_state()
        state['term'] = term
        self.write_all_state(state)

    def get_term(self):
        return self.read_all_state().get('term', 0)

    def set_voted_for(self, voted_for):
        '''candidate id that received vote in current term, starts None'''
        state = self.read_all_state()
        state['voted_for'] = voted_for
        self.write_all_state(state)

    def get_voted_for(self):
        return self.read_all_state().get('voted_for', None)

    def set_status(self, status):
        '''status of the server, either follower, candidate, or leader'''
        state = self.read_all_state()
        state['status'] = status
        self.write_all_state(state)

    def get_status(self):
        return self.read_all_state().get('status', 'follower')

    def set_log(self, log):
        '''Set the entire log at once'''
        state = self.read_all_state()
        state['log'] = log
        self.write_all_state(state)

    def get_log(self):
        return self.read_all_state().get('log', [])

    def set_commit_index(self, commit_index):
        '''Volatile according to the paper, but gonna persist anyways I think'''
        state = self.read_all_state()
        state['commit_index'] = commit_index
        self.write_all_state(state)

    def get_commit_index(self):
        return self.read_all_state().get('commit_index', 0)

    def set_last_applied(self, last_applied):
        '''Also volatile according to the paper'''
        state = self.read_all_state()
        state['last_applied'] = last_applied
        self.write_all_state(state)

    def get_last_applied(self):
        return self.read_all_state().get('last_applied', 0)

    def set_next_index(self, next_index):
        state = self.read_all_state()
        state['next_index'] = next_index
        self.write_all_state(state)

    def set_match_index(self, match_index):
        state = self.read_all_state()
        state['match_index'] = match_index
        self.write_all_state(state)

    def reset_votes(self):
        '''Reset votes for a new election, set 'votes' to empty dict'''
        state = self.read_all_state()
        state['votes'] = {}
        state['voted_for'] = None
        self.write_all_state(state)

    def add_vote(self, candidate_id, vote):
        '''Add a vote for a candidate'''
        state = self.read_all_state()
        if 'votes' not in state:
            state['votes'] = {}
        state['votes'][candidate_id] = vote
        self.write_all_state(state)

    def get_votes(self):
        return self.read_all_state().get('votes', {})

    def get_state(self):
        return self.read_all_state()
