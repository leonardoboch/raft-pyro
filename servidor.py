import sys
import threading
import time
import random

import Pyro5.server
import Pyro5.api
from Pyro5.core import locate_ns
from Pyro5.server import expose

def main(object_id):
    try:
        name = "Object" + str(object_id)
        print(f"Starting node: {name}")
        Node(name, object_id, [
            "PYRO:Object@localhost:7001",
            "PYRO:Object@localhost:7002",
            "PYRO:Object@localhost:7003",
            "PYRO:Object@localhost:7004",
        ])
    except Pyro5.errors.CommunicationError as ce:
        print("Communication error:", ce)
    except Pyro5.errors.NamingError as ne:
        print("Error locating the Name Server:", ne)
    except Exception as e:
        print("An unexpected error occurred:", e)

@expose
class Node(object):
    def __init__(self, node_name: str, node_id: int, uris):
        self.role = 'follower'
        self.name = node_name
        self.id = node_id
        self.current_term = 0
        self.voted_for = None
        self.log = []
        self.current_leader = None
        self.uris = uris
        self.last_heartbeat = 0
        self.value = 0
        self.uncommitted_value = 0

        port = 7000 + node_id
        daemon = Pyro5.server.Daemon(port=port)
        self.uri = daemon.register(self, node_name)
        print(f"Node started with uri Node {self.id}")
        threading.Thread(target=self.run).start()
        threading.Thread(target=self.send_heartbeat).start()
        daemon.requestLoop()

    # INICIO e HEARTBEATS
    def run(self):
        time.sleep(random.randint(5, 10))
        while True:
            sleep_time = random.randint(1000, 5000) / 1000
            self.last_heartbeat = sleep_time
            time.sleep(sleep_time)
            print(f'Log: {self.log}')
            if self.role == 'follower' and self.last_heartbeat != 0:
                self.start_election()

    def send_heartbeat(self):
        while True:
            for uri in self.uris:
                if uri == self.uri:
                    continue
                try:
                    proxy = Pyro5.api.Proxy(uri)
                    proxy.receive_heartbeat(self.current_term, self.log, self.uri)
                except:
                    print(f"Node {self.id} failed to send heartbeat to {uri}")
            time.sleep(0.05)

    def receive_heartbeat(self, term, log, uri):
        if int(self.current_term) == int(term) - 1:
            self.current_term = term
            self.log = log
            self.last_heartbeat = 0
            return
        elif term == self.current_term:
            self.last_heartbeat = 0

    # ELEICAO

    def start_election(self):
        print(f"Node {self.id} started an election")
        self.role = 'candidate'
        uris = self.uris
        votes = 0
        connections = len(self.uris)

        for uri in uris:
            if uri == self.uri:
                votes += 1
                continue
            try:
                votes += self.request_vote(uri)
            except:
                connections -= 1

        # Eleito
        if votes > connections // 2 and self.role == 'candidate':
            print(f"Node {self.id} is now a leader")
            self.role = 'leader'
            self.current_term += 1
            threading.Thread(target=self.send_heartbeat).start()
            ns = locate_ns()
            ns.register('leader', self.uri)

        else:
            print(f"Node {self.id} election failed")
            self.role = 'follower'

    def request_vote(self, uri):
        print(f"Node {self.id} requesting vote to {uri}")
        proxy = Pyro5.api.Proxy(uri)
        got_vote = proxy.vote(self.current_term, self.uri)
        if got_vote:
            print(f"Node {self.id} voted from {uri}")
            return 1
        return 0

    def vote(self, candidate_term, candidate_uri):
        print(f"Node {self.id} getting vote request from {candidate_uri}")
        if candidate_term != self.current_term:
            print(
                f"Node {self.id} did not vote, term ({self.current_term}) is different from candidate term ({candidate_term})")
            return False

        if self.role != 'follower':
            print(f"Node {self.id} did not vote because it is a {self.role}")
            return False

        print(f"Node {self.id} voted for {candidate_uri}")
        return True

    # Replicacao

    def set_leader_value(self, value):
        self.uncommitted_value = value
        if self.role != 'leader':
            print('Could not commit value, node is not the actual leader')
            return

        commits = 0
        connections = len(self.uris)
        for uri in self.uris:
            if self.uri == uri:
                commits += 1
                continue

            try:
                proxy = Pyro5.api.Proxy(uri)
                proxy.set_value(value)
                commits += 1
            except:
                connections -= 1
                print(f"Node {self.id} could not set value to {value}")

        if commits <= connections // 2:
            print(f"Did not receive majority of commits")
            return

        self.commit_values()

    def set_value(self, value):
        print(f"Node {self.id} setting value to {value}")
        self.uncommitted_value = value

    def commit_values(self):
        for uri in self.uris:
            try:
                proxy = Pyro5.api.Proxy(uri)
                proxy.append_value(self.id, self.current_term)
            except:
                print(f'Node {self.id} could commit {uri} value')

    def append_value(self, leader_id, leader_term):
        print(f"Node {self.id} committing value {self.uncommitted_value}")
        obj = {
            'value': self.uncommitted_value,
            'leader': leader_id,
            'term': leader_term
        }
        self.value = self.uncommitted_value
        self.log.append(obj)

if __name__ == "__main__":
    object_id = int(sys.argv[1])
    main(object_id)
