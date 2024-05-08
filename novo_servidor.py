import sys
import threading
import time
import random
import Pyro5.server
import Pyro5.api
from Pyro5.core import locate_ns
from Pyro5.server import expose

PREFIXO_URI = "PYRO:Object"

def main(object_id):
    try:
        name = f"Object{object_id}"
        print(f"Starting node: {name}")
        uris = [f"{PREFIXO_URI}{i}@localhost:{7000 + i}" for i in range(1, 5)]
        Node(name, object_id, uris)
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
        self.running = True

        # Inicializa o servidor Pyro
        port = 7000 + node_id
        daemon = Pyro5.server.Daemon(port=port)
        self.uri = daemon.register(self, node_name)
        print(f"Node started with uri Node {self.id}")
        print(f"Node has the following uri: {self.uri}")
        
        # Inicia uma nova thread para o loop principal
        threading.Thread(target=self.run).start()
        
        # Inicia o loop do servidor Pyro
        daemon.requestLoop()


    def run(self):
        time.sleep(random.randint(5, 10))
        while self.running:
            sleep_time = random.randint(1000, 5000) / 1000
            self.last_heartbeat = sleep_time
            time.sleep(sleep_time)
            print(f'Log: {self.log}')
            # Verifica se é um seguidor e se houve um último heartbeat
            if self.role == 'follower' and self.last_heartbeat != 0:
                self.start_election()

    
    def send_heartbeat(self):
        self.running = True
        
        while self.running:
            # Lista de URIs excluindo o próprio URI
            uris_to_send = [uri for uri in self.uris if uri != self.uri]
            # Envia heartbeat para cada URI
            for uri in uris_to_send:
                try:
                    proxy = Pyro5.api.Proxy(uri)
                    proxy.receive_heartbeat(self.current_term, self.log, self.uri)
                except Pyro5.errors.CommunicationError:
                    print(f"Node {self.id} failed to send heartbeat to {uri}")
                except Pyro5.errors.SerializeError:
                    print(f"Node {self.id} encountered serialization error while sending heartbeat to {uri}")
                    
            time.sleep(0.05)

    def receive_heartbeat(self, term, log, uri):
        if int(self.current_term) == int(term) - 1:
            self.current_term = term
            self.log = log
            self.last_heartbeat = 0
            return
        elif term == self.current_term:
            self.last_heartbeat = 0

    def start_election(self):
        print(f"Node {self.id} started an election")
        self.role = 'candidate'
        votes = 0
        connections = len(self.uris)

        for uri in self.uris:
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

    def set_leader_value(self, value):
        self.uncommitted_value = value
        
        if self.role != 'leader':
            print('Could not commit value, node is not the actual leader')
            return
        
        commits = 0
        total_connections = len(self.uris)
        
        for uri in self.uris:
            # Ignorar a conexão para si mesmo
            if self.uri == uri:
                commits += 1
                continue
            try:
                proxy = Pyro5.api.Proxy(uri)
                proxy.set_value(value)
                commits += 1
            except Pyro5.errors.CommunicationError:
                # Tratamento específico para falhas de comunicação
                total_connections -= 1
                print(f"Node {self.id} could not set value to {value} at URI {uri}")
            except Pyro5.errors.SerializeError:
                # Tratamento específico para erros de serialização
                total_connections -= 1
                print(f"Node {self.id} encountered serialization error at URI {uri}")
        
        # Verificar se a maioria dos nós aceitou o valor
        if commits > total_connections // 2:
            self.commit_values()
        else:
            print(f"Did not receive majority of commits")


    def set_value(self, value):
        print(f"Node {self.id} setting value to {value}")
        self.uncommitted_value = value

    def commit_values(self):
        for uri in self.uris:
            try:
                # Tenta enviar o valor para o nó remoto
                proxy = Pyro5.api.Proxy(uri)
                proxy.append_value(self.id, self.current_term)
                print(f'Value committed to node at URI: {uri}')
            except Pyro5.errors.CommunicationError:
                # Trata erros de comunicação específicos
                print(f'Node {self.id} failed to commit value to {uri}: Communication error')
            except Pyro5.errors.SerializeError:
                # Trata erros de serialização específicos
                print(f'Node {self.id} failed to commit value to {uri}: Serialization error')
            except Exception as e:
                # Trata outros erros de forma genérica
                print(f'Node {self.id} failed to commit value to {uri}: {str(e)}')


    def append_value(self, leader_id, leader_term):
        try:
            # Verifica se o líder e o termo fornecidos são válidos
            if leader_id is None or leader_term is None:
                print("Invalid leader_id or leader_term.")
                return
            
            print(f"Node {self.id} committing value {self.uncommitted_value}")
            
            # Cria um objeto com o valor, líder e termo
            obj = {
                'value': self.uncommitted_value,
                'leader': leader_id,
                'term': leader_term
            }
            
            # Atualiza o valor atual e adiciona ao log
            self.value = self.uncommitted_value
            self.log.append(obj)
            
            # Confirmação da operação
            print("Value committed successfully.")
        except Exception as e:
            # Tratamento genérico de exceções
            print(f"An error occurred: {str(e)}")


if __name__ == "__main__":
    object_id = int(sys.argv[1])
    main(object_id)
