import random
import time
from threading import Thread

class RaftNode:
    def __init__(self, node_id, peers):
        self.node_id = node_id
        self.peers = peers
        self.state = 'follower'
        self.current_term = 0
        self.voted_for = None
        self.leader_id = None
        self.election_timeout = random.uniform(1.5, 3.0)
        self.last_heartbeat = time.time()
        self.log = []
        self.commit_index = 0
        self.last_applied = 0
        
    def start(self):
        print(f"Nodo {self.node_id} iniciado como {self.state}")
        Thread(target=self.run).start()
        
    def run(self):
        while True:
            if self.state == 'follower':
                self.follower_loop()
            elif self.state == 'candidate':
                self.candidate_loop()
            elif self.state == 'leader':
                self.leader_loop()
                
    def follower_loop(self):
        while self.state == 'follower':
            if time.time() - self.last_heartbeat > self.election_timeout:
                self.state = 'candidate'
                print(f"Nodo {self.node_id} se convierte en candidato")
                return
            time.sleep(0.1)
            
    def candidate_loop(self):
        self.current_term += 1
        self.voted_for = self.node_id
        votes_received = 1
        
        print(f"Nodo {self.node_id} solicita votos para el término {self.current_term}")
        
        # Simular solicitud de votos a otros nodos
        for peer in self.peers:
            if random.random() > 0.3:  # 70% de probabilidad de obtener voto
                votes_received += 1
                
        if votes_received > len(self.peers) / 2:
            self.become_leader()
        else:
            self.state = 'follower'
            self.election_timeout = random.uniform(1.5, 3.0)
            
    def become_leader(self):
        self.state = 'leader'
        self.leader_id = self.node_id
        print(f"Nodo {self.node_id} es ahora el líder para el término {self.current_term}")
        
    def leader_loop(self):
        while self.state == 'leader':
            # Enviar latidos a los seguidores
            print(f"Líder {self.node_id} envía latidos a los seguidores")
            self.last_heartbeat = time.time()
            
            # Simular fallo del líder (30% de probabilidad)
            if random.random() < 0.3:
                print(f"Líder {self.node_id} ha fallado!")
                self.state = 'follower'
                self.election_timeout = random.uniform(1.5, 3.0)
                return
                
            time.sleep(1)
            
    def replicate_log(self, entry):
        if self.state == 'leader':
            print(f"Líder {self.node_id} replica entrada de log: {entry}")
            self.log.append(entry)
            
            # Simular replicación a seguidores
            successful_replications = 1  # líder cuenta como 1
            
            for peer in self.peers:
                if random.random() > 0.2:  # 80% de probabilidad de replicación exitosa
                    successful_replications += 1
                    
            if successful_replications > len(self.peers) / 2:
                self.commit_index = len(self.log) - 1
                print(f"Entrada {entry} comprometida en la mayoría de nodos")
            else:
                print(f"No se pudo replicar {entry} en la mayoría")

# Simulación con 3 nodos
nodes = []
peers = [1, 2, 3]  # IDs de nodos

# Crear nodos
for node_id in peers:
    node = RaftNode(node_id, [p for p in peers if p != node_id])
    nodes.append(node)
    node.start()

# Esperar a que se elija un líder
time.sleep(5)

# Encontrar el líder y replicar una entrada
leader = None
for node in nodes:
    if node.state == 'leader':
        leader = node
        break

if leader:
    leader.replicate_log("A=1")
else:
    print("No se pudo elegir un líder")

# Simular fallo del líder
if leader:
    print(f"Simulando fallo del líder {leader.node_id}")
    leader.state = 'follower'
    
    # Esperar nueva elección
    time.sleep(5)
    
    # Encontrar nuevo líder
    new_leader = None
    for node in nodes:
        if node.state == 'leader':
            new_leader = node
            break
    
    if new_leader:
        new_leader.replicate_log("B=2")
    else:
        print("No se pudo elegir un nuevo líder")