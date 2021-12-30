"""
A Synchronization Contoller Design
Reference:
Xin YAO, Xueyu Wu, Cho-Li Wang "FluentPS: A Parameter Server Design with Low-frequency Synchronization for Distributed
Deep Learning, IEEE Cluster 2019.
"""
class Synchronizer():
    def __init__(self, selectedClient, init_process = 0) -> None:
        self.numClient = selectedClient
        self.trainprocess = init_process
        self.clientprogress = {}
        self.clientcount = {}
        # these two interfaces are used for supporting a wide range of synchronization models
        self.pushcondition = self.PushStaleness
        self.pullcondition = self.PullStaleness
        self.staleness = 0
        self.pullcallback = {}
    
    # update a client progress infomation
    def updateClient(self, client_id, progress):
        self.clientprogress[client_id] = progress
        
    def addClientProgress(self, client_id):
        progress = self.clientprogress[client_id]
        if progress not in self.clientcount:
            self.clientcount[progress] = 0
        self.clientcount[progress] = self.clientcount[progress] + 1
    
    def bufferRequest(self, client_id, progress):
        if progress not in self.pullcallback:
            self.pullcallback[progress] = [client_id]
        else:
            self.pullcallback[progress].append(client_id)
    
    def setStaleness(self, staleness):
        self.staleness = staleness
    
    # 0 - BSP, 3 - SSP, MAX_ROUND - ASP
    def PushStaleness(self):
        progress = self.trainprocess # 
        clients = []
        if self.clientcount[progress] == self.numClient:
            if progress in self.pullcallback:
                clients = self.pullcallback[progress]
                del self.pullcallback[progress]
            self.trainprocess = self.trainprocess + 1
        return clients
    
    def PullStaleness(self, client_id):
        progress = self.clientprogress[client_id]
        if self.trainprocess + self.staleness <= progress: # or progress in self.pullcallback
            self.bufferRequest(client_id, progress) #  soft barrier if using self.trainprocess
            return False
        return True
    
    def PushOfBSP(self):
        return self.PushStaleness()
    
    def PullOfBSP(self, client_id):
        return self.PullStaleness(client_id)
    
    def PushOfSSP(self):
        return self.PushStaleness()
    
    def PullOfSSP(self, client_id):
        return self.PullStaleness(client_id)
    
    # always return True
    def PushOfASP(self):
        return self.PushStaleness()
    
    # always return True
    def PullOfASP(self, client_id):
        return self.PullStaleness(client_id)
    
if __name__ == "__main__":
    # this is a demo for validating the sync model vis staleness
    client = 20
    controller = Synchronizer(client)
    for i in range(client):
        controller.updateClient(i, controller.trainprocess)
    controller.setStaleness(3)
    max_round = 100
    block_list = []
    import random
    for i in range(max_round):
        id = random.randint(0, 100) % client
        while id in block_list or controller.clientprogress[id] >= (max_round/client):
            id = random.randint(0, 100) % client
        print(controller.clientprogress[id], ": push", id)
        controller.addClientProgress(id)
        buffer_clients = controller.pushcondition()
        if len(buffer_clients) > 0:
            print("Sync at", controller.trainprocess - 1)
            print("pull", block_list, buffer_clients)
            for i in buffer_clients:
                print(controller.clientprogress[i], ": pull", i)
                controller.updateClient(i, controller.clientprogress[i]+1)
            block_list.clear()
        if not controller.pullcondition(id):
            block_list.append(id)
        else:
            print(controller.clientprogress[id], ": pull", id)
            controller.updateClient(id, controller.clientprogress[id]+1)


