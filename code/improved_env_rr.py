#!/usr/bin/env python
# -*- coding: utf-8 -*-

#######################################################
# coding by YingzhuLiu & Xuanzuo                      #
# the improved envitonment of RP/TS processor         #
# and RR baseline                                     #
# input: task file output_5000.txt (small scale)      #
# or output_200000.txt (large scale)                  #
# output: the total energy cost                       #
# the reject rate and run time                        #
#######################################################

class Task(object):   
    """
    information of each task
    parent, child base on dependency
    jobID, index, CPU, RAM, disk extracted from user data
    status indicates the current status of the task
    """
    def __init__(self, jobID, index, CPU, RAM, disk, status):
        import random
        import time
        self.parent = []
        self.child = []
        self.jobID = jobID
        self.index = index
        self.CPU = CPU
        self.RAM = RAM
        self.disk = disk
        self.sub_task = []
        self.relative = None
        self.server = -1
        self.vm = -1
        self.status = status  #-1: rejected, 0: finished, 1: ready, 2: running
        self.runtime = random.randint(1, 10)/1000.0
        self.ddl = time.time() + self.runtime + random.randint(100, 1000)/200.0
        self.endtime = 0
        
class DAG(object):
    """
    Transform job queue to task ready queue
    """
    def __init__(self, fname, num_task):
        self.fname = fname
        self.num_task = num_task
        self.job = []
        self.task = []
        self.subtask = []
    
    def readfile(self):
        """
        Read the input job file
        All task are initialized to ready status
        """
        num_task = 0
        with open(self.fname, 'r') as f:
            task = []            
            for line in f:
                if line[0] == 'J':
                    if len(task) != 0:
                        self.job.append(task)
                        task = []
                else:
                    info = list(line.strip(' ').split())
                    task.append(Task(info[1], info[2], float(info[4]), float(info[5]), float(info[6]), 1))
                    num_task += 1
                if num_task == self.num_task: 
                    break
            if len(task) != 0:
                self.job.append(task)
                
    def checkParent(self, task):
        """
        Check whether task not dependent on others
        Return True if task is independent
        """
        return len(task.parent) == 0
    
    def checkRing(self, parent, child): 
        """
        Check whether there is a loop between parent and child
        Return True if has loop
        """
        if parent.index == child.index:
            return True
        if len(child.child) == 0:
            return False
        for c in child.child:
            if self.checkRing(parent, c):
                return True
        return False
    
    
    def buildDAG(self):
        """
        Randomly build dependencies between tasks within each job
        """
        import random
        for job in self.job:           
            for task in job:
                i = random.randint(0, len(job) - 1)
                if i < 0:
                    continue
                parent = job[i]
                if self.checkRing(parent, task) == False:
                    task.parent.append(parent)
                    parent.child.append(task)
    
    def rejTask(self, task):
        """
        If one task is rejected
        Then all tasks that depended on this task will be rejected
        """
        task.status = -1
        if task.relative:
            for t in task.relative.sub_task:
                t.status = -1
        for c in task.child:
            self.rejTask(c)
    
    def hasParent(self, task):
        """
        When a task are finished
        Remove it from the parent for all child tasks
        """
        for c in task.parent:
            if c.status == 1:  #still has parent
                return True
        return False
    
    def updateStatus(self, task):
        """
        Given jobid and taskid, change status of all tasks that depend on it
        If the task with "-1" status, reject this tasks' all child tasks
        If the task with "0" status, remove it from all child tasks
        """
#         job_i, task_i = self.findTask(task.jobID, task.index)
#         if job_i == -1 or task_i == -1:
#             print("WRONG: The task with jobID: ", task.jobID, " and taskID: ", task.index, " not exist.")
#             return
#         job = self.job[job_i]
#         task = job[task_i]
        if task.status == -1:
            self.rejTask(task)
#         elif task.status == 0:
#             self.rmParent(task, task_i, job)

    def initTask(self):
        """
        run readfile and buildDAG functions
        """
        self.readfile()
        self.buildDAG()
        self.generate_subtask()
        
    def divideTask(self, task):
        """
        Devide one task to random num_task sub tasks 
        which can be run in diff servers
        Suppose first num_task - 1 sub tasks are independcy
        The last sub task depend on first num_task - 1 sub tasks
        """
        import random
        import time
        num_task = random.randint(1, 5)
        percent = 1
        CPU = task.CPU
        RAM = task.RAM
        disk = task.disk
        sub_task = []
        for i in range(1, num_task):
            p = round(random.uniform(0, percent), 3)
            #basic info
            sub_t = Task(task.jobID, task.index, CPU * p, RAM * p, disk * p, task.status)
            #parent child tasks
            for pt in task.parent:
                sub_t.parent.append(pt)
            for ct in task.child:
                sub_t.child.append(ct)
            #ddl
            sub_t.ddl = time.time() + task.runtime * p + random.randint(1, 1000)/20.0
            #runtime
            sub_t.runtime = task.runtime * p
            percent -= p
            sub_task.append(sub_t)
            
        sub_t = Task(task.jobID, task.index, CPU * (percent + random.uniform(0, 0.1)), RAM * (percent + random.uniform(0, 0.1)), disk * (percent + random.uniform(0, 0.1)), task.status) #add additional CPU, RAM, runtime cost to the last subtask, because it needs to communicate with others
        for pt in task.parent:
            sub_t.parent.append(pt)
        for ct in task.child:
            sub_t.child.append(ct)
        #ddl
        sub_t.ddl = time.time() + task.runtime * percent + random.randint(1, 1000)/20.0
        #runtime
        sub_t.runtime = task.runtime * (percent + random.uniform(0, 0.1))
        sub_task.append(sub_t)
        for t in range(0, len(sub_task) - 1):
            sub_task[len(sub_task) - 1].parent.append(sub_task[t])
#         print(len(sub_task), cout)
        for t in sub_task:
            t.relative = task
#         for t in sub_task:
#             print("jobID", t.jobID, "index", t.index, "p", len(t.parent), "c", len(t.child))

        return sub_task
    
    def generate_subtask(self):
        """
        Put all sub tasks to one subtask queue
        """
        for job in self.job:
            for task in job:
                task.sub_task = self.divideTask(task)
                for t in task.sub_task:
                    self.subtask.append(t)
#         i = 0
#         for t in self.subtask:
# #             if i != t.relative.index:
# #                 print("P: jobID", t.relative.jobID, t.index, len(t.parent), len(t.child))
# #                 print("P: jobID", t.relative.jobID, "index", t.relative.index, "CPU", t.relative.CPU)
#             i = t.relative.index
#             print("S: jobID", t.jobID, t.index, len(t.parent))
#             for p in t.parent:
#                 print("jobID", t.jobID, "index", t.index, end=",")
#             print()
#             print("S: jobID", t.jobID, "index", t.index, "CPU", t.CPU)
#             print(len(t.relative.sub_task), t.CPU, t.relative.CPU)
    
    def taskQueue(self): 
        """
        Build the sub_task ready queue
        Just put the one whose status is 1 
        and whose parent are all finished
        """
        
        for task in self.subtask:
            if task.status == 1 and self.hasParent(task) == False:
                self.task.append(task)


    def printTask(self):
        for j in self.task:
            print(j.jobID, ",", j.CPU, j.RAM, j.disk, len(j.sub_task))
#             for t in j.sub_task:
#                 print(t.jobID, ",", t.CPU, t.RAM, t.disk)


# class environment(object):
#     """docstring for environment
#     the environment of RP/TS processor
#     read the task from txt file
#     calculate the Reward Function
#     interface with DQN and baseline
#     """
#     def __init__(self, scale, fname, num_task, num_server):
#         """
#         initial the variable
#         We assume each server has 10 VM
#         For small-scale problems: 
#             200 servers
#             10 server farms
#         For large-scale problems:
#             4000 servers
#             70 server farms
#         All servers have unit CPU, RAM, and Disk space
#         """
#         self.curtime = 0
#         self.scale = scale
#         self.fname = fname
#         self.task = []
#         self.dag = DAG(self.fname, num_task)
#         self.VMNum = 5
#         self.rej = 0
#         self.num_task = num_task
#         self.severNum = num_server
#         self.totalcost = 0
#         if self.scale == 'small':
# #             self.severNum = 200
#             self.farmNum = 10
#         elif self.scale == 'large':
# #             self.severNum = 4000
#             self.farmNum = int(self.severNum / 50)
#         self.init_severs()
# #         print(self.severNum, "servers", end=' ')
        
        
#     def init_severs(self):
#         """
#         Set the initial values for each server and Vms
#         Each server has unit CPU, RAM, and local disk space
#         Each VM has 1/n unit CPU and RAM
#         """
#         self.severs = [[1,1,1]for _ in range(self.severNum)]
#         self.VM = [[[1.0/self.VMNum, 1.0/self.VMNum]for _ in range(self.VMNum)]for _ in range(self.severNum)]
#         self.VMtask = [[[]for _ in range(self.VMNum)]for _ in range(self.severNum)]
# #         print(self.num_task, "requests")
        
#     def generateQueue(self):
#         """
#         Generate task queue
#         Add tasks whose parents' status is 0(finished) to the queue
#         """
#         self.dag.taskQueue()
#         self.task = self.dag.task 

#     def setFarm(self):
#         """
#         Randomly set the servers to each farm
#         Each farm has at least 1 server and at most 2*m/n-1 servers
#         Initial power usage for each servers and each farm
#         """
#         import random
#         self.farmOri = []
#         m = self.severNum
#         n = self.farmNum
#         for _ in range(self.farmNum-1):
#             f = random.randint(0,int(2*m/n))
#             m -= f
#             n -= 1
#             self.farmOri.append(f)
#         self.farmOri.append(m)
#         self.pwrPre = [0]*self.severNum #power usage pre sever
#         self.pwrPFarm = [0]*self.farmNum #power usage per farm
# #         print (self.farmOri)

#     def elecPrice(self, t, pwr):
#         """
#         The energy cost on time t
#         threshold get from "Impact of dynamic energy pricing schemes on a novel multi-user home energy management system"
#         price get from "Optimal residential load
#         control with price prediction in real-time electricity pricing environ- ments"
#         """
#         threshold = 1.5
#         if pwr < threshold:
#             p = 5.91 #dynamic price
#         else:
#             p = 8.27
#         return pwr * p

#     def getPwr(self, r, c):
#         """
#         Implement the energy consumption model
#         r: the requires CPU
#         c: the total(unit) CPU
#         The parameters' value get from "An energy and deadline aware resource provisioning, scheduling and optimization framework for cloud systems"
#         """
#         # eq.2
#         if r > 0:
#             pwrS = 1
#         else:
#             pwrS = 0
#         alpha = 0.5 #alpha
#         beta = 10 #beta
#         Ur = r/c # eq.1
#         if Ur < 0.7:
#             pwrDy = alpha * Ur
#         else:
#             pwrDy = 0.7 * alpha + (Ur - 0.7)**2 * beta
#         return pwrDy+pwrS

#     def rewardFcn1(self, f, t):
#         """
#         Implement the reward function for each farm
#         For stage 1: choose the farm
#         """
#         # eq.5
#         pwrCFarm = []
#         for i in range(len(self.farmOri)):
#             if i == f:
#                 self.pwrPFarm[i] += t
#             pwrc = self.getPwr(self.pwrPFarm[i], self.farmOri[i])
#             pwrCFarm.append(pwrc)
#         pwr = sum(pwrCFarm) - sum(self.pwrPFarm)
#         self.pwrPFarm = pwrCFarm
#         print (self.elecPrice(1, pwr))

#     def rewardFcn2(self):
#         """
#         Implement the reward function for each server
#         For stage 2: choose the server
#         """
#         # eq.6
#         pwrCur = []
#         for m in self.severs:
#             pwrc = self.getPwr(1-m[-3], 1)
#             pwrCur.append(pwrc)
# #             print("pwrc", pwrc)
#         pwr = sum(pwrCur) - sum(self.pwrPre)
#         self.totalcost += sum(pwrCur)
# #         print("sum(pwrCur)", sum(pwrCur), "sum(self.pwrPre)", sum(self.pwrPre))
#         self.pwrPre = pwrCur
# #         print("pwr",pwr)
# #         print ("energy cost: ", round(self.elecPrice(1, pwr), 3))

# #     def release(self):
# #         """
# #         Randomly release resources from each VM
# #         And set the corresponding task as finished
# #         """
# #         import random
# #         ranSer = random.randint(0, self.severNum-1)
# #         ranVM = random.randint(0, self.VMNum-1)
# #         if self.VMtask[ranSer][ranVM]:
# #             random.shuffle(self.VMtask[ranSer][ranVM])
# #             t = self.VMtask[ranSer][ranVM].pop()
# #             t.status = 0
# #             for sub_t in t.parent[0].sub_task:
# #                 self.VM[sub_t.server][sub_t.vm][0] += float(sub_t.CPU)
# #                 self.VM[sub_t.server][sub_t.vm][1] += float(sub_t.RAM)
 
#     def releaseByTime(self, server_i, vm_j):
#         """
#         Release tasks in VM with index server_i and vm_j
#         If task's endtime < current time, this task can be released
#         """
#         import time
#         curtime = time.time()
#         for t in self.VMtask[server_i][vm_j]:
#             if t.endtime < curtime:
#                 t.status = 0
# #                 print(t.ddl, t.runtime, t.endtime, curtime)
#                 self.VM[server_i][vm_j][0] += float(t.CPU)
#                 self.VM[server_i][vm_j][1] += float(t.RAM)
#                 self.VMtask[server_i][vm_j].remove(t)
# #                 for c in t.child:
# #                     count = 0
# #                     for p in c.parent:
# #                         if p == 0:
# #                             count += 1
# #                     if count == len(c.parent):
# #                         self.task.append(c)

#     def training(self):
#         """
#         Run the RR baseline in the  environment
#         Read the task file
#         Set the variables
#         """
#         #send one tesk to dqn and calculate reward
#         self.dag.initTask()
#         self.generateQueue()
#         self.setFarm()
#         print(self.farmNum, end=' ')
#         print(self.severNum, end=' ')
#         print(self.num_task, end=' ')
#         import time
#         time_start=time.time()
#         self.RR()
#         time_end=time.time()
#         timecost = round(time_end-time_start, 3)
# #         print('Time cost', timecost,'s', end=' ')
# #         print('cost', self.totalcost)
        
#         print(timecost, end=' ')
#         print(round(self.totalcost, 3), end=' ')
#         print()

#     def checkRej(self, server_i, vm_j, task):
#         """
#         Check whether this task should be rejected in ith sever, jth VM
#         Reject task when current time + task's runtime > task's ddl
#         """
#         import time
#         if task.CPU > 1/self.VMNum or task.RAM > 1/self.VMNum:
#             self.rej += 1 
#             return -1
#         remain_cpu = self.VM[server_i][vm_j][0] - float(task.CPU)
#         remain_ram = self.VM[server_i][vm_j][1] - float(task.RAM)
#         curtime = time.time()
#         if curtime + task.runtime <= task.ddl:
#             if remain_cpu >= 0 and remain_ram >=0:
#                 return 0  # do not reject
#             else:
#                 return 1  # reject temporarily because cpu or ram
#         else:
#             self.rej += 1 
#             return -1  #reject because ddl

#     def RR(self):
#         """
#         The baseline(Round-Robin) of project
#         Arrange task server by server, VM by VM
#         For each server find one VM for this task
#         """
#         i = 0 #no.sever 
#         acc = 0
#         while len(self.task) != 0:
#             #assign all tasks in current queue
#             while len(self.task) != 0:
#                 for t in self.task:
#                     if t.status == -1: #rejected
#                         self.dag.updateStatus(t)
#                         self.task.remove(t)
#                     elif t.status == 1:   #ready           
#                         server_pass = 0
#                         server_i = i 
#                         vm_j = 0
#                         #find which server
#                         self.releaseByTime(server_i, vm_j)
#                         rej = self.checkRej(server_i, vm_j, t)
#                         if rej == -1:  #rejected due to ddl
#                             t.status = -1
#                         while rej != 0 and server_pass <= self.severNum and t.status == 1: 
#                             rej = self.checkRej(server_i, vm_j, t)
#                             #find which VM
#                             while vm_j < len(self.VM[server_i]) and rej != 0 and t.status == 1:
#                                 if rej == -1:  #rejected due to ddl
#                                     t.status = -1
#                                 self.releaseByTime(server_i, vm_j)
#                                 vm_j += 1
#                             if vm_j == len(self.VM[server_i]):  #this server not meet the requirement
#                                 server_pass += 1
#                                 vm_j = 0
#                                 server_i += 1  #go to next server
#                             else:
#                                 break
#                             if server_i == self.severNum:
#                                 server_i = 0       
#                             rej = self.checkRej(server_i, vm_j, t)
#                         if server_pass <= self.severNum and vm_j < len(self.VM[server_i]) and t.status == 1:
#                             #arrange task to server_i, vm_j VM
#                             decision = [server_i, vm_j]
#     #                             print(server_i, vm_j)
#                             self.VMtask[decision[0]][decision[1]].append(t)
#                             self.VM[decision[0]][decision[1]][0] -= float(t.CPU)
#                             self.VM[decision[0]][decision[1]][1] -= float(t.RAM)
#                             self.severs[decision[0]][0] -= float(t.CPU)  
#                             self.severs[decision[0]][1] -= float(t.RAM) 
#                             import time
#                             curtime = time.time()
#                             t.endtime = curtime + t.runtime
#                             i += 1
#                             if i == self.severNum:
#                                 i = 0
#                             t.status = 2  #set statue to running
#                             self.task.remove(t)    
#                             self.rewardFcn2()
#                             acc += 1
#             self.generateQueue()
#         print(round(1 - acc / self.num_task, 3), end=' ')            

class environment(object):
    """docstring for environment
    the environment of RP/TS processor
    read the task from txt file
    calculate the Reward Function
    interface with DQN and baseline
    """
    def __init__(self, scale, fname, num_task, num_server):
        """
        initial the variable
        We assume each server has 10 VM
        For small-scale problems: 
            200 servers
            10 server farms
        For large-scale problems:
            4000 servers
            70 server farms
        All servers have unit CPU, RAM, and Disk space
        """
        self.scale = scale
        self.fname = fname
        self.task = []
        self.dag = DAG(self.fname, num_task)
        self.VMNum = 2
        self.rej = 0
        self.num_task = num_task
        self.severNum = num_server
        self.totalcost = 0
        if self.scale == 'small':
#             self.severNum = 200
            self.farmNum = 10
        elif self.scale == 'large':
#             self.severNum = 4000
            self.farmNum = int(self.severNum / 50)
        print(self.farmNum, end=' ')
        print(self.severNum, end=' ')
        print(self.num_task, end=' ')
        self.init_severs()
        
    def init_severs(self):
        """
        Set the initial values for each server and Vms
        Each server has unit CPU, RAM, and local disk space
        Each VM has 1/n unit CPU and RAM
        """
        self.severs = [[1,1,1]for _ in range(self.severNum)]
        self.VM = [[[1.0/self.VMNum, 1.0/self.VMNum]for _ in range(self.VMNum)]for _ in range(self.severNum)]
        self.VMtask = [[[]for _ in range(self.VMNum)]for _ in range(self.severNum)]
#         print(self.num_task, "requests")

    def generateQueue(self):
        """
        Generate task queue
        Add tasks whose parents' status is 0(finished) to the queue
        """
        self.dag.taskQueue()
        self.task = self.dag.task 

    def setFarm(self):
        """
        Randomly set the servers to each farm
        Each farm has at least 1 server and at most 2*m/n-1 servers
        Initial power usage for each servers and each farm
        """
        import random
        self.farmOri = []
        m = self.severNum
        n = self.farmNum
        for _ in range(self.farmNum-1):
            f = random.randint(0,int(2*m/n))
            m -= f
            n -= 1
            self.farmOri.append(f)
        self.farmOri.append(m)
        self.pwrPre = [0]*self.severNum #power usage pre sever
        self.pwrPFarm = [0]*self.farmNum #power usage per farm
#         print (self.farmOri)

    def elecPrice(self, t, pwr):
        """
        The energy cost on time t
        threshold get from "Impact of dynamic energy pricing schemes on a novel multi-user home energy management system"
        price get from "Optimal residential load
        control with price prediction in real-time electricity pricing environ- ments"
        """
        threshold = 1.5
        if pwr < threshold:
            p = 5.91 #dynamic price
        else:
            p = 8.27
        return pwr * p

    def getPwr(self, r, c):
        """
        Implement the energy consumption model
        r: the requires CPU
        c: the total(unit) CPU
        The parameters' value get from "An energy and deadline aware resource provisioning, scheduling and optimization framework for cloud systems"
        """
        # eq.2
        if r > 0:
            pwrS = 1
        else:
            pwrS = 0
        alpha = 0.5 #alpha
        beta = 10 #beta
        Ur = r/c # eq.1
        if Ur < 0.7:
            pwrDy = alpha * Ur
        else:
            pwrDy = 0.7 * alpha + (Ur - 0.7)**2 * beta
        return pwrDy+pwrS

    def rewardFcn1(self, f, t):
        """
        Implement the reward function for each farm
        For stage 1: choose the farm
        """
        # eq.5
        pwrCFarm = []
        for i in range(len(self.farmOri)):
            if i == f:
                self.pwrPFarm[i] += t
            pwrc = self.getPwr(self.pwrPFarm[i], self.farmOri[i])
            pwrCFarm.append(pwrc)
        pwr = sum(pwrCFarm) - sum(self.pwrPFarm)
        self.pwrPFarm = pwrCFarm
        print (self.elecPrice(1, pwr))

    def rewardFcn2(self):
        """
        Implement the reward function for each server
        For stage 2: choose the server
        """
        # eq.6
        pwrCur = []
        for m in self.severs:
            pwrc = self.getPwr(1-m[-3], 1)
            pwrCur.append(pwrc)
#             print("pwrc", pwrc)
        pwr = sum(pwrCur) - sum(self.pwrPre)
        self.totalcost += sum(pwrCur)
#         print("sum(pwrCur)", sum(pwrCur), "sum(self.pwrPre)", sum(self.pwrPre))
        self.pwrPre = pwrCur
#         print("pwr",pwr)
#         print ("energy cost: ", round(self.elecPrice(1, pwr), 3))

    def releaseByTime(self, server_i, vm_j):
        """
        Release tasks in VM with index server_i and vm_j
        If task's endtime < current time, this task can be released
        """
        import time
        curtime = time.time()
        for t in self.VMtask[server_i][vm_j]:
            if t.endtime < curtime:
                t.status = 0
#                 print(t.ddl, t.runtime, t.endtime, curtime)
                self.VM[server_i][vm_j][0] += float(t.CPU)
                self.VM[server_i][vm_j][1] += float(t.RAM)
                self.VMtask[server_i][vm_j].remove(t)
#                 for c in t.child:
#                     count = 0
#                     for p in c.parent:
#                         if p == 0:
#                             count += 1
#                     if count == len(c.parent):
#                         self.task.append(c)
                        
                
    def training(self):
        """
        Run the RR baseline in the  environment
        Read the task file
        Set the variables
        """
        #send one tesk to dqn and calculate reward
#         print(self.severNum, "servers", end=' ')
        self.dag.initTask()
        self.generateQueue()
        self.setFarm()
        import time
        time_start=time.time()
        self.RR()
        time_end=time.time()
        timecost = round(time_end-time_start, 3)
#         print('Time cost', timecost,'s', end=' ')
#         print('cost', self.totalcost)
        print(timecost, end=' ')
        print(round(self.totalcost, 3), end=' ')
        print()

    def checkRej(self, server_i, vm_j, task):
        """
        Check whether this task should be rejected in ith sever, jth VM
        Reject task when current time + task's runtime > task's ddl
        """
        import time
        if task.CPU > 1/self.VMNum or task.RAM > 1/self.VMNum:
            self.rej += 1 
            return -1
        remain_cpu = self.VM[server_i][vm_j][0] - float(task.CPU)
        remain_ram = self.VM[server_i][vm_j][1] - float(task.RAM)
        curtime = time.time()
        if curtime + task.runtime <= task.ddl:
            if remain_cpu >= 0 and remain_ram >=0:
                return 0  # do not reject
            else:
                return 1  # reject temporarily because cpu or ram
        else:
            self.rej += 1 
#             print("rej")
            return -1  #reject because ddl
        
    def RR(self):
        """
        The baseline(Round-Robin) of project
        Arrange task server by server, VM by VM
        For each server find one VM for this task
        """
        i = 0 #no.sever 
        acc = 0
        while len(self.task) != 0:
            #assign all tasks in current queue
#             print("task len", len(self.task))
            while len(self.task) != 0:
#                 print(len(self.task))
                for t in self.task:
#                     print(t.jobID, t.index, t.status)
                    if t.status == -1: #rejected
                        self.dag.updateStatus(t)
                        self.task.remove(t)
                    elif t.status == 1:   #ready           
                        server_pass = 0
                        server_i = i 
                        vm_j = 0
                        #find which server
                        self.releaseByTime(server_i, vm_j)
                        rej = self.checkRej(server_i, vm_j, t)
                        if rej == -1:  #rejected due to ddl
                            t.status = -1
                        while rej != 0 and server_pass <= self.severNum and t.status == 1: 
                            rej = self.checkRej(server_i, vm_j, t)
                            #find which VM
                            while vm_j < len(self.VM[server_i]) and rej != 0 and t.status == 1:
                                if rej == -1:  #rejected due to ddl
                                    t.status = -1
                                self.releaseByTime(server_i, vm_j)
                                vm_j += 1
                            if vm_j == len(self.VM[server_i]):  #this server not meet the requirement
                                server_pass += 1
                                vm_j = 0
                                server_i += 1  #go to next server
                            else:
                                break
                            if server_i == self.severNum:
                                server_i = 0       
                            rej = self.checkRej(server_i, vm_j, t)
                        if server_pass <= self.severNum and vm_j < len(self.VM[server_i]) and t.status == 1:
                            #arrange task to server_i, vm_j VM
                            decision = [server_i, vm_j]
    #                             print(server_i, vm_j)
                            self.VMtask[decision[0]][decision[1]].append(t)
                            self.VM[decision[0]][decision[1]][0] -= float(t.CPU)
                            self.VM[decision[0]][decision[1]][1] -= float(t.RAM)
                            self.severs[decision[0]][0] -= float(t.CPU)  
                            self.severs[decision[0]][1] -= float(t.RAM) 
                            import time
                            curtime = time.time()
                            t.endtime = curtime + t.runtime
                            i += 1
                            if i == self.severNum:
                                i = 0
                            t.status = 2  #set statue to running
                            flag = True
                            for p in t.relative.sub_task:
                                if p.status == 1:
                                    flag = False
                            if flag == True:
                                t.relative.status = 0
                                acc += 1
                                self.rewardFcn2()
                            self.task.remove(t)   
#                             print(acc)
            self.generateQueue()
#         for t in self.dag.task
        print(round(1 - acc / self.num_task, 3), end=' ') 
 

p1 = environment('small', 'output_5000.txt', 1000, 100)
p1.training()


