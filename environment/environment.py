#!/usr/bin/env python
# -*- coding: utf-8 -*-

######################################
# coding by YingzhuLiu & Xuanzuo     #
# the envitonment of RP/TS processor #
# input: a task file(output_5000.txt)#
# output: the total energy cost      #
# the reject rate            	     #
######################################

class Task(object):   
    """
    information of each task
    parent, child base on dependency
    jobID, index, CPU, RAM, disk extracted from user data
    status indicates the current status of the task
    """
    def __init__(self, jobID, index, CPU, RAM, disk, status):
        self.parent = []
        self.child = []
        self.jobID = jobID
        self.index = index
        self.CPU = CPU
        self.RAM = RAM
        self.disk = disk
        self.status = status  #-1: rejected, 0: finished, 1: ready
        self.ddl = 0
        self.runtime = 0
        
class DAG(object):
    """
    Transform job queue to task ready queue
    """
    def __init__(self, fname, num_task):
        self.fname = fname
        self.num_task = num_task
        self.job = []
        self.task = []
    
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
                    task.append(Task(info[1], info[2], info[4], info[5], info[6], 1))
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
    
    def checkRing(self, parent, child, job): 
        """
        Check whether there is a loop between parent and child
        Return True if has loop
        """
        if parent.index == child.index:
            return True
        if len(child.child) == 0:
            return False
        for c in child.child:
            if self.checkRing(parent, job[c], job):
                return True
        return False
    
    
    def buildDAG(self):
        """
        Randomly build dependencies between tasks within each job
        """
        import random
        for job in self.job:           
            for k in range(len(job)):
                task = job[k]
                i = random.randint(-len(job), len(job) - 1)
                if i < 0:
                    continue
                parent = job[i]
                if self.checkRing(parent, task, job) == False:
                    task.parent.append(i)
                    parent.child.append(k)
                    
    def findTask(self, jobid, taskid):
        """
        Find the task by jobId and taskId
        Input: jobId and taskId
        Output: task inedx
        """
        for i in range(len(self.job)):
            tasks = self.job[i]
            if(tasks[0].jobID == jobid):
                for j in range(len(tasks)):
                    if tasks[j].index == taskid:
                        return i, j
        return -1, -1
    
    def rejTask(self, task, job):
        """
        If one task is rejected
        Then all tasks that depended on this task will be rejected
        """
        task.status = -1
        for c in task.child:
            self.rejTask(job[c], job)
    
    def rmParent(self, task, task_i, job):
        """
        When a task are finished
        Remove it from the parent for all child tasks
        """
        for c in task.child:
            job[c].parent.remove(task_i)
    
    def updateStatus(self, task):
        """
        Given jobid and taskid, change status of all tasks that depend on it
        If the task with "-1" status, reject this tasks' all child tasks
        If the task with "0" status, remove it from all child tasks
        """
        job_i, task_i = self.findTask(task.jobID, task.index)
        if job_i == -1 or task_i == -1:
            print("WRONG: The task with jobID: ", task.jobID, " and taskID: ", task.index, " not exist.")
            return
        job = self.job[job_i]
        task = job[task_i]
        if task.status == -1:
            self.rejTask(task, job)
        elif task.status == 0:
            self.rmParent(task, task_i, job)
     
    def taskQueue(self): 
        """
        Build the task ready queue
        """
        self.readfile()
        self.buildDAG()
        for job in self.job:
            num_task = len(job)
            while num_task > 0:
                for task in job:
                    if task.status == 1 and len(task.parent) == 0:
                        self.task.append(task)
                        task.status = 0
                        self.updateStatus(task)
                        num_task -= 1
        for t in self.task:
            t.status = 1
        print(len(self.task), "requests")

    def printTask(self):
        for j in self.task:
            print(j.jobID, ",", j.index, ",", j.status)                  


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
        self.curtime = 0
        self.scale = scale
        self.fname = fname
        self.task = []
        self.dag = DAG(self.fname, num_task)
        self.VMNum = 10
        if self.scale == 'small':
#             self.severNum = 200
            self.farmNum = 10
        elif self.scale == 'large':
#             self.severNum = 4000
            self.farmNum = 70
        self.severNum = num_server
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

    def generateQueue(self):
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
        pwr = sum(pwrCur) - sum(self.pwrPre)
        self.pwrPre = pwrCur
        print ("energy cost: ", round(self.elecPrice(1, pwr), 3))

    def release(self):
        """
        Randomly release resources from each VM
        And set the corresponding task as finished
        """
        import random
        ranSer = random.randint(0, self.severNum-1)
        ranVM = random.randint(0, self.VMNum-1)
        if self.VMtask[ranSer][ranVM]:
            random.shuffle(self.VMtask[ranSer][ranVM])
            t = self.VMtask[ranSer][ranVM].pop()
            t.status = 0
            self.VM[ranSer][ranVM][0] += float(t.CPU)
            self.VM[ranSer][ranVM][1] += float(t.RAM)

    def training(self):
        """
        Run the DQN/baseline in the RP/TS processor environment
        Read the task file by readfile()
        Set the variables
        Pass tasks to agents in real time
        Get the corresponding reward value
        Reject task when R_cpu ≥ C_cpu or R_ram < C_ram
        """
        #send one tesk to dqn and calculate reward
        print(self.severNum, "servers", end=' ')
        self.generateQueue()
        self.setFarm()
        import time
        time_start=time.time()
        self.RR()
        time_end=time.time()
        print('Time cost', round(time_end-time_start, 3),'s', end=' ')
#         self.dqn()
        self.rewardFcn2()

    def checkRej(self, server_i, vm_j, task):
        """
        Check whether this task should be rejected in ith sever, jth VM
        Reject task when remain_cpu or remain_ram or remain_ram < 0
        """
        remain_cpu = self.VM[server_i][vm_j][0] - float(task.CPU)
        remain_ram = self.VM[server_i][vm_j][1] - float(task.RAM)
        if remain_cpu >= 0 and remain_ram >=0 and self.curtime + task.runtime <= task.ddl:
            return False
        return True
        # remain_cpu = self.severs[i][0] - float(task.CPU)
        # remain_ram = self.severs[i][1] - float(task.RAM)
        # remain_disk = self.severs[i][2] - float(task.disk)
        # if remain_cpu >= 0 and remain_ram >= 0 and remain_disk >= 0:
        #     return False
        # return True
    
    def dqn(self):
        """
        The DQN of project
        Provide current status information
        Reject task when run out of CPU or RAM
        This is just a virtual part and will be replaced by real dqn part
        """
        rej = 0
        for t in self.task:
#             print(float(t.CPU))
            for vm in self.VM[5]:
                # print "vm"
                self.release()
            decision = [0, 5]
            if not self.checkRej(decision[0], decision[1], t):
                self.VMtask[decision[0]][decision[1]].append(t)
                self.VM[decision[0]][decision[1]][0] -= float(t.CPU)
                self.VM[decision[0]][decision[1]][1] -= float(t.RAM)
                self.severs[decision[0]][0] -= float(t.CPU)  
                self.severs[decision[0]][1] -= float(t.RAM) 
                self.severs[decision[0]][2] -= float(t.disk)
            else:
                t.status = -1
                self.dag.updateStatus(t)
                rej += 1
        print(rej)

        
    def RR(self):
        """
        The baseline(Round-Robin) of project
        Arrange task server by server
        For each server find one VM for this task
        """
        i = 0 #no.sever
        rej = 0  #number of rejected task  
        for t in self.task:
            if t.status != 1:  #rejected or finished
                continue            
            if i == self.severNum:
                i = 0
            for vm in self.VM[i]:
                self.release()
            server_pass = 0
            server_i = i 
            vm_j = 0
            #find which server
            while self.checkRej(server_i, 0, t) and server_pass <= self.severNum:
                vm_j = 1   
                #find which VM
                while vm_j < len(self.VM[server_i]) and self.checkRej(server_i, vm_j, t):
                    vm_j += 1
                if vm_j == len(self.VM[server_i]):  #this server not meet the requirement
                    server_pass += 1
                    server_i += 1  #go to next server
                else:
                    break
                if server_i == self.severNum:
                    server_i = 0           
            if server_pass > self.severNum or vm_j >= len(self.VM[server_i]):  #reject task
                t.status = -1
                self.dag.updateStatus(t)
                rej += 1
                continue
            decision = [server_i, vm_j]
            self.VMtask[decision[0]][decision[1]].append(t)
            self.VM[decision[0]][decision[1]][0] -= float(t.CPU)
            self.VM[decision[0]][decision[1]][1] -= float(t.RAM)
            self.severs[decision[0]][0] -= float(t.CPU)  
            self.severs[decision[0]][1] -= float(t.RAM) 
            self.severs[decision[0]][2] -= float(t.disk)
            t.status = 0  #finish task
            i += 1
        print("Reject rate: ", rej, end=' ')        

p1 = environment('small', 'output_5000.txt', 1000, 300)
p1.training()


