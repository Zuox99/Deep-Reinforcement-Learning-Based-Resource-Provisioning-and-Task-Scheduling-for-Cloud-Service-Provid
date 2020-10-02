"""""
This code is taken from https://github.com/philtabor/Deep-Q-Learning-Paper-To-Code and modified as per our requirement
Shahid Mohammed
"""""

import numpy as np
import torch.nn as nn
import torch.nn.functional as F
import torch.optim as optim
import torch as T
import random

#from util import plot_learning_curve

class LinearDeepQNetwork(nn.Module):
    def __init__(self, lr, n_actions, input_dims):
        super(LinearDeepQNetwork, self).__init__()

        self.fc1 = nn.Linear(input_dims, 128)
        self.fc2 = nn.Linear(128, n_actions)

        self.optimizer = optim.Adam(self.parameters(), lr=lr)
        self.loss = nn.MSELoss()
        self.device = T.device('cuda:0' if T.cuda.is_available() else 'cpu')
        self.to(self.device)

    def forward(self, state):
        layer1 = F.relu(self.fc1(state))
        actions = self.fc2(layer1)
        return actions


class Agent():
    def __init__(self, input_dims, n_actions, lr, gamma=0.99,
                 epsilon=1.0, eps_dec=1e-5, eps_min=0.01):
        self.lr = lr
        self.input_dims = input_dims
        self.n_actions = n_actions
        self.gamma = gamma
        self.epsilon = epsilon
        self.eps_dec = eps_dec
        self.eps_min = eps_min
        self.action_space = [i for i in range(self.n_actions)]

        self.Q = LinearDeepQNetwork(self.lr, self.n_actions, self.input_dims)

    def choose_action(self, state):
        if np.random.random() > self.epsilon:
            state1 = T.tensor(state, dtype=T.float).to(self.Q.device)
            # state =
            actions = self.Q.forward(state1)
            action = T.argmax(actions).item()
        else:
            action = np.random.choice(self.action_space)

        return action

    def decrement_epsilon(self):
        self.epsilon = self.epsilon - self.eps_dec \
                        if self.epsilon > self.eps_min else self.eps_min

    def learn(self, state, action, reward, state_):
        self.Q.optimizer.zero_grad()
        states = T.tensor(state, dtype=T.float).to(self.Q.device)
        actions = T.tensor(action).to(self.Q.device)
        rewards = T.tensor(reward).to(self.Q.device)
        states_ = T.tensor(state_, dtype=T.float).to(self.Q.device)

        q_pred = self.Q.forward(states)[actions]

        q_next = self.Q.forward(states_).max()

        q_target = reward + self.gamma*q_next

        loss = self.Q.loss(q_target, q_pred).to(self.Q.device)
        loss.backward()
        self.Q.optimizer.step()
        self.decrement_epsilon()

def extractData(fileName):
    # taken from https://www.tutorialspoint.com/How-to-read-text-file-into-a-list-or-array-with-Python
    f = open(fileName, 'r+')
    # taken from https://docs.scipy.org/doc/numpy/reference/generated/numpy.fromstring.html
    data = [np.fromstring(line, dtype=float, sep=' ') for line in f.readlines()]

    f.close()
    return data

"""""
Random environment for DQN model
"""""

def getReward(action):
    return random.randint(0, 10)

def getNextState(action, input):
    i = random.randint(0, 99)
    return input[i]

"""""
This code for to train the DQN model with our data
"""""

if __name__ == '__main__':
    input = extractData('input.txt')
    input1 = np.array(input)

    episodes = 10
    scores = []
    eps_history = []

    agent = Agent(lr=0.0001, input_dims=7,
                  n_actions=10)
    f = open("results_dqn.txt", "w")
    for i in range(episodes):
        score = 0
        done = False
        initial_state = input1[0]
        curr_state = initial_state
        j = 0
        while not done:
            action = agent.choose_action(initial_state)
            next_state = getNextState(action, input1)

            reward = getReward(action)
            score += reward
            agent.learn(curr_state, action, reward, next_state)
            curr_state = next_state

            j += 1
            if j >= 1000 :
                done = True
        scores.append(score)
        eps_history.append(agent.epsilon)

        avg_score = np.mean(scores[-100:])
        f.write("{}{}{}\n".format('episode ', i, 'score %.1f avg score %.1f epsilon %.2f' %
                                  (score, avg_score, agent.epsilon)))

    f.close()
