# -*- coding: utf-8 -*-
"""
Created on Fri Oct 16 11:39:06 2015

@author: shequan.jiang
"""
import matplotlib.pyplot as plt
import numpy as np
import sklearn
import matplotlib

def predict(model, x):
    W1, b1, W2, b2 = model['W1'], model['b1'], model['W2'], model['b2']
    # Forward propagation
    z1 = x.dot(W1) + b1
    a1 = np.tanh(z1)
    z2 = a1.dot(W2) + b2
    exp_scores = np.exp(z2)
    probs = exp_scores / np.sum(exp_scores, axis=1, keepdims=True)
    return np.argmax(probs, axis=1)
    
def calculate_loss(model):
    W1, b1, W2, b2 = model['W1'], model['b1'], model['W2'], model['b2']
    # Forward propagation to calculate our predictions
    z1 = X.dot(W1) + b1
    a1 = np.tanh(z1)
    z2 = a1.dot(W2) + b2
    exp_scores = np.exp(z2)
    probs = exp_scores / np.sum(exp_scores, axis=1, keepdims=True)
    # Calculating the loss
    corect_logprobs = -np.log(probs[range(num_examples), y])
    data_loss = np.sum(corect_logprobs)
    # Add regulatization term to loss (optional)
    data_loss += reg_lambda/2 * (np.sum(np.square(W1)) + np.sum(np.square(W2)))
    return 1./num_examples * data_loss

def build_model(nn_hdim, num_passes=20000, print_loss=False):
    # Initialize the parameters to random values. We need to learn these.
    np.random.seed(0)
    W1 = np.random.randn(nn_input_dim, nn_hdim) / np.sqrt(nn_input_dim)
    b1 = np.zeros((1, nn_hdim))
    W2 = np.random.randn(nn_hdim, nn_output_dim) / np.sqrt(nn_hdim)
    b2 = np.zeros((1, nn_output_dim))
    # This is what we return at the end
    model = {}    
    # Gradient descent. For each batch...
    for i in xrange(0, num_passes):
        # Forward propagation
        z1 = X.dot(W1) + b1
        a1 = np.tanh(z1)
        z2 = a1.dot(W2) + b2
        exp_scores = np.exp(z2)
        probs = exp_scores / np.sum(exp_scores, axis=1, keepdims=True)
        # Backpropagation
        delta3 = probs
        delta3[range(num_examples), y] -= 1
        dW2 = (a1.T).dot(delta3)
        db2 = np.sum(delta3, axis=0, keepdims=True)
        delta2 = delta3.dot(W2.T) * (1 - np.power(a1, 2))
        dW1 = np.dot(X.T, delta2)
        db1 = np.sum(delta2, axis=0)
        # Add regularization terms (b1 and b2 don't have regularization terms)
        dW2 += reg_lambda * W2
        dW1 += reg_lambda * W1
        # Gradient descent parameter update
        W1 += -epsilon * dW1
        b1 += -epsilon * db1
        W2 += -epsilon * dW2
        b2 += -epsilon * db2
        # Assign new parameters to the model
        model = { 'W1': W1, 'b1': b1, 'W2': W2, 'b2': b2}
        # Optionally print the loss.
        # This is expensive because it uses the whole dataset, so we don't want to do it too often.
        if print_loss and i % 1000 == 0:
          print "Loss after iteration %i: %f" %(i, calculate_loss(model))
    return model
    
if __name__ == "__main__":    
    fr = open("tmp_train_feature2_card_no_isstudent.tsv",'r')                        #处理训练集
    num_examples = len(fr.readlines())
    X = np.zeros((num_examples,2))
    y = np.zeros(num_examples, dtype = int)
    linearrtmp = 2*[0]
    
    nn_input_dim = 2 # input layer dimensionality
    nn_output_dim = 2 # output layer dimensionality
    epsilon = 0.01 # learning rate for gradient descent
    reg_lambda = 0.01 # regularization strength 
    
    index = 0
    for line in open("tmp_train_feature2_card_no_isstudent.tsv",'r'):
        list1=line.strip().split("\t")
        for i in range(2):
            if list1[i+1] == 'NULL':
                list1[i+1] = 1
            linearrtmp[i] = float(list1[i+1])
        X[index,0:2] = linearrtmp[0:2]
        y[index] = int(list1[19])
        index += 1
    
    fr = open("tmp_rand20_user_feature2_card_no_student.tsv",'r')                   #处理测试集
    num_examples2 = len(fr.readlines())
    X2 = np.zeros((num_examples2,2))
    y2 = np.ones((num_examples2,1), dtype = int)
    linearrtmp =2*[0]
    
    index = 0
    for line in open("tmp_rand20_user_feature2_card_no_student.tsv",'r'):
        list2=line.strip().split("\t")
        for i in range(2):
            if list2[i+1] == 'NULL':
                list2[i+1] = 1
            linearrtmp[i] = float(list2[i+1])
        X2[index,0:2] = linearrtmp[0:2]
        y2[index] = int(list2[19])
        index += 1
    
    model = build_model(4, num_passes=16000, print_loss=True)
    result = predict(model, X2)
    
    P1 = 0
    P2 = 0
    P3 = 0
    total = 0
    for i in range(num_examples2):
        if (y2[i] == 1):
            P1 += 1 
        if (result[i] == 1):
            P2 += 1
        if (y2[i] == 1 and result[i] == 1):
            P3 += 1
        total += 1
    print "召回率：",float(P3)/float(P1)
    print "准确率：",float(P3)/float(P2)
    print "测试集样本数：",total
    print "预测学生总数：",P2
    print "实际学生总数：",P1