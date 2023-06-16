import numpy as np

class logisticRegression:
    
    def __init__(self, lr = 0.001, numIters = 10000):
        self.lr = lr
        self.numIters = numIters
        self.weights = None
        self.bias = None
        
    def fit(self, x, y):
        #init params
        numSamples, numFeatures = x.shape
        self.weights = np.zeros(numFeatures)
        self.bias = 0
        
        #gradient descent
        for _ in range(self.numIters):
            linear_model = np.dot(x, self.weights) + self.bias
            y_predicted = self._sigmoid(linear_model)
            
            dw = ( 1 /numSamples)*np.dot(x.T, (y_predicted - y))
            db = (1 / numSamples)* np.sum(y_predicted - y)
            
            self.weights -= self.lr * dw
            self.bias -= self.lr * db
            
    def predict(self, x):
        linear_model = np.dot(x, self.weights) + self.bias
        y_predicted = self._sigmoid(linear_model)
        y_predicted_cls = [1 if i > 0.5 else 0 for i in y_predicted]
        return y_predicted_cls
    
    def _sigmoid(self, x):
        return 1/(1+np.exp(-x))