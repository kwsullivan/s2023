import numpy as np
from sklearn.model_selection import train_test_split
from sklearn import datasets
import matplotlib as plt
import seaborn as sns

from logRegression import logisticRegression
from breakDownByLog import getByLog

#dummy dataset
#bc = datasets.load_breast_cancer()
br = getByLog()
print(br)

#split dataset in features and target variable
feature_cols = ['FeedbackSpeed', 'FeedbackPressure']
X = br[feature_cols] # Features
y = br.label # Target variable



X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=1234)

def accuracy(y_true, y_pred):
    accuracy = np.sum(y_true == y_pred)/len(y_true)
    return accuracy

regressor = logisticRegression(lr=0.0001, numIters=1000)
regressor.fit(X_train, y_train)
predictions = regressor.predict(X_test)

print("LR classification accuracy", accuracy(y_test, predictions))

