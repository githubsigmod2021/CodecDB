import numpy as np


class DataSet:
    def __init__(self, data, label):
        self.data = data
        self.label = label
        self.size = self.data.shape[0]
        self.start = 0

    def next_batch(self, batch_size):
        if self.start == self.size:
            perm = np.random.permutation(self.size)
            self.data = self.data[perm]
            self.label = self.label[perm]
            self.start = 0
        start = self.start
        end = min(start + batch_size, self.size)
        self.start = end
        return [self.data[start:end], self.label[start:end]]


def normalize(data, dim):
    mean = np.mean(data, dim)
    stdvar = np.std(data, dim)
    return np.divide(np.subtract(data, mean), stdvar)


def drop(data, idx):
    return np.delete(data, idx, axis=1)

def fetch(data, idx):
    return data[:, idx];

class DataSplit:
    def __init__(self, data, label, ratio):
        dsize = len(data);
        train_size = int(ratio * dsize)
        test_size = dsize - train_size
        permu = np.random.permutation(dsize).tolist()
        permu_train = permu[:train_size]
        permu_test = permu[train_size:]

        ndata = normalize(data, 0)

        self.trainSet = DataSet(ndata[permu_train], label[permu_train])
        self.testSet = DataSet(ndata[permu_test], label[permu_test])

    def getTrain(self):
        return self.trainSet

    def getTest(self):
        return self.testSet


if __name__ == "__main__":
    ds = DataSplit(np.array([1, 2, 3, 4, 5]), np.array([6, 7, 8, 9, 10]), 0.7)
    train = ds.getTrain()
    test = ds.getTest()
    print(train.data)
    print(train.label)
