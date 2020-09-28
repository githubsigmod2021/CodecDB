import numpy as np
from encsel.dataset import DataSet

data = np.asarray([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11])
label = data + 1

ds = DataSet(data, label)
batch = ds.next_batch(3)
assert(3 == batch[0].shape[0])
assert(3 == batch[1].shape[0])

batch = ds.next_batch(4)
assert(4 == batch[0].shape[0])
assert(4 == batch[1].shape[0])

batch = ds.next_batch(40)
assert(5 == batch[0].shape[0])
assert(5 == batch[1].shape[0])