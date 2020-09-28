import collections
import io

import numpy as np
import tensorflow as tf

hidden_dim = 1000
input_size = 28 * 28
output_size = 10

train_data_file = "/home/harper/dataset/mnist/train-images.idx3-ubyte"
train_label_file = "/home/harper/dataset/mnist/train-labels.idx1-ubyte"
test_data_file = "/home/harper/dataset/mnist/t10k-images.idx3-ubyte"
test_label_file = "/home/harper/dataset/mnist/t10k-labels.idx1-ubyte"

Datasets = collections.namedtuple("Datasets", ['train', 'test'])


class Dataset(object):
    def __init__(self, data, label):
        self.data = data
        self.label = label
        self.size = self.data.shape[0]
        perm = np.random.permutation(self.size)
        self.data = self.data[perm]
        self.label = self.label[perm]

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


def read_data(file):
    with io.open(file, 'rb') as stream:
        magic = stream.read(4)

        num_record = np.frombuffer(stream.read(4), np.dtype(np.uint32).newbyteorder(">"))[0]

        raw = stream.read(input_size * num_record)
        flat = np.frombuffer(raw, np.uint8).astype(np.float32) / 255
        result = flat.reshape([-1, input_size])
        return result


def read_label(file):
    with io.open(file, 'rb') as stream:
        magic = stream.read(4)
        num_record = np.frombuffer(stream.read(4), np.dtype(np.uint32).newbyteorder(">"))[0]
        raw = stream.read(num_record)
        return np.frombuffer(raw, np.uint8).astype(np.int32)


def read_datasets():
    train_data = read_data(train_data_file)
    train_label = read_label(train_label_file)
    test_data = read_data(test_data_file)
    test_label = read_label(test_label_file)

    return Datasets(train=Dataset(train_data, train_label),
                    test=Dataset(test_data, test_label))


mnist = read_datasets()

x = tf.placeholder(tf.float32, [None, input_size], name="x")
label = tf.placeholder(tf.int64, [None], name="label")

with tf.name_scope("layer1"):
    w1 = tf.Variable(tf.truncated_normal([input_size, hidden_dim], stddev=0.01), name="w1")
    b1 = tf.Variable(tf.zeros([hidden_dim]), name="b1")
    layer1_out = tf.nn.relu(tf.matmul(x, w1) + b1, "l1o")

with tf.name_scope("layer2"):
    w2 = tf.Variable(tf.truncated_normal([hidden_dim, output_size], stddev=0.01), name="w2")
    b2 = tf.Variable(tf.zeros([output_size]), name="b2")
    layer2_out = tf.matmul(layer1_out, w2) + b2
with tf.name_scope("loss"):
    cross_entropy = tf.nn.sparse_softmax_cross_entropy_with_logits(labels=label, logits=layer2_out,
                                                                   name="cross_entropy")

cross_entropy = tf.reduce_mean(cross_entropy)

with tf.name_scope("sgd"):
    train_step = tf.train.AdamOptimizer(1e-3).minimize(cross_entropy)

with tf.name_scope("accuracy"):
    prediction = tf.argmax(layer2_out, 1, name="prediction")
    correct_prediction = tf.equal(prediction, label)
    accuracy = tf.reduce_mean(tf.cast(correct_prediction, tf.float32))

with tf.name_scope("summary"):
    tf.summary.histogram('b1', b1)
    tf.summary.histogram('w1', w1)
    tf.summary.histogram('w2', w2)
    tf.summary.histogram('b2', b2)
    tf.summary.scalar('accuracy', accuracy)

merged = tf.summary.merge_all()

train_writer = tf.summary.FileWriter("/home/harper/tftemp")
train_writer.add_graph(tf.get_default_graph())
builder = tf.saved_model.builder.SavedModelBuilder("/home/harper/mnistmodel")

with tf.Session() as sess:
    sess.run(tf.global_variables_initializer())

    for i in range(5000):
        batch = mnist.train.next_batch(50)
        if batch is None:
            break
        if i % 100 == 0:
            train_accuracy, merged_summary = sess.run([accuracy, merged], feed_dict={x: batch[0], label: batch[1]})
            train_writer.add_summary(merged_summary, i)
            print('step %d, training accuracy %g' % (i, train_accuracy))
            print(
                'test accuracy %g' % accuracy.eval(feed_dict={x: mnist.test.data, label: mnist.test.label}))
        _, merged_summary = sess.run([train_step, merged], feed_dict={x: batch[0], label: batch[1]})
        train_writer.add_summary(merged_summary, i)
    print(
        'test accuracy %g' % accuracy.eval(feed_dict={x: mnist.test.data, label: mnist.test.label}))

    # Build Signature to save to model
    signature = tf.saved_model.signature_def_utils.build_signature_def(
        inputs={
            'input': tf.saved_model.utils.build_tensor_info(x)
        },
        outputs={
            'output': tf.saved_model.utils.build_tensor_info(prediction)
        },
        method_name=tf.saved_model.signature_constants.PREDICT_METHOD_NAME
    )
    builder.add_meta_graph_and_variables(sess,
                                         [tf.saved_model.tag_constants.SERVING],
                                         signature_def_map={
                                             tf.saved_model.signature_constants.DEFAULT_SERVING_SIGNATURE_DEF_KEY:
                                                 signature})
builder.save()
train_writer.close()
