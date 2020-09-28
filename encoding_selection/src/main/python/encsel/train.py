import sys
import numpy as np
import shutil
import tensorflow as tf
from tensorflow.contrib.learn.python.learn.datasets.base import load_csv_without_header

from encsel.dataset import DataSet, DataSplit

num_feature = 19;
hidden_dim = 1000;


def build_graph(num_class):
    x = tf.placeholder(tf.float32, [None, num_feature], name="x")
    label = tf.placeholder(tf.int64, [None], name="label")

    with tf.name_scope("layer1"):
        w1 = tf.Variable(tf.truncated_normal([num_feature, hidden_dim], stddev=0.01), name="w1")
        b1 = tf.Variable(tf.zeros([hidden_dim]), name="b1")
        layer1_out = tf.tanh(tf.matmul(x, w1) + b1, "output")

    with tf.name_scope("layer2"):
        w2 = tf.Variable(tf.truncated_normal([hidden_dim, num_class], stddev=0.01), name="w2")
        b2 = tf.Variable(tf.zeros([num_class]), name="b2")
        layer2_out = tf.add(tf.matmul(layer1_out, w2), b2, "output")

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

    return x, label, train_step, accuracy, prediction


def train(data_file, model_path, num_class):
    ds = load_csv_without_header(data_file, np.int32, np.float32, 19);
    dsplit = DataSplit(ds.data,ds.target,0.75)
    dtrain = dsplit.getTrain()
    dtest = dsplit.getTest()

    x, label, train_step, accuracy, prediction = build_graph(num_class)
    builder = tf.saved_model.builder.SavedModelBuilder(model_path)

    with tf.Session() as sess:
        sess.run(tf.global_variables_initializer())

        for i in range(5000):
            batch = dtrain.next_batch(50)
            if batch is None:
                break
            if i % 100 == 0:
                train_accuracy = accuracy.eval(feed_dict={x: batch[0], label: batch[1]})
                print('step %d, training accuracy %g' % (i, train_accuracy))
                test_accuracy = accuracy.eval(feed_dict={x: dtest.data, label:dtest.label})
                print('step %d, test accuracy %g' % (i, test_accuracy))
            train_step.run(feed_dict={x: batch[0], label: batch[1]})

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


def print_usage():
    print("Usage: train.py <data_file> <model_path> <num_class>")
    exit(0)


def main():
    if len(sys.argv) != 4:
        print_usage()
    data_file = sys.argv[1]
    model_path = sys.argv[2]
    num_class = int(sys.argv[3])
    train(data_file, model_path, num_class)


def main2():
    shutil.rmtree("/home/harper/enc_workspace/int_model/",ignore_errors=True)
    train("/home/harper/enc_workspace/int_train.csv", '/home/harper/enc_workspace/int_model/', 5)


if __name__ == "__main__":
    main2()
