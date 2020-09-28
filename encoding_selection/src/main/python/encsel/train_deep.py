import shutil

import numpy as np
import sys
import tensorflow as tf
from tensorflow.contrib.learn.python.learn.datasets.base import load_csv_without_header

from encsel.dataset import DataSplit, drop, fetch

hidden_dim = 1000;
hidden_dim2 = 500;
hidden_dim3 = 100;

groups = [[0], [1, 2, 3, 4], [5, 6, 7, 8], [9], [10, 11, 12], [13, 14, 15], [16, 17, 18]]


def build_graph(num_feature, num_class):
    x = tf.placeholder(tf.float32, [None, num_feature], name="x")
    label = tf.placeholder(tf.int64, [None], name="label")

    with tf.name_scope("layer1"):
        w1 = tf.Variable(tf.truncated_normal([num_feature, hidden_dim], stddev=0.01), name="w1")
        b1 = tf.Variable(tf.zeros([hidden_dim]), name="b1")
        layer1_out = tf.tanh(tf.matmul(x, w1) + b1, "output")

    with tf.name_scope("hidden"):
        w2 = tf.Variable(tf.truncated_normal([hidden_dim, hidden_dim2], stddev=0.01), name="w2")
        b2 = tf.Variable(tf.zeros([hidden_dim2]), name="b2")
        layer2_out = tf.sigmoid(tf.matmul(layer1_out, w2) + b2, "output")

        w3 = tf.Variable(tf.truncated_normal([hidden_dim2, num_class], stddev=0.01), name="w3")
        b3 = tf.Variable(tf.zeros([num_class]), name="b3")
        hidden_out = tf.add(tf.matmul(layer2_out, w3), b3, "output")

        # w4 = tf.Variable(tf.truncated_normal([hidden_dim3, num_class], stddev=0.01), name="w4")
        # b4 = tf.Variable(tf.zeros([num_class]), name="b4")
        # hidden_out = tf.add(tf.matmul(layer3_out, w4), b4, "output")

        # w1o = tf.Variable(tf.truncated_normal([hidden_dim, num_class]))
        # w2o = tf.Variable(tf.truncated_normal([hidden_dim2, num_class]))
        # w3o = tf.Variable(tf.truncated_normal([hidden_dim3, num_class]))
        #
        # w1p = tf.matmul(layer1_out, w1o)
        # w2p = tf.matmul(layer2_out, w2o)
        # w3p = tf.matmul(layer3_out, w3o)
        #
        # hidden_out = tf.reduce_mean(tf.stack([hidden_out, w1p, w2p, w3p]), 0)

    with tf.name_scope("loss"):
        cross_entropy = tf.nn.sparse_softmax_cross_entropy_with_logits(labels=label, logits=hidden_out,
                                                                       name="cross_entropy")
        cross_entropy = tf.reduce_mean(cross_entropy)

    with tf.name_scope("sgd"):
        train_step = tf.train.AdamOptimizer(1e-3).minimize(cross_entropy)

    with tf.name_scope("accuracy"):
        prediction = tf.argmax(hidden_out, 1, name="prediction")
        correct_prediction = tf.equal(prediction, label)
        accuracy = tf.reduce_mean(tf.cast(correct_prediction, tf.float32))

    return x, label, train_step, accuracy, prediction


def train(data_file, model_path, num_class):
    ds = load_csv_without_header(data_file, np.int32, np.float32, 19)

    for dropi in range(7):
        group = groups[dropi]
        num_feature = len(group)
        dsplit = DataSplit(fetch(ds.data, group), ds.target, 0.75)
        dtrain = dsplit.getTrain()
        dtest = dsplit.getTest()

        x, label, train_step, accuracy, prediction = build_graph(num_feature, num_class)
        signature = tf.saved_model.signature_def_utils.build_signature_def(
            inputs={
                'input': tf.saved_model.utils.build_tensor_info(x)
            },
            outputs={
                'output': tf.saved_model.utils.build_tensor_info(prediction)
            },
            method_name=tf.saved_model.signature_constants.PREDICT_METHOD_NAME
        )
        shutil.rmtree(model_path, ignore_errors=True)

        best_test = 0

        with tf.Session() as sess:
            sess.run(tf.global_variables_initializer())
            # Build Signature to save to model

            # Start training loop
            for i in range(5000):
                batch = dtrain.next_batch(50)
                if batch is None:
                    break
                if i % 100 == 0:
                    train_accuracy = accuracy.eval(feed_dict={x: batch[0], label: batch[1]})
                    # print('step %d, training accuracy %g' % (i, train_accuracy))
                    test_accuracy = accuracy.eval(feed_dict={x: dtest.data, label: dtest.label})
                    # print('step %d, test accuracy %g' % (i, test_accuracy))
                    if best_test < test_accuracy:
                        best_test = test_accuracy
                        # print("Current best, saving model")
                        shutil.rmtree(model_path, ignore_errors=True)
                        builder = tf.saved_model.builder.SavedModelBuilder(model_path)
                        builder.add_meta_graph_and_variables(sess,
                                                             [tf.saved_model.tag_constants.SERVING],
                                                             signature_def_map={
                                                                 tf.saved_model.signature_constants.DEFAULT_SERVING_SIGNATURE_DEF_KEY: signature})
                        builder.save()
                        ## Print the value just saved
                train_step.run(feed_dict={x: batch[0], label: batch[1]})

        print("Final Result: %d, %g" % (dropi, best_test))


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
    train("/home/harper/enc_workspace/int_train.csv", '/home/harper/enc_workspace/int_model_deep/', 5)


if __name__ == "__main__":
    main2()
