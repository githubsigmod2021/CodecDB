import numpy as np
import tensorflow as tf

# Compute the relative importance of each variable. This is based on normalized data

num_feature = 19
hidden_dim = 1000
hidden_dim2 = 500
num_classes = 5

tf.reset_default_graph()
#
with tf.name_scope("layer1"):
    v1 = tf.get_variable("w1", shape=[num_feature, hidden_dim])
#
with tf.name_scope("hidden"):
    v2 = tf.get_variable("w2", shape=[hidden_dim, hidden_dim2])
    v3 = tf.get_variable("w3", shape=[hidden_dim2, num_classes])

# Add ops to save and restore all the variables.

# Later, launch the model, use the saver to restore variables from disk, and
# do some work with the model.
with tf.Session() as sess:
    sess.run(tf.global_variables_initializer())
    # Restore variables from disk.with tf.Session(graph=tf.Graph()) as sess:
    tf.saved_model.loader.load(sess, [tf.saved_model.tag_constants.SERVING],
                               "/home/harper/enc_workspace/int_model_deep")
    # Check the values of the variables

    value1 = v1.eval(sess)
    value2 = v2.eval(sess)

    out = np.sum(np.abs(value2), 1)

    mulweight = np.abs(value1) * out

    sum0 = np.sum(mulweight, 0)
    mulweight = mulweight / sum0
    sum1 = np.sum(mulweight, 1)

    sumt = np.sum(sum1)

    res = 100*sum1/sumt

    for i in range(20):
        print("("+str(i)+","+str(res[i])+")")

