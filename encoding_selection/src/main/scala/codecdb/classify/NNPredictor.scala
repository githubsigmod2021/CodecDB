/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License,
 *
 * Contributors:
 *     Hao Jiang - initial API and implementation
 *
 */

package codecdb.classify

import java.nio.{ByteBuffer, ByteOrder, FloatBuffer}

import org.tensorflow.{SavedModelBundle, Tensor}

import scala.collection.JavaConversions._
//import edu.uchicago.cs.ndnn.figure.LineFigure

/**
  * Use Neural Network to choose encoding
  */

object BufferHelper {
  def makeFloatBuffer(arr: Array[Float]): FloatBuffer = {
    val bb = ByteBuffer.allocateDirect(arr.length * 4)
    bb.order(ByteOrder.nativeOrder)
    val fb = bb.asFloatBuffer
    fb.put(arr)
    fb.position(0)
    fb
  }
}

class NNPredictor(model: String, val numFeature: Int) {

  val savedModel = SavedModelBundle.load(model, "serve")
  val data = BufferHelper.makeFloatBuffer(Array.fill[Float](numFeature)(0))
  val input = Tensor.create(Array(1L, numFeature), data)
  val buffer = ByteBuffer.allocateDirect(8).order(ByteOrder.nativeOrder()).asLongBuffer()

  def predict(feature: Array[Double]): Int = {
    val result = savedModel.session().runner().fetch("accuracy/prediction").feed("x", input).run()
    buffer.clear()
    result(0).writeTo(buffer)
    buffer.get(0).toInt
  }
}

object NNLoader extends App {
  val savedModel = SavedModelBundle.load("/home/harper/IdeaProjects/enc-selector/src/main/nnmodel/int_model", "serve")

  var result = savedModel.session().runner().fetch("layer1/w1").fetch("layer2/w2").run()

  var w1 = result(0)
  var w2 = result(1)
  var array1 = Array.ofDim[Float](19,1000)
  var array2 = Array.ofDim[Float](1000,5)
  w1.copyTo(array1)
  w2.copyTo(array2)

  println(array1(0)(0))
  println(array2(0)(0))
}
