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

import org.tensorflow.{SavedModelBundle, Tensor, TensorFlow}

import scala.collection.JavaConversions._

object MnistLoadModel extends App {

  val savedModel = SavedModelBundle.load("/home/harper/mnistmodel", "serve")

  val data = makeFloatBuffer(Array.fill[Float](28 * 28)(0))

  val input = Tensor.create(Array(1L, 784), data)

//  savedModel.graph().operations().foreach(o => println(o.name()))

  val result = savedModel.session().runner().fetch("accuracy/prediction").feed("x", input).run()

  val lbuffer = ByteBuffer.allocateDirect(8).order(ByteOrder.nativeOrder()).asLongBuffer()
  result(0).writeTo(lbuffer)
  println(lbuffer.get(0))


  def makeFloatBuffer(arr: Array[Float]): FloatBuffer = {
    val bb = ByteBuffer.allocateDirect(arr.length * 4)
    bb.order(ByteOrder.nativeOrder)
    val fb = bb.asFloatBuffer
    fb.put(arr)
    fb.position(0)
    fb
  }
}
