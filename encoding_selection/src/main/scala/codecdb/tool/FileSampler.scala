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

package codecdb.tool

import scala.io.Source
import scala.util.Random
import java.io.FileOutputStream
import java.io.PrintWriter

object FileSampler extends App {

  val file = "/home/harper/enc_workspace/date.tmp"
  val train = "/home/harper/enc_workspace/train.txt"
  val test = "/home/harper/enc_workspace/test.txt"
  val trainRate = 0.01
  val testRate = 0.001

  val trainOut = new PrintWriter(new FileOutputStream(train))
  val testOut = new PrintWriter(new FileOutputStream(test))
  Source.fromFile(file).getLines.foreach(line => {
    val rand = Random.nextDouble()
    if (rand < trainRate)
      trainOut.println(line)
    if (rand < testRate)
      testOut.println(line)
  })

  trainOut.close()
  testOut.close()
}