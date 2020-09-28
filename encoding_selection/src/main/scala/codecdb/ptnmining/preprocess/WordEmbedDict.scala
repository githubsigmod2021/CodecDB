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

package codecdb.ptnmining.preprocess

import codecdb.wordvec.WordSource
import org.nd4j.linalg.api.ndarray.INDArray
import org.nd4j.linalg.ops.transforms.Transforms

import scala.collection.mutable.HashMap

object WordEmbedDict {
  val bufferSize = 500
}

/**
  * Read word embedding files
  *
  * @param source the dictionary file to load
  */
class WordEmbedDict(source: WordSource) {

  private val buffer = new HashMap[String, Option[INDArray]]
  private val additional = new HashMap[String, Option[INDArray]]

  def find(key: String): Option[INDArray] = {
    if (additional.contains(key))
      additional.getOrElse(key, None)
    else
      buffer.getOrElseUpdate(key, {
        if (buffer.size >= WordEmbedDict.bufferSize) {
          buffer.drop(buffer.size - WordEmbedDict.bufferSize + 1)
        }
        load(key)
      })
  }

  def addPhrase(text: String, words: Array[String]) = {
    val sum = words.flatMap(find).reduce((a, b) => a.add(b))
    additional.put(text, Some(sum))
  }

  /**
    * Compute the cosine similarity between two vectors
    */
  def compare(word1: String, word2: String): Double = {
    val data1 = find(word1)
    val data2 = find(word2)
    if (data1.isEmpty || data2.isEmpty) {
      throw new IllegalArgumentException("Word not found")
    }
    Transforms.cosineSim(data1.get, data2.get)
  }

  protected def load(key: String): Option[INDArray] = {
    return source.fetch(key).get(key)
  }
}