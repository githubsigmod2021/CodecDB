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

package codecdb.wordvec

import javax.persistence._

import codecdb.tool.WordVectorQuery.src
import org.nd4j.linalg.api.ndarray.INDArray
import org.nd4j.linalg.factory.Nd4j

import scala.beans.BeanProperty
import scala.collection.{Map, mutable}
import scala.io.Source
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

trait WordSource {
  def fetch(words: String*): Map[String, INDArray]
}

class FileWordSource(val filePath: String) extends WordSource {

  override def fetch(words: String*): Map[String, INDArray] = {
    val buffer = new mutable.HashMap[String, INDArray]
    Source.fromFile(filePath).getLines().takeWhile(p => buffer.size < words.size)
      .foreach(line => {
        val sp = line.split("\\s+")
        if (words.contains(sp(0))) {
          buffer.put(sp(0), Nd4j.create(sp.drop(1).map(_.toDouble)))
        }
      })
    buffer
  }
}

class DbWordSource extends WordSource {

  private val em = Persistence.createEntityManagerFactory("word-vector").createEntityManager()

  override def fetch(words: String*): Map[String, INDArray] = {
    val wvs = em.createQuery("SELECT w FROM WordVector w where w.word in :words", classOf[WordVector])
      .setParameter("words", words.toList.asJava).getResultList
    wvs.map(w => (w.word, Nd4j.create(w.vector.split("\\s+").map(_.toDouble)))).toMap
  }
}

@Entity
@Table(name = "word_vec")
class WordVector {


  @BeanProperty
  @Id
  @Column(name = "word")
  var word: String = ""
  @BeanProperty
  @Column(name = "vector")
  var vector: String = ""

  def this(w: String, v: String) {
    this()
    this.word = w
    this.vector = v
  }
}