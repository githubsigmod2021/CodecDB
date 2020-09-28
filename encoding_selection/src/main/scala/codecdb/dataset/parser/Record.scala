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
package codecdb.dataset.parser

trait Record {
  def apply(idx: Int): String

  def length(): Int

  def toString: String

  def iterator(): Iterator[String]
}

class DefaultRecord(val content: Array[String]) extends Record {

  def apply(idx: Int): String = {
    content(idx)
  }

  def length(): Int = {
    content.length
  }

  override def toString: String = {
    content.mkString("$$")
  }

  def iterator(): Iterator[String] = {
    content.toIterator
  }
}

class BlankRecord(size: Int) extends Record {
  val blankIterator = new Iterator[String] {
    var counter = 0

    def hasNext = counter < BlankRecord.this.size

    def next = {
      counter += 1; ""
    }
  }

  def apply(idx: Int): String = ""

  def length(): Int = size

  override def toString: String = ""

  def iterator(): Iterator[String] = blankIterator
}

object Record {
  val EMPTY = new DefaultRecord(Array[String]())
}