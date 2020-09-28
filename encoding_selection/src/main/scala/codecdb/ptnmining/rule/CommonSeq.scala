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
 * under the License.
 *
 * Contributors:
 *     Hao Jiang - initial API and implementation
 */

package codecdb.ptnmining.rule

import scala.collection.mutable.ArrayBuffer


/**
  * Look for common sequences from a list of tokens
  *
  */

object CommonSeq {
  val DEFAULT_SEQ_LENGTH = 1
  // Percentage that a common sequence is not in some sentence
  // TODO: the tolerance is not supported now
  val DEFAULT_TOLERANCE = 0.1
}

class CommonSeq(val seqLength: Int = CommonSeq.DEFAULT_SEQ_LENGTH,
                val tolerance: Double = CommonSeq.DEFAULT_TOLERANCE) {

  implicit def bool2int(b: Boolean) = if (b) 1 else 0

  var positions = new ArrayBuffer[Seq[(Int, Int)]]

  /**
    * Look for common sequence in a list of lines. For implementation
    * simplicity, only the longest common seq is returned
    *
    * @param lines
    * @return common sequences
    */
  def find[T](lines: Seq[Seq[T]],
              equal: (T, T) => Boolean = (a: T, b: T) => a.equals(b)): Seq[Seq[T]] = {
    positions.clear

    val commons = new ArrayBuffer[Seq[T]]()
    commons += lines.head

    if (lines.length == 1) {
      positions += Seq((0, lines.head.size))
      return commons
    }
    var firstLine = true
    lines.drop(1).foreach(line => {
      if (commons.nonEmpty) {
        val commonsBetween = new ArrayBuffer[Seq[(Int, Int, Int)]]

        // val commonsBetween = commons.map(between(_, line, equal))
        var mask = 0
        commons.foreach(cm => {
          val btwn = between(cm, line.drop(mask), equal)
          commonsBetween += btwn.map(x => (x._1, x._2 + mask, x._3))
          if (btwn.nonEmpty)
            mask = btwn.last._2 + btwn.last._3
        })

        // Remove positions that are no longer valid
        val emptyIndices = commonsBetween.zipWithIndex
          .filter(_._1.isEmpty).map(_._2).toSet
        emptyIndices.isEmpty match {
          case true => Unit
          case false => {
            val newpos = positions.view.map(pos => pos.zipWithIndex
              .filter(act => {
                !emptyIndices.contains(act._2)
              }).map(_._1)).force
            positions.clear
            positions ++= newpos
          }
        }
        // In this algorithm, the commons between will not overlap
        val nonOverlap = commonsBetween.filter(_.nonEmpty)
        nonOverlap.isEmpty match {
          case true => commons.clear
          case false => {
            // Split the positions
            if (firstLine) {
              // For the first and second lines
              positions += nonOverlap(0).map(cmn => (cmn._1, cmn._3))
              positions += nonOverlap(0).map(cmn => (cmn._2, cmn._3))
              firstLine = false
            } else {
              positions = positions.map(pos => {
                pos.zip(nonOverlap).flatMap(pair => {
                  val oldpos = pair._1
                  val newseps = pair._2
                  newseps.map(newsep => (oldpos._1 + newsep._1, newsep._3))
                })
              })
              positions += nonOverlap.flatten.map(item => (item._2, item._3))
            }
            commons.clear
            commons ++= positions.last.map(pos => line.slice(pos._1, pos._1 + pos._2))
          }
        }
      }
    })
    commons
  }

  /**
    * Find Common sub-sequence in two sequences
    *
    * This method will find the longest subsequence with minimal separations.
    * E.g., if there are two candidates (0,1,2,3) and ((-1,4) (8,5)).
    * Although both has size 4, the first one will be chosen as it has no separation.
    *
    * @param a     the first sequence
    * @param b     the second sequence
    * @param equal equality test function
    * @return sequence of common symbols with length >= <code>sequence_length</code>.
    *         (a_start, b_start, length)
    */
  def between[T](a: Seq[T], b: Seq[T], equal: (T, T) => Boolean): Seq[(Int, Int, Int)] = {
    if (a.isEmpty || b.isEmpty)
      return Seq[(Int, Int, Int)]()
    val data = Array.fill(a.length + 1)(Array.fill(b.length + 1)(0))
    val longest = Array.fill(a.length + 1)(Array.fill(b.length + 1)((0, 0)))
    val path = Array.fill(a.length + 1)(new Array[(Int, Int, Int)](b.length + 1))

    (0 to a.length).foreach(i => {
      data(i)(0) = 0
      longest(i)(0) = (0, 0)
      path(i)(0) = (i, 0, 0)
    })
    (0 to b.length).foreach(i => {
      data(0)(i) = 0
      longest(0)(i) = (0, 0)
      path(0)(i) = (0, i, 0)
    })

    // Find the longest common in order subsequences

    // Ways to construct a longest subsequence
    // 1. Use the current sequence end at (i,j) of length l, together with L(i-l,j-l)
    // 2. Do not use the current sequence at (i,j), use max(L(i,j-1), L(i-1,j))
    for (i <- 1 to a.length; j <- 1 to b.length) {
      data(i)(j) = equal(a(i - 1), b(j - 1)) match {
        case true => data(i - 1)(j - 1) + 1
        case false => 0
      }

      val l1 = data(i)(j) match {
        case 0 => (-1, 0)
        case l => {
          val prev = longest(i - data(i)(j))(j - data(i)(j))
          (prev._1 + l, prev._2 + 1)
        }
      }
      val l2 = longest(i - 1)(j)
      val l3 = longest(i)(j - 1)
      longest(i)(j) = Array(l1, l2, l3).maxBy(a => (a._1, -a._2))
      // Record path
      longest(i)(j) match {
        case c2 if c2 == l2 => path(i)(j) = (i - 1, j, 0)
        case c3 if c3 == l3 => path(i)(j) = (i, j - 1, 0)
        case c1 if c1 == l1 => path(i)(j) = (i - data(i)(j), j - data(i)(j), data(i)(j))
      }
    }

    val result = new ArrayBuffer[(Int, Int, Int)]()

    // Construct the longest sequence from path
    var i = a.length
    var j = b.length
    while (i != 0 && j != 0) {
      val step = path(i)(j)
      if (step._3 != 0)
        result += step
      i = step._1
      j = step._2
    }
    if (path(i)(j)._3 != 0)
      result += path(i)(j)
    result.reverse
  }
}

