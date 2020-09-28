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
package codecdb.util.word

import org.apache.commons.lang.StringUtils

import scala.collection.mutable.{ArrayBuffer, Buffer, HashMap}

class WordSplit {

  def split(raw: String): (Buffer[String], Double) = {
    // Remove all numbers
    val input = raw.replaceAll("""\d""", "_")
    input match {
      case x if !x.equals(x.toUpperCase()) && !x.equals(x.toLowerCase()) => {
        // Camel style
        var separated = x.replaceAll("(?<!^)([A-Z])(?=[a-z])", "_$1")
        separated = separated.replaceAll("(?<=[a-z])([A-Z])", "_$1")
        separated = separated.toLowerCase()
        split(separated)
      }
      case x if x.contains("_") => {
        // Separator
        val parts = x.split("_+")
        var fidelity = 1d
        (parts.map(part => {
          val lookup = Dict.lookup(part); fidelity *= lookup._2; lookup._1 })
          .filter(StringUtils.isNotEmpty).toBuffer, fidelity)
      }
      case _ => {
        guessMemory.clear
        guessSplit(input, 0, input.length)
      }
    }
  }

  /**
    * Dynamic Programming for Guess abbreviation
    */
  protected val guessMemory = new HashMap[(Int, Int), (Buffer[String], Double)]()

  protected def guessSplit(input: String, fromPos: Int, toPos: Int): (Buffer[String], Double) = {
    guessMemory.getOrElseUpdate((fromPos, toPos), {
      // Scan and recognize
      (fromPos, toPos) match {
        case (f, t) if f >= t => (ArrayBuffer.empty[String], 1)
        case (f, t) if f == t - 1 => {
          val lookup = Dict.lookup(input.substring(fromPos, toPos))
          (ArrayBuffer(lookup._1), lookup._2)
        }
        case _ => {
          // TODO Early stop
          ((fromPos + 1 to toPos).map(i => {
            val left = Dict.lookup(input.substring(fromPos, i))
            val right = guessSplit(input, i, toPos)
            (left._1 +: right._1, left._2 * right._2)
          })).maxBy(t => (t._2 - 0.1 * t._1.length))
        }
      }
    })
  }
}