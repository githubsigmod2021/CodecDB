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

package codecdb.ptnmining.matching

import codecdb.ptnmining.{PDoubleAny, PEmpty, PIntAny, PIntRange, PSeq, PToken, PUnion, PWordAny, Pattern}
import codecdb.ptnmining.parser.{TDouble, TInt, TWord, Token, Tokenizer}
import edu.uchicago.cs.encsel.ptnmining._
import edu.uchicago.cs.encsel.ptnmining.parser._

import scala.collection.mutable.ArrayBuffer

/**
  * Find a possible match between a pattern and a sequence of token.
  * If a match can be found, a mapping between pattern node and token
  * will be returned.
  *
  * TODO The algorithm need to be improved to increase efficiency
  */
object NaiveMatcher extends PatternMatcher {

  /**
    * Match a token sequence with corresponding pattern
    *
    * As the token sequence will be toured several times, use
    * Seq rather than iterator here
    *
    * @param ptn   Pattern
    * @param input token sequences as string
    * @return None if not match
    *         Some(Record) if match found
    */
  def matchon(ptn: Pattern, input: String): Option[Record] = {
    val tokens = Tokenizer.tokenize(input).toSeq
    matchon(ptn, tokens)
  }

  def matchon(ptn: Pattern, tokens: Seq[Token]): Option[Record] = {
    val matchNode = Match.build(ptn)
    var items = matchNode.next

    while (items.nonEmpty) {
      val matched = matchItems(items, tokens)
      if (matched.isDefined) {
        matched.get.choices ++= matchNode.choices
        return matched
      }
      items = matchNode.next
    }
    None
  }

  def matchItems(ptns: Seq[Pattern], tokens: Seq[Token]): Option[Record] = {
    val record = new Record

    var pointer = 0
    var matched = true
    ptns.foreach(ptn => {
      if (matched) {
        if (pointer >= tokens.length)
          matched = false
        else
          ptn match {
            case PEmpty => {}
            case wany: PWordAny => {
              matched &= tokens(pointer).isInstanceOf[TWord]
              record.add(ptn.getName, tokens(pointer).value)
              pointer += 1
            }
            case iany: PIntAny => {
              matched &= tokens(pointer).isInstanceOf[TInt]
              record.add(ptn.getName, tokens(pointer).value)
              pointer += 1
            }
            case dany: PDoubleAny => {
              matched &= tokens(pointer).isInstanceOf[TDouble]
              record.add(ptn.getName, tokens(pointer).value)
              pointer += 1
            }
            case token: PToken => {
              matched &= tokens(pointer).equals(token.token)
              record.add(ptn.getName, tokens(pointer).value)
              pointer += 1
            }
            case range: PIntRange => {
              matched &= {
                if (tokens(pointer).isInstanceOf[TInt]) {
                  val intValue = tokens(pointer).asInstanceOf[TInt].intValue
                  record.rangeDeltas += ((range.name, intValue - range.min))
                  range.min <= intValue && range.max >= intValue
                } else {
                  false
                }
              }
              record.add(ptn.getName, tokens(pointer).value)
              pointer += 1
            }
            case _ => throw new IllegalArgumentException("Non-matchable Pattern:"
              + ptn.getClass.getSimpleName)
          }
      }
    })
    matched match {
      case true => Some(record)
      case false => None
    }
  }
}

private object Match {
  def build(ptn: Pattern): Match = {
    ptn match {
      case seq: PSeq => new SeqMatch(seq)
      case union: PUnion => new UnionMatch(union)
      case _ => new SimpleMatch(ptn)
    }
  }
}

private trait Match {

  def next: Seq[Pattern]

  def reset(): Unit

  /**
    * Record the choices of Union under this match
    *
    * @return (Union_Name, Choice)
    */
  def choices: Map[String, (Int, Int)]
}

private class SimpleMatch(ptn: Pattern) extends Match {
  private var used = false

  def next = used match {
    case true => Seq.empty[Pattern]
    case false => {
      used = true
      Seq(ptn)
    }
  }

  def reset() = used = false

  def choices = Map.empty[String, (Int, Int)]
}

private class SeqMatch(seq: PSeq) extends Match {

  val children = seq.content.map(Match.build)

  val items = new ArrayBuffer[Seq[Pattern]]

  def next = {
    if (items.isEmpty) {
      items ++= children.map(_.next)
    } else {
      // Look for next
      var pointer = items.length - 1
      var foundNext = false
      while (pointer >= 0 && !foundNext) {
        items.remove(pointer)
        children(pointer).next match {
          case e if e.isEmpty => {
            children(pointer).reset()
          }
          case x => {
            items += x
            foundNext = true
            pointer += 2
          }
        }
        pointer -= 1
      }
      if (foundNext) {
        for (i <- pointer until children.length)
          items += children(i).next
      }
    }
    items.flatten
  }

  def reset() = children.foreach(_.reset())

  def choices: Map[String, (Int, Int)] = children.map(_.choices).reduce((a1, a2) => a1 ++ a2)
}

private class UnionMatch(union: PUnion) extends Match {

  val children = union.content.map(Match.build)
  var counter = 0

  def next = {
    children(counter).next match {
      case empty if empty.isEmpty => {
        if (counter < children.length - 1) {
          counter += 1
          next
        } else {
          empty
        }
      }
      case valid => valid
    }
  }

  def reset() = {
    children.foreach(_.reset())
    counter = 0
  }

  def choices: Map[String, (Int, Int)] = children(counter).choices +
    (union.getName -> (counter, children.length))
}
