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

package codecdb.ptnmining

import codecdb.ptnmining.matching.NamingVisitor
import codecdb.ptnmining.parser.Token
import codecdb.ptnmining.rule.{CommonSeqRule, DataRewriteRule, IntegerRangeRule, MergeGroupRule, SuccinctRule, UseAnyRule}
import edu.uchicago.cs.encsel.ptnmining.parser._
import edu.uchicago.cs.encsel.ptnmining.rule._

import scala.collection.mutable

/**
  * Created by harper on 3/16/17.
  */

object Pattern {

  val rules = Array(new CommonSeqRule, new SuccinctRule, new MergeGroupRule, new IntegerRangeRule, new UseAnyRule)

  def generate(in: Seq[Seq[Token]]): Pattern = {
    // Generate a direct pattern by translating tokens

    val translated = PUnion(in.map(l => PSeq(l.map(new PToken(_)))))

    rules.foreach(rule => {
      rule match {
        case data: DataRewriteRule => data.generateOn(in)
        case _ => Unit
      }
    })
    // Repeatedly refine the pattern using supplied rules
    var toRefine: Pattern = translated
    var needRefine = true
    var refineResult: Pattern = toRefine
    while (needRefine) {
      val refined = refine(toRefine)
      if (refined._2) {
        toRefine = refined._1
      } else {
        needRefine = false
        refineResult = refined._1
      }
    }

    val validated = validate(refineResult)
    validated
  }

  protected def refine(root: Pattern): (Pattern, Boolean) = {
    var current = root

    rules.foreach(rule => {
      rule.reset
      current = rule.rewrite(current)
      if (rule.happened) {
        // Apply the first valid rule
        return (current, true)
      }
    })
    (root, false)
  }

  def validate(ptn: Pattern): Pattern = {
    ptn
  }
}

trait PatternVisitor {

  protected val path = new mutable.Stack[Pattern]

  def on(ptn: Pattern): Unit

  def enter(container: Pattern): Unit = path.push(container)

  def exit(container: Pattern): Unit = path.pop()
}


trait Pattern {
  private[ptnmining] var name = ""

  def getName = name

  /**
    * @return all leaf patterns
    */
  def flatten: Seq[Pattern] = Seq(this)

  /**
    * Recursively visit the pattern elements starting from the root
    *
    * @param visitor
    */
  def visit(visitor: PatternVisitor): Unit = visitor.on(this)

  def naming() = visit(new NamingVisitor)

  def numChar: (Int, Int)
}

class PToken(t: Token) extends Pattern {
  val token = t

  override def numChar: (Int, Int) = (token.numChar, token.numChar)

  def canEqual(other: Any): Boolean = other.isInstanceOf[PToken]

  override def equals(other: Any): Boolean = other match {
    case that: PToken =>
      (that canEqual this) &&
        token == that.token
    case _ => false
  }

  override def hashCode(): Int = token.hashCode()

  override def toString = token.toString
}

object PSeq {

  def apply(content: Seq[Pattern]): Pattern = {
    val filtered = content.filter(_ != PEmpty)
    filtered.length match {
      case 0 => PEmpty
      case 1 => filtered.head
      case _ => new PSeq(filtered)
    }
  }

  def collect(content: Pattern*): Pattern = apply(content)
}

class PSeq(cnt: Seq[Pattern]) extends Pattern {
  val content = cnt.toList

  override def flatten: Seq[Pattern] = content.flatMap(_.flatten)

  override def visit(visitor: PatternVisitor): Unit = {
    visitor.on(this)
    visitor.enter(this)
    content.foreach(_.visit(visitor))
    visitor.exit(this)
  }

  override def numChar: (Int, Int) = {
    content.map(_.numChar).reduce((a, b) => {
      (a._1 + b._1, a._2 + b._2)
    })
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[PSeq]

  override def equals(other: Any): Boolean = other match {
    case that: PSeq =>
      (that canEqual this) &&
        content == that.content
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(content)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString = "<S>(%s)".format(content.map(_.toString).mkString(","))

}

object PUnion {

  def apply(content: Seq[Pattern]): Pattern = {
    content.toSet.size match {
      case 0 => PEmpty
      case 1 => content.head
      case _ => new PUnion(content)
    }
  }

  def collect(content: Pattern*): Pattern = apply(content)
}

class PUnion(cnt: Seq[Pattern]) extends Pattern {
  val content = cnt.toSet.toList

  override def flatten: Seq[Pattern] = content.flatMap(_.flatten)

  override def visit(visitor: PatternVisitor): Unit = {
    visitor.on(this)
    visitor.enter(this)
    content.foreach(_.visit(visitor))
    visitor.exit(this)
  }

  override def numChar: (Int, Int) = {
    content.map(_.numChar).reduce((a, b) => {
      (Math.min(a._1, b._1), Math.max(a._2, b._2))
    })
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[PUnion]

  override def equals(other: Any): Boolean = other match {
    case that: PUnion =>
      (that canEqual this) &&
        content == that.content
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(content)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString: String = "<U>(%s)".format(content.map(_.toString).mkString(","))
}

object PEmpty extends Pattern {
  override def numChar: (Int, Int) = (0, 0)

  override def toString = "<empty>"
}

abstract class PAny(var minLength: Int, var maxLength: Int) extends Pattern {

  override def numChar: (Int, Int) = (minLength, maxLength)

  def canEqual(other: Any): Boolean = other.getClass.eq(this.getClass)

  override def equals(other: Any): Boolean = other match {
    case that: PAny =>
      (that canEqual this) &&
        minLength == that.minLength &&
        maxLength == that.maxLength
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(super.hashCode(), minLength, maxLength)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}

class PWordAny(minl: Int = 1, maxl: Int = -1)
  extends PAny(minl, maxl) {
  def this(limit: Int) = this(limit, limit)

  override def toString: String = "<wordany %d:%d>".format(minLength, maxLength)
}

class PDoubleAny(minl: Int = 1, maxl: Int = -1)
  extends PAny(minl, maxl) {
  def this(ml: Int) = this(ml, ml)

  override def toString: String = "<doubleany>"
}

class PIntAny(minl: Int = 1, maxl: Int = -1,
              var hasHex: Boolean = false) extends PAny(minl, maxl) {

  def this(limit: Int, hasHex: Boolean) = this(limit, limit, hasHex)

  def this(limit: Int) = this(limit, false)


  override def equals(other: Any): Boolean = other match {
    case that: PIntAny =>
      (that.canEqual(this)) &&
        minLength == that.minLength &&
        maxLength == that.maxLength &&
        hasHex == that.hasHex
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(super.hashCode(), minLength, maxLength, hasHex)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString: String = "<intany %d:%d>".format(minLength, maxLength)
}

class PWordDigitAny(minl: Int = 1, maxl: Int = -1)
  extends PAny(minl, maxl) {
  def this(limit: Int) = this(limit, limit)

  override def toString: String = "<any %d:%d>".format(minLength, maxLength)
}

/**
  * As we are extracting patterns from a small subset, determining range from
  * that is prone to error
  *
  * @deprecated
  */
@deprecated
class PIntRange extends Pattern {
  var min: BigInt = BigInt(0)
  var max: BigInt = BigInt(0)

  def this(min: BigInt, max: BigInt) {
    this()
    this.min = min
    this.max = max
  }

  def this(min: Int, max: Int) {
    this(BigInt(min), BigInt(max))
  }

  override def hashCode(): Int = this.min.hashCode() * 13 + this.max.hashCode()

  override def equals(obj: scala.Any): Boolean = {
    if (eq(obj.asInstanceOf[AnyRef]))
      return true
    obj match {
      case range: PIntRange => {
        this.min == range.min && this.max == range.max
      }
      case _ => super.equals(obj)
    }
  }

  override def numChar: (Int, Int) = {
    val factor = Math.log(2) / Math.log(10)
    val digitCount = (factor * max.bitLength + 1).toInt
    if (BigInt(10).pow(digitCount - 1).compareTo(max) > 0)
      (digitCount - 1, digitCount - 1)
    else
      (digitCount, digitCount)
  }
}
