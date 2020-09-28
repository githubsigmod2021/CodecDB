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

package codecdb.ptnmining.parser

/**
  * Token is a data processing unit
  */
trait Token {
  def value: String

  def isData: Boolean = false

  /**
    * Token length in bytes
    */
  def length: Int = 1

  def numChar = value.length

  override def toString = value

  def canEqual(other: Any): Boolean = other.getClass.eq(this.getClass)

  override def equals(other: Any): Boolean = other match {
    case that: Token =>
      (that canEqual this) &&
        value == that.value
    case _ => false
  }

  override def hashCode(): Int = value.hashCode
}

class TWord(v: AnyRef) extends Token {
  val value = v.toString

  override def isData = true

  override def length = value.length
}

class TInt(v: AnyRef) extends Token {
  val value = v.toString
  val isHex = "[A-Fa-f]".r.findFirstMatchIn(value).isDefined

  override def isData = true

  def intValue = isHex match {
    case true => BigInt(value, 16)
    case false => BigInt(value)
  }

  override def length = 4
}

class TDouble(v: AnyRef) extends Token {
  val value = v.toString

  override def isData = true

  def doubleValue = BigDecimal(value)

  override def length = {
    val double = doubleValue.toDouble
    double == double.toFloat match {
      case true => 4
      case false => 8
    }
  }
}

class TSymbol(v: AnyRef) extends Token {
  val value = v.toString
}

class TSpace extends TSymbol(" ")

@deprecated("not in use right now")
class TPara(t: Int, l: Boolean) extends Token {
  val paraType = t
  val left = l

  def matches(another: TPara) =
    another.paraType == this.paraType && another.left != this.left

  def value: String = {
    val data = Array("(", ")", "{", "}", "[", "]")
    data(paraType * 2 + (if (left) 0 else 1))
  }
}

@deprecated("deprecated with TPara, grouping is done in parser")
class TGroup(t: Int, l: Seq[Token]) extends Token {
  val sym = t
  val content = l

  def value = "G(%s)".format(content.map(_.value).mkString(""))
}
