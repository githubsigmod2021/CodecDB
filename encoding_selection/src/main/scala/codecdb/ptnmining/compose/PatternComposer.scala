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

package codecdb.ptnmining.compose

class PatternComposer(pattern: String) {

  private var booleanColIndex: Set[Int] = _

  private var optionalColIndex: Set[Int] = _

  private var groups: Seq[GroupToken] = _

  private var probe = -1

  val (format, numGroup) = parse(pattern)

  protected def parse(pattern: String): (String, Int) = {
    val parsed = RegexParser.parse(pattern)

    groups = parsed.filter(_.isInstanceOf[GroupToken]).map(_.asInstanceOf[GroupToken])

    optionalColIndex = groups.zipWithIndex.filter(p => isOptional(p._1)).map(_._2).toSet

    booleanColIndex = groups.zipWithIndex.filter(p => isBoolean(p._1)).map(_._2).toSet

    if (numGroup > optionalColIndex.size) {
      probe = ((0 until numGroup).toSet -- optionalColIndex).head
    }

    val patternStr = parsed.map(_ match {
      case g: GroupToken => {
        "%s"
      }
      case s: SimpleToken => {
        s.content match {
          case '^' | '$' => ""
          case 's' => if (s.escape) " " else "s"
          case other => s.toString
        }
      }
      case _ => throw new IllegalArgumentException
    }).mkString("")

    (patternStr, parsed.filter(_.isInstanceOf[GroupToken]).size)
  }

  private def isOptional(token: GroupToken): Boolean = {
    token.rep == 1 || token.rep == 2 ||
      token.children.forall(c => c.rep == 1 || c.rep == 2)
  }

  private def isBoolean(token: GroupToken): Boolean = {
    token.rep == 1 && token.children.size == 1 && {
      token.children.last match {
        case s: SimpleToken => {
          s.rep == 0 && s.escape == false && !Character.isLetterOrDigit(s.content)
        }
        case _ => false
      }
    }
  }

  def booleanColumns: Set[Int] = booleanColIndex

  def optionalColumns: Set[Int] = optionalColIndex

  def group(index: Int): String = groups(index).children.mkString("")

  def compose(data: Seq[String]): String = {
    if (data.length != numGroup)
      throw new IllegalArgumentException("Expecting %d columns, receiving %d columns".format(numGroup, data.length))

    if (probe != -1 && null == data(probe)) {
      return ""
    }

    val dataArray = data.toArray
    booleanColumns.foreach(i => {
      dataArray(i) = if (data(i) == "true") group(i).head.toString else ""
    })


    format.format(dataArray: _*)
  }
}
