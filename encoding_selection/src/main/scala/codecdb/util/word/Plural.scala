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

import codecdb.dataset.parser.csv.CSVParser
import codecdb.dataset.schema.Schema
import codecdb.model.DataType

import scala.collection.mutable.{ArrayBuffer, HashMap}

object Plural {

  var rules = new ArrayBuffer[Rule]()

  install(new PluralDictRule)
  install(new IesRule)
  install(new EsRule)
  install(new SRule)

  def install(rule: Rule): Unit = { rules += rule }

  def removePlural(input: String): String = {
    rules.foreach(rule => {
      val apply = rule.remove(input)
      if (null != apply) {
        // Make sure it adds back
        if (addPlural(apply).equals(input))
          return apply
      }
    })
    input
  }

  def addPlural(input: String): String = {
    rules.foreach(rule => {
      val apply = rule.add(input)
      if (null != apply) {
        return apply
      }
    })
    input
  }
}

trait Rule {
  def add(input: String): String

  def remove(input: String): String
}

class DictRule extends Rule {
  def add(input: String) = null
  def remove(input: String): String = Dict.strictLookup(input) match { case false => null case true => input }
}

class PluralDictRule extends Rule {

  val dictFile = "word/plural.txt"

  val schema = new Schema(Array((DataType.STRING, "plural"), (DataType.STRING, "origin")), false)

  var dict = new HashMap[String, String]()
  var inverse = new HashMap[String, String]()

  def loadDict(): Unit = {
    val parser = new CSVParser()
    val dicturi = Thread.currentThread().getContextClassLoader.getResourceAsStream(dictFile)
    val records = parser.parse(dicturi, schema)
    records.foreach { record => { dict += ((record(0), record(1))); inverse += ((record(1), record(0))); } }
  }
  def add(input: String): String = inverse.getOrElse(input, null)
  def remove(input: String): String = dict.getOrElse(input, null)
}

class IesRule extends Rule {
  def add(input: String): String = {
    input match {
      case endy if endy endsWith ("[^aeiou]y") => input.replaceAll("y$", "ies")
      case _ => null
    }
  }
  def remove(input: String): String = {
    input match {
      case ies if ies endsWith ("ies") => input.replaceAll("ies$", "y")
      case _ => null
    }
  }
}

class EsRule extends Rule {
  def add(input: String): String = {
    input match {
      case end2 if end2.endsWith("ch") || end2.endsWith("sh") => { input + "es" }
      case end1 if end1.endsWith("s") || end1.endsWith("x") || end1.endsWith("z") => { input + "es" }
      case _ => null
    }
  }
  def remove(input: String): String = {
    input match {
      case es if es endsWith ("es") => input.replaceAll("(?<!^)es$", "")
      case _ => null
    }
  }
}

class SRule extends Rule {
  def add(input: String): String = input + "s"
  def remove(input: String): String = {
    input match {
      case s if s endsWith ("s") => input.replaceAll("(?<!^)s$", "")
      case _ => null
    }
  }
}
