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

package codecdb.ptnmining.gen

import java.io.{FileWriter, PrintWriter}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
  * Created by harper on 5/31/17.
  */
object DataGen extends App {


  def range(from: Int, to: Int, padding: Int): IndexedSeq[String] = {
    val format = padding match {
      case -1 => "%d"
      case _ => "%0" + padding + "d"
    }
    (from to to).map(format.format(_))
  }

  def randUpper(copy: Int): String = {
    (0 until copy).map(i => {
      ('A' + Random.nextInt(26)).toChar.toString
    }).mkString("")
  }

  def randLower(copy: Int): String = {
    (0 until copy).map(i => {
      ('a' + Random.nextInt(26)).toChar.toString
    }).mkString("")
  }

  def randNum(copy: Int): String = {
    (0 until copy).map(i => {
      Random.nextInt(10).toString
    }).mkString("")
  }

  def pattern(data: String, pattern: String): String = {
    "<s> %s </s>\t<s> %s </s>".format(data.toCharArray.map(_.toString).mkString(" "), pattern)
  }

  def genDate(copy: Int): Iterable[String] = {
    val longyears = range(1900, 2050, 4)
    val shortyears = range(0, 99, 2)
    val months = range(0, 12, 2)
    val days = range(0, 30, 2)


    (0 until copy).flatMap(i => {
      val longyear = longyears(Random.nextInt(longyears.size))
      val shortyear = shortyears(Random.nextInt(shortyears.size))
      val month = months(Random.nextInt(months.size))
      val day = days(Random.nextInt(days.size))

      val result = new ArrayBuffer[String]()

      // pattern 1, yyyy-mm-dd
      val ptn1 = "%s-%s-%s".format(longyear, month, day)
      result += pattern(ptn1, "<NUM> - <NUM> - <NUM>")
      // pattern 2, mm/dd/yyyy
      val ptn2 = "%s/%s/%s".format(month, day, longyear)
      result += pattern(ptn2, "<NUM> / <NUM> / <NUM>")
      // pattern 3, mm/dd/yy
      val ptn3 = "%s/%s/%s".format(month, day, shortyear)
      result += pattern(ptn3, "<NUM> / <NUM> / <NUM>")
      result
    })
  }

  def genTime(copy: Int): IndexedSeq[String] = {
    // hh:mm:ss
    val hours = range(0, 23, 2)
    val mins = range(0, 59, 2)
    val secs = range(0, 59, 2)
    (0 until copy).map(i => {
      val hour = hours(Random.nextInt(24))
      val min = mins(Random.nextInt(60))
      val sec = secs(Random.nextInt(60))
      pattern("%s:%s:%s".format(hour, min, sec), "<NUM> : <NUM> : <NUM>")
    })
  }

  def genPhone(copy: Int): IndexedSeq[String] = {
    val as = range(0, 999, 3)
    val bs = range(0, 999, 3)
    val cs = range(0, 9999, 4)

    (0 until copy).flatMap(i => {
      val a = as(Random.nextInt(1000))
      val b = bs(Random.nextInt(1000))
      val c = cs(Random.nextInt(10000))
      Array(
        pattern("(%s)%s-%s".format(a, b, c), "( <NUM> ) <NUM> - <NUM>"),
        pattern("%s-%s-%s".format(a, b, c), "<NUM> - <NUM> - <NUM>")
      )
    })
  }

  def genIpAddress(copy: Int): IndexedSeq[String] = {
    val seq = range(0, 255, -1)
    (0 until copy).map(i => {
      val ip1 = seq(Random.nextInt(256))
      val ip2 = seq(Random.nextInt(256))
      val ip3 = seq(Random.nextInt(256))
      val ip4 = seq(Random.nextInt(256))
      pattern("%s.%s.%s.%s".format(ip1, ip2, ip3, ip4), "<NUM> . <NUM> . <NUM> . <NUM>")
    })
  }

  def genPrefixString(copy: Int): IndexedSeq[String] = {

    (0 until copy).map(i => {
      val b = new StringBuilder()
      val cmax = Random.nextInt(3) + 2
      b.append(randUpper(cmax))
      val max = Random.nextInt(5) + 5
      b.append(randNum(max))
      pattern(b.toString(), "<WORD> <NUM>")
    })
  }

  def genDashString(copy: Int): IndexedSeq[String] = {
    (0 until copy).flatMap(i => {
      val cmax = Random.nextInt(3) + 2
      val cmax2 = Random.nextInt(3) + 2
      val max = Random.nextInt(5) + 5
      Array(pattern("%s-%s".format(randUpper(cmax), randUpper(cmax2)), "<WORD> - <WORD>"),
        pattern("%s-%s".format(randUpper(cmax), randNum(max)), "<WORD> - <NUM>"))
    })
  }

  // Separator is -
  def genSeparateString(copy: Int): IndexedSeq[String] = {
    (0 until copy).map(i => {
      val b = new StringBuilder()
      val cmax1 = Random.nextInt(2) + 2
      val cmax2 = Random.nextInt(3) + 2
      val nmax1 = Random.nextInt(3) + 3
      val nmax2 = Random.nextInt(5) + 2

      b.append(randUpper(cmax1)).append("-").append(randUpper(cmax2))
        .append(randNum(nmax1)).append("-").append(randNum(nmax2))
      pattern(b.toString(), "<WORD> - <WORD> <NUM> - <NUM>")
    })
  }

  def genPart: Unit = {
    val output = "part.train"

    val writer = new PrintWriter(new FileWriter(output))

    genDate(5000).foreach(writer.println)
    genTime(5000).foreach(writer.println)
    genIpAddress(5000).foreach(writer.println)
    genPhone(5000).foreach(writer.println)
    genPrefixString(5000).foreach(writer.println)
    genDashString(5000).foreach(writer.println)
    writer.close()
  }

  def genWhole: Unit = {
    val output = "whole.test"

    val writer = new PrintWriter(new FileWriter(output))

    genSeparateString(1000).foreach(writer.println)

    writer.close()
  }

  genPart
}
