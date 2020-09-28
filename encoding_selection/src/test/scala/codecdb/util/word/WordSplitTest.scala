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
 */

package codecdb.util.word

import org.junit.Assert._
import org.junit.Test


class WordSplitTest {

  @Test
  def testSplitAbbrv(): Unit = {
    val split = new WordSplit()
    var res = split.split("RPTYR")
    assertEquals(2, res._1.length)
    //noinspection ZeroIndexToHead
    assertEquals("report", res._1(0))
    assertEquals("year", res._1(1))

    res = split.split("SMYS")
    assertEquals(2, res._1.length)
    //noinspection ZeroIndexToHead
    assertEquals("some", res._1(0))
    assertEquals("year", res._1(1))
  }

  @Test
  def testSplitLong: Unit = {
    val split = new WordSplit()
    var res = split.split("inspectioncode")
    assertEquals(2, res._1.length)
    //noinspection ZeroIndexToHead
    assertEquals("inspection", res._1(0))
    assertEquals("code", res._1(1))

    res = split.split("inspectioncodes")
    assertEquals(2, res._1.length)
    //noinspection ZeroIndexToHead
    assertEquals("inspection", res._1(0))
    assertEquals("code", res._1(1))
  }

  @Test
  def testSplitCombined: Unit = {
    val split = new WordSplit()
    val res = split.split("actualcmd")
    assertEquals(2, res._1.length)
    //noinspection ZeroIndexToHead
    assertEquals("actual", res._1(0))
    assertEquals("command", res._1(1))
  }

  @Test
  def testSplitPlural: Unit = {
    val split = new WordSplit()
    val res = split.split("ReportYears")
    assertEquals(2, res._1.length)
    //noinspection ZeroIndexToHead
    assertEquals("report", res._1(0))
    assertEquals("year", res._1(1))
  }

  @Test
  def testRemoveNumber: Unit = {
    val split = new WordSplit()
    val res = split.split("column522342Day")
    assertEquals(2, res._1.length)
    //noinspection ZeroIndexToHead
    assertEquals("column", res._1(0))
    assertEquals("day", res._1(1))
  }

  @Test
  def testSplitCamel: Unit = {
    val split = new WordSplit()
    var res = split.split("NOVIssuedDate")
    assertEquals(3, res._1.length)
    //noinspection ZeroIndexToHead
    assertEquals("nov", res._1(0))
    assertEquals("issued", res._1(1))
    assertEquals("date", res._1(2))

    res = split.split("GeneralID")
    assertEquals(2, res._1.length)
    //noinspection ZeroIndexToHead
    assertEquals("general", res._1(0))
    assertEquals("id", res._1(1))
  }
}