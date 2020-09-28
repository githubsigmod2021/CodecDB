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

class DictTest {

  @Test
  def testLookup(): Unit = {
    val book = Dict.lookup("book")
    assertEquals("book", book._1)
    val rpt = Dict.lookup("rpt")
    assertEquals("report", rpt._1)
    val cmd = Dict.lookup("cmd")
    assertEquals("command", cmd._1)
    val yr = Dict.lookup("yr")
    assertEquals("year", yr._1)
    val dt = Dict.lookup("dt")
    assertEquals("date", dt._1)
    val zip = Dict.lookup("zip")
    assertEquals("zip", zip._1)
    val non = Dict.lookup("jiang")
    assertEquals("jiang", non._1)
  }

  @Test
  def testAbbrv: Unit = {
    assertEquals("rpt", Dict.abbreviate("repeat"))
    assertEquals("rpt", Dict.abbreviate("report"))
  }

  @Test
  def testCorrectWord: Unit = {
    assertEquals("identification", Dict.lookup("identificaiton")._1)
  }

  @Test
  def testPlural: Unit = {
    val codes = Dict.lookup("codes")
    assertEquals("code", codes._1)
  }
}