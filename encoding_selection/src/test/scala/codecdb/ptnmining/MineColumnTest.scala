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

package codecdb.ptnmining

import java.io.File

import codecdb.dataset.column.Column
import codecdb.model.DataType
import codecdb.ptnmining.parser.{TSpace, TSymbol, TWord}
import edu.uchicago.cs.encsel.ptnmining.parser.TSpace
import junitx.framework.FileAssert
import org.junit.Assert._
import org.junit.Test

import scala.io.Source

class MineColumnTest {

  @Test
  def testSplit: Unit = {
    val col = new Column(null, 1, "sample", DataType.STRING)
    col.colFile = new File("src/test/resource/colsplit/column").toURI

    val pattern = PSeq.collect(
      new PToken(new TWord("MIR")),
      new PToken(new TSymbol("-")),
      new PIntAny(6),
      new PToken(new TSymbol("-")),
      new PWordAny(3),
      new PToken(new TSymbol("-")),
      new PIntAny(4, true),
      new PToken(new TSymbol("+")),
      new PIntAny
    )
    val subcolumns = MineColumn.split(col, pattern)
    assertEquals(5, subcolumns.size)

    assertEquals(DataType.INTEGER, subcolumns(0).dataType)
    assertEquals(DataType.STRING, subcolumns(1).dataType)
    assertEquals(DataType.INTEGER, subcolumns(2).dataType)
    assertEquals(DataType.LONG, subcolumns(3).dataType)
    assertEquals(DataType.STRING, subcolumns(4).dataType)
    assertArrayEquals(Array[AnyRef]("301401", "228104", "", "323421", "243242", "", "423432"),
      Source.fromFile("src/test/resource/colsplit/column.0").getLines().toArray[AnyRef])

    assertArrayEquals(Array[AnyRef]("KWR", "KKP", "", "WOP", "DMN", "", "OOP"),
      Source.fromFile("src/test/resource/colsplit/column.1").getLines().toArray[AnyRef])

    assertArrayEquals(Array[AnyRef]("12829", "20500", "", "17124", "16931", "", "4660"),
      Source.fromFile("src/test/resource/colsplit/column.2").getLines().toArray[AnyRef])

    assertArrayEquals(Array[AnyRef]("23423432432423432423423", "123", "", "74234232342323", "423442242342342342342", "", "3242323423432423423"),
      Source.fromFile("src/test/resource/colsplit/column.3").getLines().toArray[AnyRef])

    assertArrayEquals(Array[AnyRef]("WORKHARD", "WORKHARD", "WORKWORKWORKHARD"),
      Source.fromFile("src/test/resource/colsplit/column.unmatch").getLines().toArray[AnyRef])
  }

  @Test
  def testSplitRealData1: Unit = {
    // "^(\\w{3})-(\\w+)(-)?([0-9a-fA-F]{0,5})(-)?(\\d{0,4})(-)?(\\d{0,4})$:"
    val pattern = PSeq.collect(
      new PWordDigitAny(3),
      new PToken(new TSymbol("-")),
      new PWordDigitAny(1, -1),
      PUnion.collect(
        new PToken(new TSymbol("-")),
        PEmpty
      ),
      new PIntAny(0, 5, true),
      PUnion.collect(
        new PToken(new TSymbol("-")),
        PEmpty
      ),
      new PIntAny(0, 4),
      PUnion.collect(
        new PToken(new TSymbol("-")),
        PEmpty
      ),
      new PIntAny(0, 4)
    )

    val col = new Column(null, 1, "sample", DataType.STRING)
    col.colFile = new File("src/test/resource/colsplit/realdata1").toURI

    val subcolumns = MineColumn.split(col, pattern)
    assertEquals(9, subcolumns.size)

    assertEquals(DataType.STRING, subcolumns(0).dataType)
    assertEquals(DataType.STRING, subcolumns(1).dataType)
    assertEquals(DataType.BOOLEAN, subcolumns(2).dataType)
    assertEquals(DataType.INTEGER, subcolumns(3).dataType)
    assertEquals(DataType.BOOLEAN, subcolumns(4).dataType)
    assertEquals(DataType.INTEGER, subcolumns(5).dataType)
    assertEquals(DataType.BOOLEAN, subcolumns(6).dataType)
    assertEquals(DataType.INTEGER, subcolumns(7).dataType)
    assertEquals(DataType.STRING, subcolumns(8).dataType)

    (0 to 7).foreach(i =>
      FileAssert.assertEquals(i.toString, new File("src/test/resource/colsplit/realdata1col/realdata1.%d".format(i)),
        new File("src/test/resource/colsplit/realdata1.%d".format(i)))
    )
    FileAssert.assertEquals(new File("src/test/resource/colsplit/realdata1col/realdata1.unmatch"),
      new File("src/test/resource/colsplit/realdata1.unmatch"))
  }

  @Test
  def testSplitRealData2: Unit = {
    // "^(\\d{4})-(\\d{2})-(\\d{2}) (\\d{2}):(\\d{2}):(\\d+\\.?\\d*)$"
    val pattern = PSeq.collect(
      new PIntAny(4),
      new PToken(new TSymbol("-")),
      new PIntAny(2),
      new PToken(new TSymbol("-")),
      new PIntAny(2),
      new PToken(new TSpace),
      new PIntAny(2),
      new PToken(new TSymbol(":")),
      new PIntAny(2),
      new PToken(new TSymbol(":")),
      new PDoubleAny(1, -1)
    )

    val col = new Column(null, 1, "sample", DataType.STRING)
    col.colFile = new File("src/test/resource/colsplit/realdata2").toURI

    val subcolumns = MineColumn.split(col, pattern)
    assertEquals(7, subcolumns.size)

    assertEquals(DataType.INTEGER, subcolumns(0).dataType)
    assertEquals(DataType.INTEGER, subcolumns(1).dataType)
    assertEquals(DataType.INTEGER, subcolumns(2).dataType)
    assertEquals(DataType.INTEGER, subcolumns(3).dataType)
    assertEquals(DataType.INTEGER, subcolumns(4).dataType)
    assertEquals(DataType.DOUBLE, subcolumns(5).dataType)
    assertEquals(DataType.STRING, subcolumns(6).dataType)

    (0 to 5).foreach(i =>
      FileAssert.assertEquals(new File("src/test/resource/colsplit/realdata2col/realdata2.%d".format(i)),
        new File("src/test/resource/colsplit/realdata2.%d".format(i)))
    )
    FileAssert.assertEquals(new File("src/test/resource/colsplit/realdata2col/realdata2.unmatch"),
      new File("src/test/resource/colsplit/realdata2.unmatch"))

  }

  @Test
  def testTypeOf: Unit = {
    val pattern = PUnion.collect(PEmpty, new PIntAny(5, true))
    assertEquals(DataType.INTEGER, MineColumn.typeof(pattern))

    val pattern2 = PUnion.collect(PEmpty, new PToken(new TSymbol("-")))
    assertEquals(DataType.BOOLEAN, MineColumn.typeof(pattern2))
  }

  @Test
  def testSplitDouble: Unit = {
    val col = new Column(null, 1, "sample", DataType.DOUBLE)
    col.colFile = new File("src/test/resource/colsplit/double_col").toURI

    val subs = MineColumn.splitDouble(col)

    assertEquals(2, subs.length)
    assertEquals(DataType.LONG, subs(0).dataType)
    assertEquals(DataType.INTEGER, subs(1).dataType)

    assertArrayEquals(Array[AnyRef]("234234", "2424", "", "42232344234233243423", "", "423245"),
      Source.fromFile("src/test/resource/colsplit/double_col.0").getLines().toArray[AnyRef])

    assertArrayEquals(Array[AnyRef]("42", "0", "", "423", "", "423"),
      Source.fromFile("src/test/resource/colsplit/double_col.1").getLines().toArray[AnyRef])
  }
}
