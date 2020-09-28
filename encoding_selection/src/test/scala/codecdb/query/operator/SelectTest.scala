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

package codecdb.query.operator

import java.io.File
import java.net.URI

import codecdb.query.{HColumnPredicate, Predicate, TempTable, VColumnPredicate}
import codecdb.query.tpch.TPCHSchema
import edu.uchicago.cs.encsel.query.VColumnPredicate
import org.apache.parquet.io.api.Binary
import org.apache.parquet.schema.MessageType
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConversions._

class SelectTest {
  @Test
  def testColumnMapping: Unit = {
    val select = new Select() {
      override def select(input: URI, p: Predicate, schema: MessageType, projectIndices: Array[Int]): TempTable = {
        null
      }
    };

    val predicate = new HColumnPredicate((data: Any) => false, 4);

    val (columnMap, projectMap, nonPredictIndices) = select.columnMapping(predicate, TPCHSchema.lineitemSchema, Array(1, 3, 6, 8));

    assertEquals(5, columnMap.size)
    assertEquals(0, columnMap.getOrElse(1, -1))
    assertEquals(1, columnMap.getOrElse(3, -1))
    assertEquals(2, columnMap.getOrElse(4, -1))
    assertEquals(3, columnMap.getOrElse(6, -1))
    assertEquals(4, columnMap.getOrElse(8, -1))

    assertEquals(4, projectMap.size)

    assertEquals(0, projectMap.getOrElse(1, -1))
    assertEquals(1, projectMap.getOrElse(3, -1))
    assertEquals(2, projectMap.getOrElse(6, -1))
    assertEquals(3, projectMap.getOrElse(8, -1))

    assertEquals(4, nonPredictIndices.size)
    assertEquals(0, nonPredictIndices.toList(0))
    assertEquals(1, nonPredictIndices.toList(1))
    assertEquals(3, nonPredictIndices.toList(2))
    assertEquals(4, nonPredictIndices.toList(3))
  }

  @Test
  def testNoPredicateColMapping: Unit = {
    val select = new Select() {
      override def select(input: URI, p: Predicate, schema: MessageType, projectIndices: Array[Int]): TempTable = {
        null
      }
    };

    val predicate = null

    val (columnMap, projectMap, nonPredictIndices) = select.columnMapping(predicate, TPCHSchema.lineitemSchema, Array(1, 3, 6, 8));

    assertEquals(4, columnMap.size)
    assertEquals(0, columnMap.getOrElse(1, -1))
    assertEquals(1, columnMap.getOrElse(3, -1))
    assertEquals(2, columnMap.getOrElse(6, -1))
    assertEquals(3, columnMap.getOrElse(8, -1))

    assertEquals(4, projectMap.size)

    assertEquals(0, projectMap.getOrElse(1, -1))
    assertEquals(1, projectMap.getOrElse(3, -1))
    assertEquals(2, projectMap.getOrElse(6, -1))
    assertEquals(3, projectMap.getOrElse(8, -1))

    assertEquals(4, nonPredictIndices.size)
    assertEquals(0, nonPredictIndices.toList(0))
    assertEquals(1, nonPredictIndices.toList(1))
    assertEquals(2, nonPredictIndices.toList(2))
    assertEquals(3, nonPredictIndices.toList(3))
  }
}

class HorizontalSelectTest {

  @Test
  def testSelectInPredicate: Unit = {

    val input = new File("src/test/resource/query_select/customer_100.parquet").toURI
    val predicate = new HColumnPredicate((data) => data.toString.toInt >= 90, 0)
    val temptable = new HorizontalSelect().select(input, predicate, TPCHSchema.customerSchema, Array(0, 1, 2))

    assertEquals(11, temptable.getRecords.size)

    (0 to 10).foreach(i => {
      var record = temptable.getRecords()(i)
      assertEquals(3, record.getData.length)
      assertEquals(i + 90, record.getData()(0).toString.toInt)
    })
  }

  @Test
  def testSelectNotInPredicate: Unit = {
    val input = new File("src/test/resource/query_select/customer_100.parquet").toURI
    val predicate = new HColumnPredicate((data) => data.toString.toInt >= 90, 0)
    val temptable = new HorizontalSelect().select(input, predicate, TPCHSchema.customerSchema, Array(1, 2, 5, 7))

    assertEquals(11, temptable.getRecords.size)

    (0 to 10).foreach(i => {
      var record = temptable.getRecords()(i)
      assertEquals(4, record.getData.length)
      assertEquals("Customer#000000%03d".format(i + 90), record.getData()(0).asInstanceOf[Binary].toStringUsingUTF8)
    })
  }

  @Test
  def testSelectNotInPredicate2: Unit = {
    val input = new File("src/test/resource/query_select/customer_100.parquet").toURI
    val predicate = new HColumnPredicate((data) => data.toString.toInt == 3, 3)
    val temptable = new HorizontalSelect().select(input, predicate, TPCHSchema.customerSchema, Array(1, 2, 5, 7))

    assertEquals(7, temptable.getRecords.size)

  }

  @Test
  def testNullPredicate: Unit = {
    val input = new File("src/test/resource/query_select/customer_100.parquet").toURI
    val predicate = null
    val temptable = new HorizontalSelect().select(input, predicate, TPCHSchema.customerSchema, Array(1, 2, 5, 7))

    assertEquals(100, temptable.getRecords.size)

    (0 to 99).foreach(i => {
      var record = temptable.getRecords()(i)
      assertEquals(4, record.getData.length)
      assertEquals("Customer#000000%03d".format(i + 1), record.getData()(0).asInstanceOf[Binary].toStringUsingUTF8)
    })
  }
}

class VerticalSelectTest {

  @Test
  def testSelectNotInPredicate: Unit = {
    val input = new File("src/test/resource/query_select/customer_100.parquet").toURI
    val predicate = new VColumnPredicate((data) => data.toString.toInt >= 90 && data.toString.toInt % 2 == 0, 0)
    val temptable = new VerticalSelect().select(input, predicate, TPCHSchema.customerSchema, Array(0, 1, 2))

    assertEquals(3, temptable.getColumns.size)
    for (i <- 0 until 3)
      assertEquals(6, temptable.getColumns()(i).getData.size)

    (0 until 6).foreach(i => {
      assertEquals(i * 2 + 90, temptable.getColumns()(0).getData()(i).toString.toInt)
    })

    assertEquals("IfVNIN9KtkScJ9dUjK3Pg5gY1aFeaXewwf",
      temptable.getColumns()(2).getData()(2).asInstanceOf[Binary].toStringUsingUTF8)
  }

  @Test
  def testSelectInPredicate: Unit = {
    val input = new File("src/test/resource/query_select/customer_100.parquet").toURI
    val predicate = new VColumnPredicate((data) => data.toString.toInt >= 90 && data.toString.toInt % 2 == 0, 0)
    val temptable = new VerticalSelect().select(input, predicate, TPCHSchema.customerSchema, Array(1, 2, 5, 7))

    for (i <- 0 until 4)
      assertEquals(6, temptable.getColumns()(i).getData.size)

    (0 until 6).foreach(i => {
      assertEquals("Customer#000000%03d".format(i * 2 + 90),
        temptable.getColumns()(0).getData()(i).asInstanceOf[Binary].toStringUsingUTF8)
    })

    assertEquals("IfVNIN9KtkScJ9dUjK3Pg5gY1aFeaXewwf",
      temptable.getColumns()(1).getData()(2).asInstanceOf[Binary].toStringUsingUTF8)
  }

  @Test
  def testSelectNotInPredicate2: Unit = {
    val input = new File("src/test/resource/query_select/customer_100.parquet").toURI
    val predicate = new VColumnPredicate((data) => data.toString.toInt == 3, 3)
    val temptable = new VerticalSelect().select(input, predicate, TPCHSchema.customerSchema, Array(1, 2, 5, 7))

    assertEquals(7, temptable.getColumns()(0).getData.length)

  }

  @Test
  def testNullPredicate: Unit = {
    val input = new File("src/test/resource/query_select/customer_100.parquet").toURI
    val predicate = null
    val temptable = new VerticalSelect().select(input, predicate, TPCHSchema.customerSchema, Array(1, 2, 5, 7))

    for (i <- 0 until 4)
      assertEquals(100, temptable.getColumns()(i).getData.size)

    (0 to 99).foreach(i => {
      assertEquals("Customer#000000%03d".format(i + 1), temptable.getColumns()(0).getData()(i).asInstanceOf[Binary].toStringUsingUTF8)
    })
  }
}