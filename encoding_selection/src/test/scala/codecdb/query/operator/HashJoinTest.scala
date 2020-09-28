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

import codecdb.query.ColumnTempTable
import org.apache.parquet.io.api.Binary
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConversions._

class HashJoinTest {

  @Test
  def testJoin: Unit = {

    val result = new HashJoin().join(new File("src/test/resource/query/person").toURI, TestSchemas.personSchema,
      new File("src/test/resource/query/contact").toURI, TestSchemas.contactSchema,
      (0, 1), Array(0, 1, 2), Array(2))

    assertTrue(result.isInstanceOf[ColumnTempTable])

    val tempTable = result.asInstanceOf[ColumnTempTable]
    assertEquals(4, tempTable.getColumns.size)

    val col0data = tempTable.getColumns()(0).getData
    assertEquals(8, col0data.size)
    assertEquals(1, col0data(0))
    assertEquals(1, col0data(1))
    assertEquals(1, col0data(2))
    assertEquals(2, col0data(3))
    assertEquals(2, col0data(4))
    assertEquals(3, col0data(5))
    assertEquals(3, col0data(6))
    assertEquals(3, col0data(7))

    val col1data = tempTable.getColumns()(1).getData
    assertEquals(8, col1data.size)
    assertEquals("John", col1data(0).asInstanceOf[Binary].toStringUsingUTF8)
    assertEquals("John", col1data(1).asInstanceOf[Binary].toStringUsingUTF8)
    assertEquals("John", col1data(2).asInstanceOf[Binary].toStringUsingUTF8)
    assertEquals("Maria", col1data(3).asInstanceOf[Binary].toStringUsingUTF8)
    assertEquals("Maria", col1data(4).asInstanceOf[Binary].toStringUsingUTF8)
    assertEquals("Tanaka", col1data(5).asInstanceOf[Binary].toStringUsingUTF8)
    assertEquals("Tanaka", col1data(6).asInstanceOf[Binary].toStringUsingUTF8)
    assertEquals("Tanaka", col1data(7).asInstanceOf[Binary].toStringUsingUTF8)

    val col2data = tempTable.getColumns()(2).getData
    assertEquals(8, col2data.size)
    assertEquals(1948, col2data(0))
    assertEquals(1948, col2data(1))
    assertEquals(1948, col2data(2))
    assertEquals(1840, col2data(3))
    assertEquals(1840, col2data(4))
    assertEquals(2011, col2data(5))
    assertEquals(2011, col2data(6))
    assertEquals(2011, col2data(7))

    val col3data = tempTable.getColumns()(3).getData
    assertEquals(8, col3data.size)
    assertEquals("1342 E Mod Ave", col3data(0).asInstanceOf[Binary].toStringUsingUTF8)
    assertEquals("3414 W Donne St.", col3data(1).asInstanceOf[Binary].toStringUsingUTF8)
    assertEquals("442 E Keep Rd.", col3data(2).asInstanceOf[Binary].toStringUsingUTF8)
    assertEquals("4251 Main Street", col3data(3).asInstanceOf[Binary].toStringUsingUTF8)
    assertEquals("313 Mary Street", col3data(4).asInstanceOf[Binary].toStringUsingUTF8)
    assertEquals("1111 Keeway Ave.", col3data(5).asInstanceOf[Binary].toStringUsingUTF8)
    assertEquals("24 W Rosad Blvd.", col3data(6).asInstanceOf[Binary].toStringUsingUTF8)
    assertEquals("424 E Keemay Ave.", col3data(7).asInstanceOf[Binary].toStringUsingUTF8)
  }
}
