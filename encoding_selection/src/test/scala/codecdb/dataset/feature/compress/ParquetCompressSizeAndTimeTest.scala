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

package codecdb.dataset.feature.compress

import java.io.File

import codecdb.dataset.column.Column
import codecdb.model.DataType
import org.junit.Assert._
import org.junit.Test

class ParquetCompressSizeAndTimeTest {

  val codecs = Array("SNAPPY", "GZIP", "LZO")

  @Test
  def testExtractInt: Unit = {
    val encs = Array("PLAIN", "DICT", "BP", "RLE", "DELTABP")

    val col = new Column(new File("src/test/resource/test_columner.csv").toURI, 0, "id", DataType.INTEGER)
    col.colFile = new File("src/test/resource/coldata/test_col_int.data").toURI

    val feature = ParquetCompressFileSizeAndTime.extract(col)
    assertEquals(encs.size * codecs.size * 4, feature.size)
    val fa = feature.toArray

    val cross = for (i <- encs; j <- codecs) yield (i, j)

    cross.zipWithIndex.foreach(p => {
      val name = "%s_%s".format(p._1._1, p._1._2)

      assertEquals("%s_file_size".format(name), fa(p._2 * 4).name)
      assertEquals(new File("src/test/resource/coldata/test_col_int.data.%s".format(name)).length(), fa(p._2 * 4).value, 0.001)
      assertEquals("CompressEncFileSize", fa(p._2 * 4).featureType)
      assertEquals("CompressTimeUsage", fa(p._2 * 4 + 1).featureType)
      assertEquals("CompressTimeUsage", fa(p._2 * 4 + 2).featureType)
      assertEquals("CompressTimeUsage", fa(p._2 * 4 + 3).featureType)
      assertEquals("%s_wctime".format(name), fa(p._2 * 4 + 1).name)
      assertEquals("%s_cputime".format(name), fa(p._2 * 4 + 2).name)
      assertEquals("%s_usertime".format(name), fa(p._2 * 4 + 3).name)
      assertTrue(fa(p._2 * 4 + 1).value > 0)
      assertTrue(fa(p._2 * 4 + 2).value > 0)
      assertTrue(fa(p._2 * 4 + 3).value >= 0)
    })
  }

  @Test
  def testExtractString: Unit = {
    val encs = Array("PLAIN", "DICT", "DELTA", "DELTAL")

    val col = new Column(new File("src/test/resource/test_columner.csv").toURI, 0, "id", DataType.STRING)
    col.colFile = new File("src/test/resource/coldata/test_col_str.data").toURI

    val feature = ParquetCompressFileSizeAndTime.extract(col)
    assertEquals(encs.size * codecs.size * 4, feature.size)
    val fa = feature.toArray

    val cross = for (i <- encs; j <- codecs) yield (i, j)

    cross.zipWithIndex.foreach(p => {
      val name = "%s_%s".format(p._1._1, p._1._2)

      assertEquals("%s_file_size".format(name), fa(p._2 * 4).name)
      assertEquals(new File("src/test/resource/coldata/test_col_str.data.%s".format(name)).length(), fa(p._2 * 4).value, 0.001)
      assertEquals("CompressEncFileSize", fa(p._2 * 4).featureType)

      assertEquals("CompressTimeUsage", fa(p._2 * 4 + 1).featureType)
      assertEquals("CompressTimeUsage", fa(p._2 * 4 + 2).featureType)
      assertEquals("CompressTimeUsage", fa(p._2 * 4 + 3).featureType)
      assertEquals("%s_wctime".format(name), fa(p._2 * 4 + 1).name)
      assertEquals("%s_cputime".format(name), fa(p._2 * 4 + 2).name)
      assertEquals("%s_usertime".format(name), fa(p._2 * 4 + 3).name)
      assertTrue(fa(p._2 * 4 + 1).value > 0)
      assertTrue(fa(p._2 * 4 + 2).value > 0)
      assertTrue(fa(p._2 * 4 + 3).value >= 0)
    })
  }

  @Test
  def testExtractLong: Unit = {
    val encs = Array("PLAIN", "DICT", "DELTABP")

    val col = new Column(new File("src/test/resource/test_columner.csv").toURI, 0, "id", DataType.LONG)
    col.colFile = new File("src/test/resource/coldata/test_col_long.data").toURI

    val feature = ParquetCompressFileSizeAndTime.extract(col)
    assertEquals(encs.size * codecs.size * 4, feature.size)
    val fa = feature.toArray

    val cross = for (i <- encs; j <- codecs) yield (i, j)

    cross.zipWithIndex.foreach(p => {
      val name = "%s_%s".format(p._1._1, p._1._2)

      assertEquals("%s_file_size".format(name), fa(p._2 * 4).name)
      assertEquals(new File("src/test/resource/coldata/test_col_long.data.%s".format(name)).length(), fa(p._2 * 4).value, 0.001)
      assertEquals("CompressEncFileSize", fa(p._2 * 4).featureType)

      assertEquals("CompressTimeUsage", fa(p._2 * 4 + 1).featureType)
      assertEquals("CompressTimeUsage", fa(p._2 * 4 + 2).featureType)
      assertEquals("CompressTimeUsage", fa(p._2 * 4 + 3).featureType)
      assertEquals("%s_wctime".format(name), fa(p._2 * 4 + 1).name)
      assertEquals("%s_cputime".format(name), fa(p._2 * 4 + 2).name)
      assertEquals("%s_usertime".format(name), fa(p._2 * 4 + 3).name)
      assertTrue(fa(p._2 * 4 + 1).value > 0)
      assertTrue(fa(p._2 * 4 + 2).value > 0)
      assertTrue(fa(p._2 * 4 + 3).value >= 0)
    })
  }

  @Test
  def testExtractDouble: Unit = {
    val encs = Array("PLAIN", "DICT")

    val col = new Column(new File("src/test/resource/test_columner.csv").toURI, 0, "id", DataType.DOUBLE)
    col.colFile = new File("src/test/resource/coldata/test_col_double.data").toURI

    val feature = ParquetCompressFileSizeAndTime.extract(col)
    assertEquals(encs.size * codecs.size * 4, feature.size)
    val fa = feature.toArray

    val cross = for (i <- encs; j <- codecs) yield (i, j)

    cross.zipWithIndex.foreach(p => {
      val name = "%s_%s".format(p._1._1, p._1._2)
      assertEquals("%s_file_size".format(name), fa(p._2 * 4).name)
      assertEquals(new File("src/test/resource/coldata/test_col_double.data.%s".format(name)).length(), fa(p._2 * 4).value, 0.001)
      assertEquals("CompressEncFileSize", fa(p._2 * 4).featureType)

      assertEquals("CompressTimeUsage", fa(p._2 * 4 + 1).featureType)
      assertEquals("CompressTimeUsage", fa(p._2 * 4 + 2).featureType)
      assertEquals("CompressTimeUsage", fa(p._2 * 4 + 3).featureType)
      assertEquals("%s_wctime".format(name), fa(p._2 * 4 + 1).name)
      assertEquals("%s_cputime".format(name), fa(p._2 * 4 + 2).name)
      assertEquals("%s_usertime".format(name), fa(p._2 * 4 + 3).name)
      assertTrue(fa(p._2 * 4 + 1).value > 0)
      assertTrue(fa(p._2 * 4 + 2).value > 0)
      assertTrue(fa(p._2 * 4 + 3).value >= 0)
    })
  }

  @Test
  def testExtractBoolean: Unit = {
    val encs = Array("PLAIN")

    val col = new Column(new File("src/test/resource/test_columner.csv").toURI, 0, "id", DataType.BOOLEAN)
    col.colFile = new File("src/test/resource/coldata/test_col_boolean.data").toURI

    val feature = ParquetCompressFileSizeAndTime.extract(col)
    assertEquals(encs.size * codecs.size * 4, feature.size)
    val fa = feature.toArray

    val cross = for (i <- encs; j <- codecs) yield (i, j)

    cross.zipWithIndex.foreach(p => {
      val name = "%s_%s".format(p._1._1, p._1._2)
      assertEquals("%s_file_size".format(name), fa(p._2 * 4).name)
      assertEquals(new File("src/test/resource/coldata/test_col_boolean.data.%s".format(name)).length(), fa(p._2 * 4).value, 0.001)
      assertEquals("CompressEncFileSize", fa(p._2 * 4).featureType)

      assertEquals("CompressTimeUsage", fa(p._2 * 4 + 1).featureType)
      assertEquals("CompressTimeUsage", fa(p._2 * 4 + 2).featureType)
      assertEquals("CompressTimeUsage", fa(p._2 * 4 + 3).featureType)
      assertEquals("%s_wctime".format(name), fa(p._2 * 4 + 1).name)
      assertEquals("%s_cputime".format(name), fa(p._2 * 4 + 2).name)
      assertEquals("%s_usertime".format(name), fa(p._2 * 4 + 3).name)
      assertTrue(fa(p._2 * 4 + 1).value > 0)
      assertTrue(fa(p._2 * 4 + 2).value > 0)
      assertTrue(fa(p._2 * 4 + 3).value >= 0)
    })
  }
}
