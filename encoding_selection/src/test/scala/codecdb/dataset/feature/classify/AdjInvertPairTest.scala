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

package codecdb.dataset.feature.classify

import java.io.File

import codecdb.dataset.column.Column
import codecdb.model.DataType
import org.junit.Assert.assertEquals
import org.junit.Test

/**
  * Created by harper on 5/10/17.
  */
class AdjInvertPairTest {

  @Test
  def testExtract: Unit = {
    val col = new Column(null, -1, "", DataType.INTEGER)
    col.colFile = new File("src/test/resource/coldata/test_col_int.data").toURI
    val features = AdjInvertPair.extract(col).toArray

    assertEquals(3, features.length)
    assertEquals("AdjInvertPair", features(0).featureType)
    assertEquals("totalpair", features(0).name)
    assertEquals(12, features(0).value, 0.001)

    assertEquals("AdjInvertPair", features(1).featureType)
    assertEquals("ivpair", features(1).name)
    assertEquals(0.8333, features(1).value, 0.001)

    assertEquals("AdjInvertPair", features(2).featureType)
    assertEquals("kendallstau", features(2).name)
    assertEquals(0.1667, features(2).value, 0.001)
  }

  @Test
  def testExtractEmpty: Unit = {
    val col = new Column(null, -1, "", DataType.INTEGER)
    col.colFile = new File("src/test/resource/coldata/test_col_empty.dat").toURI
    val features = AdjInvertPair.extract(col).toArray

    assertEquals(3, features.length)
    assertEquals("AdjInvertPair", features(0).featureType)
    assertEquals("totalpair", features(0).name)
    assertEquals(23, features(0).value, 0.001)

    assertEquals("AdjInvertPair", features(1).featureType)
    assertEquals("ivpair", features(1).name)
    assertEquals(0, features(1).value, 0.001)

    assertEquals("AdjInvertPair", features(2).featureType)
    assertEquals("kendallstau", features(2).name)
    assertEquals(1, features(2).value, 0.001)
  }
}
