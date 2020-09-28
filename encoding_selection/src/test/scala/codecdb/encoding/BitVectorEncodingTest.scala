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

package codecdb.encoding

import java.io.{DataInputStream, File, FileInputStream}

import codecdb.dataset.column.Column
import org.junit.Assert._
import org.junit.Test

class BitVectorEncodingTest {

  @Test
  def testEncoding: Unit = {
    val dummyColumn = new Column
    dummyColumn.colFile = new File("src/test/resource/encoding/bitvector_input").toURI
    val encodedFile = new File("src/test/resource/encoding/bitvector_encoded")
    new BitVectorEncoding().encode(dummyColumn, encodedFile.toURI)

    val encodedData = new DataInputStream(new FileInputStream(encodedFile))
    val dictOffset = encodedData.readLong()
    val numEntry = encodedData.readLong()

    assertEquals(22, numEntry)
    assertEquals(28, dictOffset)

    val buffer = new Array[Byte](3);

    encodedData.read(buffer);

    assertEquals(0x45, buffer(0));
    assertEquals(0x68, buffer(1));
    assertEquals(0x20, buffer(2));

    encodedData.read(buffer)

    assertEquals(2, buffer(0));
    assertEquals(0, buffer(1));
    assertEquals(4, buffer(2));

    encodedData.read(buffer)

    assertEquals(8, buffer(0));
    assertEquals(0x82.toByte, buffer(1));
    assertEquals(3, buffer(2));

    encodedData.close
  }

  @Test
  def testEncodingLargeFile: Unit = {
//    val dummyColumn = new Column
//    dummyColumn.colFile = new File("src/test/resource/encoding/bitvector_input2").toURI
//    val encodedFile = new File("src/test/resource/encoding/bitvector_encoded2")
//    new BitVectorEncoding().encode(dummyColumn, encodedFile.toURI)
  }
}

