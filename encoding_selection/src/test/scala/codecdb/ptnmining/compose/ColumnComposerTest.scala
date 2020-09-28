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

package codecdb.ptnmining.compose

import org.junit.Test
import org.junit.Assert._

class ColumnComposerTest {

  @Test
  def testCompose: Unit = {
    val colComposer = new ColumnComposer("^MIR-([0-9a-fA-F]+)-([0-9a-fA-F]+)-(\\d+)(-)?(\\d*)$",
      new ChildColumnLoader {
        var counter = 0

        override def next: Seq[String] = {
          counter += 1
          Seq(counter.toString,
            (counter * 100).toString,
            (counter * 1000).toString,
            if (counter % 2 == 0) "true" else "false",
            (counter % 2).toString)
        }

        override def skip: Unit = {}
      })
    val result = (0 to 10).map(i =>
      colComposer.getString)

    val expect = Array("MIR-1-100-10001", "MIR-2-200-2000-0",
      "MIR-3-300-30001", "MIR-4-400-4000-0", "MIR-5-500-50001",
      "MIR-6-600-6000-0", "MIR-7-700-70001", "MIR-8-800-8000-0",
      "MIR-9-900-90001", "MIR-10-1000-10000-0","MIR-11-1100-110001")

    (0 to 10).foreach(i => {
      assertEquals(expect(i), result(i))
    })
  }
}
