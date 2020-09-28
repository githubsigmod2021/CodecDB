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

package codecdb.tool

import codecdb.tool.mem.MemoryMonitor

import scala.collection.mutable.ArrayBuffer
import org.junit.Assert._
import org.junit.Test

class MemoryMonitorTest {
  @Test
  def testMonitor: Unit = {

    Thread.sleep(10000l)

    MemoryMonitor.INSTANCE.start()

    var list = new ArrayBuffer[String]
    (0 to 10000000).foreach(list += String.valueOf(_))
    list = null
    val stat = MemoryMonitor.INSTANCE.stop()

    assertTrue(stat.max > 100000000)
    assertTrue(0 < stat.min)

//    System.gc()
//    Thread.sleep(5000l)

    MemoryMonitor.INSTANCE.start()

    //    val list2 = new ArrayBuffer[String]
    //    (0 to 100000).foreach(list2 += String.valueOf(_))
    Thread.sleep(1000l)
    val stat2 = MemoryMonitor.INSTANCE.stop()

    assertTrue(stat2.max > 100)
    assertTrue(0 < stat2.min)

  }
}
