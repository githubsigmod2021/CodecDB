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

package codecdb.tool.mem

import java.lang.management.ManagementFactory

class MemoryMonitor private {

  private var thread: MemoryMonitorThread = null;

  def start(): Unit = {
    thread = new MemoryMonitorThread();
    thread.start();
  }

  def stop(): MemoryStat = {
    thread.monitorStop = true;
    thread.join()
    return new MemoryStat(thread.memoryMin, thread.memoryMax)
  }

}

class MemoryStat(var min: Long, var max: Long) {
}

class MemoryMonitorThread extends Thread {

  var monitorStop = false;

  var memoryMin = Long.MaxValue;
  var memoryMax = 0l;

  var interval = 100l;

  setName("MemoryMonitorThread")
  setDaemon(true)

  override def run(): Unit = {
    while (!monitorStop) {
      val memory = ManagementFactory.getMemoryMXBean.getHeapMemoryUsage.getUsed

      memoryMax = Math.max(memoryMax, memory)
      memoryMin = Math.min(memoryMin, memory)

      Thread.sleep(interval)
    }
    val memory = ManagementFactory.getMemoryMXBean.getHeapMemoryUsage.getUsed
    memoryMax = Math.max(memoryMax, memory)
    memoryMin = Math.min(memoryMin, memory)
  }
}

object MemoryMonitor {

  val INSTANCE = new MemoryMonitor();

}
