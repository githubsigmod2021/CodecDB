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
 *
 */

package codecdb.util.perf

import java.lang.management.ManagementFactory

class Profiler {

  val threadBean = ManagementFactory.getThreadMXBean

  var running = false

  var wcsum = 0L
  var cpusum = 0L
  var usersum = 0L

  var wcstart = 0L
  var cpustart = 0L
  var userstart = 0L

  def mark: Unit = {
    wcstart = System.currentTimeMillis()
    cpustart = threadBean.getCurrentThreadCpuTime
    userstart = threadBean.getCurrentThreadUserTime
    running = true
  }

  def pause: Unit = {
    wcsum += System.currentTimeMillis() - wcstart
    cpusum += threadBean.getCurrentThreadCpuTime - cpustart
    usersum += threadBean.getCurrentThreadUserTime - userstart
    running = false
  }

  def stop: ProfileBean = {
    if (running)
      pause
    return new ProfileBean(wcsum, cpusum, usersum)
  }

  def reset: Unit = {
    wcsum = 0
    cpusum = 0
    usersum = 0
  }
}

class ProfileBean(val wallclock: Long, val cpu: Long, val user: Long)