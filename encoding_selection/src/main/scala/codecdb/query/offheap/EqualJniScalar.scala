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

package codecdb.query.offheap

import java.nio.ByteBuffer

class EqualJniScalar(val target: Int, val entryWidth: Int) extends Predicate {
  System.loadLibrary("EqualJniScalar")

  override def execute(input: ByteBuffer, offset: Int, size: Int): ByteBuffer = {
    if (input.isDirect)
      return executeDirect(input, offset, size, target, entryWidth);
    else
      return executeHeap(input.array(), offset, size, target, entryWidth);
  }

  @native def executeDirect(input: ByteBuffer, offset: Int, size: Int, target: Int, entryWidth: Int): ByteBuffer;

  @native def executeHeap(input: Array[Byte], offset: Int, size: Int, target: Int, entryWidth: Int): ByteBuffer;
}
