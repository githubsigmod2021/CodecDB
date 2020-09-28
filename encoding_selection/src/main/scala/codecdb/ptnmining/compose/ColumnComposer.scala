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

class ColumnComposer(val pattern: String, val loader: ChildColumnLoader) {

  private var current: String = null

  private var currentPosition = 0L

  private val composer: PatternComposer = new PatternComposer(pattern)

  // TODO Should save this information in META
  def getTotalValueCount: Long = 0


  def getString: String = {
    current = composer.compose(loader.next)
    currentPosition += 1
    return current
  }

  def skip: Unit = {
    loader.skip
    currentPosition += 1
  }
}
