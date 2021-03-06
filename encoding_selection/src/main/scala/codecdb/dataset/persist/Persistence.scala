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
package codecdb.dataset.persist

import codecdb.dataset.column.Column
import codecdb.dataset.persist.file.FilePersistence
import codecdb.dataset.persist.jpa.JPAPersistence
import codecdb.model.DataType

/**
  * Interface for persisting columns and features
  */
trait Persistence {
  def save(datalist: Traversable[Column])

  def load(): Iterator[Column]

  def lookup(dataType: DataType): Iterator[Column]

  def clean()

}

object Persistence {
  private val impl = new JPAPersistence

  def get: Persistence = impl
}