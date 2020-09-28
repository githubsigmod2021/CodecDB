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

package codecdb.dataset

import codecdb.dataset.feature.classify.Sortness
import codecdb.dataset.persist.Persistence
import codecdb.dataset.persist.jpa.JPAPersistence

/**
  * Created by harper on 5/8/17.
  */
object SingleColumnInspect extends App {

  val p = Persistence.get.asInstanceOf[JPAPersistence]

  val column = p.find(4)

  val sort = new Sortness(100)

  val features = sort.extract(column)

  Unit
}
