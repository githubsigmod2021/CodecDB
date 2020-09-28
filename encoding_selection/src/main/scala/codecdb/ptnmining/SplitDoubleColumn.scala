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

package codecdb.ptnmining

import codecdb.dataset.persist.jpa.{ColumnWrapper, JPAPersistence}
import codecdb.model.DataType
import edu.uchicago.cs.encsel.dataset.persist.jpa.{ColumnWrapper, JPAPersistence}

import scala.collection.JavaConverters._

object SplitDoubleColumn extends App {

  val persist = new JPAPersistence

  val dcols = persist.em.createQuery("SELECT d FROM Column d WHERE d.dataType = :dt ORDER BY d.id ASC", classOf[ColumnWrapper]).setParameter("dt", DataType.DOUBLE).getResultList.asScala


  dcols.foreach(col => {
    println(col.id)
    val sub = MineColumn.splitDouble(col)
    if (!sub.isEmpty) {
      persist.save(sub)
    }
  })
}
