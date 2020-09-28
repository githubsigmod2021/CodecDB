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

package codecdb.report

import java.io.File

import codecdb.dataset.persist.jpa.{ColumnWrapper, JPAPersistence}
import codecdb.model.DataType
import edu.uchicago.cs.encsel.dataset.persist.jpa.{ColumnWrapper, JPAPersistence}

import scala.collection.JavaConverters._
import scala.collection.mutable

object RawColumnSizeStat extends App {

  val sql = "SELECT c FROM Column c WHERE c.parentWrapper IS NULL"
  val em = new JPAPersistence().em

  val sizeCounter = new mutable.HashMap[DataType, Long]

  em.createQuery(sql, classOf[ColumnWrapper]).getResultList.asScala.foreach(col => {
    val len = new File(col.colFile).length()
    val exist = sizeCounter.getOrElseUpdate(col.dataType, 0L)
    sizeCounter.put(col.dataType, exist + len)
  })

  sizeCounter.foreach(e => {
    println("%s,%d".format(e._1.name(), e._2))
  })

}
