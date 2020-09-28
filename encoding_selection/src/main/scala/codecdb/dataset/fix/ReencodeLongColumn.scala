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

package codecdb.dataset.fix

import codecdb.dataset.feature.resource.ParquetEncFileSize
import codecdb.dataset.persist.jpa.{ColumnWrapper, JPAPersistence}
import edu.uchicago.cs.encsel.dataset.persist.jpa.JPAPersistence

import scala.collection.JavaConverters._

object ReencodeLongColumn extends App {

  val sql = "SELECT child.* FROM col_data child WHERE NOT EXISTS( SELECT 1 FROM feature f WHERE f.col_id = child.id AND f.type = 'EncFileSize') AND child.parent_id IS NOT NULL"

  val persist = new JPAPersistence()
  val em = persist.em

  em.createNativeQuery(sql, classOf[ColumnWrapper]).getResultList.asScala.foreach(c => {
    val col = c.asInstanceOf[ColumnWrapper]
    col.replaceFeatures(ParquetEncFileSize.extract(col))
    persist.save(Iterable(col))
  })
}
