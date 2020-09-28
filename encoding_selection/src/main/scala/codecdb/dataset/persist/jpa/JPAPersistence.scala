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

package codecdb.dataset.persist.jpa

import codecdb.dataset.column.Column
import codecdb.dataset.persist.Persistence
import codecdb.model.DataType
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions.asScalaBuffer

class JPAPersistence extends Persistence {
  val logger = LoggerFactory.getLogger(getClass)

  val em = JPAPersistence.emf.createEntityManager()

  def save(datalist: Traversable[Column]) = {
    em.getTransaction.begin()
    try {
      datalist.map(ColumnWrapper.fromColumn).foreach { data => {
        data.id match {
          case 0 => em.persist(data)
          case _ => em.merge(data)
        }
      }
      }
      em.getTransaction.commit()
    } catch {
      case e: Exception => {
        logger.warn("Exception while saving data", e)
        if (em.getTransaction.isActive)
          em.getTransaction.rollback()
        throw new RuntimeException(e)
      }
    }
  }

  def load(): Iterator[Column] = {
    val query = em.createQuery("SELECT c FROM Column c ORDER BY c.id", classOf[ColumnWrapper])
    val res = query.getResultList.map(_.asInstanceOf[Column]).toIterator
    res
  }

  def clean() = {
    em.getTransaction.begin()
    try {
      val query = em.createQuery("DELETE FROM Column c", classOf[ColumnWrapper])
      query.executeUpdate()
      em.getTransaction.commit()
    } catch {
      case e: Exception => {
        em.getTransaction.rollback()
        throw new RuntimeException(e)
      }
    }
  }

  def find(id: Int): Column = {
    val res = em.createQuery("SELECT c FROM Column c where c.id = :id",
      classOf[ColumnWrapper]).setParameter("id", id).getSingleResult
    res
  }

  def lookup(dataType: DataType): Iterator[Column] = {
    val result = em.createQuery("SELECT c FROM Column c where c.dataType = :dt", classOf[ColumnWrapper])
      .setParameter("dt", dataType).getResultList.map(_.asInstanceOf[Column]).toIterator
    result
  }
}

object JPAPersistence {
  val emf = javax.persistence.Persistence.createEntityManagerFactory("enc-selector")
}