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

package codecdb.ptnmining.persist

import codecdb.dataset.column.Column
import codecdb.dataset.persist.jpa.ColumnWrapper
import javax.persistence.{EntityManager, EntityManagerFactory, NoResultException, Persistence}

trait PatternPersistence {

  def save(col: Column, pattern: String)

  def find(col: Column): String
}

object JPAPatternPersistence extends PatternPersistence {

  private[persist] val em = Persistence.createEntityManagerFactory("enc-selector").createEntityManager()

  override def save(col: Column, pattern: String): Unit = {
    if (!col.isInstanceOf[ColumnWrapper]) {
      throw new UnsupportedOperationException
    }
    em.getTransaction.begin()
    em.merge(new PatternWrapper(col.asInstanceOf[ColumnWrapper], pattern))
    em.getTransaction.commit()
  }

  override def find(col: Column): String = {
    if (!col.isInstanceOf[ColumnWrapper]) {
      throw new UnsupportedOperationException
    }
    val cw = col.asInstanceOf[ColumnWrapper]
    try {
      val pw = em.createQuery("SELECT p FROM Pattern p WHERE p.column = :c", classOf[PatternWrapper])
        .setParameter("c", cw).getSingleResult
      pw.pattern
    } catch {
      case e: NoResultException => null
    }
  }
}
