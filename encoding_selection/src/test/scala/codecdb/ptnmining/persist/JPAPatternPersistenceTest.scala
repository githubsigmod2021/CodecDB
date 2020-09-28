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

import java.io.File

import codecdb.dataset.feature.Feature
import codecdb.dataset.persist.jpa.JPAPersistence
import codecdb.model.DataType
import edu.uchicago.cs.encsel.dataset.persist.jpa.JPAPersistence
import org.junit.{Before, Test}
import org.junit.Assert._

class JPAPatternPersistenceTest {
  @Before
  def cleanSchema: Unit = {
    val em = JPAPersistence.emf.createEntityManager()

    em.getTransaction.begin()
    em.createNativeQuery("DELETE FROM col_info;").executeUpdate()
    em.createNativeQuery("DELETE FROM col_pattern;").executeUpdate()
    em.createNativeQuery("DELETE FROM feature;").executeUpdate()
    em.createNativeQuery("DELETE FROM col_data;").executeUpdate()

    em.createNativeQuery("INSERT INTO col_data (id, file_uri, idx, name, data_type, origin_uri, parent_id) VALUES (?, ?, ?, ?, ?, ?, ?);")
      .setParameter(1, 20)
      .setParameter(2, "file:/home/harper/IdeaProjects/enc-selector/aab")
      .setParameter(3, 5)
      .setParameter(4, "a")
      .setParameter(5, "STRING")
      .setParameter(6, "file:/home/harper/IdeaProjects/enc-selector/ccd")
      .setParameter(7, null).executeUpdate()
    em.createNativeQuery("INSERT INTO feature(col_id,type, name, value) VALUES(?, ?, ?, ?);").setParameter(1, 20)
      .setParameter(2, "P").setParameter(3, "M").setParameter(4, 2.4).executeUpdate()

    em.getTransaction.commit()
    em.getEntityManagerFactory.getCache.evictAll()
    em.close()
  }

  @Test
  def testSaveAndFind: Unit = {
    val col = new JPAPersistence().find(20)
    assertEquals(null, JPAPatternPersistence.find(col))
    JPAPatternPersistence.save(col, "MMP")
    assertEquals("MMP", JPAPatternPersistence.find(col))
    JPAPatternPersistence.save(col, "DEQ")
    assertEquals("DEQ", JPAPatternPersistence.find(col))
  }
}
