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
 */

package codecdb.ptnmining.preprocess

import org.junit.Assert._
import org.junit.Test

/**
  * Created by harper on 3/26/17.
  */
class SearchTableTest {

  @Test
  def testSearchTable:Unit = {
    val st = new SearchTable

    st.add(Array("a","b"))
    st.add(Array("a","b","d"))
    st.add(Array("a","t","t"))
    st.add(Array("c","d","e"))

    assertTrue(st.accept("a"))
    assertFalse(st.isEnd)
    assertTrue(st.accept("b"))
    assertTrue(st.isEnd)
    assertTrue(st.accept("d"))
    assertTrue(st.isEnd)
    assertFalse(st.accept("w"))

    st.reset

    assertTrue(st.accept("a"))
    assertFalse(st.isEnd)
    assertTrue(st.accept("b"))
    assertTrue(st.isEnd)
    assertFalse(st.accept("c"))
    assertTrue(st.isEnd)
  }
}
