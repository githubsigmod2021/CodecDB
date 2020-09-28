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

import codecdb.ptnmining.parser.Tokenizer
import org.junit.Assert._
import org.junit.Test
import org.nd4j.linalg.factory.Nd4j

/**
  * Created by harper on 3/9/17.
  */
class FrequentWordTest {


  @Test
  def testMerge: Unit = {

    val data = Array("This is New York",
      "New York has the best ",
      "Where New York can",
      "What New York good at",
      "No New York at good",
      "312 New York",
      "New York",
      "kk New York Mpv",
      "5221 New York",
      "4242 New York",
      "242 New York St.")

    val tokens = data.map(Tokenizer.tokenize(_).toSeq).toSeq

    val fw = new FrequentWord
    fw.init(tokens)

    val merged = fw.merge()

    assertEquals(11, merged.length)

    //noinspection ZeroIndexToHead
    assertEquals(5, merged(0).length)
    //noinspection ZeroIndexToHead
    assertEquals("New York", merged(0)(4).value)

    assertEquals(8, merged(1).length)
    //noinspection ZeroIndexToHead
    assertEquals("New York", merged(1)(0).value)
  }

  /*
    @Test
    def testGroup: Unit = {

      val hspots = Array(Array("p.o.", "st."), Array("rd."),
        Array("ap", "rd."), Array("avenue"), Array("apt", "ave."),
        Array("apt", "e", "road"))
        .map(_.toSeq).toSeq
      val finder = new FrequentWord

      val group = finder.group(hspots)

      assertEquals(3, group.size)
      println(group)
      assertTrue(group(0).contains("p.o."))
      assertTrue(group(0).contains("ap"))
      assertTrue(group(0).contains("apt"))
    }

  */
  @Test
  def testChildren: Unit = {

    val children = FrequentWord.children(Array("a", "b", "c", "d", "e", "f", "g"))
      .map(_._1.map(_.asInstanceOf[AnyRef]).toArray).toArray

    assertArrayEquals(Array[AnyRef]("a", "b", "c", "d", "e", "f", "g"), children(0))
    assertArrayEquals(Array[AnyRef]("a", "b", "c", "d", "e", "f"), children(1))
    assertArrayEquals(Array[AnyRef]("b", "c", "d", "e", "f", "g"), children(2))
    assertArrayEquals(Array[AnyRef]("a", "b", "c", "d", "e"), children(3))
    assertArrayEquals(Array[AnyRef]("b", "c", "d", "e", "f"), children(4))
    assertArrayEquals(Array[AnyRef]("c", "d", "e", "f", "g"), children(5))
    assertArrayEquals(Array[AnyRef]("a", "b", "c", "d"), children(6))
    assertArrayEquals(Array[AnyRef]("b", "c", "d", "e"), children(7))
    assertArrayEquals(Array[AnyRef]("c", "d", "e", "f"), children(8))
    assertArrayEquals(Array[AnyRef]("d", "e", "f", "g"), children(9))
    assertArrayEquals(Array[AnyRef]("a", "b", "c"), children(10))
  }

  @Test
  def testSimilar: Unit = {
    val d0 = Nd4j.create(Array(1d, 0))
    val d90 = Nd4j.create(Array(0d, 1))
    val d180 = Nd4j.create(Array(-1d, 0))

    val a = Array(d180, d0, d180)
    val b = Array(d0)

    val similar = FrequentWord.similar(a, b)

    assertTrue(Array((1, 0)).deep == similar.toArray.deep)

    val a2 = Array(d180, d0, d180)
    val b2 = Array(d0, d0)
    val similar2 = FrequentWord.similar(a2, b2)
    assertTrue(Array((1, 0)).deep == similar2.toArray.deep)

    val a3 = Array(d180, d180)
    val b3 = Array(d0, d0)

    val similar3 = FrequentWord.similar(a3, b3)
    assertTrue(Array.empty[(Int, Int)].deep == similar3.toArray.deep)

    val a4 = Array(d180, d0, d90, d0)
    val b4 = Array(d0, d0)
    val similar4 = FrequentWord.similar(a4, b4)
    assertTrue(Array((1, 0), (3, 1)).deep == similar4.toArray.deep)
  }

}
