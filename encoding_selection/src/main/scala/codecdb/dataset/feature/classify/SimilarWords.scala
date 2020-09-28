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

package codecdb.dataset.feature.classify

import java.io.InputStream
import java.util.concurrent.{Callable, Executors, Future}

import codecdb.dataset.column.Column
import codecdb.dataset.feature.{Feature, FeatureExtractor}
import edu.uchicago.cs.encsel.dataset.feature.FeatureExtractor

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random


class SimilarWords(val msgSize: Int = (1 << 8) - 1) extends FeatureExtractor {
  override def featureType: String = "SimilarWords"

  override def supportFilter: Boolean = true

  val windowSize = (1 << 15) - 1

  val threshold = 10

  val threadPool = Executors.newFixedThreadPool(30)

  override def extract(column: Column, input: InputStream, prefix: String): Iterable[Feature] = {
    val fType = "%s%s".format(prefix, featureType)

    val fpr = new Fingerprint(msgSize)

    //    val buffer = new Array[Byte](windowSize)
    var size = 0
    var skipProb = 1.0 / msgSize

    val info = new BlockInfo
    info.counter = 0

    val futures = new ArrayBuffer[Future[BlockInfo]]()

    do {
      if (futures.size >= threshold && Random.nextDouble() >= skipProb) {
        val start = System.currentTimeMillis()
        input.skip(windowSize)
        size = windowSize
      } else {
        val buffer = new Array[Byte](windowSize)
        size = input.read(buffer)
        val bsize = size
        val callable: Callable[BlockInfo] = new Callable[BlockInfo] {
          def call: BlockInfo = {
            scanBlock(buffer, bsize, fpr)
          }
        }
        futures += threadPool.submit(callable)
      }
    }
    while (size == windowSize)

    try {
      futures.foreach(f => info.merge(f.get))
    } catch {
      case e: Exception => throw new RuntimeException(e)
    }
    return Iterable(
      new Feature(fType, "ratio", info.compressionRatio),
      new Feature(fType, "msglen_entropy", info.msglenEntropy),
      new Feature(fType, "dist_entropy", info.msgDistEntropy),
      new Feature(fType, "char_entropy", info.charEntropy),
      new Feature(fType, "block_count", info.counter)
    )
  }

  def scanBlock(buffer: Array[Byte], size: Int, fpr: Fingerprint): BlockInfo = {
    val exists = new mutable.HashMap[Long, Int]
    //    val exists = BloomFilterWrapper.create(1000000)
    //    val exists = new Roaring64NavigableMap()
    exists += ((0, 0))
    //    exists.add(0)
    //    exists.put(0l)
    var suffixs = new SuffixBuffer(msgSize + 1, fpr)

    val msgcounter = Array.fill[Int](msgSize + 1)(0)
    val distcounter = new mutable.HashMap[Int, Int]
    val charcounter = Array.fill[Int](257)(0)

    var pointer = 0
    while (pointer < size) {
      // Look forward for the longest prefix
      var fpointer = pointer

      var msgfp: Long = 0
      var msgdist: Int = 0

      // Find the longest prefix
      while (fpointer < size && fpointer - pointer < msgSize && exists.contains(msgfp)) {

        var newchar = buffer(fpointer).toInt
        if (newchar < 0)
          newchar += 255
        msgdist = exists.getOrElse(msgfp, 0)
        //        msgdist = 0
        msgfp = fpr.combine(msgfp, fpointer - pointer, newchar)
        if (exists.contains(msgfp)) {
          msgdist = exists.getOrElse(msgfp, 0)
          //          msgdist = 0
          // Update suffix and update substrings
          suffixs.shiftIn(newchar)
          fpointer += 1
          val newsubstr = suffixs.values(Math.min(fpointer, msgSize))
          exists ++= newsubstr.zipWithIndex.map(pair => (pair._1, fpointer - pair._2))
          //          exists.add(newsubstr: _*)
          //          newsubstr.foreach(exists.put(_))
          //          exists.putAll(suffixs.valuesAsBloomFilter(Math.min(fpointer, msgSize)))
        }
      }

      if (fpointer < size && fpointer - pointer < msgSize) {
        // Add the new char to suffix
        var newchar = buffer(fpointer).toInt
        if (newchar < 0)
          newchar += 255
        suffixs.shiftIn(newchar)
        charcounter(newchar) += 1
        fpointer += 1
        val newsubstr = suffixs.values(Math.min(fpointer, msgSize))
        exists ++= newsubstr.zipWithIndex.map(pair => (pair._1, fpointer - pair._2))
        //        newsubstr.foreach(exists.put(_))
        //        exists.add(newsubstr: _*)
        //        exists.putAll(suffixs.valuesAsBloomFilter(Math.min(fpointer, msgSize)))
      } else {
        // Reach message size limit
        charcounter(0) += 1
      }

      var msglen = fpointer - pointer
      pointer = fpointer

      msgcounter(msglen) += 1
      val distc = distcounter.getOrElse(msgdist, 0)
      distcounter.put(msgdist, distc + 1)
    }

    val blockInfo = new BlockInfo
    blockInfo.counter = 1

    val msgcounterSum = msgcounter.sum.toDouble
    val distcounterSum = distcounter.values.sum.toDouble

    blockInfo.compressionRatio = msgcounterSum / size
    blockInfo.msglenEntropy = msgcounter.map(probMap(msgcounterSum)).sum
    blockInfo.msgDistEntropy = distcounter.values.map(probMap(distcounterSum)).sum
    blockInfo.charEntropy = charcounter.map(probMap(msgcounterSum)).sum

    return blockInfo
  }

  def probMap(sum: Double): (Int) => Double = {
    (value: Int) => {
      value match {
        case 0 => 0
        case _ => {
          val prob = value / sum
          -prob * Math.log(prob)
        }
      }
    }
  }
}

class BlockInfo {

  var compressionRatio: Double = 0

  var msglenEntropy: Double = 0

  var msgDistEntropy: Double = 0

  var charEntropy: Double = 0

  var counter = 0

  def merge(other: BlockInfo) = {
    this.compressionRatio =
      (this.compressionRatio * this.counter + other.compressionRatio * other.counter) / (this.counter + other.counter)
    this.msglenEntropy =
      (this.msglenEntropy * this.counter + other.msglenEntropy * other.counter) / (this.counter + other.counter)
    this.msgDistEntropy =
      (this.msgDistEntropy * this.counter + other.msgDistEntropy * other.counter) / (this.counter + other.counter)
    this.charEntropy =
      (this.charEntropy * this.counter + other.charEntropy * other.counter) / (this.counter + other.counter)
    this.counter += other.counter
  }

}

/**
  * SuffixBuffer(i) is the fingerprint of suffix with size i.
  * size range is 1 to msgSize - 1
  *
  * @param size
  * @param fpr
  */
class SuffixBuffer(val size: Int, val fpr: Fingerprint) {

  var pointer = 0

  val storage = Array.fill[Long](size)(0l)

  def shift(offset: Int) = {
    pointer += offset
  }

  def apply(index: Int): Long = {
    storage((pointer + index) % size)
  }

  def update(index: Int, value: Long) = {
    storage((pointer + index) % size) = value
  }

  def shiftIn(value: Long) = {
    for (i <- 1 until size - 1) {
      update(i, fpr.combine(apply(i), i, value))
    }
    // Shift pointer back
    pointer -= 1
    while (pointer < 0)
      pointer += size
    pointer %= size

    update(1, value)
    update(0, 0l)
  }

  def values(length: Int) = (1 to length).map(apply(_))

}

object ModularMath {
  val p = 2806624493l

  def add(a: Long, b: Long): Long = {
    return (a + b) % p
  }

  def sub(a: Long, b: Long): Long = {
    return (a + p - b) % p
  }

  def mul(a: Long, b: Long): Long = {
    return a * b % p
  }
}

class Fingerprint(val msgSize: Int = (1 << 8) - 1) {
  val p = 2806624493l
  var r: Long = (Math.abs(Random.nextLong()) % p)
  var rinv = inverse()
  var rp = new Array[Long](msgSize);
  var rinvp = new Array[Long](msgSize);
  rp(0) = 1
  rinvp(0) = 1


  for (i <- 1 until msgSize) {
    rp(i) = ModularMath.mul(rp(i - 1), r)
    rinvp(i) = ModularMath.mul(rinvp(i - 1), rinv)
  }

  def get(s: String): Long = {
    s.zipWithIndex.map(pair => ModularMath.mul(pair._1, rp(pair._2))).reduce(ModularMath.add)
  }

  def combine(lfp: Long, llen: Int, rfp: Long): Long = {
    ModularMath.add(ModularMath.mul(rfp, rp(llen)), lfp)
  }

  def divide(fp: Long, pow: Int): Long = {
    ModularMath.mul(fp, rinvp(pow))
  }

  def shifth(fp: Long, shift: Int): Long = {
    ModularMath.mul(fp, rp(shift))
  }

  def shiftl(fp: Long, lfp: Long, lfpl: Int): Long = {
    ModularMath.mul(ModularMath.sub(fp, lfp), rinvp(lfpl))
  }

  /**
    * Compute the inverse of r using extended Euclidean algorithm
    *
    * @return
    */
  def inverse(): Long = {
    var data0 = Array(p, 1, 0)
    var data1 = Array(r, 0, 1)
    do {
      val div = data0(0) / data1(0)
      var newdata = data0.zip(data1).map(pair => pair._1 - pair._2 * div)
      data0 = data1
      data1 = newdata
    } while (data1(0) != 1)
    var cand = data1(2)
    while (cand < 0) {
      cand += p
    }
    return cand
  }
}