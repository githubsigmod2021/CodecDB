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

package codecdb.query.offheap

import java.nio.{BufferOverflowException, ByteBuffer, ByteOrder}

object EqualScalar {
  var resBuffer = ByteBuffer.allocateDirect(1024 * 1024).order(ByteOrder.LITTLE_ENDIAN)
}

class EqualScalar(val target: Int, val entryWidth: Int, val dm: Boolean = true) extends Predicate {

  var buffer: Long = 0
  val BUFFER_SIZE = 64

  def execute(input: ByteBuffer, offset: Int, size: Int): ByteBuffer = {
    val bufferSize = Math.ceil(size.toDouble / BUFFER_SIZE).toInt * BUFFER_SIZE / 8
    val result = dm match {
      case true => {
        ByteBuffer.allocateDirect(bufferSize).order(ByteOrder.LITTLE_ENDIAN)
      }
      case false => {
        if (bufferSize > EqualScalar.resBuffer.capacity()) {
          EqualScalar.resBuffer = ByteBuffer.allocateDirect(bufferSize).order(ByteOrder.LITTLE_ENDIAN);
        } else {
          EqualScalar.resBuffer.rewind
        }
        EqualScalar.resBuffer
      }
    }

    if (entryWidth < 26) {
      // Each entry is guaranteed in a integer
      scanInt(input, offset, size, result)
    } else { // Each entry is in a long
      scanLong(input, offset, size, result)
    }
    return result
  }

  private def scanInt(input: ByteBuffer, offset: Int, size: Int, result: ByteBuffer): Unit = {
    buffer = 0
    // Entry before this number can be read using getInt
    val entryInInt = (input.capacity() - offset - 4) * 8 / entryWidth + 1
    val intLimit = Math.min(entryInInt, size)
    val mask = (1 << entryWidth) - 1

    for (i <- 0 until intLimit) {
      val byteIndex = i * entryWidth / 8
      val byteOffset = i * entryWidth % 8

      val readValue = input.getInt(offset + byteIndex) >> byteOffset

      (readValue & mask) match {
        case b if b == target => {
          buffer |= (1L << (i % BUFFER_SIZE))
        }
        case _ => {}
      }

      if (i % BUFFER_SIZE == BUFFER_SIZE - 1) {
        result.putLong(buffer)
        buffer = 0
      }
    }
    // These entries need to be read bytes by bytes
    for (i <- intLimit until size) {
      scanByByte(input, offset, i, result)
    }
    if (size % BUFFER_SIZE != 0)
      result.putLong(buffer)
    result.flip()
  }

  private def scanLong(input: ByteBuffer, offset: Int, size: Int, result: ByteBuffer): Unit = {
    buffer = 0
    // Entry before this number can be read using getLong
    val entryInLong = (input.capacity() - offset - 8) * 8 / entryWidth + 1
    val longLimit = Math.min(size, entryInLong)
    val mask = (1L << entryWidth) - 1

    for (i <- 0 until longLimit) {
      val byteIndex = i * entryWidth / 8
      val byteOffset = i * entryWidth % 8

      val readValue = input.getLong(offset + byteIndex) >> byteOffset

      (readValue & mask) match {
        case b if b == target => {
          buffer |= (1L << (i % BUFFER_SIZE))
        }
        case _ => {}
      }

      if (i % BUFFER_SIZE == BUFFER_SIZE - 1) {
        try {
          result.putLong(buffer)
        } catch {
          case e: BufferOverflowException => {
            println("Overflow")
          }
        }
        buffer = 0
      }
    }
    // These entries need to be read bytes by bytes
    for (i <- longLimit until size) {
      scanByByte(input, offset, i, result)
    }
    if (size % BUFFER_SIZE != 0)
      result.putLong(buffer)
    result.flip
  }

  private def scanByByte(input: ByteBuffer, offset: Int, i: Int, result: ByteBuffer): Unit = {
    val byteIndex = i * entryWidth / 8
    val byteOffset = i * entryWidth % 8

    val byteToRead = Math.ceil((byteOffset + entryWidth).toDouble / 8).toInt

    var readValue = 0L

    for (i <- 0 until byteToRead) {
      readValue |= ((input.get(offset + byteIndex + i).toInt & 0xff) << (i * 8))
    }
    readValue >>= byteOffset
    val mask = (1L << entryWidth) - 1

    (readValue & mask) match {
      case b if b == target => {
        buffer |= (1L << i % BUFFER_SIZE)
      }
      case _ => {}
    }

    if (i % BUFFER_SIZE == BUFFER_SIZE - 1) {
      result.putLong(buffer)
      buffer = 0
    }
  }
}
