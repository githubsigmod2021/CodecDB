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

package codecdb.encoding

import java.io.{File, RandomAccessFile}
import java.net.URI
import java.nio.MappedByteBuffer
import java.nio.channels.FileChannel.MapMode
import java.nio.charset.StandardCharsets

import codecdb.dataset.column.Column
import codecdb.model.{DataType, FloatEncoding, IntEncoding, StringEncoding}
import com.google.gson.{Gson, JsonObject}
import edu.uchicago.cs.encsel.model.FloatEncoding
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.io.{BufferedSource, Source}

object BitVectorEncoding {
  val DICT_MAX_SIZE = 500 * 1024 * 1024;
  val MEM_MAX_SIZE = 2 * 1024 * 1024 * 1024;
}

class BitVectorEncoding extends Encoding {

  override def encode(input: Column, output: URI): Unit = {
    val source = Source.fromFile(input.colFile)
    val source2 = Source.fromFile(input.colFile)

    val plainSize = new File(input.colFile).length();
    val outputFile = new RandomAccessFile(new File(output), "rw")

    try {
      val dict = new mutable.HashMap[String, Int]();
      var counter = 0L;
      var sizeCounter = 0L;
      // First pass, generate dict and count length
      source.getLines().foreach(l => {
        counter += 1;
        dict.getOrElseUpdate(l, dict.size)
        sizeCounter += 4 + l.length
        if (sizeCounter >= BitVectorEncoding.DICT_MAX_SIZE) {
          throw new EncodingException("Dict size exceed maximal allowed")
        }
      })
      // Store dict as json object
      var jsondict = new JsonObject;
      dict.foreach(f => {
        jsondict.addProperty(f._1, f._2);
      })
      var dictstr = new Gson().toJson(jsondict)
      var dictbytes = dictstr.getBytes(StandardCharsets.UTF_8)

      // Compute file size, allocate space
      val bitvecSize = Math.ceil(counter.toDouble / 8).toLong
      val bitmapSize = bitvecSize * dict.size

      // 64 bit for dictionary offset
      // 64 bit for number of items
      // bitmap
      // dictionary
      val bitmapOffset: Long = 8 + 8L
      // Write dictionary
      val fileSize = bitmapOffset + bitmapSize + dictbytes.length


      // Early stop if this encoding is bad
      if (fileSize > 2 * plainSize) {
        throw new EncodingException("Encoded size exceed plain size");
      }

      outputFile.seek(0)
      outputFile.writeLong(bitmapOffset + bitmapSize)
      outputFile.writeLong(counter)

      // Write dictionary
      val dictBuffer = outputFile.getChannel.map(MapMode.READ_WRITE, bitmapOffset + bitmapSize, dictbytes.length);
      dictBuffer.put(dictbytes)
      dictBuffer.force()

      // Second pass, write bit vectors
      var pos: Long = bitmapOffset;
      val size: Long = bitmapSize;
      var buffer = outputFile.getChannel.map(MapMode.READ_WRITE, pos, size);
      buffer.load();

      var offset = 0l
      source2.getLines().foreach(line => {
        val idx: Int = dict.getOrElse(line, -1)
        val byteOffset: Long = idx * bitvecSize + offset / 8
        val bitOffset = offset % 8

        val bufferOffset = byteOffset % size
        val byte = buffer.get(bufferOffset.toInt)
        buffer.put(bufferOffset.toInt, (byte | (1 << bitOffset)).toByte)
        offset += 1
      })
      outputFile.setLength(fileSize)

    } finally {
      source.close();
      source2.close();
      outputFile.close();
    }
  }

  override def enctype(dt: DataType): String = {
    dt match {
      case DataType.INTEGER => IntEncoding.BITVECTOR.name()
      case DataType.FLOAT => FloatEncoding.BITVECTOR.name()
      case DataType.STRING => StringEncoding.BITVECTOR.name()
      case DataType.DOUBLE => FloatEncoding.BITVECTOR.name()
      case DataType.LONG => IntEncoding.BITVECTOR.name()
      case _ => throw new UnsupportedOperationException
    }
  }
}
