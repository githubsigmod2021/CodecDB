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

package codecdb.query.tpch

import java.io.File
import java.net.URI

import codecdb.dataset.feature.Feature
import codecdb.model.{FloatEncoding, IntEncoding, StringEncoding}
import codecdb.query.VColumnPredicate
import codecdb.query.operator.VerticalSelect
import codecdb.util.perf.Profiler
import edu.uchicago.cs.encsel.model.FloatEncoding
import org.apache.parquet.column.ColumnDescriptor
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Type.Repetition
import org.apache.parquet.schema.{MessageType, PrimitiveType}

import scala.collection.JavaConverters._

object ReadPerfTPCHColumn extends App {
  val inputFile = new File(args(0))
  val fileName = inputFile.getName
  val schema = TPCHSchema.lineitemSchema
  val compressions = Array(CompressionCodecName.UNCOMPRESSED, CompressionCodecName.LZO, CompressionCodecName.GZIP)

  val profiler = new Profiler
  val predicate = new VColumnPredicate((data) => true, 0)
  val select = new VerticalSelect() {
    override def createRecorder(schema: MessageType) = new NostoreColumnTempTable(schema)
  }

  def scan(t: ColumnDescriptor, index: Int, encoding: String, codec: CompressionCodecName): Unit = {
    try {
      val fileName = "%s.col%d.%s_%s".format(inputFile.getAbsolutePath, index, encoding, codec.name());
      if (!new File(fileName).exists())
        return
      val encfile = new URI(fileName)

      val colschema = new MessageType("default",
        new PrimitiveType(Repetition.OPTIONAL, t.getType, "value")
      )
      profiler.reset
      profiler.mark
      select.select(encfile, predicate, colschema, Array(0))
      profiler.pause
      val time = profiler.stop

      print("%d, %s, %s, %d\n".format(index, encoding, codec.name(), time.wallclock))
    } catch {
      case e: Exception => {
        e.printStackTrace()
        Iterable[Feature]()
      }
    }
  }


  schema.getColumns.asScala.zipWithIndex.foreach(c => {
    c._1.getType match {
      case PrimitiveTypeName.INT32 => {
        IntEncoding.values().toList.filter(_.parquetEncoding() != null).foreach(encoding => {
          compressions.foreach(codec => {
            scan(c._1, c._2, encoding.name(), codec)
          })
        })
      }
      case PrimitiveTypeName.BINARY => {
        StringEncoding.values().toList.filter(_.parquetEncoding() != null).foreach(encoding => {
          compressions.foreach(codec => {
            scan(c._1, c._2, encoding.name, codec)
          })
        })
      }
      case PrimitiveTypeName.DOUBLE => {
        FloatEncoding.values().toList.filter(_.parquetEncoding() != null).foreach(encoding => {
          compressions.foreach(codec => {
            scan(c._1, c._2, encoding.name, codec)
          })
        })
      }
      case _ => {

      }
    }
  })
}
