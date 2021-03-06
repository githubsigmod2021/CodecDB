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

package codecdb.query.simdscan

import java.io.File
import java.nio.ByteBuffer

import codecdb.parquet.{EncReaderProcessor, ParquetReaderHelper}
import codecdb.query.NonePrimitiveConverter
import codecdb.query.bitmap.RoaringBitmap
import codecdb.query.tpch.TPCHSchema
import codecdb.util.perf.Profiler
import edu.uchicago.cs.encsel.parquet.EncReaderProcessor
import org.apache.parquet.VersionParser
import org.apache.parquet.column.ColumnDescriptor
import org.apache.parquet.column.impl.ColumnReaderImpl
import org.apache.parquet.column.page.DataPage.Visitor
import org.apache.parquet.column.page.{DataPageV1, DataPageV2, PageReadStore}
import org.apache.parquet.hadoop.metadata.BlockMetaData

object SimdQ1 extends App {

  val profiler = new Profiler

  val time = ParquetReaderHelper.profile(new File(args(0)).toURI, new EncReaderProcessor {
    override def processRowGroup(version: VersionParser.ParsedVersion,
                                 meta: BlockMetaData,
                                 rowGroup: PageReadStore): Unit = {
      // quantity 4
      // Extend price 5
      // discount 6
      // tax 7
      // line status 9
      // ship date 10
      // ship date <= '1998-09-01'
      // Scan ship date to generate bitmap

      profiler.reset
      profiler.mark

      val shipDateCol = TPCHSchema.lineitemSchema.getColumns().get(10)
      val quantityCol = TPCHSchema.lineitemSchema.getColumns().get(4)
      val lineStatusCol = TPCHSchema.lineitemSchema.getColumns.get(9)
      val shipDateReader = new ColumnReaderImpl(shipDateCol, rowGroup.getPageReader(shipDateCol),
        new NonePrimitiveConverter, version)
      val quantityReader = new ColumnReaderImpl(quantityCol, rowGroup.getPageReader(quantityCol),
        new NonePrimitiveConverter, version)
      val lineStatusReader = new ColumnReaderImpl(lineStatusCol, rowGroup.getPageReader(lineStatusCol),
        new NonePrimitiveConverter, version)
      // Generate bitmap
      //      val bitmap = new RoaringBitmap
      //      for (i <- 0L until shipDateReader.getTotalValueCount) {
      //        val date = shipDateReader.getBinary.toStringUsingUTF8
      //        if (date.compareTo("1998-09-01") <= 0) {
      //          bitmap.set(i, true)
      //        }
      //        shipDateReader.consume()
      //      }
      val bitmap = new RoaringBitmap
      val shipDatePageReader = rowGroup.getPageReader(shipDateCol)
      SimdScan.scanBitpackedColumn(shipDateCol,shipDatePageReader,meta.getRowCount ,13)

      profiler.pause
      val genbm = profiler.stop

      println("Generate Native Bitmap: count %d, time %d".format(shipDateReader.getTotalValueCount, genbm.wallclock))

      val quantityPageReader = rowGroup.getPageReader(quantityCol)
      SimdScan.decodeBitpackedColumn(quantityCol,quantityPageReader,meta.getRowCount,15)

      val lsPageReader = rowGroup.getPageReader(lineStatusCol)
      SimdScan.decodeBitpackedColumn(lineStatusCol,lsPageReader,meta.getRowCount,2)

      profiler.pause
      val scanbm = profiler.stop

      println("Decode Native: count %d, time %d".format(shipDateReader.getTotalValueCount, scanbm.wallclock))
      profiler.reset
      profiler.mark
      // Use bitmap to scan and decode other columns
      val selected = Array(5, 6, 7).map(i => {
        val cd = TPCHSchema.lineitemSchema.getColumns().get(i)
        new ColumnReaderImpl(cd, rowGroup.getPageReader(cd), new NonePrimitiveConverter, version)
      })

      var counter = 0
      // Scan all columns
      bitmap.foreach((index) => {
        while (counter < index) {
          selected.foreach(col => {
            col.skip()
            col.consume()
          })
          counter += 1
        }
        selected.foreach(col => {
          col.writeCurrentValueToConverter()
          col.consume()
        })
        counter += 1
      })

      profiler.pause
      val scan = profiler.stop
      println("Decode Others: count %d, time %d".format(shipDateReader.getTotalValueCount, scan.wallclock))
    }
  })

  println("Total time: %d".format(time.wallclock))



}
