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

import java.nio.ByteBuffer

import org.apache.parquet.column.ColumnDescriptor
import org.apache.parquet.column.page.DataPage.Visitor
import org.apache.parquet.column.page.{DataPageV1, DataPageV2, PageReader}

object SimdScan{

  val simdScanner = new SimdScanner

  def scanBitpackedColumn(cd:ColumnDescriptor, reader:PageReader,total:Long,entryWidth:Int): ByteBuffer= {
    var page = reader.readPage()
//    val buffer = ByteBuffer.allocateDirect(total.toInt*entryWidth*2)
    while (page != null) {
      val pageResult = page.accept(new Visitor[ByteBuffer]() {
        override def visit(dataPageV1: DataPageV1): ByteBuffer = {
          simdScanner.scanBitpacked(dataPageV1.getBytes.toByteBuffer, 0, dataPageV1.getValueCount, 5, entryWidth)
        }

        override def visit(dataPageV2: DataPageV2): ByteBuffer = {
          simdScanner.scanBitpacked(dataPageV2.getData.toByteBuffer, 0, dataPageV2.getValueCount, 5, entryWidth)
        }
      })
      page = reader.readPage()
//      buffer.put(pageResult)
    }
    return null;
  }

  def decodeBitpackedColumn(cd:ColumnDescriptor, reader:PageReader,total:Long, entryWidth:Int):ByteBuffer= {
    var page = reader.readPage()
//    val buffer = ByteBuffer.allocateDirect(total.toInt*entryWidth*2)
    while (page != null) {
      val pageResult= page.accept(new Visitor[ByteBuffer]() {
        override def visit(dataPageV1: DataPageV1): ByteBuffer = {
          simdScanner.decodeBitpacked(dataPageV1.getBytes.toByteBuffer,0,dataPageV1.getValueCount,entryWidth)
        }

        override def visit(dataPageV2: DataPageV2): ByteBuffer = {
          simdScanner.decodeBitpacked(dataPageV2.getData.toByteBuffer,0,dataPageV2.getValueCount,entryWidth)
        }
      })

//      buffer.put(pageResult)
      page = reader.readPage()
    }
//    buffer
    null
  }
}
