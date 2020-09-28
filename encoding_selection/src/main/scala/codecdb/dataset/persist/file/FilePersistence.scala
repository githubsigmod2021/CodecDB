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
package codecdb.dataset.persist.file

import java.io._

import codecdb.dataset.column.Column
import codecdb.dataset.persist.Persistence
import codecdb.model.DataType

import scala.collection.mutable.HashSet

/**
  * A thread safe implementation of <code>Persistence</code> backed by file storage
  */
class FilePersistence extends Persistence {

  val storage = new File("storage.dat")
  var datalist = new scala.collection.mutable.HashSet[Column]()
  datalist ++= load()

  def save(datalist: Traversable[Column]) = {
    this.synchronized {
      this.datalist ++= datalist

      val objwriter = new ObjectOutputStream(new FileOutputStream(storage))
      objwriter.writeObject(this.datalist)
      objwriter.close()
    }
  }

  def clean() = {
    this.synchronized {
      this.datalist.clear()

      val objwriter = new ObjectOutputStream(new FileOutputStream(storage))
      objwriter.writeObject(this.datalist)
      objwriter.close()
    }
  }

  def load(): Iterator[Column] = {
    this.synchronized {
      try {
        val objreader = new ObjectInputStream(new FileInputStream(storage))
        val data = objreader.readObject().asInstanceOf[HashSet[Column]]
        objreader.close()

        return data.clone().iterator
      } catch {
        case _: FileNotFoundException => {
          return Iterator[Column]()
        }
      }
    }
  }

  def lookup(dataType: DataType): Iterator[Column] = {
    load().filter(_.dataType == dataType)
  }
}