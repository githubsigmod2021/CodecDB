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

package codecdb.app.carbon

import java.io.File
import java.nio.file.{Files, Path, Paths}

import scala.collection.JavaConversions._

object GenCarbonScript extends App {

  //  println(folderSize(Paths.get(new File("/home/harper/Repositories/incubator-carbondata/bin/carbonshellstore/default/event_time_57463913727907242513").toURI())))

  compareSize()

  def carbonScript(): Unit = {
    val dir = Paths.get(new File("/home/harper/dataset/carbon").toURI)

    Files.list(dir).iterator().toArray.foreach(file => {
      val filename = file.getFileName.toString.replaceAll("\\.tmp$", "")
      println("""cc.sql("CREATE TABLE IF NOT EXISTS %s(id string) STORED BY 'carbondata'")""".format(filename))
      println("""cc.sql("LOAD DATA INPATH '%s' INTO TABLE %s")""".format(file.toString, filename))
    })
  }

  def addHeader(): Unit = {

  }

  def compareSize(): Unit = {
    val dir = Paths.get(new File("/home/harper/dataset/carbon").toURI)
    val carbondir = Paths.get(new File("/home/harper/Repositories/incubator-carbondata/bin/carbonshellstore/default").toURI)
    Files.list(dir).iterator().toArray.foreach(file => {
      val filename = file.getFileName.toString.replaceAll("\\.tmp$", "").toLowerCase()
      val filesize = new File(file.toUri).length()
      val folder = carbondir.resolve(filename)
      if (Files.exists(folder)) {
        val foldersize = folderSize(folder)
        println("%s,%d,%d".format(filename, filesize, foldersize))
      }
    })
  }

  def folderSize(folder: Path): Long = {
    Files.walk(folder).iterator().filter { !Files.isDirectory(_) }.map(p => new File(p.toUri).length()).sum
  }
}