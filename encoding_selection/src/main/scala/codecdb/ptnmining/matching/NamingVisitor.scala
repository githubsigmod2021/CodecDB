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

package codecdb.ptnmining.matching

import codecdb.ptnmining.{Pattern, PatternVisitor}
import edu.uchicago.cs.encsel.ptnmining.PatternVisitor

import scala.collection.mutable

/**
  * Assign unique name to each pattern node
  */
class NamingVisitor extends PatternVisitor {
  private val counter = new mutable.Stack[Int]
  counter.push(0)

  override def on(ptn: Pattern): Unit = {
    val parentName = path.isEmpty match {
      case true => ""
      case false => //noinspection ZeroIndexToHead
        path(0).name
    }
    var current = counter.pop
    ptn.name = "%s_%d".format(parentName, current)
    current += 1
    counter.push(current)
  }

  override def enter(container: Pattern): Unit = {
    super.enter(container)
    counter.push(0)
  }

  override def exit(container: Pattern): Unit = {
    super.exit(container)
    counter.pop
  }
}
