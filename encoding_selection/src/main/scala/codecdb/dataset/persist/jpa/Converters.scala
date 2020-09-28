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

package codecdb.dataset.persist.jpa

import java.net.URI

import codecdb.model.DataType
import javax.persistence.AttributeConverter

import scala.util.Try
import org.eclipse.persistence.mappings.DatabaseMapping
import org.eclipse.persistence.sessions.Session
import org.eclipse.persistence.mappings.converters.Converter

class URIConverter extends AttributeConverter[URI, String] with Converter {
  def convertToDatabaseColumn(objectValue: URI): String = Try(objectValue.asInstanceOf[URI].toString).getOrElse("")
  def convertToEntityAttribute(dataValue: String): URI = Try { new URI(dataValue.toString) }.getOrElse(null)
  
  def convertObjectValueToDataValue(objectValue: Object, session: Session): String = convertToDatabaseColumn(objectValue.asInstanceOf[URI])
  def convertDataValueToObjectValue(dataValue: Object, session: Session): URI = convertToEntityAttribute(dataValue.toString)
  def isMutable: Boolean = false
  def initialize(mapping: DatabaseMapping, session: Session): Unit = {}

}

class DataTypeConverter extends AttributeConverter[DataType, String] with Converter {
  def convertToDatabaseColumn(objectValue: DataType): String = Try { objectValue.asInstanceOf[DataType].name() }.getOrElse("")
  def convertToEntityAttribute(dataValue: String): DataType = Try { DataType.valueOf(dataValue.toString) }.getOrElse(null)

  def convertObjectValueToDataValue(objectValue: Object, session: Session): Object = convertToDatabaseColumn(objectValue.asInstanceOf[DataType])
  def convertDataValueToObjectValue(dataValue: Object, session: Session): Object = convertToEntityAttribute(dataValue.toString)
  def isMutable: Boolean = false
  def initialize(mapping: DatabaseMapping, session: Session): Unit = {}

}