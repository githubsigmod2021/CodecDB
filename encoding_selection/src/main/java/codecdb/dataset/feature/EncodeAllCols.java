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
 *     Author - initial API and implementation
 *
 */

package codecdb.dataset.feature;

import codecdb.dataset.persist.jpa.ColumnWrapper;
import codecdb.dataset.persist.jpa.JPAPersistence;
import codecdb.parquet.ParquetWriterHelper;
import codecdb.dataset.persist.jpa.ColumnWrapper;
import codecdb.dataset.persist.jpa.JPAPersistence;
import codecdb.model.FloatEncoding;
import codecdb.model.IntEncoding;
import codecdb.model.LongEncoding;
import codecdb.model.StringEncoding;
import codecdb.parquet.ParquetWriterHelper;

import javax.persistence.EntityManager;
import java.util.List;


public class EncodeAllCols {

    public static void main(String[] args) throws Exception {
        final int start = (args.length >= 1) ? Integer.parseInt(args[0]) : 0;
        System.out.println(String.format("Start is %d", start));
        EntityManager em = JPAPersistence.emf().createEntityManager();
        List<ColumnWrapper> columns = em.createQuery("select c from Column c", ColumnWrapper.class).getResultList();

        for (ColumnWrapper col : columns) {
            System.out.println(col.id());
            if (col.id() >= start) {
                switch (col.dataType()) {
                    case STRING:
                        for (StringEncoding e : StringEncoding.values()) {
                            if (e.parquetEncoding() != null) {
                                try {
                                    ParquetWriterHelper.singleColumnString(col.colFile(), e);
                                } catch (IllegalArgumentException ex) {
                                }
                            }
                        }
                        break;
                    case LONG:
                        for (LongEncoding e : LongEncoding.values()) {
                            if (e.parquetEncoding() != null) {
                                try {
                                    ParquetWriterHelper.singleColumnLong(col.colFile(), e);
                                } catch (IllegalArgumentException ex) {
                                }
                            }
                        }
                        break;
                    case INTEGER:
                        for (IntEncoding e : IntEncoding.values()) {
                            if (e.parquetEncoding() != null) {
                                try {
                                    ParquetWriterHelper.singleColumnInt(col.colFile(), e);
                                } catch (IllegalArgumentException ex) {
                                }
                            }
                        }
                        break;
                    case FLOAT:
                        for (FloatEncoding e : FloatEncoding.values()) {
                            if (e.parquetEncoding() != null) {
                                try {
                                    ParquetWriterHelper.singleColumnFloat(col.colFile(), e);
                                } catch (IllegalArgumentException ex) {
                                }
                            }
                        }
                        break;
                    case DOUBLE:
                        for (FloatEncoding e : FloatEncoding.values()) {
                            if (e.parquetEncoding() != null) {
                                try {
                                    ParquetWriterHelper.singleColumnDouble(col.colFile(), e);
                                } catch (IllegalArgumentException ex) {
                                }
                            }
                        }
                        break;
                    case BOOLEAN:
                        break;
                }
            }
        }
    }
}
