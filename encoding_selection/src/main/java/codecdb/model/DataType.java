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

package codecdb.model;

import org.apache.commons.lang.StringUtils;

import java.util.Comparator;

/**
 * @author Cathy
 */
public enum DataType {
    INTEGER, LONG, FLOAT, DOUBLE, STRING, BOOLEAN;

    public boolean check(String input) {
        input = input.trim();
        // Do not check empty
        if (StringUtils.isEmpty(input))
            return true;
        try {
            switch (this) {
                case INTEGER:
                    Integer.parseInt(input);
                    break;
                case LONG:
                    Long.parseLong(input);
                    break;
                case FLOAT:
                    Float.parseFloat(input);
                    break;
                case DOUBLE:
                    Double.parseDouble(input);
                    break;
                case STRING:
                    break;
                case BOOLEAN:
                    Boolean.parseBoolean(input);
                    break;
            }
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    public Comparator<String> comparator() {
        switch (this) {
            case INTEGER:
                return (String a, String b) -> {
                    String at = a.trim();
                    String bt = b.trim();
                    if (StringUtils.isEmpty(at)) {
                        return StringUtils.isEmpty(bt) ? 0 : -1;
                    }
                    if (StringUtils.isEmpty(bt)) {
                        return StringUtils.isEmpty(at) ? 0 : 1;
                    }
                    return Integer.compare(Integer.parseInt(at), Integer.parseInt(bt));
                };
            case LONG:
                return (String a, String b) -> {
                    String at = a.trim();
                    String bt = b.trim();
                    if (StringUtils.isEmpty(at)) {
                        return StringUtils.isEmpty(bt) ? 0 : -1;
                    }
                    if (StringUtils.isEmpty(bt)) {
                        return StringUtils.isEmpty(at) ? 0 : 1;
                    }
                    return Long.compare(Long.parseLong(at), Long.parseLong(bt));
                };
            case FLOAT:
                return (String a, String b) -> {
                    String at = a.trim();
                    String bt = b.trim();
                    if (StringUtils.isEmpty(at)) {
                        return StringUtils.isEmpty(bt) ? 0 : -1;
                    }
                    if (StringUtils.isEmpty(bt)) {
                        return StringUtils.isEmpty(at) ? 0 : 1;
                    }
                    return Float.compare(Float.parseFloat(at), Float.parseFloat(bt));
                };
            case DOUBLE:
                return (String a, String b) -> {
                    String at = a.trim();
                    String bt = b.trim();
                    if (StringUtils.isEmpty(at)) {
                        return StringUtils.isEmpty(bt) ? 0 : -1;
                    }
                    if (StringUtils.isEmpty(bt)) {
                        return StringUtils.isEmpty(at) ? 0 : 1;
                    }
                    return Double.compare(Double.parseDouble(at), Double.parseDouble(bt));
                };
            case STRING:
                return Comparator.naturalOrder();
            case BOOLEAN:
                return Comparator.naturalOrder();
        }
        throw new IllegalArgumentException();
    }
}
