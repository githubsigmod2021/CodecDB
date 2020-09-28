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
 *     Author - initial API and implementation
 */

package codecdb.dict;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;

public class GoogleBloomFilterDict implements Dict {

    private BloomFilter inner;

    public GoogleBloomFilterDict(int[] data, double p) {
        inner = BloomFilter.create(Funnels.integerFunnel(), data.length, p);
        for (int d : data)
            inner.put(d);
    }

    @Override
    public boolean contain(int[] data, double sampling, double threshold) {
        int thresholdNum = (int) (data.length * sampling * threshold);
        int count = 0;
        for (int d : data) {
            if (inner.mightContain(d)) {
                count++;
                if (count > thresholdNum)
                    return true;
            }
        }
        return false;
    }
}
