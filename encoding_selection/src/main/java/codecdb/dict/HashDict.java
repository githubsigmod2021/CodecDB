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

import java.util.HashSet;
import java.util.Set;

public class HashDict implements Dict {

    Set<Integer> dict;
    double probability = 1;

    public HashDict(int[] data, double p) {
        dict = new HashSet<>();

        for (int i : data) {
            dict.add(i);
        }
        this.probability = 1 - p;
    }

    public boolean contain(int[] data, double sampling, double threshold) {
        int thresholdNum = (int)(data.length * sampling * threshold);
        // Check p data in random, if all success then pass
//        int check = Math.min((int) Math.ceil(data.length * probability), data.length);
        int count = 0;
        for (int i = 0; i < data.length; i++) {
            if (dict.contains(data[i])) {
                count++;
                if(count > threshold)
                    return true;
            }
        }
        return false;
    }
}
