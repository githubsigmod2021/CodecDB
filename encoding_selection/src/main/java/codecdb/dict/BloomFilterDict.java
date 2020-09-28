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

import java.util.Random;

public class BloomFilterDict implements Dict {

    long a[];
    long b[];
    int slots[];
    // A Large Prime
    int prime = Integer.MAX_VALUE;
    int numHash = 0;
    int numSlot = 0;
    double probability = 1;

    public BloomFilterDict(int[] data, double p) {
        this.probability = p;
        double LN2 = Math.log(2);
        this.numSlot = (int) (-data.length * Math.log(p) / (LN2 * LN2));
        this.numHash = Math.max(1, (int) Math.round(numSlot / data.length * LN2));

        a = new long[numHash];
        b = new long[numHash];
        slots = new int[(int) Math.ceil(((double) numSlot) / 32)];
        Random rand = new Random(System.currentTimeMillis());
        for (int i = 0; i < numHash; i++) {
            a[i] = rand.nextInt(prime - 1) + 1;
            b[i] = rand.nextInt(prime);
        }
        // Create bloom filter
        for (int d : data) {
            for (int idx = 0; idx < numHash; idx++) {
                set(hash(d, idx));
            }
        }
    }

    @Override
    public boolean contain(int[] data, double sampling, double threshold) {
        int thresholdNum = (int) (data.length * sampling * threshold);
        int count = 0;
        for (int d : data) {
            if (contain(d)) {
                count++;
                if (count > thresholdNum)
                    return true;
            }
        }
        return false;
    }

    protected boolean contain(int data) {
        for (int idx = 0; idx < numHash; idx++) {
            if (!get(hash(data, idx))) {
                return false;
            }
        }
        return true;
    }

    protected int hash(int key, int index) {
        return (int) ((key * a[index] + b[index]) % prime);
    }

    protected boolean get(int offset) {
        int modular = offset % numSlot;
        int intpos = modular / 32;
        int intoff = modular % 32;
        return (slots[intpos] & (1 << intoff)) != 0;
    }

    protected void set(int offset) {
        int modular = offset % numSlot;
        int intpos = modular / 32;
        int intoff = modular % 32;
        slots[intpos] |= 1 << intoff;
    }
}
