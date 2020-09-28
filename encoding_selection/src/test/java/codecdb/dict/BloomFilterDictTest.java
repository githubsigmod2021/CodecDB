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

import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BloomFilterDictTest {
    @Test
    public void testContainInExisting() {
        Random r = new Random(System.currentTimeMillis());

        int data[] = new int[1000000];
        for (int i = 0; i < data.length; i++) {
            data[i] = r.nextInt(data.length);
        }

        BloomFilterDict dict = new BloomFilterDict(data, 0.03);

//        assertTrue(dict.contain(data, 1,1));
    }

    @Test
    public void testFalsePositive() {
        Random r = new Random(System.currentTimeMillis());

        int data[] = new int[1000000];
        int test[] = new int[1000000];
        for (int i = 0; i < data.length; i++) {
            data[i] = r.nextInt(data.length);
            test[i] = r.nextInt(data.length);
        }

        BloomFilterDict dict = new BloomFilterDict(data, 0.03);

        assertFalse(dict.contain(test,1,1));
    }
}
