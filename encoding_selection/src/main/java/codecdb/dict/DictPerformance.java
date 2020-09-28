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

import codecdb.util.perf.ProfileBean;
import codecdb.util.perf.Profiler;
import codecdb.util.perf.ProfileBean;
import codecdb.util.perf.Profiler;

import java.text.MessageFormat;
import java.util.Random;

public class DictPerformance {

    static double p = 0.01;
    static int dataSize = 10000000;

    static Random rand = new Random(System.currentTimeMillis());
    static int data[] = new int[dataSize];

    static Dict dict;

    static int repeat = 5;

    static double sampling = 0.1;
    static double threshold = 0.7;

    static {
        // Fill data
        for (int i = 0; i < data.length; i++) {
            data[i] = rand.nextInt(Integer.MAX_VALUE);
        }
        dict = new HashDict(data, p);
    }

    public static void main(String[] args) {
        int step = 1000000;
        double[] sel = new double[]{0, 0.25, 0.5, 0.75, 0.9, 0.99, 0.999999, 1};

        for (int j = 0; j < sel.length; j++) {
            for (int i = 1; i <= 10; i++) {
                int testSize = step * i;
                runDict(testSize, sel[j], j);
            }
        }
    }

    public static void runDict(int testSize, double selectivity, int selidx) {

        int test[] = new int[testSize];

        Profiler profiler = new Profiler();

        for (int r = 0; r < repeat; r++) {
            for (int i = 0; i < testSize; i++) {
                if (rand.nextDouble() <= selectivity) {
                    test[i] = data[i];
                } else {
                    test[i] = rand.nextInt(Integer.MAX_VALUE);
                }
            }

            profiler.mark();
            dict.contain(test, sampling, threshold);
            profiler.pause();
        }
        ProfileBean result = profiler.stop();

        System.out.println(MessageFormat.format("{0},{1},{2}", String.valueOf(testSize), String.valueOf(selidx), String.valueOf(result.cpu() / repeat)));
    }

}
