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

package codecdb.query.bitmap;

import java.util.function.LongConsumer;

public class TrivialBitmap implements Bitmap {

    private long length;

    private long[] data;

    public TrivialBitmap(long length) {
        this.length = length;
        this.data = new long[(int) Math.ceil(((double) length) / 64)];
    }

    @Override
    public void set(long index, boolean value) {
        int li = (int) (index / 64);
        int offset = (int) (index % 64);

        data[li] = data[li] | (1L << offset);
    }

    @Override
    public boolean test(long index) {
        int li = (int) (index / 64);
        int offset = (int) (index % 64);
        return (data[li] & (1L << offset)) != 0;
    }

    @Override
    public void foreach(LongConsumer consumer) {
        for (long i = 0; i < length; i++) {
            if (test(i)) {
                consumer.accept(i);
            }
        }
    }

    @Override
    public Bitmap and(Bitmap another) {
        if (another instanceof TrivialBitmap) {
            TrivialBitmap tba = (TrivialBitmap) another;
            for (int i = 0; i < data.length; i++) {
                data[i] &= tba.data[i];
            }
            return this;
        }
        throw new UnsupportedOperationException();
    }

    @Override
    public Bitmap or(Bitmap another) {
        if (another instanceof TrivialBitmap) {
            TrivialBitmap tba = (TrivialBitmap) another;
            for (int i = 0; i < data.length; i++) {
                data[i] |= tba.data[i];
            }
            return this;
        }
        throw new UnsupportedOperationException();
    }
}
