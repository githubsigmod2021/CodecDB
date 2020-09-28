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

import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.util.function.LongConsumer;

public class RoaringBitmap implements Bitmap {

    Roaring64NavigableMap roaring = new Roaring64NavigableMap();

    @Override
    public void set(long index, boolean value) {
        if (value)
            roaring.add(index);
    }

    @Override
    public boolean test(long index) {
        return roaring.contains(index);
    }

    @Override
    public void foreach(LongConsumer consumer) {
        roaring.forEach(new RoaringLongConsumer(consumer));
    }

    @Override
    public Bitmap and(Bitmap another) {
        if (another instanceof RoaringBitmap) {
            RoaringBitmap rb = (RoaringBitmap) another;
            this.roaring.and(rb.roaring);
            return this;
        }
        throw new UnsupportedOperationException();
    }

    @Override
    public Bitmap or(Bitmap another) {
        if (another instanceof Roaring64NavigableMap) {
            RoaringBitmap rb = (RoaringBitmap) another;
            this.roaring.or(rb.roaring);
            return this;
        }
        throw new UnsupportedOperationException();
    }

    protected static final class RoaringLongConsumer implements org.roaringbitmap.longlong.LongConsumer {

        private LongConsumer delegate;

        public RoaringLongConsumer(LongConsumer lc) {
            this.delegate = lc;
        }

        @Override
        public void accept(long value) {
            delegate.accept(value);
        }
    }
}