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

package codecdb.hadoop;

import jdk.nashorn.internal.runtime.arrays.ArrayIndex;
import sun.misc.Unsafe;

import java.io.IOException;
import java.io.InputStream;

class DirectByteArray {

    Unsafe unsafe = Unsafe.getUnsafe();

    private final long startIndex;

    private long size;

    public DirectByteArray(long size) {
        this.size = size;
        startIndex = unsafe.allocateMemory(size);
        unsafe.setMemory(startIndex, size, (byte) 0);
    }

    public void set(long index, byte value) {
        if (index >= size)
            throw new ArrayIndexOutOfBoundsException();
        unsafe.putByte(index, value);
    }

    public int get(long index) {
        if (index >= size)
            throw new ArrayIndexOutOfBoundsException();
        return unsafe.getByte(index);
    }

    public void destroy() {
        unsafe.freeMemory(startIndex);
    }

    public int copy(long index, byte[] buffer, int offset, int length) {
        int copylen = length;
        if (index + length >= size)
            copylen = (int) (size - 1 - index);
        unsafe.copyMemory(null, startIndex + index, buffer, Unsafe.ARRAY_BYTE_BASE_OFFSET + offset, copylen);
        return copylen;
    }

    public long size() {
        return size;
    }

    public void from(InputStream from, long size) throws IOException {
        if(size>this.size)
            throw new ArrayIndexOutOfBoundsException();
        byte[] buffer = new byte[1000000];
        long position = 0;
        int readcount = 0;
        while((readcount = from.read(buffer))>0) {
            copy(position, buffer,0,readcount);
        }
    }
}
