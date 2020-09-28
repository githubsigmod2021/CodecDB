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

import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.channels.FileChannel;

import sun.nio.ch.FileChannelImpl;
import sun.misc.Unsafe;

@SuppressWarnings("restriction")
public class MMapper {

    private static final Unsafe unsafe;
    private static final Method mmap;
    private static final Method unmmap;
    private static final int BYTE_ARRAY_OFFSET;

    private long addr, size;
    private final String file;

    static {
        try {
//            Field singleoneInstanceField = Unsafe.class.getDeclaredField("theUnsafe");
//            singleoneInstanceField.setAccessible(true);
//            unsafe = (Unsafe) singleoneInstanceField.get(null);
            unsafe = Unsafe.getUnsafe();

            mmap = getMethod(FileChannelImpl.class, "map0", int.class, long.class, long.class);
            unmmap = getMethod(FileChannelImpl.class, "unmap0", long.class, long.class);

            BYTE_ARRAY_OFFSET = unsafe.ARRAY_BYTE_BASE_OFFSET;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    //Bundle reflection calls to get access to the given method
    private static Method getMethod(Class<?> cls, String name, Class<?>... params) throws Exception {
        Method m = cls.getDeclaredMethod(name, params);
        m.setAccessible(true);
        return m;
    }

    //Round to next 4096 bytes
    private static long roundTo4096(long i) {
        return (i + 0xfffL) & ~0xfffL;
    }

    //Given that the location and size have been set, map that location
    //for the given length and set this.addr to the returned offset
    private void mapAndSetOffset() throws Exception {
        final RandomAccessFile backingFile = new RandomAccessFile(this.file, "rw");
        backingFile.setLength(this.size);

        final FileChannel ch = backingFile.getChannel();
        this.addr = (long) mmap.invoke(ch, 1, 0L, this.size);

        ch.close();
        backingFile.close();
    }

    public MMapper(final String file, long len) throws Exception {
        this.file = file;
        this.size = roundTo4096(len);
        mapAndSetOffset();
    }

    //Callers should synchronize to avoid calls in the middle of this, but
    //it is undesirable to synchronize w/ all access methods.
    public void remap(long nLen) throws Exception {
        unmmap.invoke(null, addr, this.size);
        this.size = roundTo4096(nLen);
        mapAndSetOffset();
    }

    public byte read(long pos) {
        return unsafe.getByte(pos + addr);
    }

    //May want to have offset & length within data as well, for both of these
    public void getBytes(long pos, byte[] data) {
        unsafe.copyMemory(null, pos + addr, data, BYTE_ARRAY_OFFSET, data.length);
    }

    public void setBytes(long pos, byte[] data) {
        unsafe.copyMemory(data, BYTE_ARRAY_OFFSET, null, pos + addr, data.length);
    }
}
