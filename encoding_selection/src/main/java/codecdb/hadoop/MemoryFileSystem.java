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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.apache.xmlbeans.impl.xb.ltgfmt.FileDesc;

import java.io.EOFException;
import java.io.FileDescriptor;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * Keep file in memory
 */
public class MemoryFileSystem extends FileSystem {

    static final URI NAME = URI.create("memory:///");

    private RawLocalFileSystem inner = new RawLocalFileSystem();

    private Map<Path, DirectByteArray> files = new HashMap<>();

    private Map<Path, FileDescriptor> fds = new HashMap<>();

    private Map<Path, FileStatus> statuses = new HashMap<>();

    public MemoryFileSystem() {
    }

    @Override
    public void initialize(URI uri, Configuration conf) throws IOException {
        super.initialize(uri, conf);
        setConf(conf);
    }


    @Override
    public URI getUri() {
        return NAME;
    }

    class MemoryFSDataInputStream extends FSInputStream implements HasFileDescriptor {

        private FileDescriptor fd;
        private DirectByteArray content;
        private long position;

        public MemoryFSDataInputStream(FileDescriptor fd, DirectByteArray data) throws IOException {
            this.fd = fd;
            this.content = data;
        }

        @Override
        public void seek(long pos) throws IOException {
            if (pos < 0) {
                throw new EOFException(
                        FSExceptionMessages.NEGATIVE_SEEK);
            }
            this.position = pos;
        }

        @Override
        public long getPos() throws IOException {
            return this.position;
        }

        @Override
        public boolean seekToNewSource(long targetPos) throws IOException {
            return false;
        }

        @Override
        public int available() throws IOException {
            return (int) (content.size() - position);
        }

        @Override
        public void close() throws IOException {
        }

        @Override
        public boolean markSupported() {
            return false;
        }

        @Override
        public int read() throws IOException {
            int value = content.get(position);
            if (value >= 0) {
                this.position++;
                statistics.incrementBytesRead(1);
            }
            return value;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            // parameter check
            validatePositionedReadArgs(position, b, off, len);
            int value = content.copy(position, b, off, len);
            if (value > 0) {
                this.position += value;
                statistics.incrementBytesRead(value);
            }
            return value;
        }

        @Override
        public int read(long position, byte[] b, int off, int len)
                throws IOException {
            // parameter check
            validatePositionedReadArgs(position, b, off, len);
            if (len == 0) {
                return 0;
            }
            int value = content.copy(position, b, off, len);
            if (value > 0) {
                statistics.incrementBytesRead(value);
            }
            return value;
        }

        @Override
        public long skip(long n) throws IOException {
            long value = position + n;
            if (value >= content.size()) {
                value = content.size() - 1 - position;
            }
            this.position += value;
            return value;
        }

        @Override
        public FileDescriptor getFileDescriptor() throws IOException {
            return fd;
        }
    }

    @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
        getFileStatus(f);

        DirectByteArray content = files.get(f);
        FileDescriptor fd = fds.get(f);
        return new FSDataInputStream(new BufferedFSInputStream(
                new MemoryFSDataInputStream(fd,content), bufferSize));
    }

    @Override
    public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
        return null;
    }

    @Override
    public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
        return null;
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        return false;
    }

    @Override
    public boolean delete(Path f, boolean recursive) throws IOException {
        return false;
    }

    /**
     * Does not support directory for now
     * @param f
     * @return
     * @throws FileNotFoundException
     * @throws IOException
     */
    @Override
    public FileStatus[] listStatus(Path f) throws FileNotFoundException, IOException {
        FileStatus fs = getFileStatus(f);
        if(fs == null) {
            throw new FileNotFoundException("File " + f + " does not exist");
        }
        return new FileStatus[]{fs};
    }

    @Override
    public void setWorkingDirectory(Path new_dir) {

    }

    @Override
    public Path getWorkingDirectory() {
        return null;
    }

    @Override
    public boolean mkdirs(Path f, FsPermission permission) throws IOException {
        return false;
    }

    private void loadFromLocal(Path memoryPath) throws IOException, URISyntaxException {
        URI memoryUri = memoryPath.toUri();
        URI fileUri = new URI("file://"+memoryUri.getPath());
        Path localPath = new Path(fileUri);
        if(inner.exists(localPath)) {
            FileStatus fs = inner.getFileStatus(localPath);

            statuses.put(memoryPath, fs);
            // Load the file into memory
            DirectByteArray content = new DirectByteArray(fs.getLen());

            try(FSDataInputStream readStream = inner.open(localPath)) {
                content.from(readStream, fs.getLen());
               fds.put(memoryPath,readStream.getFileDescriptor());
            }

            files.put(memoryPath, content);
        }
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
        if(statuses.containsKey(f)) {
            return statuses.get(f);
        }
        // Check file system to load the file
        try {
            loadFromLocal(f);
        } catch (URISyntaxException e) {
            throw new RuntimeException();
        }
        FileStatus fs = statuses.get(f);
        if(fs == null) {
            throw new FileNotFoundException();
        }
        return fs;
    }
}
