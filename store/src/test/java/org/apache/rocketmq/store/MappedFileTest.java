/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * $Id: MappedFileTest.java 1831 2013-05-16 01:39:51Z vintagewang@apache.org $
 */
package org.apache.rocketmq.store;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import org.apache.rocketmq.common.UtilAll;
import org.junit.After;
import org.junit.Test;
import sun.nio.ch.DirectBuffer;

import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("all")
public class MappedFileTest {
    private final String storeMessage = "Once, there was a chance for me!";

    public static void main(String[] args) throws IOException {
        String dir = "D:\\Users\\jchen26\\Desktop\\test_project\\rocketmq-1\\store\\target\\unit_test_store\\MappedFileTest";
        String pathname = "000";
        ensureDirOK(dir);
        File file = new File(dir + "/" + pathname);
        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
        MappedByteBuffer map = randomAccessFile.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, 150);
        map.put("wo shi tain cai".getBytes());
        System.out.println("xxx");
        randomAccessFile.close();
        ((DirectBuffer)map).cleaner().clean();
        File file1 = new File("D:\\Users\\jchen26\\Desktop\\test_project\\rocketmq-1\\store\\target\\unit_test_store");
        UtilAll.deleteFile(file1);
    }

    @Test
    public void testBuffer() throws IOException {
        String dir = "target/unit_test_store/MappedFileTest";
        String pathname = "000";
        ensureDirOK(dir);
        File file = new File(dir + "/" + pathname);
        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
        MappedByteBuffer map = randomAccessFile.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, 150);
        map.put("wo shi tain cai".getBytes());
        System.out.println("xxx");
    }

    @SuppressWarnings("all")
    private static void ensureDirOK(final String dirName) {
        if (dirName != null) {
            File f = new File(dirName);
            if (!f.exists()) {
                boolean result = f.mkdirs();
            }
        }
    }

    @Test
    public void testSelectMappedBuffer() throws IOException {
        MappedFile mappedFile = new MappedFile("target/unit_test_store/MappedFileTest/000", 1024 * 64);
        boolean result = mappedFile.appendMessage(storeMessage.getBytes());
        assertThat(result).isTrue();

        SelectMappedBufferResult selectMappedBufferResult = mappedFile.selectMappedBuffer(0);
        byte[] data = new byte[storeMessage.length()];
        selectMappedBufferResult.getByteBuffer().get(data);
        String readString = new String(data);

        assertThat(readString).isEqualTo(storeMessage);

        mappedFile.shutdown(1000);
        assertThat(mappedFile.isAvailable()).isFalse();
        selectMappedBufferResult.release();
        assertThat(mappedFile.isCleanupOver()).isTrue();
        assertThat(mappedFile.destroy(1000)).isTrue();
    }

    @After
    public void destory() {
        System.out.println("ppp");
        File file = new File("target/unit_test_store");
        UtilAll.deleteFile(file);
        manuelDestory();
    }

    @Test
    public void manuelDestory() {
        File file = new File("target/unit_test_store");
        UtilAll.deleteFile(file);
    }
}
