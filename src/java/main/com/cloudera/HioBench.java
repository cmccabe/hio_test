/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera;

import java.io.InputStream;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.lang.Thread;
import java.lang.System;
import java.net.URI;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.StringTokenizer;

import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * This benchmark tests I/O in HDFS.
 */
public class HioBench { //extends Configured {
  private static void usage(int retval) {
    System.err.println(
        "HioBench: tests random HDFS I/O.\n" +
        "\n" +
        "Java system properties to set:\n" +
        "hio.nthreads [number-of-threads]   Number of simultaneous threads\n" +
        "hio.ngigs.to.read [gigabytes]      Number of gigabytes to read in each thread\n" +
        "hio.nmegs.to.read [megs]           Number of megabytes to read in each thread\n" +
        "hio.read.chunk.bytes [bytes]       Number of bytes to read in each chunk (default 512)\n" +
        "hio.ngigs.in.file [gigabytes]      Number of gigabytes in the file to write\n" +
        "hio.hdfs.uri [uri]                 The HDFS URI to talk to.\n" +
        "hio.hdfs.file.name [name]          The name of the input file to use.\n" +
        "                                   If the file already exists, we will use it\n" +
        "                                   rather than rewriting it.\n" +
        "dump.conf                          If set, we will dump out our\n" +
        "                                   configuration to stdout when\n" +
        "                                   starting up.\n" +
        "\n" +
        "A few notes about configuration:\n" +
        "If you want to be sure that your reads hit the disk, you need to set\n" +
        "hio.ngigs.in.file to something much higher than the available memory size.\n" +
        "Otherwise, you're mostly reading from the page cache, which may or may not\n" +
        "be what you want.\n" +
        "\n" +
        "The more threads you have, the more 'seeky' your workload will be, since\n" +
        "each thread independently seeks and reads.\n"
    );
    System.exit(retval);
  }

  static int getIntOrDie(String key) {
    String val = System.getProperty(key);
    if (val == null) {
      System.err.println("You must set the integer property " + key + "\n\n");
      usage(1);
    }
    return Integer.parseInt(val);
  }

  static int getIntWithDefault(String key, int defaultVal) {
    String val = System.getProperty(key);
    if (val == null) {
      return defaultVal;
    }
    return Integer.parseInt(val);
  }

  static String getStringOrDie(String key) {
    String val = System.getProperty(key);
    if (val == null) {
      System.err.println("You must set the string property " + key + "\n\n");
      usage(1);
    }
    return val;
  }

  static String getStringWithDefault(String key, String defaultVal) {
    String val = System.getProperty(key);
    if (val == null) {
      return defaultVal;
    }
    return val;
  }

  static void fillArrayWithExpected(byte arr[], long off, int fillLen) {
    byte incr = (byte)(off & 0xff);
    for (int i = 0; i < fillLen; i++) {
      arr[i] = incr++;
    }
  }

  static void writeFile(FileSystem fs)
    throws IOException
  {
    FSDataOutputStream fos = fs.create(options.filePath, (short)1);
    byte arr[] = new byte[65536];
    try {
      for (long off = 0; off < options.nBytesInFile; ) {
        fillArrayWithExpected(arr, off, arr.length);
        long rem = options.nBytesInFile - off;
        int lim = rem > arr.length ? arr.length : (int)rem;
        fos.write(arr, 0, lim);
        off += lim;
      }
    } finally {
      fos.close();
    }
  }

  static private class Options {
    public final int nThreads;
    public final long nBytesToRead;
    public final int nReadChunkBytes;
    public final int nGigsInFile;
    public final long nBytesInFile;
    public final String hdfsUri;
    public final String filename;
    public final Path filePath;
    public final String testType;
    public final boolean dumpConf;

    public Options() {
      nThreads = getIntOrDie("hio.nthreads");
      long nGigsToRead = getIntWithDefault("hio.ngigs.to.read", 0);
      long nMegsToRead = getIntWithDefault("hio.nmegs.to.read", 0);
      if ((nGigsToRead != 0) && (nMegsToRead != 0)) {
        throw new RuntimeException("can't set both hio.ngigs.to.read and " +
            "hio.nmegs.to.read!");
      } else if (nGigsToRead != 0) {
        nBytesToRead = nGigsToRead * 1024L * 1024L * 1024L;
      } else if (nMegsToRead != 0) {
        nBytesToRead = nMegsToRead * 1024L * 1024L;
      } else {
        throw new RuntimeException("you must set either hio.ngigs.to.read or " +
            "hio.nmegs.to.read.");
      }
      nReadChunkBytes = getIntWithDefault("hio.read.chunk.bytes", 1048576);
      nGigsInFile = getIntOrDie("hio.ngigs.in.file");
      nBytesInFile = nGigsInFile * 1024L * 1024L * 1024L;
      hdfsUri = getStringOrDie("hio.hdfs.uri");
      filename = getStringWithDefault("hio.hdfs.file.name",
          "/hio_bench_test." + System.currentTimeMillis());
      dumpConf = (System.getProperty("dump.conf") != null);
      testType = getStringWithDefault("hio.hdfs.test.type", "random");
      filePath = new Path(filename);
    }
  };

  private static Options options;

  static private class WorkerThread extends Thread {
    private final boolean shouldPrint;
    private final FSDataInputStream fis;
    private Throwable exception = null;
    private final static int TRIES_BETWEEN_TIMECHECK = 20;
    private final static int MILISECONDS_BETWEEN_PRINT = 5000;
    private final BenchReader benchReader;

    interface BenchReader {
      void read(FSDataInputStream fis) throws IOException;
      void init(FSDataInputStream fis) throws IOException;
    }

    static class RandomSeekBenchReader implements BenchReader {
      final private byte expect[];
      final private byte got[];
      final private Options options;
      final private Random random = new Random(System.nanoTime());

      RandomSeekBenchReader(Options options) {
        this.expect = new byte[options.nReadChunkBytes];
        this.got = new byte[options.nReadChunkBytes];
        this.options = options;
      }

      public void init(FSDataInputStream fis) throws IOException {
      }

      public void read(FSDataInputStream fis) throws IOException {
        // Using modulo here isn't great, but it's good enough here.
        long off = random.nextLong();
        if (off < 0) off = -off;
        off %= (options.nBytesInFile - options.nReadChunkBytes - 1);
        fillArrayWithExpected(expect, off, expect.length);
        readFully(fis, off, got, 0, got.length);
        compareArrays(expect, got);
      }
    }

    static class SequentialBenchReader implements BenchReader {
      final private byte expect[];
      final private byte got[];
      private long off;
      final private Options options;

      SequentialBenchReader(Options options, int idx) {
        this.expect = new byte[options.nReadChunkBytes];
        this.got = new byte[options.nReadChunkBytes];

        /*
         * If there are 3 threads, put each one 1/3 through the file.
         * Example: if file length is 100, start threads at 0, 33, 66.
         *
         * 0----------------100
         * 0    33     66
         *
         * and so forth.
         */
        this.off = idx * options.nBytesInFile;
        this.off /= options.nThreads;

        this.options = options;
      }

      public void init(FSDataInputStream fis) throws IOException {
        fis.seek(this.off);
      }

      public void read(FSDataInputStream fis) throws IOException {
        if (off + expect.length >= options.nBytesInFile) {
          off = 0;
          fis.seek(off);
        }
        fillArrayWithExpected(expect, off, expect.length);
        readFully(fis, got, 0, got.length);
        compareArrays(expect, got);
        off += got.length;
      }
    }

    static BenchReader createBenchReader(Options options, int idx) {
      if (options.testType.equals("random")) {
        return new RandomSeekBenchReader(options);
      } else if (options.testType.equals("sequential")) {
        return new SequentialBenchReader(options, idx);
      } else {
        throw new RuntimeException("can't understand testType " + options.testType +
            ": valid values are 'random' and 'sequential'");
      }
    }

    public WorkerThread(boolean shouldPrint, FileSystem fs,
          BenchReader benchReader) throws IOException {
      this.shouldPrint = shouldPrint;
      this.fis = fs.open(options.filePath);
      this.benchReader = benchReader;
    }

    public static void readFully(FSDataInputStream in, byte buf[],
        int arrayOff, int len) throws IOException {
      while (len > 0) {
        int ret = in.read(buf, arrayOff, len);
        if (ret < 0) {
          throw new IOException( "Premature EOF from inputStream reading " +
              len + "bytes from " + options.filename);
        }
        len -= ret;
        arrayOff += ret;
      }
    }

    public static void readFully(FSDataInputStream in, long off, byte buf[],
        int arrayOff, int len) throws IOException {
      while (len > 0) {
        int ret = in.read(off + arrayOff, buf, arrayOff, len);
        if (ret < 0) {
          throw new IOException( "Premature EOF from inputStream reading " +
              len + "bytes from offset " + off + " in " + options.filename);
        }
        len -= ret;
        arrayOff += ret;
      }
    }

    public static void compareArrays(byte expect[], byte got[]) throws IOException {
      int bad = -1;
      for (int i = 0; i < options.nReadChunkBytes; i++) {
        if (got[i] != expect[i]) {
          bad = i;
          break;
        }
      }
      if (bad != -1) {
        throw new IOException("compareArrays: error on byte " + bad + ".\n" +
            "Expected: " + Hex.encodeHexString(expect) + "\n" +
            "Got:      " + Hex.encodeHexString(got) + "\n");
      }
    }

    public void run () {
      int checkTimeCounter = 0;
      long prevPrintTime = 0;
      try {
        long amtRead = 0;

        benchReader.init(fis);
        while (amtRead < options.nBytesToRead) {
          benchReader.read(fis);
          amtRead += options.nReadChunkBytes;

          if (shouldPrint) {
            if (checkTimeCounter++ == TRIES_BETWEEN_TIMECHECK) {
              long now = System.currentTimeMillis();
              if (now > prevPrintTime + MILISECONDS_BETWEEN_PRINT) {
                prevPrintTime = now;
                float percent = amtRead * 100;
                percent /= options.nBytesToRead;
                System.out.println("thread1: read amtRead = " + amtRead + " out of " +
                    options.nBytesToRead + " (" + percent + "%" + ")");
              }
              checkTimeCounter = 0;
            }
          }
        }
      } catch (Throwable t) {
        t.printStackTrace(System.err);
        exception = t;
      }
    }

    public Throwable getException() {
      return exception;
    }
  }

  static String prettyPrintByteSize(float size) {
    if (size < 1024) {
      return String.format("%f bytes", size);
    } else if (size < (1024 * 1024)) {
      return String.format("%f KBytes", size / 1024);
    } else if (size < (1024 * 1024 * 1024)) {
      return String.format("%f MBytes", size / (1024 * 1024));
    } else {
      return String.format("%f GBytes", size / (1024 * 1024 * 1024));
    }
  }

  public static void main(String[] args) throws Exception {
    options = new Options();
    final Configuration conf = new Configuration();
    if (options.dumpConf) {
      Configuration.dumpConfiguration(conf, new PrintWriter(System.out));
    }
    final FileSystem fs = FileSystem.get(new URI(options.hdfsUri), conf);

    if (!fs.exists(options.filePath)) {
      System.out.println("no file at " + options.filePath + "; writing " +
          "new file now with length " + options.nGigsInFile + " gigs...");
      writeFile(fs);
      System.out.println("done.");
    } else if (fs.getLength(options.filePath) != options.nBytesInFile) {
      System.out.println("existing file " + options.filename + " has length " +
        fs.getLength(options.filePath) + ", but we wanted length " +
        options.nBytesInFile + ".  Re-creating.");
      writeFile(fs);
      System.out.println("done.");
    } else {
      System.out.println("using existing file at " + options.filePath +
          " of length " + options.nGigsInFile + " gigs.");
    }

    long nanoStart = System.nanoTime();
    WorkerThread threads[] = new WorkerThread[options.nThreads];
    for (int i = 0; i < options.nThreads; i++) {
      threads[i] = new WorkerThread(i == 0, fs, WorkerThread.createBenchReader(options, i));
    }
    for (int i = 0; i < options.nThreads; i++) {
      threads[i].start();
    }
    for (int i = 0; i < options.nThreads; i++) {
      threads[i].join();
    }
    for (int i = 0; i < options.nThreads; i++) {
      Throwable t = threads[i].getException();
      if (t != null) {
        System.err.println("there were exceptions.  Aborting.");
        System.exit(1);
      }
    }
    long nanoEnd = System.nanoTime();
    fs.close();
    long totalIo = options.nThreads;
    totalIo *= options.nBytesToRead;
    float nanoDiff = nanoEnd - nanoStart;
    float seconds = nanoDiff / 1000000000;
    System.out.println(String.format("Using %d threads, read %s in %f seconds",
        options.nThreads, prettyPrintByteSize(totalIo), seconds));
    float rate = totalIo / seconds;
    System.out.println("Average rate was " + prettyPrintByteSize(rate) + "/s");
  }
}
