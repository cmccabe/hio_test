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

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.Thread;
import java.lang.System;
import java.net.URI;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.StringTokenizer;

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
        "hio.ngigs.in.file [gigabytes]      Number of gigabytes in the file to write\n" +
        "hio.hdfs.uri [uri]                 The HDFS URI to talk to.\n"
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

  static String getStringOrDie(String key) {
    String val = System.getProperty(key);
    if (val == null) {
      System.err.println("You must set the string property " + key + "\n\n");
      usage(1);
    }
    return val;
  }

  public static void main(String[] args) throws Exception {
    int nThreads = getIntOrDie("hio.nthreads");
    int nGigsToRead = getIntOrDie("hio.ngigs.to.read");
    int nGigsInFile = getIntOrDie("hio.ngigs.in.file");
    String hdfsUri = getStringOrDie("hio.hdfs.uri");

    System.err.println("exiting.");
  }
}
