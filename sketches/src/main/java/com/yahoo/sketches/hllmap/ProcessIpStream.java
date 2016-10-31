/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hllmap;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.InetAddress;

/**
 * Processes an input stream of IP-ID integer pairs from Standard-In into the UniqueCountMap.
 * Each pair of the input stream defines a single IP associated with a single ID.
 * The input stream may consist of many duplicate IPs and or duplicate IDs.
 * The pair is of the form:
 *
 * <p><code>&lt;IP&gt;&lt;TAB&gt;&lt;ID&lt;&lt;line-separator&gt;.</code></p>
 *
 * <p>The end of the stream is a null input line.</p>
 *
 * <p>At the end of the stream, UniqueCountMap.toString() is called and sent to Standard-Out.</p>
 *
 * <p>A typical command line might be as follows:</p>
 *
 * <p><code>cat IPTabIdPairs.txt | 
 * java -cp hllmap.jar:sketches-core-0.8.2-SNAPSHOT-with-shaded-memory.jar com.yahoo.sketches.hllmap.ProcessIpStream</code></p>
 */
public class ProcessIpStream {

  private ProcessIpStream() {}

  /**
   * Main entry point.
   * @param args optional initial number of entries in the base table
   * @throws RuntimeException Generally an IOException.
   */
  public static void main(String[] args) throws RuntimeException {
    int initialNumEntries = 1000;
    if (args.length > 0) {
      initialNumEntries = Integer.parseInt(args[0]);
    }
    processIpStream(initialNumEntries);
  }

  private static void processIpStream(final int initialNumEntries) {
    String line = "";
    UniqueCountMap map = new UniqueCountMap(initialNumEntries, 4, 1024);
    long count = 0;
    long updateTime_nS = 0;
    try (BufferedReader br = new BufferedReader(new InputStreamReader(System.in))) {
      while ((line = br.readLine()) != null) {
        String[] tokens = line.split("\t");
        checkLen(tokens);
        byte[] iAddBytes = InetAddress.getByName(tokens[0]).getAddress();
        byte[] valBytes = tokens[1].getBytes();
        long start_nS = System.nanoTime();
        map.update(iAddBytes, valBytes);
        long end_nS = System.nanoTime();
        updateTime_nS += end_nS - start_nS;
        count++;
      }
      println(map.toString());
      println("Lines Read: " + count);
      println("nS Per update: " + ((double)updateTime_nS / count));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  static final byte[] intToBytes(int v, byte[] arr) {
    for (int i = 0; i < 4; i++) {
      arr[i] = (byte) (v & 0XFF);
      v >>>= 8;
    }
    return arr;
  }

  static final byte[] longToBytes(long v, byte[] arr) {
    for (int i = 0; i < 8; i++) {
      arr[i] = (byte) (v & 0XFFL);
      v >>>= 8;
    }
    return arr;
  }

  private static final void checkLen(String[] tokens) {
    int len = tokens.length;
    if (len != 2) throw new IllegalArgumentException("Args.length must be 2: " + len);
  }

  private static void println(String s) { System.out.println(s); }

}
