/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hllmap;

import static com.yahoo.sketches.Util.zeroPad;

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
 * java -cp hllmap.jar:sketches-core-0.8.2-SNAPSHOT-with-shaded-memory.jar
 * com.yahoo.sketches.hllmap.ProcessIpStream</code></p>
 */
public class ProcessIpStream {
  private static final String LS = System.getProperty("line.separator");

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
    ProcessIpStream pips = new ProcessIpStream();
    pips.processIpStream(initialNumEntries);
  }

  private void processIpStream(final int initialNumEntries) {
    StringBuilder sb = new StringBuilder();
    long start_mS = System.currentTimeMillis();
    String line = "";
    long lineCount = 0; // = update count
    UniqueCountMap map = new UniqueCountMap(initialNumEntries, 4, 1024);
    long updateTime_nS = 0;
    try (BufferedReader br = new BufferedReader(new InputStreamReader(System.in))) {
      while ((line = br.readLine()) != null) {
        String[] tokens = line.split("\t");
        checkLen(tokens);
        lineCount++;
        byte[] iAddBytes = InetAddress.getByName(tokens[0]).getAddress();
        byte[] valBytes = tokens[1].getBytes();
        long start_nS = System.nanoTime();
        map.update(iAddBytes, valBytes);
        long end_nS = System.nanoTime();
        updateTime_nS += end_nS - start_nS;

      }
      String thisSimpleName = this.getClass().getSimpleName();
      sb.append("# ").append(thisSimpleName).append(" SUMMARY: ").append(LS);
      sb.append(map.toString()).append(LS);
      sb.append("  Lines Read                : ").append(String.format("%,d", lineCount)).append(LS);
      int ipCount = map.getActiveEntries();
      sb.append("  IP Count                  : ").append(String.format("%,d",ipCount)).append(LS);
      sb.append("  Update / Line Count       : ").append(String.format("%,d",lineCount)).append(LS);
      sb.append("  nS Per update             : ")
          .append(String.format("%,.3f", ((updateTime_nS * 1.0) / lineCount))).append(LS);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    long total_mS = System.currentTimeMillis() - start_mS;
    sb.append("  Total Task Time           : ").append(milliSecToString(total_mS)).append(LS);
    sb.append("  Task nS Per Update        : ")
        .append(String.format("%,.3f", ((total_mS * 1E6) / lineCount))).append(LS);
    sb.append("# END PROCESS SUMMARY").append(LS);
    println(sb.toString());
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

  /**
   * Returns the given time in milliseconds formatted as Hours:Min:Sec.mSec
   * @param mS the given nanoseconds
   * @return the given time in milliseconds formatted as Hours:Min:Sec.mSec
   */
  //temporarily copied from SNAPSHOT com.yahoo.sketches.TestingUtil (test branch)
  public static String milliSecToString(long mS) {
    long rem_mS = (long)(mS % 1000.0);
    long rem_sec = (long)((mS / 1000.0) % 60.0);
    long rem_min = (long)((mS / 60000.0) % 60.0);
    long hr  =     (long)(mS / 3600000.0);
    String mSstr = zeroPad(Long.toString(rem_mS), 3);
    String secStr = zeroPad(Long.toString(rem_sec), 2);
    String minStr = zeroPad(Long.toString(rem_min), 2);
    return String.format("%d:%2s:%2s.%3s", hr, minStr, secStr, mSstr);
  }

  private static void println(String s) { System.out.println(s); }

}
