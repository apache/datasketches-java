/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import static com.yahoo.sketches.TestingUtil.milliSecToString;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.InetAddress;

/**
 * Processes an input stream of IP-ID integer pairs from Standard-In into the UniqueCountMap.
 * Each pair of the input stream defines a single IP associated with a single ID.
 * The input stream may consist of many duplicate IPs and or duplicate IDs.
 * The pair is of the form:
 *
 * <p><code>&lt;IP&gt;&lt;TAB&gt;&lt;ID&gt;&lt;line-separator&gt;.</code></p>
 *
 * <p>The end of the stream is a null input line.</p>
 *
 * <p>At the end of the stream, UniqueCountMap.toString() is called and sent to Standard-Out.</p>
 *
 * <p>To run, create a jar of the test code for sketches-core.
 * A typical command line might be as follows:</p>
 *
 * <p><code>cat IPTabIdPairs.txt | java -cp sketches-core-test.jar:\
 * sketches-core-0.8.2-SNAPSHOT.jar: \
 * memory-0.8.2-SNAPSHOT.jar \
 * com.yahoo.sketches.hll.ProcessIpStream</code></p>
 */
public class ProcessIpStream {
  static final String LS = System.getProperty("line.separator");
  private static final int IP_BYTES = 4;
  private static final int INIT_ENTRIES = 1000;

  private ProcessIpStream() {}

  /**
   * Main entry point.
   * @param args optional initial number of entries in the base table
   * @throws RuntimeException Generally an IOException.
   */
  public static void main(String[] args) throws RuntimeException {
    int initialNumEntries = INIT_ENTRIES;
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
    long lineCount = 0;
    long updateCount = 0;

    UniqueCountMap map = new UniqueCountMap(initialNumEntries, IP_BYTES);
    long updateTime_nS = 0;
    try (BufferedReader br = new BufferedReader(new InputStreamReader(System.in, UTF_8))) {

      while ((line = br.readLine()) != null) {
        String[] tokens = line.split("\t");
        checkLen(tokens);
        lineCount++;
        byte[] iAddBytes = InetAddress.getByName(tokens[0]).getAddress();
        byte[] valBytes = tokens[1].getBytes(UTF_8);
        long start_nS = System.nanoTime();
        map.update(iAddBytes, valBytes);
        long end_nS = System.nanoTime();
        updateTime_nS += end_nS - start_nS;
      }
      int ipCount = map.getActiveEntries();
      updateCount = lineCount;

      String className = this.getClass().getSimpleName();
      printStats(sb, className, map, lineCount, ipCount, updateCount, updateTime_nS);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    long total_mS = System.currentTimeMillis() - start_mS;
    printTaskTime(sb, total_mS, updateCount);
  }

  static void printStats(StringBuilder sb, String className, UniqueCountMap map,
      long lineCount, int ipCount, long updateCount, long updateTime_nS) {
    sb.append("# ").append(className).append(" SUMMARY: ").append(LS);
    sb.append(map.toString()).append(LS);
    sb.append("  Lines Read                : ").append(String.format("%,d", lineCount)).append(LS);
    sb.append("  IP Count                  : ").append(String.format("%,d",ipCount)).append(LS);
    sb.append("  Update Count              : ").append(String.format("%,d",updateCount)).append(LS);
    sb.append("  nS Per update             : ")
        .append(String.format("%,.3f", ((updateTime_nS * 1.0) / updateCount))).append(LS);
  }

  static void printTaskTime(StringBuilder sb, long total_mS, long updateCount) {
    sb.append("  Total Task Time           : ").append(milliSecToString(total_mS)).append(LS);
    sb.append("  Task nS Per Update        : ")
        .append(String.format("%,.3f", ((total_mS * 1E6) / updateCount))).append(LS);
    sb.append("# END PROCESS SUMMARY").append(LS);
    println(sb.toString());
  }

  static final void checkLen(String[] tokens) {
    int len = tokens.length;
    if (len != 2) throw new IllegalArgumentException("Args.length must be 2: " + len);
  }

  static void println(String s) { System.out.println(s); }
}
