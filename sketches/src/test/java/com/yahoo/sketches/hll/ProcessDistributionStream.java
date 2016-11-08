/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import static com.yahoo.sketches.TestingUtil.milliSecToString;

import java.io.BufferedReader;
import java.io.InputStreamReader;

/**
 * Processes an input stream of pairs of integers from Standard-In into the UniqueCountMap.
 * The input stream defines a distribution whereby each pair defines the number of keys with the
 * corresponding number of unique IDs. Each pair is of the form:
 *
 * <p><code>&lt;NumIDs&gt;&lt;TAB&gt;&lt;NumKeys&gt;&lt;line-separator&gt;.</code></p>
 *
 * <p>For each input pair, this model generates <i>NumIDs</i> unique identifiers for each of
 * <i>NumKeys</i> (also unique) and inputs them into the UniqueCountMap.</p>
 *
 * <p>The end of the stream is a null input line.</p>
 *
 * <p>At the end of the stream, UniqueCountMap.toString() is called and sent to Standard-Out.</p>
 *
 * <p>A typical command line might be as follows:</p>
 *
 * <p><code>
 * cat NumIDsTabNumKeys.txt | java -cp sketches-core-0.8.2-SNAPSHOT-with-shaded-memory.jar \
 * com.yahoo.sketches.hllmap.ProcessDistributionStream
 * </code></p>
 */
public class ProcessDistributionStream {
  private static final String LS = System.getProperty("line.separator");
  private static final int HLL_K = 1024;
  private static final int IP_BYTES = 4;
  private static final int INIT_ENTRIES = 1000;

  private ProcessDistributionStream() {}

  /**
   * Main entry point.
   * @param args Not used.
   * @throws RuntimeException Generally an IOException.
   */
  public static void main(String[] args) throws RuntimeException {
    ProcessDistributionStream pds = new ProcessDistributionStream();
    pds.processDistributionModel();
  }

  private void processDistributionModel() {
    StringBuilder sb = new StringBuilder();
    long start_mS = System.currentTimeMillis();
    String line = "";
    long lineCount = 0;
    long updateCount = 0;
    int ipCount = 0;
    byte[] ipBytes = new byte[IP_BYTES];
    byte[] valBytes = new byte[Long.BYTES];

    UniqueCountMap map = new UniqueCountMap(INIT_ENTRIES, IP_BYTES, HLL_K);
    long updateTime_nS = 0;
    try (BufferedReader br = new BufferedReader(new InputStreamReader(System.in))) {

      while ((line = br.readLine()) != null) {
        String[] tokens = line.split("\t");
        checkLen(tokens);
        lineCount++;
        long numValues = Long.parseLong(tokens[0]); //Verify the token order!
        long numIps = Long.parseLong(tokens[1]);

        for (long nips = 0; nips < numIps; nips++) {
          ipCount++;
          MapTestingUtil.intToBytes(ipCount, ipBytes);
          for (long vals = 0; vals < numValues; vals++) {
            long start_nS = System.nanoTime();
            updateCount++; // never repeated for any ip
            map.update(ipBytes, MapTestingUtil.longToBytes(updateCount, valBytes));
            long end_nS = System.nanoTime();
            updateTime_nS += end_nS - start_nS;
          }
        }
      }

      String className = this.getClass().getSimpleName();
      printStats(sb, className, map, lineCount, ipCount, updateCount, updateTime_nS);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    long total_mS = System.currentTimeMillis() - start_mS;
    printTaskTime(sb, total_mS, updateCount);
  }

  private static void printStats(StringBuilder sb, String className, UniqueCountMap map,
      long lineCount, int ipCount, long updateCount, long updateTime_nS) {
    sb.append("# ").append(className).append(" SUMMARY: ").append(LS);
    sb.append(map.toString()).append(LS);
    sb.append("  Lines Read                : ").append(String.format("%,d", lineCount)).append(LS);
    sb.append("  IP Count                  : ").append(String.format("%,d",ipCount)).append(LS);
    sb.append("  Update Count              : ").append(String.format("%,d",updateCount)).append(LS);
    sb.append("  nS Per update             : ")
        .append(String.format("%,.3f", ((updateTime_nS * 1.0) / updateCount))).append(LS);
  }

  private static void printTaskTime(StringBuilder sb, long total_mS, long updateCount) {
    sb.append("  Total Task Time           : ").append(milliSecToString(total_mS)).append(LS);
    sb.append("  Task nS Per Update        : ")
        .append(String.format("%,.3f", ((total_mS * 1E6) / updateCount))).append(LS);
    sb.append("# END PROCESS SUMMARY").append(LS);
    println(sb.toString());
  }

  private static final void checkLen(String[] tokens) {
    int len = tokens.length;
    if (len != 2) throw new IllegalArgumentException("Args.length must be 2: " + len);
  }

  static void println(String s) { System.out.println(s); }

}
