/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.quantiles;

import static com.yahoo.sketches.Util.LS;
import static com.yahoo.sketches.quantiles.Util.computeRetainedItems;

import java.util.Arrays;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.SketchesArgumentException;

/**
 * Utilities that support the doubles quantiles algorithms.
 *
 * <p>This class contains a highly specialized sort called blockyTandemMergeSort().
 * It also contains methods that are used while building histograms and other common
 * functions.</p>
 *
 * @author Lee Rhodes
 */
final class DoublesUtil {

  private DoublesUtil() {}

  /**
   * Returns an on-heap copy of the given sketch
   * @param sketch the given sketch
   * @return a copy of the given sketch
   */
  static HeapDoublesSketch copyToHeap(final DoublesSketch sketch) {
    final HeapDoublesSketch qsCopy;
    qsCopy = HeapDoublesSketch.newInstance(sketch.getK());
    qsCopy.putN(sketch.getN());
    qsCopy.putMinValue(sketch.getMinValue());
    qsCopy.putMaxValue(sketch.getMaxValue());
    qsCopy.putBaseBufferCount(sketch.getBaseBufferCount());
    qsCopy.putBitPattern(sketch.getBitPattern());
    final double[] combBuf = sketch.getCombinedBuffer();
    qsCopy.putCombinedBuffer(Arrays.copyOf(combBuf, combBuf.length));
    qsCopy.putCombinedBufferItemCapacity(sketch.getCombinedBufferItemCapacity());
    return qsCopy;
  }

  /**
   * Checks the validity of the memory capacity assuming n, k and compact.
   * @param k the given value of k
   * @param n the given value of n
   * @param compact true if memory is in compact form
   * @param memCapBytes the memory capacity in bytes
   */
  static void checkMemCapacity(final int k, final long n, final boolean compact,
      final long memCapBytes) {
    final int metaPre = Family.QUANTILES.getMaxPreLongs() + 2;
    final int retainedItems = computeRetainedItems(k, n);
    final int reqBufBytes;
    if (compact) {
      reqBufBytes = (metaPre + retainedItems) << 3;
    } else { //not compact
      final int totLevels = Util.computeNumLevelsNeeded(k, n);
      reqBufBytes = (totLevels == 0)
          ? (metaPre + retainedItems) << 3
          : (metaPre + (2 + totLevels) * k) << 3;
    }
    if (memCapBytes < reqBufBytes) {
      throw new SketchesArgumentException("Possible corruption: Memory capacity too small: "
          + memCapBytes + " < " + reqBufBytes);
    }
  }

  /**
   * Check the validity of the given serialization version
   * @param serVer the given serialization version
   */
  static void checkDoublesSerVer(final int serVer) {
    final int max = DoublesSketch.DOUBLES_SER_VER;
    final int min = DoublesSketch.MIN_DOUBLES_SER_VER;
    if ((serVer > max) || (serVer < min)) {
      throw new SketchesArgumentException(
          "Possible corruption: Unsupported Serialization Version: " + serVer);
    }
  }

  static String toString(final boolean sketchSummary, final boolean dataDetail,
      final DoublesSketch sketch) {
    final StringBuilder sb = new StringBuilder();
    if (dataDetail) {
      sb.append(getDataDetail(sketch));
    }
    if (sketchSummary) {
      sb.append(getSummary(sketch));
    }
    return sb.toString();
  }

  static String getDataDetail(final DoublesSketch sketch) {
    final StringBuilder sb = new StringBuilder();
    final String thisSimpleName = sketch.getClass().getSimpleName();
    sb.append(LS).append("### Quantiles ").append(thisSimpleName).append(" DATA DETAIL: ")
      .append(LS);

    final int k = sketch.getK();
    final long n = sketch.getN();
    final int bbCount = sketch.getBaseBufferCount();
    final long bitPattern = sketch.getBitPattern();
    final double[] combBuf  = sketch.getCombinedBuffer();

    //output the base buffer

    sb.append("   BaseBuffer   : ");
    for (int i = 0; i < bbCount; i++) {
      sb.append(String.format("%10.1f", combBuf[i]));
    }
    sb.append(LS);

    //output all the levels
    final int combBufSize = combBuf.length;
    if (n >= 2 * k) {
      sb.append("   Valid | Level");
      for (int j = 2 * k; j < combBufSize; j++) { //output level data starting at 2K
        if (j % k == 0) { //start output of new level
          final int levelNum = j / k - 2;
          final String validLvl = ((1L << levelNum) & bitPattern) > 0 ? "    T  " : "    F  ";
          final String lvl = String.format("%5d", levelNum);
          sb.append(Util.LS).append("   ").append(validLvl).append(" ").append(lvl).append(": ");
        }
        sb.append(String.format("%10.1f", combBuf[j]));
      }
      sb.append(LS);
    }
    sb.append("### END DATA DETAIL").append(LS);
    return sb.toString();
  }

  static String getSummary(final DoublesSketch sketch) {
    final StringBuilder sb = new StringBuilder();
    final String thisSimpleName = sketch.getClass().getSimpleName();
    final int k = sketch.getK();
    final String kStr = String.format("%,d", k);
    final long n = sketch.getN();
    final String nStr = String.format("%,d", n);
    final String bbCntStr = String.format("%,d", sketch.getBaseBufferCount());
    final String combBufCapStr = String.format("%,d", sketch.getCombinedBufferItemCapacity());
    final long bitPattern = sketch.getBitPattern();
    final int neededLevels = Util.computeNumLevelsNeeded(k, n);
    final int totalLevels = Util.computeTotalLevels(bitPattern);
    final int validLevels = Util.computeValidLevels(bitPattern);
    final String retItemsStr = String.format("%,d", sketch.getRetainedItems());
    final String bytesStr = String.format("%,d", sketch.getStorageBytes());
    final double eps = Util.EpsilonFromK.getAdjustedEpsilon(k);
    final String epsPctStr = String.format("%.3f%%", eps * 100.0);

    sb.append(Util.LS).append("### Quantiles ").append(thisSimpleName).append(" SUMMARY: ")
      .append(LS);
    sb.append("   K                            : ").append(kStr).append(LS);
    sb.append("   N                            : ").append(nStr).append(LS);
    sb.append("   Levels (Needed, Total, Valid): ")
      .append(neededLevels + ", " + totalLevels + ", " + validLevels).append(LS);
    sb.append("   Level Bit Pattern            : ")
      .append(Long.toBinaryString(bitPattern)).append(LS);
    sb.append("   BaseBufferCount              : ").append(bbCntStr).append(LS);
    sb.append("   Combined Buffer Capacity     : ").append(combBufCapStr).append(LS);
    sb.append("   Retained Items               : ").append(retItemsStr).append(LS);
    sb.append("   Storage Bytes                : ").append(bytesStr).append(LS);
    sb.append("   Normalized Rank Error        : ").append(epsPctStr).append(LS);
    sb.append("   Min Value                    : ")
      .append(String.format("%,.3f", sketch.getMinValue())).append(LS);
    sb.append("   Max Value                    : ")
      .append(String.format("%,.3f", sketch.getMaxValue())).append(LS);
    sb.append("### END SKETCH SUMMARY").append(LS);
    return sb.toString();
  }

  static String printMemData(final Memory mem, final int k, final int n) {
    if (n == 0) { return ""; }
    final StringBuilder sb = new StringBuilder();
    sb.append(LS).append("### ").append("MEM DATA DETAIL:").append(LS);
    final String fmt1 = "%n%10.1f, ";
    final String fmt2 = "%10.1f, ";
    final int bbCount = Util.computeBaseBufferItems(k, n);
    final int ret = Util.computeRetainedItems(k, n);
    sb.append("BaseBuffer Data:");
    for (int i = 0; i < bbCount; i++) {
      final double d = mem.getDouble(32 + i * 8);
      if (i % k != 0) { sb.append(String.format(fmt2, d)); }
      else { sb.append(String.format(fmt1, d)); }
    }
    sb.append(LS + LS + "Level Data:");
    for (int i = 0; i < ret - bbCount; i++) {
      final double d = mem.getDouble(32 + i * 8 + bbCount * 8);
      if (i % k != 0) { sb.append(String.format(fmt2, d)); }
      else { sb.append(String.format(fmt1, d)); }
    }
    sb.append(LS + "### END DATA DETAIL").append(LS);
    return sb.toString();
  }

}
