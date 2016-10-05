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
  static HeapDoublesSketch copy(DoublesSketch sketch) {
    HeapDoublesSketch qsCopy;
    qsCopy = HeapDoublesSketch.newInstance(sketch.getK());
    qsCopy.n_ = sketch.getN();
    qsCopy.minValue_ = sketch.getMinValue();
    qsCopy.maxValue_ = sketch.getMaxValue();
    qsCopy.combinedBufferItemCapacity_ = sketch.getCombinedBufferItemCapacity();
    qsCopy.baseBufferCount_ = sketch.getBaseBufferCount();
    qsCopy.bitPattern_ = sketch.getBitPattern();
    double[] combBuf = sketch.getCombinedBuffer();
    qsCopy.combinedBuffer_ = Arrays.copyOf(combBuf, combBuf.length);
    return qsCopy;
  }

  /**
   * Checks the validity of the memory capacity assuming n, k and compact.
   * @param k the given value of k
   * @param n the given value of n
   * @param compact true if memory is in compact form
   * @param memCapBytes the memory capacity in bytes
   */
  static void checkMemCapacity(int k, long n, boolean compact, long memCapBytes) {
    int metaPre = Family.QUANTILES.getMaxPreLongs() + 2;
    int retainedItems = computeRetainedItems(k, n);
    int reqBufBytes;
    if (compact) {
      reqBufBytes = (metaPre + retainedItems) << 3;
    } else { //not compact
      int totLevels = Util.computeNumLevelsNeeded(k, n);
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
  static void checkDoublesSerVer(int serVer) {
    int max = DoublesSketch.DOUBLES_SER_VER;
    int min = DoublesSketch.MIN_DOUBLES_SER_VER;
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
    sb.append(LS).append("### ").append(thisSimpleName).append(" DATA DETAIL: ").append(LS);

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
    int combBufSize = combBuf.length;
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
    final long n = sketch.getN();
    final String nStr = String.format("%,d", n);
    final int bbCount = sketch.getBaseBufferCount();
    final long bitPattern = sketch.getBitPattern();
    final int totLevels = Util.computeNumLevelsNeeded(k, n);
    final int validLevels = Util.computeValidLevels(bitPattern);
    final boolean empty = sketch.isEmpty();
    final int preBytes = empty ? Long.BYTES : 2 * Long.BYTES;
    final int retItems = sketch.getRetainedItems();
    final String retItemsStr = String.format("%,d", retItems);
    final int bytes = preBytes + (retItems + 2) * Double.BYTES;
    final double eps = Util.EpsilonFromK.getAdjustedEpsilon(k);
    final String epsPct = String.format("%.3f%%", eps * 100.0);

    sb.append(Util.LS).append("### ").append(thisSimpleName).append(" SUMMARY: ").append(LS);
    sb.append("   K                            : ").append(k).append(LS);
    sb.append("   N                            : ").append(nStr).append(LS);
    sb.append("   Levels (Total, Valid)        : ")
      .append(totLevels + ", " + validLevels).append(LS);
    sb.append("   Level Bit Pattern            : ")
      .append(Long.toBinaryString(bitPattern)).append(LS);
    sb.append("   BaseBufferCount              : ").append(bbCount).append(LS);
    sb.append("   Retained Items               : ").append(retItemsStr).append(LS);
    sb.append("   Storage Bytes                : ").append(String.format("%,d", bytes)).append(LS);
    sb.append("   Normalized Rank Error        : ").append(epsPct).append(LS);
    sb.append("   Min Value                    : ")
      .append(String.format("%,.3f", sketch.getMinValue())).append(LS);
    sb.append("   Max Value                    : ")
      .append(String.format("%,.3f", sketch.getMaxValue())).append(LS);
    sb.append("### END SKETCH SUMMARY").append(LS);
    return sb.toString();
  }

  static String printMemData(Memory mem, int k, int n) {
    if (n == 0) return "";
    final StringBuilder sb = new StringBuilder();
    sb.append(LS).append("### ").append("MEM DATA DETAIL:").append(LS);
    String fmt1 = "%n%10.1f, ";
    String fmt2 = "%10.1f, ";
    int bbCount = Util.computeBaseBufferItems(k, n);
    int ret = Util.computeRetainedItems(k, n);
    sb.append("BaseBuffer Data:");
    for (int i = 0; i < bbCount; i++) {
      double d = mem.getDouble(32 + i * 8);
      if (i % k != 0) sb.append(String.format(fmt2, d));
      else sb.append(String.format(fmt1, d));
    }
    sb.append(LS + LS + "Level Data:");
    for (int i = 0; i < ret - bbCount; i++) {
      double d = mem.getDouble(32 + i * 8 + bbCount * 8);
      if (i % k != 0) sb.append(String.format(fmt2, d));
      else sb.append(String.format(fmt1, d));
    }
    sb.append(LS + "### END DATA DETAIL").append(LS);
    return sb.toString();
  }

}
