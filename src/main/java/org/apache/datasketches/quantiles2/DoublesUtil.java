/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.datasketches.quantiles2;

import static org.apache.datasketches.common.Util.LS;
import static org.apache.datasketches.quantiles2.ClassicUtil.DOUBLES_SER_VER;
import static org.apache.datasketches.quantiles2.ClassicUtil.computeCombinedBufferItemCapacity;
import static org.apache.datasketches.quantiles2.ClassicUtil.computeNumLevelsNeeded;
import static org.apache.datasketches.quantiles2.ClassicUtil.computeTotalLevels;
import static org.apache.datasketches.quantiles2.ClassicUtil.computeValidLevels;
import static org.apache.datasketches.quantiles2.ClassicUtil.getNormalizedRankError;

import java.util.Arrays;

import org.apache.datasketches.common.SketchesArgumentException;

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
  static HeapUpdateDoublesSketch copyToHeap(final DoublesSketch sketch) {
    final HeapUpdateDoublesSketch qsCopy;
    qsCopy = HeapUpdateDoublesSketch.newInstance(sketch.getK());
    qsCopy.putN(sketch.getN());
    qsCopy.putMinItem(sketch.isEmpty() ? Double.NaN : sketch.getMinItem());
    qsCopy.putMaxItem(sketch.isEmpty() ? Double.NaN : sketch.getMaxItem());
    qsCopy.putBaseBufferCount(sketch.getBaseBufferCount());
    qsCopy.putBitPattern(sketch.getBitPattern());

    if (sketch.isCompact()) {
      final int combBufItems = computeCombinedBufferItemCapacity(sketch.getK(), sketch.getN());
      final double[] combBuf = new double[combBufItems];
      qsCopy.putCombinedBuffer(combBuf);
      final DoublesSketchAccessor sketchAccessor = DoublesSketchAccessor.wrap(sketch);
      final DoublesSketchAccessor copyAccessor = DoublesSketchAccessor.wrap(qsCopy);
      // start with BB
      copyAccessor.putArray(sketchAccessor.getArray(0, sketchAccessor.numItems()),
              0, 0, sketchAccessor.numItems());

      long bitPattern = sketch.getBitPattern();
      for (int lvl = 0; bitPattern != 0L; ++lvl, bitPattern >>>= 1) {
        if ((bitPattern & 1L) > 0L) {
          sketchAccessor.setLevel(lvl);
          copyAccessor.setLevel(lvl);
          copyAccessor.putArray(sketchAccessor.getArray(0, sketchAccessor.numItems()),
                  0, 0, sketchAccessor.numItems());
        }
      }
    } else {
      final double[] combBuf = sketch.getCombinedBuffer();
      qsCopy.putCombinedBuffer(Arrays.copyOf(combBuf, combBuf.length));
    }
    return qsCopy;
  }

  /**
   * Check the validity of the given serialization version
   * @param serVer the given serialization version
   * @param minSupportedSerVer the oldest serialization version supported
   */
  static void checkDoublesSerVer(final int serVer, final int minSupportedSerVer) {
    final int max = DOUBLES_SER_VER;
    if ((serVer > max) || (serVer < minSupportedSerVer)) {
      throw new SketchesArgumentException(
          "Possible corruption: Unsupported Serialization Version: " + serVer);
    }
  }

  static String toString(final boolean withLevels, final boolean withLevelsAndItems,
      final DoublesSketch sk) {
    final StringBuilder sb = new StringBuilder();
    sb.append(getSummary(sk));
    if (withLevels) {
      sb.append(outputLevels(sk));
    }
    if (withLevelsAndItems) {
      sb.append(outputDataDetail(sk));
    }
    return sb.toString();
  }

  private static String getSummary(final DoublesSketch sk) {
    final StringBuilder sb = new StringBuilder();
    final String thisSimpleName = sk.getClass().getSimpleName();
    final int k = sk.getK();
    final String kStr = String.format("%,d", k);
    final long n = sk.getN();
    final String nStr = String.format("%,d", n);
    final String bbCntStr = String.format("%,d", sk.getBaseBufferCount());
    final String combBufCapStr = String.format("%,d", sk.getCombinedBufferItemCapacity());
    final long bitPattern = sk.getBitPattern();
    final int neededLevels = computeNumLevelsNeeded(k, n);
    final int totalLevels = computeTotalLevels(bitPattern);
    final int validLevels = computeValidLevels(bitPattern);
    final String retItemsStr = String.format("%,d", sk.getNumRetained());
    final int preBytes = sk.isEmpty() ? Long.BYTES : 2 * Long.BYTES;
    final String cmptBytesStr = String.format("%,d", sk.getCurrentCompactSerializedSizeBytes());
    final String updtBytesStr = String.format("%,d", sk.getCurrentUpdatableSerializedSizeBytes());
    final double epsPmf = getNormalizedRankError(k, true);
    final String epsPmfPctStr = String.format("%.3f%%", epsPmf * 100.0);
    final double eps =  getNormalizedRankError(k, false);
    final String epsPctStr = String.format("%.3f%%", eps * 100.0);
    final String memCap = sk.hasMemorySegment() ? Long.toString(sk.getMemorySegment().byteSize()) : "";
    final double minItem = sk.isEmpty() ? Double.NaN : sk.getMinItem();
    final double maxItem = sk.isEmpty() ? Double.NaN : sk.getMaxItem();

    sb.append(LS).append("### Classic Quantiles ").append(thisSimpleName).append(" SUMMARY: ").append(LS);
    sb.append("    Empty                        : ").append(sk.isEmpty()).append(LS);
    sb.append("    Memory, Capacity bytes       : ").append(sk.hasMemorySegment()).append(", ").append(memCap).append(LS);
    sb.append("    Estimation Mode              : ").append(sk.isEstimationMode()).append(LS);
    sb.append("    K                            : ").append(kStr).append(LS);
    sb.append("    N                            : ").append(nStr).append(LS);
    sb.append("    Levels (Needed, Total, Valid): ").append(neededLevels + ", " + totalLevels + ", " + validLevels)
      .append(LS);
    sb.append("    Level Bit Pattern            : ").append(Long.toBinaryString(bitPattern)).append(LS);
    sb.append("    Base Buffer Count            : ").append(bbCntStr).append(LS);
    sb.append("    Combined Buffer Capacity     : ").append(combBufCapStr).append(LS);
    sb.append("    Retained Items               : ").append(retItemsStr).append(LS);
    sb.append("    Preamble Bytes               : ").append(preBytes).append(LS);
    sb.append("    Compact Storage Bytes        : ").append(cmptBytesStr).append(LS);
    sb.append("    Updatable Storage Bytes      : ").append(updtBytesStr).append(LS);
    sb.append("    Normalized Rank Error        : ").append(epsPctStr).append(LS);
    sb.append("    Normalized Rank Error (PMF)  : ").append(epsPmfPctStr).append(LS);
    sb.append("    Min Item                     : ")
      .append(String.format("%12.6e", minItem)).append(LS);
    sb.append("    Max Item                     : ")
      .append(String.format("%12.6e", maxItem)).append(LS);
    sb.append("### END SKETCH SUMMARY").append(LS);
    return sb.toString();
  }

  private static <T> String outputLevels(final DoublesSketch sk) {
    final String name = sk.getClass().getSimpleName();
    final int k = sk.getK();
    final long n = sk.getN();
    final int totNumLevels = computeNumLevelsNeeded(k, n);
    final long bitPattern = sk.getBitPattern();
    final StringBuilder sb =  new StringBuilder();
    sb.append(LS).append("### ").append(name).append(" LEVELS ABOVE BASE BUF:").append(LS);
    if (totNumLevels == 0) {
      sb.append("    <NONE>").append(LS);
    } else {
      sb.append("    Level |  Valid |  Weight").append(LS);
      for (int i = 0; i < totNumLevels; i++) {
        final String wt = "" + (1L << (i + 1));
        final String valid = getValidFromLevel(i, bitPattern) ? "T" : "F";
        final String row = String.format("  %7s %8s %9s", i, valid, wt);
        sb.append(row).append(LS);
      }
    }
    sb.append("### END LEVELS ABOVE BASE BUF").append(LS);
    return sb.toString();
  }

  private static <T> String outputDataDetail(final DoublesSketch sk) {
    final String name = sk.getClass().getSimpleName();
    final int k = sk.getK();
    final long n = sk.getN();
    final long bitPattern = sk.getBitPattern();
    final int bbCount = sk.getBaseBufferCount();
    final int combBufCap = sk.getCombinedBufferItemCapacity();
    final StringBuilder sb =  new StringBuilder();

    sb.append(LS).append("### ").append(name).append(" DATA DETAIL: ").append(LS);
    final double[] items  = sk.getCombinedBuffer();
    if (n == 0) {
      sb.append("    <NO DATA>").append(LS);
    } else {
      sb.append("  Index | Level | Valid | Item").append(LS);
      for (int i = 0; i < combBufCap; i++) {
        final int levelNum = getLevelNum(k, i);
        final String lvlStr = (levelNum == -1) ? "BB" : ("" + levelNum);
        final String validLvl = getValidFromIndex(levelNum, bitPattern, i, bbCount) ? "T" : "F";
        final String row = String.format("%7s %7s %7s   %s", i, lvlStr, validLvl, items[i]);
        sb.append(row).append(LS);
      }
    }
    sb.append("### END DATA DETAIL").append(LS);
    return sb.toString();
  }

  private static boolean getValidFromIndex(final int levelNum, final long bitPattern, final int index,
      final int bbCount) {
    return ((levelNum == -1) && (index < bbCount)) || getValidFromLevel(levelNum, bitPattern);
  }

  private static boolean getValidFromLevel(final int levelNum, final long bitPattern) {
    return ((1L << levelNum) & bitPattern) > 0;
  }

  private static int getLevelNum(final int k, final int index) {
    final int twoK = 2 * k;
    return index < twoK ? - 1 : (index - twoK) / k;
  }

}
