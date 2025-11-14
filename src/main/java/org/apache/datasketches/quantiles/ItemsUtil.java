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

package org.apache.datasketches.quantiles;

import static org.apache.datasketches.common.Util.LS;
import static org.apache.datasketches.quantiles.ClassicUtil.computeNumLevelsNeeded;
import static org.apache.datasketches.quantiles.ClassicUtil.computeTotalLevels;
import static org.apache.datasketches.quantiles.ClassicUtil.computeValidLevels;
import static org.apache.datasketches.quantiles.ClassicUtil.getNormalizedRankError;

import java.util.Arrays;

import org.apache.datasketches.common.SketchesArgumentException;

/**
 * Utility class for generic quantiles sketch.
 *
 * @author Kevin Lang
 * @author Alexander Saydakov
 */
final class ItemsUtil {

  private ItemsUtil() {}

  static final int ITEMS_SER_VER = 3;
  static final int PRIOR_ITEMS_SER_VER = 2;

  /**
   * Check the validity of the given serialization version
   * @param serVer the given serialization version
   */
  static void checkItemsSerVer(final int serVer) {
    if ((serVer == ITEMS_SER_VER) || (serVer == PRIOR_ITEMS_SER_VER)) { return; }
    throw new SketchesArgumentException(
        "Possible corruption: Invalid Serialization Version: " + serVer);
  }

  /**
   * Called when the base buffer has just acquired 2*k elements.
   * @param <T> the data type
   * @param sketch the given quantiles sketch
   */
  @SuppressWarnings("unchecked")
  static <T> void processFullBaseBuffer(final QuantilesItemsSketch<T> sketch) {
    final int bbCount = sketch.getBaseBufferCount();
    final long n = sketch.getN();
    assert bbCount == (2 * sketch.getK()); // internal consistency check

    // make sure there will be enough levels for the propagation
    ItemsUpdateImpl.maybeGrowLevels(sketch, n); // important: n_ was incremented by update before we got here

    // this aliasing is a bit dangerous; notice that we did it after the possible resizing
    final Object[] baseBuffer = sketch.getCombinedBuffer();

    Arrays.sort((T[]) baseBuffer, 0, bbCount, sketch.getComparator());
    ItemsUpdateImpl.inPlacePropagateCarry(
        0,
        null, 0,  // this null is okay
        (T[]) baseBuffer, 0,
        true, sketch);
    sketch.baseBufferCount_ = 0;
    Arrays.fill(baseBuffer, 0, 2 * sketch.getK(), null); // to release the discarded objects
    assert (n / (2L * sketch.getK())) == sketch.getBitPattern();  // internal consistency check
  }

  static <T> String toString(final boolean withLevels, final boolean withLevelsAndItems,
      final QuantilesItemsSketch<T> sk) {
    final StringBuilder sb = new StringBuilder();
    final String thisSimpleName = sk.getClass().getSimpleName();
    final int bbCount = sk.getBaseBufferCount();
    final int combBufCap = sk.getCombinedBufferAllocatedCount();
    final int k = sk.getK();
    final long bitPattern = sk.getBitPattern();

    final long n = sk.getN();
    final String nStr = String.format("%,d", n);
    final String bbCntStr = String.format("%,d", bbCount);
    final String combBufCapStr = String.format("%,d", combBufCap);
    final int neededLevels = computeNumLevelsNeeded(k, n);
    final int totalLevels = computeTotalLevels(bitPattern);
    final int validLevels = computeValidLevels(bitPattern);
    final int numRetained = sk.getNumRetained();
    final String numRetainedStr = String.format("%,d", numRetained);
    final int preBytes = sk.isEmpty() ? Long.BYTES : 2 * Long.BYTES;
    final double epsPmf = getNormalizedRankError(k, true);
    final String epsPmfPctStr = String.format("%.3f%%", epsPmf * 100.0);
    final double eps =  getNormalizedRankError(k, false);
    final String epsPctStr = String.format("%.3f%%", eps * 100.0);
    final T minItem = sk.isEmpty() ? null : sk.getMinItem();
    final T maxItem = sk.isEmpty() ? null : sk.getMaxItem();
    sb.append(LS).append("### Classic Quantiles ").append(thisSimpleName).append(" SUMMARY: ").append(LS);
    sb.append("    Empty                        : ").append(sk.isEmpty()).append(LS);
    sb.append("    Estimation Mode              : ").append(sk.isEstimationMode()).append(LS);
    sb.append("    K                            : ").append(k).append(LS);
    sb.append("    N                            : ").append(nStr).append(LS);
    sb.append("    Levels (Needed, Total, Valid): ").append(neededLevels + ", " + totalLevels + ", " + validLevels)
    .append(LS);
    sb.append("    Level Bit Pattern            : ").append(Long.toBinaryString(bitPattern)).append(LS);
    sb.append("    Base Buffer Count            : ").append(bbCntStr).append(LS);
    sb.append("    Combined Buffer Capacity     : ").append(combBufCapStr).append(LS);
    sb.append("    Retained Items               : ").append(numRetainedStr).append(LS);
    sb.append("    Preamble Bytes               : ").append(preBytes).append(LS);
    sb.append("    Normalized Rank Error        : ").append(epsPctStr).append(LS);
    sb.append("    Normalized Rank Error (PMF)  : ").append(epsPmfPctStr).append(LS);
    sb.append("    Min Item                     : ").append(minItem).append(LS);
    sb.append("    Max Item                     : ").append(maxItem).append(LS);
    sb.append("### END SKETCH SUMMARY").append(LS);

    if (withLevels) { sb.append(outputLevels(sk)); }
    if (withLevelsAndItems) { sb.append(outputDataDetail(sk)); }
    return sb.toString();
  }

  private static <T> String outputLevels(final QuantilesItemsSketch<T> sk) {
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

  private static <T> String outputDataDetail(final QuantilesItemsSketch<T> sk) {
    final String name = sk.getClass().getSimpleName();
    final int k = sk.getK();
    final long n = sk.getN();
    final long bitPattern = sk.getBitPattern();
    final int bbCount = sk.getBaseBufferCount();
    final int combBufCap = sk.getCombinedBufferAllocatedCount();
    final StringBuilder sb =  new StringBuilder();

    sb.append(LS).append("### ").append(name).append(" DATA DETAIL: ").append(LS);
    final Object[] items  = sk.getCombinedBuffer();
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
