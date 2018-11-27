/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.quantiles;

import static com.yahoo.sketches.Util.LS;

import java.util.Arrays;
import java.util.Comparator;

import com.yahoo.sketches.SketchesArgumentException;

/**
 * Utility class for generic quantiles sketch.
 *
 * <p>This class contains a highly specialized sort called blockyTandemMergeSort().
 * It also contains methods that are used while building histograms and other common
 * functions.</p>
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
   * Checks the sequential validity of the given array of values.
   * They must be unique, monotonically increasing and not null.
   * @param <T> the data type
   * @param values given array of values
   * @param comparator the comparator for data type T
   */
  static final <T> void validateValues(final T[] values, final Comparator<? super T> comparator) {
    final int lenM1 = values.length - 1;
    for (int j = 0; j < lenM1; j++) {
      if ((values[j] != null) && (values[j + 1] != null)
          && (comparator.compare(values[j], values[j + 1]) < 0)) {
        continue;
      }
      throw new SketchesArgumentException(
          "Values must be unique, monotonically increasing and not null.");
    }
  }

  /**
   * Called when the base buffer has just acquired 2*k elements.
   * @param <T> the data type
   * @param sketch the given quantiles sketch
   */
  @SuppressWarnings("unchecked")
  static <T> void processFullBaseBuffer(final ItemsSketch<T> sketch) {
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

  static <T> String toString(final boolean sketchSummary, final boolean dataDetail,
      final ItemsSketch<T> sketch) {
    final StringBuilder sb = new StringBuilder();
    final String thisSimpleName = sketch.getClass().getSimpleName();
    final int bbCount = sketch.getBaseBufferCount();
    final int combAllocCount = sketch.getCombinedBufferAllocatedCount();
    final int k = sketch.getK();
    final long bitPattern = sketch.getBitPattern();

    if (dataDetail) {
      sb.append(Util.LS).append("### ").append(thisSimpleName).append(" DATA DETAIL: ").append(Util.LS);
      final Object[] items  = sketch.getCombinedBuffer();

      //output the base buffer
      sb.append("   BaseBuffer   :");
      if (bbCount > 0) {
        for (int i = 0; i < bbCount; i++) {
          sb.append(' ').append(items[i]);
        }
      }
      sb.append(Util.LS);
      //output all the levels
      final int numItems = combAllocCount;
      if (numItems > (2 * k)) {
        sb.append("   Valid | Level");
        for (int j = 2 * k; j < numItems; j++) { //output level data starting at 2K
          if ((j % k) == 0) { //start output of new level
            final int levelNum = j > (2 * k) ? (j - (2 * k)) / k : 0;
            final String validLvl = ((1L << levelNum) & bitPattern) > 0 ? "    T  " : "    F  ";
            final String lvl = String.format("%5d", levelNum);
            sb.append(Util.LS).append("   ").append(validLvl).append(" ").append(lvl).append(":");
          }
          sb.append(' ').append(items[j]);
        }
        sb.append(Util.LS);
      }
      sb.append("### END DATA DETAIL").append(Util.LS);
    }

    if (sketchSummary) {
      final long n = sketch.getN();
      final String nStr = String.format("%,d", n);
      final int numLevels = Util.computeNumLevelsNeeded(k, n);
      final String bufCntStr = String.format("%,d", combAllocCount);
      final int preBytes = sketch.isEmpty() ? Long.BYTES : 2 * Long.BYTES;
      final double epsPmf = Util.getNormalizedRankError(k, true);
      final String epsPmfPctStr = String.format("%.3f%%", epsPmf * 100.0);
      final double eps =  Util.getNormalizedRankError(k, false);
      final String epsPctStr = String.format("%.3f%%", eps * 100.0);
      final int numSamples = sketch.getRetainedItems();
      final String numSampStr = String.format("%,d", numSamples);
      sb.append(Util.LS).append("### ").append(thisSimpleName).append(" SUMMARY: ").append(Util.LS);
      sb.append("   K                            : ").append(k).append(Util.LS);
      sb.append("   N                            : ").append(nStr).append(Util.LS);
      sb.append("   BaseBufferCount              : ").append(bbCount).append(Util.LS);
      sb.append("   CombinedBufferAllocatedCount : ").append(bufCntStr).append(Util.LS);
      sb.append("   Total Levels                 : ").append(numLevels).append(Util.LS);
      sb.append("   Valid Levels                 : ").append(Util.computeValidLevels(bitPattern))
        .append(Util.LS);
      sb.append("   Level Bit Pattern            : ").append(Long.toBinaryString(bitPattern))
        .append(Util.LS);
      sb.append("   Valid Samples                : ").append(numSampStr).append(Util.LS);
      sb.append("   Preamble Bytes               : ").append(preBytes).append(Util.LS);
      sb.append("   Normalized Rank Error        : ").append(epsPctStr).append(LS);
      sb.append("   Normalized Rank Error (PMF)  : ").append(epsPmfPctStr).append(LS);
      sb.append("   Min Value                    : ").append(sketch.getMinValue()).append(Util.LS);
      sb.append("   Max Value                    : ").append(sketch.getMaxValue()).append(Util.LS);
      sb.append("### END SKETCH SUMMARY").append(Util.LS);
    }
    return sb.toString();
  }

}
