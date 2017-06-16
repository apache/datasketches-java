/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.quantiles;

import java.util.Arrays;
import java.util.Comparator;

final class ItemsUpdateImpl {

  private ItemsUpdateImpl() {}

  //important: newN might not equal n_
  // This only increases the size and does not touch or move any data.
  static <T> void maybeGrowLevels(final ItemsSketch<T> sketch, final long newN) {
    // important: newN might not equal n_
    final int k = sketch.getK();
    final int numLevelsNeeded = Util.computeNumLevelsNeeded(k, newN);
    if (numLevelsNeeded == 0) {
      // don't need any levels yet, and might have small base buffer; this can happen during a merge
      return;
    }
    // from here on we need a full-size base buffer and at least one level
    assert newN >= 2L * k;
    assert numLevelsNeeded > 0;
    final int spaceNeeded = (2 + numLevelsNeeded) * k;
    if (spaceNeeded <= sketch.getCombinedBufferAllocatedCount()) {
      return;
    }
    // copies base buffer plus old levels
    sketch.combinedBuffer_ = Arrays.copyOf(sketch.getCombinedBuffer(), spaceNeeded);
    sketch.combinedBufferItemCapacity_ = spaceNeeded;
  }

  @SuppressWarnings("unchecked")
  static <T> void inPlacePropagateCarry(
      final int startingLevel,
      final T[] sizeKBuf, final int sizeKStart,
      final T[] size2KBuf, final int size2KStart,
      final boolean doUpdateVersion,
      final ItemsSketch<T> sketch) { // else doMergeIntoVersion
    final Object[] levelsArr = sketch.getCombinedBuffer();
    final long bitPattern = sketch.getBitPattern();
    final int k = sketch.getK();

    final int endingLevel = Util.lowestZeroBitStartingAt(bitPattern, startingLevel);

    if (doUpdateVersion) { // update version of computation
      // its is okay for sizeKbuf to be null in this case
      zipSize2KBuffer(
          size2KBuf, size2KStart,
          levelsArr, (2 + endingLevel) * k,
          k);
    } else { // mergeInto version of computation
      System.arraycopy(
          sizeKBuf, sizeKStart,
          levelsArr, (2 + endingLevel) * k,
          k);
    }

    for (int lvl = startingLevel; lvl < endingLevel; lvl++) {
      assert (bitPattern & (1L << lvl)) > 0; // internal consistency check
      mergeTwoSizeKBuffers(
          (T[]) levelsArr, (2 + lvl) * k,
          (T[]) levelsArr, (2 + endingLevel) * k,
          size2KBuf, size2KStart,
          k, sketch.getComparator());
      zipSize2KBuffer(
          size2KBuf, size2KStart,
          levelsArr, (2 + endingLevel) * k,
          k);
      // to release the discarded objects
      Arrays.fill(levelsArr, (2 + lvl) * k, (2 + lvl + 1) * k, null);
    } // end of loop over lower levels

    // update bit pattern with binary-arithmetic ripple carry
    sketch.bitPattern_ = bitPattern + (1L << startingLevel);
  }

  //note: this version refers to the ItemsSketch.rand
  private static void zipSize2KBuffer(
      final Object[] bufA, final int startA, // input
      final Object[] bufC, final int startC, // output
      final int k) {
    final int randomOffset = ItemsSketch.rand.nextBoolean() ? 1 : 0;
    final int limC = startC + k;
    for (int a = startA + randomOffset, c = startC; c < limC; a += 2, c++) {
      bufC[c] = bufA[a];
    }
  }

  //note: this version uses a comparator
  private static <T> void mergeTwoSizeKBuffers(
      final T[] keySrc1, final int arrStart1,
      final T[] keySrc2, final int arrStart2,
      final T[] keyDst,  final int arrStart3,
      final int k, final Comparator<? super T> comparator) {
    final int arrStop1 = arrStart1 + k;
    final int arrStop2 = arrStart2 + k;

    int i1 = arrStart1;
    int i2 = arrStart2;
    int i3 = arrStart3;
    while (i1 < arrStop1 && i2 < arrStop2) {
      if (comparator.compare(keySrc2[i2], keySrc1[i1]) < 0) {
        keyDst[i3++] = keySrc2[i2++];
      } else {
        keyDst[i3++] = keySrc1[i1++];
      }
    }

    if (i1 < arrStop1) {
      System.arraycopy(keySrc1, i1, keyDst, i3, arrStop1 - i1);
    } else {
      assert i2 < arrStop2;
      System.arraycopy(keySrc1, i2, keyDst, i3, arrStop2 - i2);
    }
  }

}
