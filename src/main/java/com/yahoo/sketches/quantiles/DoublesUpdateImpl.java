/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.quantiles;

/**
 * The doubles update algorithms for quantiles.
 *
 * @author Lee Rhodes
 * @author Kevin Lang
 */
final class DoublesUpdateImpl {

  private DoublesUpdateImpl() {}

  /**
   * Returns item capacity needed based on new value of n and k, which may or may not be larger than
   * current space allocated.
   * @param k current value of k
   * @param newN the new value of n
   * @return item capacity based on new value of n and k. It may not be different.
   */
  //important: newN might not equal n_
  // This only increases the size and does not touch or move any data.
  static int getRequiredItemCapacity(final int k, final long newN) {
    final int numLevelsNeeded = Util.computeNumLevelsNeeded(k, newN);
    if (numLevelsNeeded == 0) {
      // don't need any levels yet, and might have small base buffer; this can happen during a merge
      return 2 * k;
    }
    // from here on we need a full-size base buffer and at least one level
    assert newN >= (2L * k);
    assert numLevelsNeeded > 0;
    final int spaceNeeded = (2 + numLevelsNeeded) * k;
    return spaceNeeded;
  }

  /**
   * This is used to propagate-carry (ripple-carry) an update that will cause the full, sorted
   * base buffer to empty into the levels hierarchy, thus creating a ripple effect up
   * through the higher levels. It is also used during merge operations with the only difference
   * is the base buffer(s) could have valid data and is less than full.
   * This distinction is determined by the <i>doUpdateVersion</i> flag.
   *
   * <p>Prior to this method being called, any extra space for the combined buffer required
   * by either the update or merge operations must already be allocated.</p>
   *
   * <p><b>Update Version:</b> The base buffer is initially full, and after it has been sorted and
   * zipped, will be used as a size2KBuf scratch buffer for the remaining recursive carries.
   * The lowest non-valid level, determined by the bit-pattern, will used internally as a
   * size K scratch buffer and the ultimate target.
   * Thus no additional buffer storage is required outside the combined buffer.</p>
   *
   * <p><b>Merge Version:</b> During merging, each level from the source sketch that must be
   * merged is entered into this method and is assigned to the optional source size K buffer
   * (<i>optSrcKBuf</i>). Because the base buffer may have data, a separate size2K
   * scratch buffer must be provided. The next-lowest. non-valid level, determined by the
   * bit-pattern, will used as a sizeKBuf scratch buffer.</p>
   *
   * <p><b>Downsample Merge Version:</b> This is a variant of the above Merge Version, except at
   * each level the downsampling is performed and the target level is computed for the target merge.
   * In this case the optSrcKBuf is the result of the downsample process and needs to be allocated
   * for that purpose.
   *
   * <p><b>Recursive carry:</b> This starts with a given sorted, size 2K buffer, which is zipped
   * into a size K buffer. If the next level is not valid, the size K buffer is already in position,
   * the bit pattern is updated and returned.</p>
   *
   * <p>If the next level is valid, it is merged with the size K buffer into the size 2K buffer.
   * Continue the recursion until a non-valid level becomes filled by the size K buffer,
   * the bit pattern is updated and returned.</p>
   *
   * @param startingLevel 0-based starting level
   * @param optSrcKBuf optional, size k source, read only buffer
   * @param size2KBuf size 2k scratch buffer
   * @param doUpdateVersion true if update version
   * @param k the target value of k
   * @param tgtSketchBuf the given DoublesSketchAccessor
   * @param bitPattern the current bitPattern, prior to this call
   * @return The updated bit pattern.  The updated combined buffer is output as a side effect.
   */
  static long inPlacePropagateCarry(
          final int startingLevel,
          final DoublesBufferAccessor optSrcKBuf,
          final DoublesBufferAccessor size2KBuf,
          final boolean doUpdateVersion,
          final int k,
          final DoublesSketchAccessor tgtSketchBuf,
          final long bitPattern) {
    final int endingLevel = Util.lowestZeroBitStartingAt(bitPattern, startingLevel);
    tgtSketchBuf.setLevel(endingLevel);
    if (doUpdateVersion) { // update version of computation
      // its is okay for optSrcKBuf to be null in this case
      zipSize2KBuffer(size2KBuf, tgtSketchBuf);
    } else { // mergeInto version of computation
      assert (optSrcKBuf != null);
      tgtSketchBuf.putArray(optSrcKBuf.getArray(0, k), 0, 0, k);
    }

    for (int lvl = startingLevel; lvl < endingLevel; lvl++) {
      assert (bitPattern & (1L << lvl)) > 0; // internal consistency check
      final DoublesSketchAccessor currLevelBuf = tgtSketchBuf.copyAndSetLevel(lvl);
      mergeTwoSizeKBuffers(
              currLevelBuf, // target level: lvl
              tgtSketchBuf, // target level: endingLevel
              size2KBuf);
      zipSize2KBuffer(size2KBuf, tgtSketchBuf);
    } // end of loop over lower levels

    // update bit pattern with binary-arithmetic ripple carry
    return bitPattern + (1L << startingLevel);
  }

  private static void zipSize2KBuffer(
          final DoublesBufferAccessor bufIn,
          final DoublesBufferAccessor bufOut) {
    final int randomOffset = DoublesSketch.rand.nextBoolean() ? 1 : 0;
    final int limOut = bufOut.numItems();
    for (int idxIn = randomOffset, idxOut = 0; idxOut < limOut; idxIn += 2, idxOut++) {
      bufOut.set(idxOut, bufIn.get(idxIn));
    }
  }

  private static void mergeTwoSizeKBuffers(
          final DoublesBufferAccessor src1,
          final DoublesBufferAccessor src2,
          final DoublesBufferAccessor dst) {
    assert src1.numItems() == src2.numItems();

    final int k = src1.numItems();
    int i1 = 0;
    int i2 = 0;
    int iDst = 0;
    while ((i1 < k) && (i2 < k)) {
      if (src2.get(i2) < src1.get(i1)) {
        dst.set(iDst++, src2.get(i2++));
      } else {
        dst.set(iDst++, src1.get(i1++));
      }
    }

    if (i1 < k) {
      final int numItems = k - i1;
      dst.putArray(src1.getArray(i1, numItems), 0, iDst, numItems);
    } else {
      final int numItems = k - i2;
      dst.putArray(src2.getArray(i2, numItems), 0, iDst, numItems);
    }
  }
}
