/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.cpc;

import static com.yahoo.sketches.Util.DEFAULT_UPDATE_SEED;
import static com.yahoo.sketches.cpc.Flavor.EMPTY;
import static com.yahoo.sketches.cpc.Flavor.HYBRID;
import static com.yahoo.sketches.cpc.Flavor.PINNED;
import static com.yahoo.sketches.cpc.Flavor.SLIDING;
import static com.yahoo.sketches.cpc.Flavor.SPARSE;

import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.SketchesStateException;

/**
 * The merging logic is somewhat involved, so it will be summarized here.
 *
 * <p>First, we compare the K values of the unioner and the source sketch.
 *
 * <p>If (source.K < unioner.K), we reduce the unioner's K to match, which
 * requires downsampling the unioner's internal sketch.
 *
 * <p>Here is how to perform the downsampling.
 *
 * <p>If the unioner contains a bitMatrix, downsample it by row-wise OR'ing.
 *
 * <p>If the unioner contains a sparse sketch, then create a new empty
 * sketch, and walk the old target sketch updating the new one (with modulo).
 * At the end, check whether the new target
 * sketch is still in sparse mode (it might not be, because downsampling
 * densifies the set of collected coupons). If it is NOT in sparse mode,
 * immediately convert it to a bitmatrix.
 *
 * <p>At this point, we have source.K >= unioner.K.
 * [We won't keep mentioning this, but in all of the following the
 * source's row indices are used mod unioner.K while updating the unioner's sketch.
 * That takes care of the situation where source.K > unioner.K.]
 *
 * <p>Case A: unioner is Sparse and source is Sparse. We walk the source sketch
 * updating the unioner's sketch. At the end, if the unioner's sketch
 * is no longer in sparse mode, we convert it to a bitmatrix.
 *
 * <p>Case B: unioner is bitmatrix and source is Sparse. We walk the source sketch,
 * setting bits in the bitmatrix.
 *
 * <p>In the remaining cases, we have flavor(source) > Sparse, so we immediately convert the
 * unioner's sketch to a bitmatrix (even if the unioner contains very few coupons). Then:
 *
 * <p>Case C: unioner is bitmatrix and source is Hybrid or Pinned. Then we OR the source's
 * sliding window into the bitmatrix, and walk the source's table, setting bits in the bitmatrix.
 *
 * <p>Case D: unioner is bitmatrix, and source is Sliding. Then we convert the source into
 * a bitmatrix, and OR it into the unioner's bitmatrix. [Important note; merely walking the source
 * wouldn't work because of the partially inverted Logic in the Sliding flavor, where the presence of
 * coupons is sometimes indicated by the ABSENCE of rowCol pairs in the surprises table.]
 *
 * <p>How does getResult work?
 *
 * <p>If the unioner is using its accumator field, make a copy of that sketch.
 *
 * <p>If the unioner is using its bitMatrix field, then we have to convert the
 * bitMatrix back into a sketch, which requires doing some extra work to
 * figure out the values of numCoupons, offset, firstInterestingColumn, and KxQ.
 *
 * @author Lee Rhodes
 * @author Kevin Lang
 */
public class Fm85Merging {
  int lgK;
  long seed;
  // Note: at most one of bitMatrix and accumulator will be non-null at any given moment.
  // accumulator is a sketch object that is employed until it graduates out of Sparse mode.
  // At that point, it is converted into a full-sized bitMatrix, which is mathematically a sketch,
  // but doesn't maintain any of the "extra" fields of our sketch objects, so some additional work
  // is required when getResult is called at the end.
  long[] bitMatrix;
  Fm85 accumulator; //can only be empty or sparse Flavor


  /**
   * Construct this unioning object with LgK and the default update seed.
   * @param lgK The given log2 of K.
   */
  public Fm85Merging(final int lgK) {
    this(lgK, DEFAULT_UPDATE_SEED);
  }

  /**
   * Construct this unioning object with LgK and a given seed.
   * @param lgK The given log2 of K.
   * @param seed The given seed.
   */
  public Fm85Merging(final int lgK, final long seed) {
    this.lgK = lgK;
    this.seed = seed; //computeSeedHash(seed);
    bitMatrix = null;
    accumulator = new Fm85(lgK);
  }

  /**
   * Resets this merging operation. The original values of LgK and Seed are retained.
   */
  public void reset() {
    accumulator = null;
    bitMatrix = null;
  }

  //used for testing only
  static long[] getBitMatrix(final Fm85Merging unioner) {
    checkUnionState(unioner);
    return (unioner.bitMatrix != null)
        ? unioner.bitMatrix
        : Fm85.bitMatrixOfSketch(unioner.accumulator);
  }

  private static void walkTableUpdatingSketch(final Fm85 dest, final PairTable table) {
    final int[] slots = table.slots;
    final int numSlots = (1 << table.lgSize);
    assert dest.lgK <= 26;
    final int destMask = (((1 << dest.lgK) - 1) << 6) | 63; //downsamples when destlgK < srcLgK

    // Using a golden ratio stride fixes the snowplow effect.
    final double golden = 0.6180339887498949025;
    int stride =  (int) (golden * numSlots);
    assert stride >= 2;
    if (stride == ((stride >>> 1) << 1)) { stride += 1; } //force the stride to be odd
    assert (stride >= 3) && (stride < numSlots);

    for (int i = 0, j = 0; i < numSlots; i++, j += stride) {
      j &= (numSlots - 1);
      final int rowCol = slots[j];
      if (rowCol != -1) {
        Fm85.rowColUpdate(dest, rowCol & destMask);
      }
    }
  }

  private static void orTableIntoMatrix(final long[] bitMatrix, final int destLgK, final PairTable table) {
    final int[] slots = table.slots;
    final int numSlots = 1 << table.lgSize;
    final int destMask = (1 << destLgK) - 1;  // downsamples when destlgK < srcLgK
    for (int i = 0; i < numSlots; i++) {
      final int rowCol = slots[i];
      if (rowCol != -1) {
        final int col = rowCol & 63;
        final int row = rowCol >>> 6;
        bitMatrix[row & destMask] |= (1L << col); // Set the bit.
      }
    }
  }

  private static void orWindowIntoMatrix(final long[] destMatrix, final int destLgK, final byte[] srcWindow,
      final int srcOffset, final int srcLgK) {
    assert (destLgK <= srcLgK);
    final int destMask = (1 << destLgK) - 1;  // downsamples when destlgK < srcLgK
    final int srcK = 1 << srcLgK;
    for (int srcRow = 0; srcRow < srcK; srcRow++) {
      destMatrix[srcRow & destMask] |= ((srcWindow[srcRow] & 0XFFL) << srcOffset);
    }
  }

  private static void orMatrixIntoMatrix(final long[] destMatrix, final int destLgK, final long[] srcMatrix,
      final int srcLgK) {
    assert (destLgK <= srcLgK);
    final int destMask = (1 << destLgK) - 1; // downsamples when destlgK < srcLgK
    final int srcK = 1 << srcLgK;
    for (int srcRow = 0; srcRow < srcK; srcRow++) {
      destMatrix[srcRow & destMask] |= srcMatrix[srcRow];
    }
  }

  static void reduceUnionerK(final Fm85Merging unioner, final int newLgK) {
    assert (newLgK < unioner.lgK);
    checkUnionState(unioner);

    if (unioner.bitMatrix != null) { // downsample the unioner's bit matrix
      final int newK = 1 << newLgK;
      final long[] newMatrix = new long[newK];

      orMatrixIntoMatrix(newMatrix, newLgK, unioner.bitMatrix, unioner.lgK);
      unioner.bitMatrix = newMatrix;
      unioner.lgK = newLgK;
    }

    else { // downsample the unioner's accumulator
      final Fm85 oldSketch = unioner.accumulator;

      if (oldSketch.numCoupons == 0) {
        unioner.accumulator = new Fm85(newLgK, oldSketch.seed);
        unioner.lgK = newLgK;
        return;
      }

      final Fm85 newSketch = new Fm85(newLgK, oldSketch.seed);
      walkTableUpdatingSketch(newSketch, oldSketch.surprisingValueTable);

      final Flavor finalNewFlavor = Fm85.determineSketchFlavor(newSketch);
      assert (finalNewFlavor != EMPTY); //SV table had to have something in it

      if (finalNewFlavor == SPARSE) {
        unioner.accumulator = newSketch;
        unioner.lgK = newLgK;
        return;
      }

      // the new sketch has graduated beyond sparse, so convert to bitMatrix
      unioner.accumulator = null;
      unioner.bitMatrix = Fm85.bitMatrixOfSketch(newSketch);
      unioner.lgK = newLgK;
    }
  }

  static void mergeInto(final Fm85Merging unioner, final Fm85 source) {
    if (source == null) { return; }
    checkSeeds(unioner.seed, source.seed);

    final Flavor sourceFlavor = Fm85.determineSketchFlavor(source);
    if (sourceFlavor == EMPTY) { return; }

    if (source.lgK < unioner.lgK) {
      reduceUnionerK(unioner, source.lgK);
    }

    assert (source.lgK >= unioner.lgK)
      : "source.lgK=" + source.lgK + ", unioner.lgK=" + unioner.lgK;

    //CASE A
    if ((SPARSE == sourceFlavor) && (unioner.accumulator != null)) {
      final Flavor initialDestFlavor = Fm85.determineSketchFlavor(unioner.accumulator);
      assert ((EMPTY == initialDestFlavor) || (SPARSE == initialDestFlavor));

      if ((EMPTY == initialDestFlavor) && (unioner.lgK == source.lgK)) {
        unioner.accumulator = source.copy();
      }

      walkTableUpdatingSketch(unioner.accumulator, source.surprisingValueTable);
      final Flavor finalDestFlavor = Fm85.determineSketchFlavor(unioner.accumulator);
      // if the accumulator has graduated beyond sparse, switch to a bitMatrix representation
      if ((finalDestFlavor != EMPTY) && (finalDestFlavor != SPARSE)) {
        unioner.bitMatrix = Fm85.bitMatrixOfSketch(unioner.accumulator);
        unioner.accumulator = null;
      }
      return;
    }

    //CASE B
    if ((SPARSE == sourceFlavor) && (unioner.bitMatrix != null))  {
      assert (unioner.accumulator == null);
      orTableIntoMatrix(unioner.bitMatrix, unioner.lgK, source.surprisingValueTable);
      return;
    }

    assert ((HYBRID == sourceFlavor) || (PINNED == sourceFlavor) || (SLIDING == sourceFlavor));

    // source is past SPARSE mode, so make sure that dest is a bitMatrix.
    if (unioner.accumulator != null) {
      assert (unioner.bitMatrix == null);
      final Flavor destFlavor = Fm85.determineSketchFlavor(unioner.accumulator);
      assert ((EMPTY == destFlavor) || (SPARSE == destFlavor));
      unioner.bitMatrix = Fm85.bitMatrixOfSketch(unioner.accumulator);
      unioner.accumulator = null;
    }
    assert (unioner.bitMatrix != null);

    //CASE C
    if ((HYBRID == sourceFlavor) || (PINNED == sourceFlavor)) {
      orWindowIntoMatrix(unioner.bitMatrix, unioner.lgK, source.slidingWindow,
          source.windowOffset, source.lgK);
      orTableIntoMatrix(unioner.bitMatrix, unioner.lgK, source.surprisingValueTable);
      return;
    }

    //CASE D
    // SLIDING mode involves inverted logic, so we can't just walk the source sketch.
    // Instead, we convert it to a bitMatrix that can be OR'ed into the destination.
    assert (SLIDING == sourceFlavor); // Case D
    final long[] sourceMatrix = Fm85.bitMatrixOfSketch(source);
    orMatrixIntoMatrix(unioner.bitMatrix, unioner.lgK, sourceMatrix, source.lgK);
  }

  static Fm85 getResult(final Fm85Merging unioner) {
    checkUnionState(unioner);

    if (unioner.accumulator != null) { // start of case where unioner contains a sketch
      if (unioner.accumulator.numCoupons == 0) {
        final Fm85 result = new Fm85(unioner.lgK, unioner.accumulator.seed);
        result.mergeFlag = true;
        return (result);
      }
      assert (SPARSE == Fm85.determineSketchFlavor(unioner.accumulator));
      final Fm85 result = unioner.accumulator.copy();
      result.mergeFlag = true;
      return (result);
    } // end of case where unioner contains a sketch

    // start of case where unioner contains a bitMatrix
    final long[] matrix = unioner.bitMatrix;
    final int lgK = unioner.lgK;
    final Fm85 result = new Fm85(unioner.lgK, unioner.seed);

    final int k = 1 << lgK;
    final long numCoupons = Fm85Util.countBitsSetInMatrix(matrix);
    result.numCoupons = numCoupons;

    final Flavor flavor = Fm85.determineFlavor(lgK, numCoupons);
    assert ((flavor == HYBRID) || (flavor == PINNED) || (flavor == SLIDING));

    final int offset = Fm85.determineCorrectOffset(lgK, numCoupons);
    result.windowOffset = offset;

    final byte[] window = new byte[k];
    assert (result.slidingWindow == null);
    result.slidingWindow = window;

    // dynamically growing caused snowplow effect
    int newTableSize = lgK - 4; // K/16; in some cases this will end up being oversized
    if (newTableSize < 2) { newTableSize = 2; }

    final PairTable table = new PairTable(newTableSize, 6 + lgK);
    assert (result.surprisingValueTable == null);
    result.surprisingValueTable = table;

    // I believe that the following works even when the offset is zero.
    final long maskForClearingWindow = (0XFFL << offset) ^ -1L;
    final long maskForFlippingEarlyZone = (1L << offset) - 1L;
    long allSurprisesORed = 0;

    // The snowplow effect was caused by processing the rows in order,
    // but we have fixed it by using a sufficiently large hash table.
    for (int i = 0; i < k; i++) {
      long pattern = matrix[i];
      window[i] = (byte) ((pattern >>> offset) & 0XFFL);
      pattern &= maskForClearingWindow;
      pattern ^= maskForFlippingEarlyZone; // This flipping converts surprising 0's to 1's.
      allSurprisesORed |= pattern;
      while (pattern != 0) {
        final int col = Fm85Util.countTrailingZeros(pattern);
        pattern = pattern ^ (1L << col); // erase the 1.
        final int rowCol = (i << 6) | col;
        final boolean isNovel = PairTable.maybeInsert(table, rowCol);
        assert isNovel;
      }
    }

    // At this point we could shrink an oversize hash table, but the relative waste isn't very big.
    result.firstInterestingColumn = (byte) Fm85Util.countTrailingZeros(allSurprisesORed);
    if (result.firstInterestingColumn > offset) {
      result.firstInterestingColumn = (byte) offset;
    } // corner case

    // NB: the HIP-related fields will contain bogus values, but that is okay.

    result.mergeFlag = true;
    return result;
    // end of case where unioner contains a bitMatrix
  }

  private static void checkSeeds(final long seedA, final long seedB) {
    if (seedA != seedB) {
      throw new SketchesArgumentException("Hash Seeds do not match.");
    }
  }

  private static void checkUnionState(final Fm85Merging unioner) {
    if (unioner == null) {
      throw new SketchesStateException("Unioner cannot be null");
    }
    final boolean accumValid = unioner.accumulator != null;
    final boolean matrixValid = unioner.bitMatrix != null;
    if ( !(accumValid ^ matrixValid) ) {
      throw new SketchesStateException(
          "accumulator and bitMatrix cannot be both valid or both null.");
    }
    if (accumValid) {
      final Fm85 sketch = unioner.accumulator;
      if (sketch.numCoupons > 0) {
        if (   (sketch.slidingWindow != null)
            || (sketch.surprisingValueTable == null)) {
          throw new SketchesStateException("Accumulator must be Sparse Flavor");
        }
      }
      if (unioner.lgK != sketch.lgK) {
        throw new SketchesStateException("Unioner LgK must equal accumulator LgK");
      }
    }
  }

}
