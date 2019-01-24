/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.cpc;

import static com.yahoo.sketches.Util.DEFAULT_UPDATE_SEED;
import static com.yahoo.sketches.Util.iGolden;
import static com.yahoo.sketches.cpc.CpcUtil.countBitsSetInMatrix;
import static com.yahoo.sketches.cpc.Flavor.EMPTY;
import static com.yahoo.sketches.cpc.Flavor.SPARSE;

import com.yahoo.sketches.Family;
import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.SketchesStateException;

/*
 * The merging logic is somewhat involved, so it will be summarized here.
 *
 * <p>First, we compare the K values of the union and the source sketch.
 *
 * <p>If (source.K &lt; union.K), we reduce the union's K to match, which
 * requires downsampling the union's internal sketch.
 *
 * <p>Here is how to perform the downsampling.
 *
 * <p>If the union contains a bitMatrix, downsample it by row-wise OR'ing.
 *
 * <p>If the union contains a sparse sketch, then create a new empty
 * sketch, and walk the old target sketch updating the new one (with modulo).
 * At the end, check whether the new target
 * sketch is still in sparse mode (it might not be, because downsampling
 * densifies the set of collected coupons). If it is NOT in sparse mode,
 * immediately convert it to a bitmatrix.
 *
 * <p>At this point, we have source.K &ge; union.K.
 * [We won't keep mentioning this, but in all of the following the
 * source's row indices are used mod union.K while updating the union's sketch.
 * That takes care of the situation where source.K &lt; union.K.]
 *
 * <p>Case A: union is Sparse and source is Sparse. We walk the source sketch
 * updating the union's sketch. At the end, if the union's sketch
 * is no longer in sparse mode, we convert it to a bitmatrix.
 *
 * <p>Case B: union is bitmatrix and source is Sparse. We walk the source sketch,
 * setting bits in the bitmatrix.
 *
 * <p>In the remaining cases, we have flavor(source) &gt; Sparse, so we immediately convert the
 * union's sketch to a bitmatrix (even if the union contains very few coupons). Then:
 *
 * <p>Case C: union is bitmatrix and source is Hybrid or Pinned. Then we OR the source's
 * sliding window into the bitmatrix, and walk the source's table, setting bits in the bitmatrix.
 *
 * <p>Case D: union is bitmatrix, and source is Sliding. Then we convert the source into
 * a bitmatrix, and OR it into the union's bitmatrix. [Important note; merely walking the source
 * wouldn't work because of the partially inverted Logic in the Sliding flavor, where the presence of
 * coupons is sometimes indicated by the ABSENCE of rowCol pairs in the surprises table.]
 *
 * <p>How does getResult work?
 *
 * <p>If the union is using its accumulator field, make a copy of that sketch.
 *
 * <p>If the union is using its bitMatrix field, then we have to convert the
 * bitMatrix back into a sketch, which requires doing some extra work to
 * figure out the values of numCoupons, offset, fiCol, and KxQ.
 *
 */
/**
 * The union (merge) operation for the CPC sketches.
 *
 * @author Lee Rhodes
 * @author Kevin Lang
 */
public class CpcUnion {
  private final long seed;
  private int lgK;

  // Note: at most one of bitMatrix and accumulator will be non-null at any given moment.
  // accumulator is a sketch object that is employed until it graduates out of Sparse mode.
  // At that point, it is converted into a full-sized bitMatrix, which is mathematically a sketch,
  // but doesn't maintain any of the "extra" fields of our sketch objects, so some additional work
  // is required when getResult is called at the end.
  private long[] bitMatrix;
  private CpcSketch accumulator; //can only be empty or sparse Flavor

  /**
   * Construct this unioning object with the default LgK and the default update seed.
   */
  public CpcUnion() {
    this(CpcSketch.DEFAULT_LG_K, DEFAULT_UPDATE_SEED);
  }

  /**
   * Construct this unioning object with LgK and the default update seed.
   * @param lgK The given log2 of K.
   */
  public CpcUnion(final int lgK) {
    this(lgK, DEFAULT_UPDATE_SEED);
  }

  /**
   * Construct this unioning object with LgK and a given seed.
   * @param lgK The given log2 of K.
   * @param seed The given seed.
   */
  public CpcUnion(final int lgK, final long seed) {
    this.seed = seed;
    this.lgK = lgK;
    bitMatrix = null;
    // We begin with the accumulator holding an EMPTY_MERGED sketch object.
    // As an optimization the accumulator could start as NULL, but that would require changes elsewhere.
    accumulator = new CpcSketch(lgK);
  }

  /**
   * Update this union with a CpcSketch.
   * @param sketch the given CpcSketch.
   */
  public void update(final CpcSketch sketch) {
    mergeInto(this, sketch);
  }

  /**
   * Returns the result of union operations as a CPC sketch.
   * @return the result of union operations as a CPC sketch.
   */
  public CpcSketch getResult() {
    return getResult(this);
  }

  /**
   * Returns the current value of Log_base2 of K.  Note that due to merging with source sketches that
   * may have a lower value of LgK, this value can be less than what the union object was configured
   * with.
   *
   * @return the current value of Log_base2 of K.
   */
  public int getLgK() {
    return lgK;
  }

  /**
   * Return the DataSketches identifier for this CPC family of sketches.
   * @return the DataSketches identifier for this CPC family of sketches.
   */
  public static Family getFamily() {
    return Family.CPC;
  }

  //used for testing only
  long getNumCoupons() {
    if (bitMatrix != null) {
      return countBitsSetInMatrix(bitMatrix);
    }
    return accumulator.numCoupons;
  }

  //used for testing only
  static long[] getBitMatrix(final CpcUnion union) {
    checkUnionState(union);
    return (union.bitMatrix != null)
        ? union.bitMatrix
        : CpcUtil.bitMatrixOfSketch(union.accumulator);
  }

  private static void walkTableUpdatingSketch(final CpcSketch dest, final PairTable table) {
    final int[] slots = table.getSlotsArr();
    final int numSlots = (1 << table.getLgSizeInts());
    assert dest.lgK <= 26;
    final int destMask = (((1 << dest.lgK) - 1) << 6) | 63; //downsamples when destlgK < srcLgK

    /* Using the inverse golden ratio stride fixes the
     * <a href="{@docRoot}/resources/dictionary.html#SnowPlow">Snow Plow Effect</a>.
     */
    int stride =  (int) (iGolden * numSlots);
    assert stride >= 2;
    if (stride == ((stride >>> 1) << 1)) { stride += 1; } //force the stride to be odd
    assert (stride >= 3) && (stride < numSlots);

    for (int i = 0, j = 0; i < numSlots; i++, j += stride) {
      j &= (numSlots - 1);
      final int rowCol = slots[j];
      if (rowCol != -1) {
        dest.rowColUpdate(rowCol & destMask);
      }
    }
  }

  private static void orTableIntoMatrix(final long[] bitMatrix, final int destLgK, final PairTable table) {
    final int[] slots = table.getSlotsArr();
    final int numSlots = 1 << table.getLgSizeInts();
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

  private static void orWindowIntoMatrix(final long[] destMatrix, final int destLgK,
      final byte[] srcWindow, final int srcOffset, final int srcLgK) {
    assert (destLgK <= srcLgK);
    final int destMask = (1 << destLgK) - 1;  // downsamples when destlgK < srcLgK
    final int srcK = 1 << srcLgK;
    for (int srcRow = 0; srcRow < srcK; srcRow++) {
      destMatrix[srcRow & destMask] |= ((srcWindow[srcRow] & 0XFFL) << srcOffset);
    }
  }

  private static void orMatrixIntoMatrix(final long[] destMatrix, final int destLgK,
      final long[] srcMatrix, final int srcLgK) {
    assert (destLgK <= srcLgK);
    final int destMask = (1 << destLgK) - 1; // downsamples when destlgK < srcLgK
    final int srcK = 1 << srcLgK;
    for (int srcRow = 0; srcRow < srcK; srcRow++) {
      destMatrix[srcRow & destMask] |= srcMatrix[srcRow];
    }
  }

  private static void reduceUnionK(final CpcUnion union, final int newLgK) {
    assert (newLgK < union.lgK);

    if (union.bitMatrix != null) { // downsample the union's bit matrix
      final int newK = 1 << newLgK;
      final long[] newMatrix = new long[newK];

      orMatrixIntoMatrix(newMatrix, newLgK, union.bitMatrix, union.lgK);
      union.bitMatrix = newMatrix;
      union.lgK = newLgK;
    }

    else { // downsample the union's accumulator
      final CpcSketch oldSketch = union.accumulator;

      if (oldSketch.numCoupons == 0) {
        union.accumulator = new CpcSketch(newLgK, oldSketch.seed);
        union.lgK = newLgK;
        return;
      }

      final CpcSketch newSketch = new CpcSketch(newLgK, oldSketch.seed);
      walkTableUpdatingSketch(newSketch, oldSketch.pairTable);

      final Flavor finalNewFlavor = newSketch.getFlavor();
      assert (finalNewFlavor != EMPTY); //SV table had to have something in it

      if (finalNewFlavor == SPARSE) {
        union.accumulator = newSketch;
        union.lgK = newLgK;
        return;
      }

      // the new sketch has graduated beyond sparse, so convert to bitMatrix
      union.accumulator = null;
      union.bitMatrix = CpcUtil.bitMatrixOfSketch(newSketch);
      union.lgK = newLgK;
    }
  }

  private static void mergeInto(final CpcUnion union, final CpcSketch source) {
    if (source == null) { return; }
    checkSeeds(union.seed, source.seed);

    final int sourceFlavorOrd = source.getFlavor().ordinal();
    if (sourceFlavorOrd == 0) { return; } //EMPTY

    //Accumulator and bitMatrix must be mutually exclusive,
    //so bitMatrix != null => accumulator == null and visa versa
    //if (Accumulator != null) union must be EMPTY or SPARSE,
    checkUnionState(union);

    if (source.lgK < union.lgK) { reduceUnionK(union, source.lgK); }

    // if source is past SPARSE mode, make sure that union is a bitMatrix.
    if ((sourceFlavorOrd > 1) && (union.accumulator != null)) {
      union.bitMatrix = CpcUtil.bitMatrixOfSketch(union.accumulator);
      union.accumulator = null;
    }

    final int state = ((sourceFlavorOrd - 1) << 1) | ((union.bitMatrix != null) ? 1 : 0);
    switch (state) {
      case 0 : { //A: Sparse, bitMatrix == null, accumulator valid
        if ((union.accumulator.getFlavor() == EMPTY) //lgtm [java/dereferenced-value-may-be-null]
            && (union.lgK == source.lgK)) {
          union.accumulator = source.copy();
        }
        walkTableUpdatingSketch(union.accumulator, source.pairTable);
        // if the accumulator has graduated beyond sparse, switch union to a bitMatrix
        if (union.accumulator.getFlavor().ordinal() > 1) {
          union.bitMatrix = CpcUtil.bitMatrixOfSketch(union.accumulator);
          union.accumulator = null;
        }
        break;
      }
      case 1 : { //B: Sparse, bitMatrix valid, accumulator == null
        orTableIntoMatrix(union.bitMatrix, union.lgK, source.pairTable);
        break;
      }
      case 3 :   //C: Hybrid, bitMatrix valid, accumulator == null
      case 5 : { //C: Pinned, bitMatrix valid, accumulator == null
        orWindowIntoMatrix(union.bitMatrix, union.lgK, source.slidingWindow,
            source.windowOffset, source.lgK);
        orTableIntoMatrix(union.bitMatrix, union.lgK, source.pairTable);
        break;
      }
      case 7 : { //D: Sliding, bitMatrix valid, accumulator == null
        // SLIDING mode involves inverted logic, so we can't just walk the source sketch.
        // Instead, we convert it to a bitMatrix that can be OR'ed into the destination.
        final long[] sourceMatrix = CpcUtil.bitMatrixOfSketch(source);
        orMatrixIntoMatrix(union.bitMatrix, union.lgK, sourceMatrix, source.lgK);
        break;
      }
      default: throw new SketchesStateException("Illegal Union state: " + state);
    }
  }

  private static CpcSketch getResult(final CpcUnion union) {
    checkUnionState(union);

    if (union.accumulator != null) { // start of case where union contains a sketch
      if (union.accumulator.numCoupons == 0) {
        final CpcSketch result = new CpcSketch(union.lgK, union.accumulator.seed);
        result.mergeFlag = true;
        return (result);
      }
      assert (SPARSE == union.accumulator.getFlavor());
      final CpcSketch result = union.accumulator.copy();
      result.mergeFlag = true;
      return (result);
    } // end of case where union contains a sketch

    // start of case where union contains a bitMatrix
    final long[] matrix = union.bitMatrix;
    final int lgK = union.lgK;
    final CpcSketch result = new CpcSketch(union.lgK, union.seed);

    final long numCoupons = countBitsSetInMatrix(matrix);
    result.numCoupons = numCoupons;

    final Flavor flavor = CpcUtil.determineFlavor(lgK, numCoupons);
    assert ((flavor.ordinal() > SPARSE.ordinal()) );

    final int offset = CpcUtil.determineCorrectOffset(lgK, numCoupons);
    result.windowOffset = offset;

    //Build the window and pair table

    final int k = 1 << lgK;
    final byte[] window = new byte[k];
    result.slidingWindow = window;

    // LgSize = K/16; in some cases this will end up being oversized
    final int newTableLgSize = Math.max(lgK - 4, 2);
    final PairTable table = new PairTable(newTableLgSize, 6 + lgK);
    result.pairTable = table;

    // The following works even when the offset is zero.
    final long maskForClearingWindow = (0XFFL << offset) ^ -1L;
    final long maskForFlippingEarlyZone = (1L << offset) - 1L;
    long allSurprisesORed = 0;

    /* using a sufficiently large hash table avoids the
     * <a href="{@docRoot}/resources/dictionary.html#SnowPlow">Snow Plow Effect</a>
     */
    for (int i = 0; i < k; i++) {
      long pattern = matrix[i];
      window[i] = (byte) ((pattern >>> offset) & 0XFFL);
      pattern &= maskForClearingWindow;
      pattern ^= maskForFlippingEarlyZone; // This flipping converts surprising 0's to 1's.
      allSurprisesORed |= pattern;
      while (pattern != 0) {
        final int col = Long.numberOfTrailingZeros(pattern);
        pattern = pattern ^ (1L << col); // erase the 1.
        final int rowCol = (i << 6) | col;
        final boolean isNovel = PairTable.maybeInsert(table, rowCol);
        assert isNovel;
      }
    }

    // At this point we could shrink an oversize hash table, but the relative waste isn't very big.
    result.fiCol = Long.numberOfTrailingZeros(allSurprisesORed);
    if (result.fiCol > offset) {
      result.fiCol = offset;
    } // corner case

    // NB: the HIP-related fields will contain bogus values, but that is okay.

    result.mergeFlag = true;
    return result;
    // end of case where union contains a bitMatrix
  }

  private static void checkSeeds(final long seedA, final long seedB) {
    if (seedA != seedB) {
      throw new SketchesArgumentException("Hash Seeds do not match.");
    }
  }

  private static void checkUnionState(final CpcUnion union) {
    if (union == null) {
      throw new SketchesStateException("union cannot be null");
    }
    final CpcSketch accumulator = union.accumulator;
    if ( !((accumulator != null) ^ (union.bitMatrix != null)) ) {
      throw new SketchesStateException(
        "accumulator and bitMatrix cannot be both valid or both null: "
        + "accumValid = " + (accumulator != null)
        + ", bitMatrixValid = " + (union.bitMatrix != null));
    }
    if (accumulator != null) { //must be SPARSE or EMPTY
      if (accumulator.numCoupons > 0) { //SPARSE
        if ( !((accumulator.slidingWindow == null) && (accumulator.pairTable != null)) ) {
          throw new SketchesStateException(
              "Non-empty union accumulator must be SPARSE: " + accumulator.getFlavor());
        }
      } //else EMPTY
      if (union.lgK != accumulator.lgK) {
        throw new SketchesStateException("union LgK must equal accumulator LgK");
      }
    }
  }

}
