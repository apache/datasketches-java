/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.cpc;

import static com.yahoo.sketches.Util.DEFAULT_UPDATE_SEED;
import static com.yahoo.sketches.Util.invPow2;
import static com.yahoo.sketches.cpc.CpcUtil.checkLgK;
import static com.yahoo.sketches.cpc.CpcUtil.kxpByteLookup;
import static com.yahoo.sketches.cpc.RuntimeAsserts.rtAssertEquals;
import static com.yahoo.sketches.cpc.RuntimeAsserts.rtAssertTrue;
import static com.yahoo.sketches.hash.MurmurHash3.hash;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.Arrays;

import com.yahoo.sketches.SketchesStateException;

/**
 * @author Lee Rhodes
 * @author Kevin Lang
 */
public final class CpcSketch {
  final int lgK;
  final long seed;
  boolean isCompressed;
  boolean mergeFlag;    // Is the sketch the result of merging?
  long numCoupons;      // The number of coupons collected so far.

  //The following variables occur in the updateable semi-compressed type.
  byte[] slidingWindow;
  int windowOffset;
  PairTable surprisingValueTable;

  // The following variables occur in the non-updateable fully-compressed type.
  int[] compressedWindow;            //cwStream
  int cwLength; // The number of 32-bit words in this bitstream.
  int numCompressedSurprisingValues; //numSV
  int[] compressedSurprisingValues;  // csvStream
  int csvLength; // The number of 32-bit words in this bitstream.

  // Note that (as an optimization) the two bitstreams could be concatenated.

  int firstInterestingColumn; // fiCol. This is part of a speed optimization.

  double kxp;                  //used with HIP
  double hipEstAccum;          //used with HIP
  double hipErrAccum;          //not currently used

  /**
   * Constructor with log_base2 of k.
   * @param lgK the given log_base2 of k
   */
  public CpcSketch(final int lgK) {
    this(lgK, DEFAULT_UPDATE_SEED);
  }

  /**
   * Constructor with log_base2 of k and seed.
   * @param lgK the given log_base2 of k
   * @param seed the given seed
   */
  public CpcSketch(final int lgK, final long seed) {
    checkLgK(lgK);
    this.lgK = (byte) lgK;
    this.seed = (seed != 0) ? seed : DEFAULT_UPDATE_SEED ;
    kxp = 1 << lgK;
    reset();
  }

  /**
   * Returns a copy of this sketch
   * @return a copy of this sketcch
   */
  public CpcSketch copy() {
    final CpcSketch copy = new CpcSketch(lgK, seed);
    copy.isCompressed = isCompressed;
    copy.mergeFlag = mergeFlag;
    copy.numCoupons = numCoupons;
    copy.slidingWindow = (slidingWindow == null) ? null : slidingWindow.clone();
    copy.windowOffset = windowOffset;
    copy.surprisingValueTable = surprisingValueTable.copy();
    copy.compressedWindow = (compressedWindow == null) ? null : compressedWindow.clone();
    copy.cwLength = cwLength;
    copy.numCompressedSurprisingValues = numCompressedSurprisingValues;
    copy.compressedSurprisingValues = (compressedSurprisingValues == null) ? null
        : compressedSurprisingValues.clone();
    copy.csvLength = csvLength;
    copy.firstInterestingColumn = firstInterestingColumn;
    copy.kxp = kxp;
    copy.hipEstAccum = hipEstAccum;
    copy.hipErrAccum = hipErrAccum;
    return copy;
  }

  public static Flavor determineSketchFlavor(final CpcSketch sketch) {
    return determineFlavor(sketch.lgK, sketch.numCoupons);
  }

  /**
   * Returns the HIP estimate
   * @param sketch the given sketch
   * @return the HIP estimate
   */
  public static double getHIPEstimate(final CpcSketch sketch) {
    if (sketch.mergeFlag != false) {
      throw new SketchesStateException("Failed to get HIP estimate of merged sketch");
    }
    return (sketch.hipEstAccum);
  }

  public static double getIconEstimate(final CpcSketch sketch) {
    return IconEstimator.getIconEstimate(sketch.lgK, sketch.numCoupons);
  }

  /**
   * Present the given long as a potential unique item.
   *
   * @param datum The given long datum.
   */
  public void update(final long datum) {
    final long[] data = { datum };
    final long[] arr = hash(data, seed);
    hashUpdate(arr[0], arr[1]);
  }

  /**
   * Present the given double (or float) datum as a potential unique item.
   * The double will be converted to a long using Double.doubleToLongBits(datum),
   * which normalizes all NaN values to a single NaN representation.
   * Plus and minus zero will be normalized to plus zero.
   * The special floating-point values NaN and +/- Infinity are treated as distinct.
   *
   * @param datum The given double datum.
   */
  public void update(final double datum) {
    final double d = (datum == 0.0) ? 0.0 : datum; // canonicalize -0.0, 0.0
    final long[] data = { Double.doubleToLongBits(d) };// canonicalize all NaN forms
    final long[] arr = hash(data, seed);
    hashUpdate(arr[0], arr[1]);
  }

  /**
   * Present the given String as a potential unique item.
   * The string is converted to a byte array using UTF8 encoding.
   * If the string is null or empty no update attempt is made and the method returns.
   *
   * <p>Note: About 2X faster performance can be obtained by first converting the String to a
   * char[] and updating the sketch with that. This bypasses the complexity of the Java UTF_8
   * encoding. This, of course, will not produce the same internal hash values as updating directly
   * with a String. So be consistent!  Unioning two sketches, one fed with strings and the other
   * fed with char[] will be meaningless.
   * </p>
   *
   * @param datum The given String.
   */
  public void update(final String datum) {
    if ((datum == null) || datum.isEmpty()) { return; }
    final byte[] data = datum.getBytes(UTF_8);
    final long[] arr = hash(data, seed);
    hashUpdate(arr[0], arr[1]);
  }

  /**
   * Present the given byte array as a potential unique item.
   * If the byte array is null or empty no update attempt is made and the method returns.
   *
   * @param data The given byte array.
   */
  public void update(final byte[] data) {
    if ((data == null) || (data.length == 0)) { return; }
    final long[] arr = hash(data, seed);
    hashUpdate(arr[0], arr[1]);
  }

  /**
   * Present the given char array as a potential unique item.
   * If the char array is null or empty no update attempt is made and the method returns.
   *
   * <p>Note: this will not produce the same output hash values as the {@link #update(String)}
   * method but will be a little faster as it avoids the complexity of the UTF8 encoding.</p>
   *
   * @param data The given char array.
   */
  public void update(final char[] data) {
    if ((data == null) || (data.length == 0)) { return; }
    final long[] arr = hash(data, seed);
    hashUpdate(arr[0], arr[1]);
  }

  /**
   * Present the given integer array as a potential unique item.
   * If the integer array is null or empty no update attempt is made and the method returns.
   *
   * @param data The given int array.
   */
  public void update(final int[] data) {
    if ((data == null) || (data.length == 0)) { return; }
    final long[] arr = hash(data, seed);
    hashUpdate(arr[0], arr[1]);
  }

  /**
   * Present the given long array as a potential unique item.
   * If the long array is null or empty no update attempt is made and the method returns.
   *
   * @param data The given long array.
   */
  public void update(final long[] data) {
    if ((data == null) || (data.length == 0)) { return; }
    final long[] arr = hash(data, seed);
    hashUpdate(arr[0], arr[1]);
  }

  /**
   * Resets this sketch to empty but retains the original LgK and Seed.
   */
  public final void reset() {
    isCompressed = false;
    mergeFlag = false;
    numCoupons = 0;
    slidingWindow = null;
    windowOffset = 0;
    surprisingValueTable = null;
    compressedWindow = null;
    cwLength = 0;
    numCompressedSurprisingValues = 0;
    compressedSurprisingValues = null;
    csvLength = 0;
    firstInterestingColumn = 0;
    kxp = 1 << lgK;
    hipEstAccum = 0;
    hipErrAccum = 0;
  }

  public long getNumCoupons() {
    return numCoupons;
  }

  public int getWindowOffset() {
    return windowOffset;
  }

  public double getIconEstimate() {
    return IconEstimator.getIconEstimate(lgK, numCoupons);
  }

  public double getHipEstimate() {
    return hipEstAccum;
  }

  static Flavor determineFlavor(final int lgK, final long numCoupons) {
    final long c = numCoupons;
    final long k = 1L << lgK;
    final long c2 = c << 1;
    final long c8 = c << 3;
    final long c32 = c << 5;
    if (c == 0) {
      return Flavor.EMPTY;    //    0  == C <    1
    }
    if (c32 < (3 * k)) {
      return Flavor.SPARSE;   //    1  <= C <   3K/32
    }
    if (c2 < k) {
      return Flavor.HYBRID;   // 3K/32 <= C <   K/2
    }
    if (c8 < (27 * k)) {
      return Flavor.PINNED;   //   K/2 <= C < 27K/8
    }
    else {
      return Flavor.SLIDING;  // 27K/8 <= C
    }
  }

  static int determineCorrectOffset(final int lgK, final long numCoupons) {
    final long c = numCoupons;
    final long k = (1L << lgK);
    final long tmp = (c << 3) - (19L * k);        // 8C - 19K
    if (tmp < 0) { return 0; }
    return (int) (tmp >>> (lgK + 3)); // tmp / 8K
  }

  /**
   * Warning: this is called in several places, including during the
   * transitional moments during which sketch invariants involving
   * flavor and offset are out of whack and in fact we are re-imposing
   * them. Therefore it cannot rely on determineFlavor() or
   * determineCorrectOffset(). Instead it interprets the low level data
   * structures "as is".
   *
   * <p>This produces a full-size k-by-64 bit matrix from any Live sketch.
   *
   * @param sketch the given sketch
   * @return the bit matrix as an array of longs.
   */
  static long[] bitMatrixOfSketch(final CpcSketch sketch) {
    assert (sketch.isCompressed == false);
    final int k = (1 << sketch.lgK);
    final int offset = sketch.windowOffset;
    assert (offset >= 0) && (offset <= 56);

    final long[] matrix = new long[k];

    if (sketch.numCoupons == 0) {
      return matrix; // Returning a matrix of zeros rather than NULL.
    }

    //Fill the matrix with default rows in which the "early zone" is filled with ones.
    //This is essential for the routine's O(k) time cost (as opposed to O(C)).
    final long defaultRow = (1L << offset) - 1L;
    Arrays.fill(matrix, defaultRow);

    final byte[] window = sketch.slidingWindow;
    if (window != null) { // In other words, we are in window mode, not sparse mode.
      for (int i = 0; i < k; i++) { // set the window bits, trusting the sketch's current offset.
        matrix[i] |= ((window[i] & 0XFFL) << offset);
      }
    }
    final PairTable table = sketch.surprisingValueTable;
    assert (table != null);
    final int[] slots = table.slots;
    final int numSlots = 1 << table.lgSize;

    for (int i = 0; i < numSlots; i++) {
      final int rowCol = slots[i];
      if (rowCol != -1) {
        final int col = rowCol & 63;
        final int row = rowCol >>> 6;
        // Flip the specified matrix bit from its default value.
        // In the "early" zone the bit changes from 1 to 0.
        // In the "late" zone the bit changes from 0 to 1.
        matrix[row] ^= (1L << col);
      }
    }
    return matrix;
  }

  private static void promoteEmptyToSparse(final CpcSketch sketch) {
    assert sketch.numCoupons == 0;
    assert sketch.surprisingValueTable == null;
    sketch.surprisingValueTable = new PairTable(2, 6 + sketch.lgK);
  }

  //In terms of flavor, this promotes SPARSE to HYBRID.
  private static void promoteSparseToWindowed(final CpcSketch sketch) {
    final int lgK = sketch.lgK;
    final int k = (1 << lgK);
    final long c32 = sketch.numCoupons << 5;
    assert ((c32 == (3 * k)) || ((lgK == 4) && (c32 > (3 * k))));

    final byte[] window = new byte[k];

    final PairTable newTable = new PairTable(2, 6 + lgK);
    final PairTable oldTable = sketch.surprisingValueTable;

    final int[] oldSlots = oldTable.slots;
    final int oldNumSlots = (1 << oldTable.lgSize);

    assert (sketch.windowOffset == 0);

    for (int i = 0; i < oldNumSlots; i++) {
      final int rowCol = oldSlots[i];
      if (rowCol != -1) {
        final int col = rowCol & 63;
        if (col < 8) {
          final int  row = rowCol >>> 6;
          window[row] |= (1 << col);
        }
        else {
          // cannot use Table.mustInsert(), because it doesn't provide for growth
          final boolean isNovel = PairTable.maybeInsert(newTable, rowCol);
          assert (isNovel == true);
        }
      }
    }

    assert (sketch.slidingWindow == null);
    sketch.slidingWindow = window;
    sketch.surprisingValueTable = newTable;
  }

  /**
   * The KXP register is a double with roughly 50 bits of precision, but
   * it might need roughly 90 bits to track the value with perfect accuracy.
   * Therefore we recalculate KXP occasionally from the sketch's full bitMatrix
   * so that it will reflect changes that were previously outside the mantissa.
   * @param sketch the given sketch
   * @param bitMatrix the given bit Matrix
   */

  static void refreshKXP(final CpcSketch sketch, final long[] bitMatrix) {
    final int k = (1 << sketch.lgK);

    // for improved numerical accuracy, we separately sum the bytes of the U64's
    final double[] byteSums = new double[8];

    for (int j = 0; j < 8; j++) { byteSums[j] = 0.0; }

    for (int i = 0; i < k; i++) {
      long row = bitMatrix[i];
      for (int j = 0; j < 8; j++) {
        final int byteIdx = (int) (row & 0XFFL);
        byteSums[j] += kxpByteLookup[byteIdx];
        row >>>= 8;
      }
    }

    double total = 0.0;

    for (int j = 7; j-- > 0; ) { // the reverse order is important
      final double factor = invPow2(8 * j); // pow(256, -j) == pow(2, -8 * j);
      total += factor * byteSums[j];
    }

    //  fprintf (stderr, "%.3f\n", ((double) sketch.numCoupons) / k);
    //  fprintf (stderr, "%.19g\told value of KXP\n", sketch.kxp);
    //  fprintf (stderr, "%.19g\tnew value of KXP\n", total);
    //  fflush (stderr);

    sketch.kxp = total;
  }

  /**
   * This moves the sliding window
   * @param sketch the given sketch
   * @param newOffset the new offset, which must be oldOffset + 1
   */
  private static void modifyOffset(final CpcSketch sketch, final int newOffset) {
    assert ((newOffset >= 0) && (newOffset <= 56));
    assert (newOffset == (sketch.windowOffset + 1));
    assert (newOffset == determineCorrectOffset(sketch.lgK, sketch.numCoupons));

    assert (sketch.slidingWindow != null);
    assert (sketch.surprisingValueTable != null);
    final int k = 1 << sketch.lgK;

    // Construct the full-sized bit matrix that corresponds to the sketch
    final long[] bitMatrix = bitMatrixOfSketch(sketch);
    assert bitMatrix != null;

    // refresh the KXP register on every 8th window shift.
    if ((newOffset & 0x7) == 0) { refreshKXP(sketch, bitMatrix); }

    sketch.surprisingValueTable.clear();

    final PairTable table = sketch.surprisingValueTable;
    final byte[] window = sketch.slidingWindow;
    final long maskForClearingWindow = (0XFFL << newOffset) ^ -1L;
    final long maskForFlippingEarlyZone = (1L << newOffset) - 1L;
    long allSurprisesORed = 0;

    for (int i = 0; i < k; i++) {
      long pattern = bitMatrix[i];
      window[i] = (byte) ((pattern >>> newOffset) & 0XFFL);
      pattern &= maskForClearingWindow;
      // The following line converts surprising 0's to 1's in the "early zone",
      // (and vice versa, which is essential for this procedure's O(k) time cost).
      pattern ^= maskForFlippingEarlyZone;
      allSurprisesORed |= pattern; // a cheap way to recalculate firstInterestingColumn
      while (pattern != 0) {
        //TODO use probabilistic version: countTrailingZerosInUnsignedLong(allSurprisesORed)
        final int col = Long.numberOfTrailingZeros(pattern);
        pattern = pattern ^ (1L << col); // erase the 1.
        final int rowCol = (i << 6) | col;
        final boolean isNovel = PairTable.maybeInsert(table, rowCol);
        assert isNovel == true;
      }
    }
    sketch.windowOffset = newOffset;
    sketch.firstInterestingColumn = Long.numberOfTrailingZeros(allSurprisesORed);
    if (sketch.firstInterestingColumn > newOffset) {
      sketch.firstInterestingColumn = newOffset; // corner case
    }
  }

  /**
   * Call this whenever a new coupon has been collected.
   * @param sketch the given sketch
   * @param rowCol the given row / column
   */
  private static void updateHIP(final CpcSketch sketch, final int rowCol) {
    final int k = 1 << sketch.lgK;
    final int col = rowCol & 63;
    final double oneOverP = k / sketch.kxp;
    sketch.hipEstAccum += oneOverP;
    sketch.hipErrAccum += ((oneOverP * oneOverP) - oneOverP);
    sketch.kxp -= invPow2(col + 1); // notice the "+1"
  }

  private static void updateSparse(final CpcSketch sketch, final int rowCol) {
    final int k = 1 << sketch.lgK;
    final long c32pre = sketch.numCoupons << 5;
    assert (c32pre < (3L * k)); // C < 3K/32, in other words, flavor == SPARSE
    assert (sketch.surprisingValueTable != null);
    final boolean isNovel = PairTable.maybeInsert(sketch.surprisingValueTable, rowCol);
    if (isNovel) {
      sketch.numCoupons += 1;
      updateHIP(sketch, rowCol);
      final long c32post = sketch.numCoupons << 5;
      if (c32post >= (3L * k)) { promoteSparseToWindowed(sketch); } // C >= 3K/32
    }
  }

  /**
   * The flavor is HYBRID, PINNED, or SLIDING.
   * @param sketch the given sketch
   * @param rowCol the given rowCol
   */
  private static void updateWindowed(final CpcSketch sketch, final int rowCol) {
    assert ((sketch.windowOffset >= 0) && (sketch.windowOffset <= 56));
    final int k = 1 << sketch.lgK;
    final long c32pre = sketch.numCoupons << 5;
    assert c32pre >= (3L * k); // C < 3K/32, in other words flavor >= HYBRID
    final long c8pre = sketch.numCoupons << 3;
    final int w8pre = sketch.windowOffset << 3;
    assert c8pre < ((27L + w8pre) * k); // C < (K * 27/8) + (K * windowOffset)

    boolean isNovel = false;
    final int col = rowCol & 63;

    if (col < sketch.windowOffset) { // track the surprising 0's "before" the window
      isNovel = PairTable.maybeDelete(sketch.surprisingValueTable, rowCol); // inverted logic
    }
    else if (col < (sketch.windowOffset + 8)) { // track the 8 bits inside the window
      assert (col >= sketch.windowOffset);
      final int row = rowCol >>> 6;
      final byte oldBits = sketch.slidingWindow[row];
      final byte newBits = (byte) (oldBits | (1 << (col - sketch.windowOffset)));
      if (newBits != oldBits) {
        sketch.slidingWindow[row] = newBits;
        isNovel = true;
      }
    }
    else { // track the surprising 1's "after" the window
      assert col >= (sketch.windowOffset + 8);
      isNovel = PairTable.maybeInsert(sketch.surprisingValueTable, rowCol); // normal logic
    }

    if (isNovel) {
      sketch.numCoupons += 1;
      updateHIP(sketch, rowCol);
      final long c8post = sketch.numCoupons << 3;
      if (c8post >= ((27L + w8pre) * k)) {
        modifyOffset(sketch, sketch.windowOffset + 1);
        assert (sketch.windowOffset >= 1) && (sketch.windowOffset <= 56);
        final int w8post = sketch.windowOffset << 3;
        assert c8post < ((27L + w8post) * k); // C < (K * 27/8) + (K * windowOffset)
      }
    }
  }

  //also used for testing
  void hashUpdate(final long hash0, final long hash1) {
    final int kMask = (1 << lgK) - 1;
    int col = Long.numberOfLeadingZeros(hash1);
    if (col > 63) { col = 63; } // clip so that 0 <= col <= 63
    final int row = (int) (hash0 & kMask);
    int rowCol = (row << 6) | col;
    // Avoid the hash table's "empty" value which is (2^26 -1, 63) (all ones) by changing it
    // to the pair (2^26 - 2, 63), which effectively merges the two cells.
    // This case is *extremely* unlikely, but we might as well handle it.
    // It can't happen at all if lgK (or maxLgK) < 26.
    if (rowCol == -1) { rowCol ^= (1 << 6); } //set the LSB of row to 0
    rowColUpdate(rowCol);
  }

  //also used for testing
  void rowColUpdate(final int rowCol) {
    final int col = rowCol & 63;
    if (col < firstInterestingColumn) { return; } // important speed optimization
    if (isCompressed) {
      throw new SketchesStateException("Cannot update a compressed sketch.");
    }
    final long c = numCoupons;
    if (c == 0) { promoteEmptyToSparse(this); }
    final int k = 1 << lgK;
    if ((c << 5) < (3L * k)) { updateSparse(this, rowCol); }
    else { updateWindowed(this, rowCol); }
  }

  static boolean equals(final CpcSketch skA, final CpcSketch skB, final boolean skBwasMerged) {
    rtAssertEquals(skA.lgK, skB.lgK);
    rtAssertEquals(skA.isCompressed, skB.isCompressed);
    rtAssertEquals(skA.numCoupons, skB.numCoupons);
    rtAssertEquals(skA.windowOffset, skB.windowOffset);
    rtAssertEquals(skA.cwLength, skB.cwLength);
    rtAssertEquals(skA.csvLength, skB.csvLength);
    rtAssertEquals(skA.numCompressedSurprisingValues, skB.numCompressedSurprisingValues);
    PairTable.equals(skA.surprisingValueTable, skB.surprisingValueTable);
    rtAssertEquals(skA.slidingWindow, skB.slidingWindow);
    rtAssertEquals(skA.compressedWindow, skB.compressedWindow);
    rtAssertEquals(skA.compressedSurprisingValues, skB.compressedSurprisingValues);
    final int ficolA = skA.firstInterestingColumn;
    final int ficolB = skB.firstInterestingColumn;

    if (skBwasMerged) {
      rtAssertTrue(!skA.mergeFlag && skB.mergeFlag);
      // firstInterestingColumn is only updated occasionally while stream processing.
      // NB: While not very likely, it is possible for the difference to exceed 2.
      rtAssertTrue(((ficolA + 0) == ficolB)
          || ((ficolA + 1) == ficolB)
          || ((ficolA + 2) == ficolB));
    } else {
      rtAssertEquals(skA.mergeFlag, skB.mergeFlag);
      rtAssertEquals(ficolA, ficolB);
      rtAssertEquals(skA.kxp, skB.kxp, .01 * skA.kxp); //1% tollerence
      rtAssertEquals(skA.hipEstAccum, skB.hipEstAccum, 01 * skA.hipEstAccum); //1% tollerence
    }
    return true;
  }

  /**
   * @param s value to print
   */
  static void println(final String s) {
    //System.out.println(s); //disable here
  }

}
