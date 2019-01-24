/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.cpc;

import static com.yahoo.sketches.Util.DEFAULT_UPDATE_SEED;
import static com.yahoo.sketches.Util.checkSeedHashes;
import static com.yahoo.sketches.Util.computeSeedHash;
import static com.yahoo.sketches.Util.invPow2;
import static com.yahoo.sketches.Util.zeroPad;
import static com.yahoo.sketches.cpc.CpcUtil.bitMatrixOfSketch;
import static com.yahoo.sketches.cpc.CpcUtil.checkLgK;
import static com.yahoo.sketches.cpc.CpcUtil.countBitsSetInMatrix;
import static com.yahoo.sketches.hash.MurmurHash3.hash;
import static java.lang.Math.log;
import static java.lang.Math.sqrt;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.Family;

/**
 * This is a unique-counting sketch that implements the
 * <i>Compressed Probabilistic Counting (CPC, a.k.a FM85)</i> algorithms developed by Kevin Lang in
 * his paper
 * <a href="https://arxiv.org/abs/1708.06839">Back to the Future: an Even More Nearly
 * Optimal Cardinality Estimation Algorithm</a>.
 *
 * <p>This sketch is extremely space-efficient when serialized. In an apples-to-apples empirical
 * comparison against compressed HyperLogLog sketches, this new algorithm simultaneously wins on
 * the two dimensions of the space/accuracy tradeoff and produces sketches that are
 * smaller than the entropy of HLL, so no possible implementation of compressed HLL can match its
 * space efficiency for a given accuracy. As described in the paper this sketch implements a newly
 * developed ICON estimator algorithm that survives unioning operations, another
 * well-known estimator, the
 * <a href="https://arxiv.org/abs/1306.3284">Historical Inverse Probability (HIP)</a> estimator
 * does not.
 * The update speed performance of this sketch is quite fast and is comparable to the speed of HLL.
 * The unioning (merging) capability of this sketch also allows for merging of sketches with
 * different configurations of K.
 *
 * <p>For additional security this sketch can be configured with a user-specified hash seed.
 *
 * @author Lee Rhodes
 * @author Kevin Lang
 */
public final class CpcSketch {
  private static final String LS = System.getProperty("line.separator");
  private static final double[] kxpByteLookup = new double[256];
  public static final int DEFAULT_LG_K = 11;
  final long seed;
  //common variables
  final int lgK;
  long numCoupons;      // The number of coupons collected so far.
  boolean mergeFlag;    // Is the sketch the result of merging?
  int fiCol; // First Interesting Column. This is part of a speed optimization.

  int windowOffset;
  byte[] slidingWindow; //either null or size K bytes
  PairTable pairTable; //for sparse and surprising values, either null or variable size

  //The following variables are only valid in HIP varients
  double kxp;                  //used with HIP
  double hipEstAccum;          //used with HIP

  /**
   * Constructor with default log_base2 of k
   */
  public CpcSketch() {
    this(DEFAULT_LG_K, DEFAULT_UPDATE_SEED);
  }

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
    this.seed = seed;
    kxp = 1 << lgK;
    reset();
  }

  /**
   * Returns a copy of this sketch
   * @return a copy of this sketch
   */
  CpcSketch copy() {
    final CpcSketch copy = new CpcSketch(lgK, seed);
    copy.numCoupons = numCoupons;
    copy.mergeFlag = mergeFlag;
    copy.fiCol = fiCol;

    copy.windowOffset = windowOffset;
    copy.slidingWindow = (slidingWindow == null) ? null : slidingWindow.clone();
    copy.pairTable = (pairTable == null) ? null : pairTable.copy();

    copy.kxp = kxp;
    copy.hipEstAccum = hipEstAccum;
    return copy;
  }

  /**
   * Returns the best estimate of the cardinality of the sketch.
   * @return the best estimate of the cardinality of the sketch.
   */
  public double getEstimate() {
    if (mergeFlag) { return IconEstimator.getIconEstimate(lgK, numCoupons); }
    return hipEstAccum;
  }

  /**
   * Return the DataSketches identifier for this CPC family of sketches.
   * @return the DataSketches identifier for this CPC family of sketches.
   */
  public static Family getFamily() {
    return Family.CPC;
  }

  /**
   * Return the parameter LgK.
   * @return the parameter LgK.
   */
  public int getLgK() {
    return lgK;
  }

  /**
   * Returns the best estimate of the lower bound of the confidence interval given <i>kappa</i>,
   * the number of standard deviations from the mean.
   * @param kappa the given number of standard deviations from the mean: 1, 2 or 3.
   * @return the best estimate of the lower bound of the confidence interval given <i>kappa</i>.
   */
  public double getLowerBound(final int kappa) {
    if (mergeFlag) {
      return CpcConfidence.getIconConfidenceLB(lgK, numCoupons, kappa);
    }
    return CpcConfidence.getHipConfidenceLB(lgK, numCoupons, hipEstAccum, kappa);
  }

  /*
   * These empirical values for the 99.9th percentile of size in bytes were measured using 100,000
   * trials. The value for each trial is the maximum of 5*16=80 measurements that were equally
   * spaced over values of the quantity C/K between 3.0 and 8.0.  This table does not include the
   * worst-case space for the preamble, which is added by the function.
   */
  private static final int[] empiricalMaxBytes  = {
      24,     // lgK = 4
      36,     // lgK = 5
      56,     // lgK = 6
      100,    // lgK = 7
      180,    // lgK = 8
      344,    // lgK = 9
      660,    // lgK = 10
      1292,   // lgK = 11
      2540,   // lgK = 12
      5020,   // lgK = 13
      9968,   // lgK = 14
      19836,  // lgK = 15
      39532,  // lgK = 16
      78880,  // lgK = 17
      157516, // lgK = 18
      314656  // lgK = 19
  };

  /**
   * The actual size of a compressed CPC sketch has a small random variance, but the following
   * empirically measured size should be large enough for at least 99.9 percent of sketches.
   *
   * <p>For small values of <i>n</i> the size can be much smaller.
   *
   * @param lgK the given value of lgK.
   * @return the estimated maximum compressed serialized size of a sketch.
   */
  public static int getMaxSerializedBytes(final int lgK) {
    checkLgK(lgK);
    if (lgK <= 19) { return empiricalMaxBytes[lgK - 4] + 40; }
    final int k = 1 << lgK;
    return (int) (0.6 * k) + 40; // 0.6 = 4.8 / 8.0
  }

  /**
   * Returns the best estimate of the upper bound of the confidence interval given <i>kappa</i>,
   * the number of standard deviations from the mean.
   * @param kappa the given number of standard deviations from the mean: 1, 2 or 3.
   * @return the best estimate of the upper bound of the confidence interval given <i>kappa</i>.
   */
  public double getUpperBound(final int kappa) {
    if (mergeFlag) {
      return CpcConfidence.getIconConfidenceUB(lgK, numCoupons, kappa);
    }
    return CpcConfidence.getHipConfidenceUB(lgK, numCoupons, hipEstAccum, kappa);
  }

  /**
   * Return the given Memory as a CpcSketch on the Java heap using the DEFAULT_UPDATE_SEED.
   * @param mem the given Memory
   * @return the given Memory as a CpcSketch on the Java heap.
   */
  public static CpcSketch heapify(final Memory mem) {
    return heapify(mem, DEFAULT_UPDATE_SEED);
  }

  /**
   * Return the given byte array as a CpcSketch on the Java heap using the DEFAULT_UPDATE_SEED.
   * @param byteArray the given byte array
   * @return the given byte array as a CpcSketch on the Java heap.
   */
  public static CpcSketch heapify(final byte[] byteArray) {
    return heapify(byteArray, DEFAULT_UPDATE_SEED);
  }

  /**
   * Return the given Memory as a CpcSketch on the Java heap.
   * @param mem the given Memory
   * @param seed the seed used to create the original sketch from which the Memory was derived.
   * @return the given Memory as a CpcSketch on the Java heap.
   */
  public static CpcSketch heapify(final Memory mem, final long seed) {
    final CompressedState state = CompressedState.importFromMemory(mem);
    return uncompress(state, seed);
  }

  /**
   * Return the given byte array as a CpcSketch on the Java heap.
   * @param byteArray the given byte array
   * @param seed the seed used to create the original sketch from which the byte array was derived.
   * @return the given byte array as a CpcSketch on the Java heap.
   */
  public static CpcSketch heapify(final byte[] byteArray, final long seed) {
    final Memory mem = Memory.wrap(byteArray);
    return heapify(mem, seed);
  }

  /**
   * Return true if this sketch is empty
   * @return true if this sketch is empty
   */
  public boolean isEmpty() {
    return numCoupons == 0;
  }

  /**
   * Resets this sketch to empty but retains the original LgK and Seed.
   */
  public final void reset() {
    numCoupons = 0;
    mergeFlag = false;
    fiCol = 0;

    windowOffset = 0;
    slidingWindow = null;
    pairTable = null;

    kxp = 1 << lgK;
    hipEstAccum = 0;
  }

  /**
   * Return this sketch as a compressed byte array.
   * @return this sketch as a compressed byte array.
   */
  public byte[] toByteArray() {
    final CompressedState state = CompressedState.compress(this);
    final long cap = state.getRequiredSerializedBytes();
    final WritableMemory wmem = WritableMemory.allocate((int) cap);
    state.exportToMemory(wmem);
    return (byte[]) wmem.getArray();
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
   * Convience function that this Sketch is valid. This is a troubleshooting tool
   * for sketches that have been heapified from serialized images.
   *
   * <p>If you are starting with a serialized image as a byte array, first heapify
   * the byte array to a sketch, which performs a number of checks. Then use this
   * function as one additional check on the sketch.</p>
   *
   * @return true if this sketch is validated.
   */
  public boolean validate() {
    final long[] bitMatrix = bitMatrixOfSketch(this);
    final long matrixCoupons = countBitsSetInMatrix(bitMatrix);
    return matrixCoupons == numCoupons;
  }

  /**
   * Returns the current Flavor of this sketch.
   * @return the current Flavor of this sketch.
   */
  Flavor getFlavor() {
    return CpcUtil.determineFlavor(lgK, numCoupons);
  }

  /**
   * Returns the Format of the serialized form of this sketch.
   * @return the Format of the serialized form of this sketch.
   */
  Format getFormat() {
    final int ordinal;
    final Flavor f = getFlavor();
    if ((f == Flavor.HYBRID) || (f == Flavor.SPARSE)) {
      ordinal = 2 | ( mergeFlag ? 0 : 1 ); //Hybrid is serialized as SPARSE
    } else {
      ordinal = ((slidingWindow != null) ? 4 : 0)
               | (((pairTable != null) && (pairTable.getNumPairs() > 0)) ? 2 : 0)
               | ( mergeFlag ? 0 : 1 );
    }
    return Format.ordinalToFormat(ordinal);
  }

  private static void promoteEmptyToSparse(final CpcSketch sketch) {
    assert sketch.numCoupons == 0;
    assert sketch.pairTable == null;
    sketch.pairTable = new PairTable(2, 6 + sketch.lgK);
  }

  //In terms of flavor, this promotes SPARSE to HYBRID.
  private static void promoteSparseToWindowed(final CpcSketch sketch) {
    final int lgK = sketch.lgK;
    final int k = (1 << lgK);
    final long c32 = sketch.numCoupons << 5;
    assert ((c32 == (3 * k)) || ((lgK == 4) && (c32 > (3 * k))));

    final byte[] window = new byte[k];

    final PairTable newTable = new PairTable(2, 6 + lgK);
    final PairTable oldTable = sketch.pairTable;

    final int[] oldSlots = oldTable.getSlotsArr();
    final int oldNumSlots = (1 << oldTable.getLgSizeInts());

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
    sketch.pairTable = newTable;
  }

  /**
   * The KXP register is a double with roughly 50 bits of precision, but
   * it might need roughly 90 bits to track the value with perfect accuracy.
   * Therefore we recalculate KXP occasionally from the sketch's full bitMatrix
   * so that it will reflect changes that were previously outside the mantissa.
   * @param sketch the given sketch
   * @param bitMatrix the given bit Matrix
   */
  //Also used in test
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
    assert (newOffset == CpcUtil.determineCorrectOffset(sketch.lgK, sketch.numCoupons));

    assert (sketch.slidingWindow != null);
    assert (sketch.pairTable != null);
    final int k = 1 << sketch.lgK;

    // Construct the full-sized bit matrix that corresponds to the sketch
    final long[] bitMatrix = CpcUtil.bitMatrixOfSketch(sketch);

    // refresh the KXP register on every 8th window shift.
    if ((newOffset & 0x7) == 0) { refreshKXP(sketch, bitMatrix); }

    sketch.pairTable.clear();

    final PairTable table = sketch.pairTable;
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
      allSurprisesORed |= pattern; // a cheap way to recalculate fiCol
      while (pattern != 0) {
        final int col = Long.numberOfTrailingZeros(pattern);
        pattern = pattern ^ (1L << col); // erase the 1.
        final int rowCol = (i << 6) | col;
        final boolean isNovel = PairTable.maybeInsert(table, rowCol);
        assert isNovel == true;
      }
    }
    sketch.windowOffset = newOffset;
    sketch.fiCol = Long.numberOfTrailingZeros(allSurprisesORed);
    if (sketch.fiCol > newOffset) {
      sketch.fiCol = newOffset; // corner case
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
    sketch.kxp -= invPow2(col + 1); // notice the "+1"
  }

  private static void updateSparse(final CpcSketch sketch, final int rowCol) {
    final int k = 1 << sketch.lgK;
    final long c32pre = sketch.numCoupons << 5;
    assert (c32pre < (3L * k)); // C < 3K/32, in other words, flavor == SPARSE
    assert (sketch.pairTable != null);
    final boolean isNovel = PairTable.maybeInsert(sketch.pairTable, rowCol);
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

    boolean isNovel = false; //novel if new coupon
    final int col = rowCol & 63;

    if (col < sketch.windowOffset) { // track the surprising 0's "before" the window
      isNovel = PairTable.maybeDelete(sketch.pairTable, rowCol); // inverted logic
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
      isNovel = PairTable.maybeInsert(sketch.pairTable, rowCol); // normal logic
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


  //also used in test
  static CpcSketch uncompress(final CompressedState source, final long seed) {
    checkSeedHashes(computeSeedHash(seed), source.seedHash);
    final CpcSketch sketch = new CpcSketch(source.lgK, seed);
    sketch.numCoupons = source.numCoupons;
    sketch.windowOffset = source.getWindowOffset();
    sketch.fiCol = source.fiCol;
    sketch.mergeFlag = source.mergeFlag;
    sketch.kxp = source.kxp;
    sketch.hipEstAccum = source.hipEstAccum;
    sketch.slidingWindow = null;
    sketch.pairTable = null;
    CpcCompression.uncompress(source, sketch);
    return sketch;
  }

  //Used here and for testing
  void hashUpdate(final long hash0, final long hash1) {
    int col = Long.numberOfLeadingZeros(hash1);
    if (col < fiCol) { return; } // important speed optimization
    if (col > 63) { col = 63; } // clip so that 0 <= col <= 63
    final long c = numCoupons;
    if (c == 0) { promoteEmptyToSparse(this); }
    final long k = 1L << lgK;
    final int row = (int) (hash0 & (k - 1L));
    int rowCol = (row << 6) | col;

    // Avoid the hash table's "empty" value which is (2^26 -1, 63) (all ones) by changing it
    // to the pair (2^26 - 2, 63), which effectively merges the two cells.
    // This case is *extremely* unlikely, but we might as well handle it.
    // It can't happen at all if lgK (or maxLgK) < 26.
    if (rowCol == -1) { rowCol ^= (1 << 6); } //set the LSB of row to 0

    if ((c << 5) < (3L * k)) { updateSparse(this, rowCol); }
    else { updateWindowed(this, rowCol); }
  }

  //Used by union and in testing
  void rowColUpdate(final int rowCol) {
    final int col = rowCol & 63;
    if (col < fiCol) { return; } // important speed optimization
    final long c = numCoupons;
    if (c == 0) { promoteEmptyToSparse(this); }
    final long k = 1L << lgK;
    if ((c << 5) < (3L * k)) { updateSparse(this, rowCol); }
    else { updateWindowed(this, rowCol); }
  }

  /**
   * Return a human-readable string summary of this sketch
   */
  @Override
  public String toString() {
    return toString(false);
  }

  /**
   * Return a human-readable string summary of this sketch
   * @param detail include data detail
   * @return a human-readable string summary of this sketch
   */
  public String toString(final boolean detail) {
    final int numPairs = (pairTable == null) ? 0 : pairTable.getNumPairs();
    final int seedHash = Short.toUnsignedInt(computeSeedHash(seed));
    final double errConst = mergeFlag ? log(2) : sqrt(log(2) / 2.0);
    final double rse = errConst / Math.sqrt(1 << lgK);
    final StringBuilder sb = new StringBuilder();
    sb.append("### CPD SKETCH - PREAMBLE:").append(LS);
    sb.append("  Flavor         : ").append(getFlavor()).append(LS);
    sb.append("  LgK            : ").append(lgK).append(LS);
    sb.append("  Merge Flag     : ").append(mergeFlag).append(LS);
    sb.append("  Error Const    : ").append(errConst).append(LS);
    sb.append("  RSE            : ").append(rse).append(LS);
    sb.append("  Seed Hash      : ").append(Integer.toHexString(seedHash))
      .append(" | ").append(seedHash).append(LS);
    sb.append("  Num Coupons    : ").append(numCoupons).append(LS);
    sb.append("  Num Pairs (SV) : ").append(numPairs).append(LS);
    sb.append("  First Inter Col: ").append(fiCol).append(LS);
    sb.append("  Valid Window   : ").append(slidingWindow != null).append(LS);
    sb.append("  Valid PairTable: ").append(pairTable != null).append(LS);
    sb.append("  Window Offset  : ").append(windowOffset).append(LS);
    sb.append("  KxP            : ").append(kxp).append(LS);
    sb.append("  HIP Accum      : ").append(hipEstAccum).append(LS);
    if (detail) {
      sb.append(LS).append("### CPC SKETCH - DATA").append(LS);
      if (pairTable != null) {
        sb.append(pairTable.toString(true));
      }
      if (slidingWindow != null) {
        sb.append("SlidingWindow  : ").append(LS);
        sb.append("    Index Bits (lsb ->)").append(LS);
        for (int i = 0; i < slidingWindow.length; i++) {

          final String bits = zeroPad(Integer.toBinaryString(slidingWindow[i] & 0XFF), 8);
          sb.append(String.format("%9d %8s" + LS, i, bits));
        }
      }
    }
    sb.append("### END CPC SKETCH");
    return sb.toString();
  }

  /**
   * Returns a human readable string of the preamble of a byte array image of a CpcSketch.
   * @param byteArr the given byte array
   * @param detail if true, a dump of the compressed window and surprising value streams will be
   * included.
   * @return a human readable string of the preamble of a byte array image of a CpcSketch.
   */
  public static String toString(final byte[] byteArr, final boolean detail) {
    return PreambleUtil.toString(byteArr, detail);
  }

  /**
   * Returns a human readable string of the preamble of a Memory image of a CpcSketch.
   * @param mem the given Memory
   * @param detail if true, a dump of the compressed window and surprising value streams will be
   * included.
   * @return a human readable string of the preamble of a Memory image of a CpcSketch.
   */
  public static String toString(final Memory mem, final boolean detail) {
    return PreambleUtil.toString(mem, detail);
  }

  private static void fillKxpByteLookup() { //called from static initializer
    for (int b = 0; b < 256; b++) {
      double sum = 0;
      for (int col = 0; col < 8; col++) {
        final int bit = (b >>> col) & 1;
        if (bit == 0) { // note the inverted logic
          sum += invPow2(col + 1); //note the "+1"
        }
      }
      kxpByteLookup[b] = sum;
    }
  }

  static {
    fillKxpByteLookup();
  }

}
