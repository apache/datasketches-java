/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import static com.yahoo.sketches.hll.PreambleUtil.HLL_BYTE_ARRAY_START;
import static com.yahoo.sketches.hll.PreambleUtil.extractCurMode;
import static com.yahoo.sketches.hll.PreambleUtil.extractFamilyId;
import static com.yahoo.sketches.hll.PreambleUtil.extractPreInts;
import static com.yahoo.sketches.hll.PreambleUtil.extractSerVer;
import static com.yahoo.sketches.hll.PreambleUtil.extractTgtHllType;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.SketchesArgumentException;

/**
 * This is a high performance implementation of Phillipe Flajolet&#8217;s HLL sketch but with
 * significantly improved error behavior.  If the ONLY use case for sketching is counting
 * uniques and merging, the HLL sketch is the highest performing in terms of accuracy for
 * storage space consumed. For large enough counts, this HLL version (with HLL_4) can be 2 to
 * 16 times smaller than the Theta sketch family for the same accuracy.
 *
 * <p>This implementation offers three different types of HLL sketch, each with different
 * trade-offs with accuracy, space and performance. These types are specified with the
 * {@link TgtHllType} parameter.
 *
 * <p>In terms of accuracy, all three types,for the same <i>lgConfigK</i>, have the same error
 * distribution as a function of <i>n</i>, the number of unique values fed to the sketch.
 * The configuration parameter <i>lgConfigK</i> is the log-base-2 of <i>K</i>,
 * where <i>K</i> is the number of buckets or slots for the sketch.
 *
 * <p>During warmup, when the sketch has only received a small number of unique items
 * (up to about 10% of <i>K</i>), this implementation leverages a new class of estimator
 * algorithms with significantly better accuracy.
 *
 * @author Lee Rhodes
 * @author Kevin Lang
 */
public class HllSketch extends BaseHllSketch {
  private static final String LS = System.getProperty("line.separator");
  final int lgConfigK;
  HllSketchImpl hllSketchImpl = null;

  /**
   * Standard constructor configures an HLL_4 sketch as the default.
   * @param lgConfigK The Log2 of K for the target HLL sketch. This value must be
   * between 4 and 21 inclusively.
   */
  public HllSketch(final int lgConfigK) {
    this.lgConfigK = HllUtil.checkLgK(lgConfigK);
    hllSketchImpl = new CouponList(lgConfigK, TgtHllType.HLL_4, CurMode.LIST);
  }

  /**
   * Standard constructor with the type of HLL sketch to configure.
   * @param lgConfigK The Log2 of K for the target HLL sketch. This value must be
   * between 4 and 21 inclusively.
   * @param tgtHllType the desired Hll type.
   */
  public HllSketch(final int lgConfigK, final TgtHllType tgtHllType) {
    hllSketchImpl = new CouponList(lgConfigK, tgtHllType, CurMode.LIST);
    this.lgConfigK = lgConfigK;
  }

  /**
   * Special constructor used by copyAs, heapify
   * @param that another HllSketchImpl, which must already be a copy
   */
  HllSketch(final HllSketchImpl that) {
    lgConfigK = that.getLgConfigK();
    hllSketchImpl = that;
  }

  /**
   * Copy constructor used by copy().
   * @param that another HllSketch
   */
  HllSketch(final HllSketch that) {
    lgConfigK = that.getLgConfigK();
    hllSketchImpl = that.hllSketchImpl.copy();
  }

  /**
   * Heapify the given byte array, which must be a valid HllSketch image.
   * @param byteArray the given byte array
   * @return an HllSketch
   */
  public static final HllSketch heapify(final byte[] byteArray) {
    return heapify(Memory.wrap(byteArray));
  }

  /**
   * Heapify the given Memory.
   * @param mem the given Memory
   * @return an HllSketch
   */
  public static final HllSketch heapify(final Memory mem) {
    final Object memObj = ((WritableMemory) mem).getArray();
    final long memAdd = mem.getCumulativeOffset(0);
    final CurMode curMode = checkPreamble(mem, memObj, memAdd);
    final HllSketch heapSketch;
    if (curMode == CurMode.HLL) {
      final TgtHllType tgtHllType = extractTgtHllType(memObj, memAdd);
      if (tgtHllType == TgtHllType.HLL_4) {
        heapSketch = new HllSketch(Hll4Array.heapify(mem));
      } else if (tgtHllType == TgtHllType.HLL_6) {
        heapSketch = new HllSketch(Hll6Array.heapify(mem));
      } else { //Hll_8
        heapSketch = new HllSketch(Hll8Array.heapify(mem));
      }
    } else { //LIST or SET
      heapSketch = new HllSketch(CouponList.heapify(mem, curMode));
    }
    return heapSketch;
  }

  /**
   * Return a copy of this sketch.
   * @return a copy of this sketch
   */
  public HllSketch copy() {
    return new HllSketch(this);
  }

  /**
   * Return a copy of this sketch with the specified TgtHllType
   * @param tgtHllType the TgtHllType enum
   * @return a copy of this sketch with the specified TgtHllType
   */
  public HllSketch copyAs(final TgtHllType tgtHllType) {
    return new HllSketch(hllSketchImpl.copyAs(tgtHllType));
  }

  @Override
  public double getCompositeEstimate() {
    return hllSketchImpl.getCompositeEstimate();
  }

  @Override
  CurMode getCurMode() {
    return hllSketchImpl.curMode;
  }

  @Override
  public double getEstimate() {
    return hllSketchImpl.getEstimate();
  }

  @Override
  public int getLgConfigK() {
    return hllSketchImpl.getLgConfigK();
  }

  @Override
  public int getCompactSerializationBytes() {
    return hllSketchImpl.getCurrentSerializationBytes();
  }

  @Override
  public double getLowerBound(final int numStdDev) {
    return hllSketchImpl.getLowerBound(numStdDev);
  }

  /**
   * Returns the maximum size in bytes that this sketch can grow to given lgConfigK.
   * However, for the HLL_4 sketch type, this value can be exceeded in extremely rare cases.
   * If exceeded, it will be larger by only a few percent.
   *
   * @param lgConfigK The Log2 of K for the target HLL sketch. This value must be
   * between 4 and 21 inclusively.
   * @param tgtHllType the desired Hll type
   * @return the maximum size in bytes that this sketch can grow to.
   */
  public static final int getMaxSerializationBytes(final int lgConfigK,
      final TgtHllType tgtHllType) {
    final int bytes;
    if (tgtHllType == TgtHllType.HLL_4) {
      final int auxBytes = 4 << Hll4Array.getExpectedLgAuxInts(lgConfigK);
      bytes =  HLL_BYTE_ARRAY_START + (1 << (lgConfigK - 1)) + auxBytes;
    } else if (tgtHllType == TgtHllType.HLL_6) {
      bytes = HLL_BYTE_ARRAY_START + Hll6Array.byteArrBytes(lgConfigK);
    } else { //HLL_8
      bytes = HLL_BYTE_ARRAY_START + (1 << lgConfigK);
    }
    return bytes;
  }

  @Override
  public double getRelErr(final int numStdDev) {
    return hllSketchImpl.getRelErr(numStdDev);
  }

  @Override
  public double getRelErrFactor(final int numStdDev) {
    return hllSketchImpl.getRelErrFactor(numStdDev);
  }

  @Override
  public double getUpperBound(final int numStdDev) {
    return hllSketchImpl.getUpperBound(numStdDev);
  }

  /**
   * Gets the {@link TgtHllType}
   * @return the TgtHllType enum value
   */
  public TgtHllType getTgtHllType() {
    return hllSketchImpl.tgtHllType;
  }

  @Override
  public boolean isEmpty() {
    return hllSketchImpl.isEmpty();
  }

  @Override
  boolean isOutOfOrderFlag() {
    return hllSketchImpl.oooFlag;
  }

  /**
   * Resets to empty, but does not change the configured values of lgConfigK and tgtHllType.
   */
  @Override
  public void reset() {
    hllSketchImpl = new CouponList(hllSketchImpl.lgConfigK, hllSketchImpl.tgtHllType, CurMode.LIST);
  }

  /**
   * Gets the serialization of this sketch as a byte array in compact form, which is designed
   * to be heapified only. It is not directly updatable.
   * @return the serialization of this sketch as a byte array.
   */
  @Override
  public byte[] toCompactByteArray() {
    return hllSketchImpl.toCompactByteArray();
  }

  @Override
  public byte[] toUpdatableByteArray() {
    return hllSketchImpl.toUpdatableByteArray();
  }

  @Override
  public String toString() {
    return toString(true, false, false);
  }

  @Override
  public String toString(final boolean summary, final boolean hllDetail, final boolean auxDetail) {
    final StringBuilder sb = new StringBuilder();
    if (summary) {
      sb.append("### HLL SKETCH SUMMARY: ").append(LS);
      sb.append("  Log Config K   : ").append(getLgConfigK()).append(LS);
      sb.append("  Hll Target     : ").append(getTgtHllType()).append(LS);
      sb.append("  Current Mode   : ").append(getCurrentMode()).append(LS);
      sb.append("  LB             : ").append(getLowerBound(1)).append(LS);
      sb.append("  Estimate       : ").append(getEstimate()).append(LS);
      sb.append("  UB             : ").append(getUpperBound(1)).append(LS);
      sb.append("  OutOfOrder Flag: ").append(isOutOfOrderFlag()).append(LS);
      if (getCurrentMode() == CurMode.HLL) {
        final double hipAccum = hllSketchImpl.getHipAccum();
        sb.append("  CurMin         : ").append(getCurMin()).append(LS);
        sb.append("  NumAtCurMin    : ").append(getNumAtCurMin()).append(LS);
        sb.append("  HipAccum       : ").append(hipAccum).append(LS);
      }
    }
    if (hllDetail) {
      sb.append("### HLL SKETCH HLL DETAIL: ").append(LS);
      final PairIterator pitr = getIterator();
      while (pitr.nextValid()) {
        sb.append(pitr.getString()).append(LS);
      }
    }
    if (auxDetail) {
      sb.append("### HLL SKETCH AUX DETAIL: ").append(LS);
      if ((getCurrentMode() == CurMode.HLL) && (getTgtHllType() == TgtHllType.HLL_4)) {
        final PairIterator auxItr = getAuxIterator();
        if (auxItr != null) {
          while (auxItr.nextValid()) {
            sb.append(auxItr.getString()).append(LS);
          }
        }
      }
    }
    return sb.toString();
  }

  //restricted methods

  /**
   * Gets a PairIterator over the key, value pairs of the HLL array.
   * @return a PairIterator over the key, value pairs of the HLL array.
   */
  PairIterator getIterator() {
    return hllSketchImpl.getIterator();
  }

  PairIterator getAuxIterator() {
    return hllSketchImpl.getAuxIterator();
  }

  CurMode getCurrentMode() {
    return hllSketchImpl.curMode;
  }

  int getCurMin() { //for HLL 4
    return hllSketchImpl.getCurMin();
  }

  int getNumAtCurMin() { //For all HLL
    return hllSketchImpl.getNumAtCurMin();
  }

  @Override
  void couponUpdate(final int coupon) {
    hllSketchImpl = hllSketchImpl.couponUpdate(coupon);
  }

  private static CurMode checkPreamble(final Memory mem, final Object memObj, final long memAdd) {
    final int preInts = extractPreInts(memObj, memAdd);
    final int serVer = extractSerVer(memObj, memAdd);
    final int famId = extractFamilyId(memObj, memAdd);
    final CurMode curMode = extractCurMode(memObj, memAdd);
    boolean error = false;
    if (famId != Family.HLL.getID()) { error = true; }
    if (serVer != 1) { error = true; }
    if ((preInts != 2) && (preInts != 3) && (preInts != 10)) { error = true; }
    if ((curMode == CurMode.LIST) && (preInts != 2)) { error = true; }
    if ((curMode == CurMode.SET) && (preInts != 3)) { error = true; }
    if ((curMode == CurMode.HLL) && (preInts != 10)) { error = true; }
    if (error) {
      throw new SketchesArgumentException(
          "Corrupt HLL Sketch image:\n" + PreambleUtil.toString(mem));
    }
    return curMode;
  }

}
