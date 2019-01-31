/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import static com.yahoo.sketches.hll.HllUtil.EMPTY;
import static com.yahoo.sketches.hll.HllUtil.LG_AUX_ARR_INTS;
import static com.yahoo.sketches.hll.HllUtil.checkPreamble;
import static com.yahoo.sketches.hll.PreambleUtil.HLL_BYTE_ARR_START;
import static com.yahoo.sketches.hll.PreambleUtil.extractCompactFlag;
import static com.yahoo.sketches.hll.PreambleUtil.extractLgK;
import static com.yahoo.sketches.hll.PreambleUtil.extractTgtHllType;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;
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
 * <p>In terms of accuracy, all three types, for the same <i>lgConfigK</i>, have the same error
 * distribution as a function of <i>n</i>, the number of unique values fed to the sketch.
 * The configuration parameter <i>lgConfigK</i> is the log-base-2 of <i>K</i>,
 * where <i>K</i> is the number of buckets or slots for the sketch.
 *
 * <p>During warmup, when the sketch has only received a small number of unique items
 * (up to about 10% of <i>K</i>), this implementation leverages a new class of estimator
 * algorithms with significantly better accuracy.
 *
 * <p>This sketch also offers the capability of operating off-heap. Given a WritableMemory object
 * created by the user, the sketch will perform all of its updates and internal phase transitions
 * in that object, which can actually reside either on-heap or off-heap based on how it is
 * configured. In large systems that must update and merge many millions of sketches, having the
 * sketch operate off-heap avoids the serialization and deserialization costs of moving sketches
 * to and from off-heap memory-mapped files, for example, and eliminates big garbage collection
 * delays.
 *
 * @author Lee Rhodes
 * @author Kevin Lang
 */
public class HllSketch extends BaseHllSketch {
  public static final int DEFAULT_LG_K = 12;
  public static final TgtHllType DEFAULT_HLL_TYPE = TgtHllType.HLL_4;

  private static final String LS = System.getProperty("line.separator");
  HllSketchImpl hllSketchImpl = null;

  /**
   * Constructs a new on-heap sketch with the default lgConfigK and tgtHllType.
   */
  public HllSketch() {
    this(DEFAULT_LG_K, DEFAULT_HLL_TYPE);
  }

  /**
   * Constructs a new on-heap sketch with the default tgtHllType.
   * @param lgConfigK The Log2 of K for the target HLL sketch. This value must be
   * between 4 and 21 inclusively.
   */
  public HllSketch(final int lgConfigK) {
    this(lgConfigK, DEFAULT_HLL_TYPE);
  }

  /**
   * Constructs a new on-heap sketch with the type of HLL sketch to configure.
   * @param lgConfigK The Log2 of K for the target HLL sketch. This value must be
   * between 4 and 21 inclusively.
   * @param tgtHllType the desired Hll type.
   */
  public HllSketch(final int lgConfigK, final TgtHllType tgtHllType) {
    hllSketchImpl = new CouponList(HllUtil.checkLgK(lgConfigK), tgtHllType, CurMode.LIST);
  }

  /**
   * Constructs a new sketch with the type of HLL sketch to configure and the given
   * WritableMemory as the destination for the sketch. This WritableMemory is usually configured
   * for off-heap memory. What remains on the java heap is a thin wrapper object that reads and
   * writes to the given WritableMemory.
   *
   * <p>The given <i>dstMem</i> is checked for the required capacity as determined by
   * {@link #getMaxUpdatableSerializationBytes(int, TgtHllType)}.
   * @param lgConfigK The Log2 of K for the target HLL sketch. This value must be
   * between 4 and 21 inclusively.
   * @param tgtHllType the desired Hll type.
   * @param dstMem the destination memory for the sketch.
   */
  public HllSketch(final int lgConfigK, final TgtHllType tgtHllType, final WritableMemory dstMem) {
    final long minBytes = getMaxUpdatableSerializationBytes(lgConfigK, tgtHllType);
    final long capBytes = dstMem.getCapacity();
    HllUtil.checkMemSize(minBytes, capBytes);
    dstMem.clear(0, minBytes);
    hllSketchImpl = DirectCouponList.newInstance(lgConfigK, tgtHllType, dstMem);
  }

  /**
   * Copy constructor used by copy().
   * @param that another HllSketch
   */
  HllSketch(final HllSketch that) {
    hllSketchImpl = that.hllSketchImpl.copy();
  }

  /**
   * Special constructor used by copyAs, heapify
   * @param that another HllSketchImpl, which must already be a copy
   */
  HllSketch(final HllSketchImpl that) {
    hllSketchImpl = that;
  }

  /**
   * Heapify the given byte array, which must be a valid HllSketch image and may have data.
   * @param byteArray the given byte array.  This byteArray is not modified and is not retained
   * by the on-heap sketch.
   * @return an HllSketch on the java heap.
   */
  public static final HllSketch heapify(final byte[] byteArray) {
    return heapify(Memory.wrap(byteArray));
  }

  /**
   * Heapify the given Memory, which must be a valid HllSketch image and may have data.
   * @param srcMem the given Memory, which is read-only.
   * @return an HllSketch on the java heap.
   */
  public static final HllSketch heapify(final Memory srcMem) {
    final CurMode curMode = checkPreamble(srcMem);
    final HllSketch heapSketch;
    if (curMode == CurMode.HLL) {
      final TgtHllType tgtHllType = extractTgtHllType(srcMem);
      if (tgtHllType == TgtHllType.HLL_4) {
        heapSketch = new HllSketch(Hll4Array.heapify(srcMem));
      } else if (tgtHllType == TgtHllType.HLL_6) {
        heapSketch = new HllSketch(Hll6Array.heapify(srcMem));
      } else { //Hll_8
        heapSketch = new HllSketch(Hll8Array.heapify(srcMem));
      }
    } else if (curMode == CurMode.LIST) {
      heapSketch = new HllSketch(CouponList.heapifyList(srcMem));
    } else {
      heapSketch = new HllSketch(CouponHashSet.heapifySet(srcMem));
    }
    return heapSketch;
  }

  /**
   * Wraps the given WritableMemory, which must be a image of a valid updatable sketch,
   * and may have data. What remains on the java heap is a
   * thin wrapper object that reads and writes to the given WritableMemory, which, depending on
   * how the user configures the WritableMemory, may actually reside on the Java heap or off-heap.
   *
   * <p>The given <i>dstMem</i> is checked for the required capacity as determined by
   * {@link #getMaxUpdatableSerializationBytes(int, TgtHllType)}.
   * @param wmem an writable image of a valid sketch with data.
   * @return an HllSketch where the sketch data is in the given dstMem.
   */
  public static final HllSketch writableWrap(final WritableMemory wmem) {
    final boolean compact = extractCompactFlag(wmem);
    if (compact) {
      throw new SketchesArgumentException(
          "Cannot perform a writableWrap of a writable sketch image that is in compact form.");
    }
    final int lgConfigK = extractLgK(wmem);
    final TgtHllType tgtHllType = extractTgtHllType(wmem);
    final long minBytes = getMaxUpdatableSerializationBytes(lgConfigK, tgtHllType);
    final long capBytes = wmem.getCapacity();
    HllUtil.checkMemSize(minBytes, capBytes);

    final CurMode curMode = checkPreamble(wmem);
    final HllSketch directSketch;
    if (curMode == CurMode.HLL) {
      if (tgtHllType == TgtHllType.HLL_4) {
        directSketch = new HllSketch(new DirectHll4Array(lgConfigK, wmem));
      } else if (tgtHllType == TgtHllType.HLL_6) {
        directSketch = new HllSketch(new DirectHll6Array(lgConfigK, wmem));
      } else { //Hll_8
        directSketch = new HllSketch(new DirectHll8Array(lgConfigK, wmem));
      }
    } else if (curMode == CurMode.LIST) {
      directSketch =
          new HllSketch(new DirectCouponList(lgConfigK, tgtHllType, curMode, wmem));
    } else {
      directSketch =
          new HllSketch(new DirectCouponHashSet(lgConfigK, tgtHllType, wmem));
    }
    return directSketch;
  }

  /**
   * Wraps the given read-only Memory that must be a image of a valid sketch,
   * which may be in compact or updatable form, and should have data. Any attempt to update this
   * sketch will throw an exception.
   * @param srcMem a read-only image of a valid sketch.
   * @return an HllSketch, where the read-only data of the sketch is in the given srcMem.
   *
   */
  public static final HllSketch wrap(final Memory srcMem) {
    final int lgConfigK = extractLgK(srcMem);
    final TgtHllType tgtHllType = extractTgtHllType(srcMem);

    final CurMode curMode = checkPreamble(srcMem);
    final HllSketch directSketch;
    if (curMode == CurMode.HLL) {
      if (tgtHllType == TgtHllType.HLL_4) {
        directSketch = new HllSketch(new DirectHll4Array(lgConfigK, srcMem));
      } else if (tgtHllType == TgtHllType.HLL_6) {
        directSketch = new HllSketch(new DirectHll6Array(lgConfigK, srcMem));
      } else { //Hll_8
        directSketch = new HllSketch(new DirectHll8Array(lgConfigK, srcMem));
      }
    } else if (curMode == CurMode.LIST) {
      directSketch =
          new HllSketch(new DirectCouponList(lgConfigK, tgtHllType, curMode, srcMem));
    } else { //SET
      directSketch =
          new HllSketch(new DirectCouponHashSet(lgConfigK, tgtHllType, srcMem));
    }
    return directSketch;
  }

  /**
   * Return a copy of this sketch onto the Java heap.
   * @return a copy of this sketch onto the Java heap.
   */
  public HllSketch copy() {
    return new HllSketch(this);
  }

  /**
   * Return a deep copy of this sketch onto the Java heap with the specified TgtHllType.
   * @param tgtHllType the TgtHllType enum
   * @return a deep copy of this sketch with the specified TgtHllType.
   */
  public HllSketch copyAs(final TgtHllType tgtHllType) {
    return new HllSketch(hllSketchImpl.copyAs(tgtHllType));
  }

  @Override
  public double getCompositeEstimate() {
    return hllSketchImpl.getCompositeEstimate();
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
    return hllSketchImpl.getCompactSerializationBytes();
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
  public static final int getMaxUpdatableSerializationBytes(final int lgConfigK,
      final TgtHllType tgtHllType) {
    final int arrBytes;
    if (tgtHllType == TgtHllType.HLL_4) {
      final int auxBytes = 4 << LG_AUX_ARR_INTS[lgConfigK];
      arrBytes =  AbstractHllArray.hll4ArrBytes(lgConfigK) + auxBytes;
    }
    else if (tgtHllType == TgtHllType.HLL_6) {
      arrBytes = AbstractHllArray.hll6ArrBytes(lgConfigK);
    }
    else { //HLL_8
      arrBytes = AbstractHllArray.hll8ArrBytes(lgConfigK);
    }
    return HLL_BYTE_ARR_START + arrBytes;
  }

  @Override
  public TgtHllType getTgtHllType() {
    return hllSketchImpl.getTgtHllType();
  }

  @Override
  public int getUpdatableSerializationBytes() {
    return hllSketchImpl.getUpdatableSerializationBytes();
  }

  @Override
  public double getUpperBound(final int numStdDev) {
    return hllSketchImpl.getUpperBound(numStdDev);
  }

  @Override
  public boolean isCompact() {
    return hllSketchImpl.isCompact();
  }

  @Override
  public boolean isEmpty() {
    return hllSketchImpl.isEmpty();
  }

  @Override
  public boolean isMemory() {
    return hllSketchImpl.isMemory();
  }

  @Override
  public boolean isOffHeap() {
    return hllSketchImpl.isOffHeap();
  }

  @Override
  boolean isOutOfOrderFlag() {
    return hllSketchImpl.isOutOfOrderFlag();
  }

  @Override
  public boolean isSameResource(final Memory mem) {
    return hllSketchImpl.isSameResource(mem);
  }

  @Override
  public void reset() {
    hllSketchImpl = hllSketchImpl.reset();
  }

  @Override
  public byte[] toCompactByteArray() {
    return hllSketchImpl.toCompactByteArray();
  }

  @Override
  public byte[] toUpdatableByteArray() {
    return hllSketchImpl.toUpdatableByteArray();
  }

  @Override
  public String toString(final boolean summary, final boolean detail, final boolean auxDetail,
      final boolean all) {
    final StringBuilder sb = new StringBuilder();
    if (summary) {
      sb.append("### HLL SKETCH SUMMARY: ").append(LS);
      sb.append("  Log Config K   : ").append(getLgConfigK()).append(LS);
      sb.append("  Hll Target     : ").append(getTgtHllType()).append(LS);
      sb.append("  Current Mode   : ").append(getCurMode()).append(LS);
      sb.append("  LB             : ").append(getLowerBound(1)).append(LS);
      sb.append("  Estimate       : ").append(getEstimate()).append(LS);
      sb.append("  UB             : ").append(getUpperBound(1)).append(LS);
      sb.append("  OutOfOrder Flag: ").append(isOutOfOrderFlag()).append(LS);
      if (getCurMode() == CurMode.HLL) {
        final AbstractHllArray absHll = (AbstractHllArray) hllSketchImpl;
        sb.append("  CurMin         : ").append(absHll.getCurMin()).append(LS);
        sb.append("  NumAtCurMin    : ").append(absHll.getNumAtCurMin()).append(LS);
        sb.append("  HipAccum       : ").append(absHll.getHipAccum()).append(LS);
        sb.append("  KxQ0           : ").append(absHll.getKxQ0()).append(LS);
        sb.append("  KxQ1           : ").append(absHll.getKxQ1()).append(LS);
      } else {
        sb.append("  Coupon Count   : ")
          .append(((AbstractCoupons)hllSketchImpl).getCouponCount()).append(LS);
      }
    }
    if (detail) {
      sb.append("### HLL SKETCH DATA DETAIL: ").append(LS);
      final PairIterator pitr = iterator();
      sb.append(pitr.getHeader()).append(LS);
      if (all) {
        while (pitr.nextAll()) {
          sb.append(pitr.getString()).append(LS);
        }
      } else {
        while (pitr.nextValid()) {
          sb.append(pitr.getString()).append(LS);
        }
      }
    }
    if (auxDetail) {
      if ((getCurMode() == CurMode.HLL) && (getTgtHllType() == TgtHllType.HLL_4)) {
        final AbstractHllArray absHll = (AbstractHllArray) hllSketchImpl;
        final PairIterator auxItr = absHll.getAuxIterator();
        if (auxItr != null) {
          sb.append("### HLL SKETCH AUX DETAIL: ").append(LS);
          sb.append(auxItr.getHeader()).append(LS);
          if (all) {
            while (auxItr.nextAll()) {
              sb.append(auxItr.getString()).append(LS);
            }
          } else {
            while (auxItr.nextValid()) {
              sb.append(auxItr.getString()).append(LS);
            }
          }
        }
      }
    }
    return sb.toString();
  }

  /**
   * Returns a human readable string of the preamble of a byte array image of an HllSketch.
   * @param byteArr the given byte array
   * @return a human readable string of the preamble of a byte array image of an HllSketch.
   */
  public static String toString(final byte[] byteArr) {
    return PreambleUtil.toString(byteArr);
  }

  /**
   * Returns a human readable string of the preamble of a Memory image of an HllSketch.
   * @param mem the given Memory object
   * @return a human readable string of the preamble of a Memory image of an HllSketch.
   */
  public static String toString(final Memory mem) {
    return PreambleUtil.toString(mem);
  }

  //restricted methods

  /**
   * Returns a PairIterator over the key, value pairs of the HLL array.
   * @return a PairIterator over the key, value pairs of the HLL array.
   */
  PairIterator iterator() {
    return hllSketchImpl.iterator();
  }

  @Override
  CurMode getCurMode() {
    return hllSketchImpl.getCurMode();
  }

  @Override
  void couponUpdate(final int coupon) {
    if (coupon == EMPTY) { return; }
    hllSketchImpl = hllSketchImpl.couponUpdate(coupon);
  }

}
