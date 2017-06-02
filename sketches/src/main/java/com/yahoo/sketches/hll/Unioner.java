/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import static com.yahoo.sketches.hll.CurMode.HLL;
import static com.yahoo.sketches.hll.TgtHllType.HLL_8;
import static java.lang.Math.min;

/**
 * This performs union operations for HLL sketches. This unioner is configured with a <i>lgMaxK</i>
 * instead of the normal <i>lgConfigK</i>.  If this unioner is presented with sketches that have
 * a different <i>lgConfigK</i> than <i>lgMaxK</i>, the internal state of the unioner will
 * assume the smaller of the two values.
 *
 * <p>This unioner, therefore, does permit the unioning of sketches with different values of
 * <i>lgConfigK</i>.  The user should be aware that the resulting accuracy of a sketch returned
 * at the end of the unioning process will be a function of the smallest <i>lgConfigK</i> that
 * the unioner has seen.
 *
 * <p>This unioner also permits unioning of any of the three different target HllSketch types.
 *
 * <p>Although the API for this unioner parallels many of the methods of the <i>HllSketch</i>,
 * the behavior of the unioner has some fundamental differences.
 *
 * <p>First, the user cannot specify the {@link TgtHllType} for the unioner.
 * It is specified for the sketch returned with {@link #getResult(TgtHllType)}.
 *
 * <p>Second, the internal effective value of log-base-2 of <i>K</i> for the unioner can change
 * dynamically based on the smallest <i>lgConfigK</i> that the unioner has been presented so far.
 *
 *
 * @author Lee Rhodes
 * @author Kevin Lang
 */
public class Unioner extends BaseHllSketch {
  final int lgMaxK;
  private final HllSketch gadget;

  /**
   * Construct this Unioner with a given maximum log-base-2 of <i>K</i>.
   * @param lgMaxK the desired maximum log-base-2 of <i>K</i>.  This value must be
   * between 7 and 21 inclusively.
   */
  public Unioner(final int lgMaxK) {
    this.lgMaxK = lgMaxK;
    gadget = new HllSketch(lgMaxK, HLL_8);
  }

  @Override
  CurMode getCurMode() {
    return gadget.getCurMode();
  }

  @Override
  public int getCurrentSerializationBytes() {
    return gadget.getCurrentSerializationBytes();
  }

  @Override
  public double getEstimate() {
    return gadget.getEstimate();
  }

  @Override
  public int getLgConfigK() {
    return gadget.getLgConfigK();
  }

  /**
   * Returns the maximum size in bytes that this unioner can grow to given lgMaxK.
   *
   * @param lgMaxK The maximum Log2 of K for this unioner. This value must be
   * between 7 and 21 inclusively.
   * @return the maximum size in bytes that this unioner can grow to.
   */
  public static int getMaxSerializationBytes(final int lgMaxK) {
    return HllSketch.getMaxSerializationBytes(lgMaxK, TgtHllType.HLL_8);
  }

  @Override
  public double getLowerBound(final double numStdDev) {
    return gadget.getLowerBound(numStdDev);
  }

  /**
   * Return the result of this unioner with the specified {@link TgtHllType}
   * @param tgtHllType the TgtHllType enum
   * @return a copy of this sketch with the specified TgtHllType
   */
  public HllSketch getResult(final TgtHllType tgtHllType) {
    return gadget.copyAs(tgtHllType);
  }

  @Override
  public double getUpperBound(final double numStdDev) {
    return gadget.getUpperBound(numStdDev);
  }

  @Override
  public boolean isEmpty() {
    return gadget.isEmpty();
  }

  @Override
  boolean isOutOfOrderFlag() {
    return gadget.isOutOfOrderFlag();
  }

  @Override
  public void reset() {
    gadget.reset();
  }

  @Override
  public byte[] toByteArray() {
    return gadget.toByteArray();
  }

  @Override
  public String toString() {
    return gadget.toString();
  }

  @Override
  public String toString(final boolean detail) {
    return gadget.toString(detail);
  }

  /**
   * Update this unioner with the given sketch.
   * @param sketch the given sketch.
   */
  public void update(final HllSketch sketch) {
    gadget.hllSketchImpl = unionImpl(sketch.hllSketchImpl, gadget.hllSketchImpl, lgMaxK);
  }

  @Override
  void couponUpdate(final int coupon) {
    gadget.hllSketchImpl = gadget.hllSketchImpl.couponUpdate(coupon);
  }

  // Union OPERATIONS

  /**
   * Union the given source and destination sketches.
   * @param srcImpl the given source sketch
   * @param dstImpl the given destination sketch this must have a target of HLL_8 and may be
   * modified.
   * @param lgMaxK the maximum value of log2 K for this union.
   * @return the union of the two sketches.
   */
  static HllSketchImpl unionImpl(final HllSketchImpl srcImpl, final HllSketchImpl dstImpl,
      final int lgMaxK) {
    assert dstImpl.getTgtHllType() == HLL_8;
    HllSketchImpl outImpl = dstImpl;
    if ((srcImpl == null) || srcImpl.isEmpty()) { return outImpl; }

    final int hi2bits = (outImpl.isEmpty()) ? 3 : outImpl.getCurMode().ordinal();
    final int lo2bits = srcImpl.getCurMode().ordinal();

    final int sw = (hi2bits << 2) | lo2bits;
    //System.out.println("SW: " + sw);
    switch (sw) {
      case 0: { //src: LIST, gadget: LIST
        final PairIterator srcItr = srcImpl.getIterator();
        while (srcItr.nextValid()) {
          outImpl = outImpl.couponUpdate(srcItr.getPair());
        }
        //whichever is True wins:
        outImpl.putOutOfOrderFlag(outImpl.isOutOfOrderFlag() | srcImpl.isOutOfOrderFlag());
        break;
      }
      case 1: { //src: SET, gadget: LIST
        final PairIterator srcItr = srcImpl.getIterator();
        while (srcItr.nextValid()) {
          outImpl = outImpl.couponUpdate(srcItr.getPair());
        }
        outImpl.putOutOfOrderFlag(true); //SET oooFlag is always true
        break;
      }
      case 2: { //src: HLL, gadget: LIST
        //swap so that src is gadget-LIST, tgt is HLL
        //lgMaxK because LIST has effective K of 2^26
        final HllSketchImpl newSrcImplList = outImpl;
        final HllSketchImpl newDstImplHll = srcImpl;
        outImpl = HllUtil.copyOrDownsampleHll(newDstImplHll, lgMaxK);
        assert outImpl.getCurMode() == HLL;
        final PairIterator srcItr = newSrcImplList.getIterator();
        while (srcItr.nextValid()) {
          outImpl = outImpl.couponUpdate(srcItr.getPair());
        }
        //whichever is True wins:
        outImpl.putOutOfOrderFlag(outImpl.isOutOfOrderFlag() | srcImpl.isOutOfOrderFlag());
        break;
      }
      case 4: { //src: LIST, gadget: SET
        final PairIterator srcItr = srcImpl.getIterator();
        while (srcItr.nextValid()) {
          outImpl = outImpl.couponUpdate(srcItr.getPair());
        }
        outImpl.putOutOfOrderFlag(true); //SET oooFlag is always true
        break;
      }
      case 5: { //src: SET, gadget: SET
        final PairIterator srcItr = srcImpl.getIterator();
        while (srcItr.nextValid()) {
          outImpl = outImpl.couponUpdate(srcItr.getPair());
        }
        outImpl.putOutOfOrderFlag(true); //SET oooFlag is always true
        break;
      }
      case 6: { //src: HLL, gadget: SET
        //swap so that src is gadget-SET, tgt is HLL
        //lgMaxK because LIST has effective K of 2^26
        final HllSketchImpl newSrcSet = outImpl;
        final HllSketchImpl newDstImplHll = srcImpl;
        outImpl = HllUtil.copyOrDownsampleHll(newDstImplHll, lgMaxK);
        final PairIterator srcItr = newSrcSet.getIterator();
        assert outImpl.getCurMode() == HLL;
        while (srcItr.nextValid()) {
          outImpl = outImpl.couponUpdate(srcItr.getPair());
        }
        outImpl.putOutOfOrderFlag(true); //merging SET into non-empty HLL -> true
        break;
      }
      case 8: { //src: LIST, gadget: HLL
        assert outImpl.getCurMode() == HLL;
        final PairIterator srcItr = srcImpl.getIterator();
        while (srcItr.nextValid()) {
          outImpl = outImpl.couponUpdate(srcItr.getPair());
        }
        //whichever is True wins:
        outImpl.putOutOfOrderFlag(outImpl.isOutOfOrderFlag() | srcImpl.isOutOfOrderFlag());
        break;
      }
      case 9: { //src: SET, gadget: HLL
        assert outImpl.getCurMode() == HLL;
        final PairIterator srcItr = srcImpl.getIterator();
        while (srcItr.nextValid()) {
          outImpl = outImpl.couponUpdate(srcItr.getPair());
        }
        outImpl.putOutOfOrderFlag(true); //merging SET into existing HLL -> true
        break;
      }
      case 10: { //src: HLL, gadget: HLL
        final int srcLgK = srcImpl.getLgConfigK();
        final int dstLgK = outImpl.getLgConfigK();
        if ((srcLgK < dstLgK) || (outImpl.getTgtHllType() != HLL_8)) {
          final int newLgConfigK = min(outImpl.getLgConfigK(), srcImpl.getLgConfigK());
          outImpl = HllUtil.copyOrDownsampleHll(outImpl, newLgConfigK);
        }
        assert outImpl.getCurMode() == HLL;
        final PairIterator srcItr = srcImpl.getIterator();
        while (srcItr.nextValid()) {
          outImpl = outImpl.couponUpdate(srcItr.getPair());
        }
        outImpl.putOutOfOrderFlag(true); //union of two HLL modes is always true
        break;
      }
      case 12: { //src: LIST, gadget: empty
        final PairIterator srcItr = srcImpl.getIterator();
        while (srcItr.nextValid()) {
          outImpl = outImpl.couponUpdate(srcItr.getPair());
        }
        outImpl.putOutOfOrderFlag(srcImpl.isOutOfOrderFlag()); //whatever source is
        break;
      }
      case 13: { //src: SET, gadget: empty
        final PairIterator srcItr = srcImpl.getIterator();
        while (srcItr.nextValid()) {
          outImpl = outImpl.couponUpdate(srcItr.getPair());
        }
        outImpl.putOutOfOrderFlag(true); //SET oooFlag is always true
        break;
      }
      case 14: { //src: HLL, gadget: empty
        outImpl = HllUtil.copyOrDownsampleHll(srcImpl, lgMaxK);
        outImpl.putOutOfOrderFlag(srcImpl.isOutOfOrderFlag()); //whatever source is.
        break;
      }
    }
    return outImpl;
  }

}
