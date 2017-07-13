/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.SketchesArgumentException;

/**
 * The HllSketch implementation
 *
 * @author Lee Rhodes
 */
abstract class HllSketchImpl {
  final int lgConfigK;
  final TgtHllType tgtHllType;
  final CurMode curMode;
  boolean oooFlag = false; //Out-Of-Order Flag

  /**
   * Standard constructor.
   * @param lgConfigK the configured Lg K
   * @param tgtHllType the type of target HLL sketch
   * @param curMode the current mode of the sketch LIST, SET, or HLL
   */
  HllSketchImpl(final int lgConfigK,
      final TgtHllType tgtHllType,
      final CurMode curMode) {
    this.lgConfigK = lgConfigK;
    this.tgtHllType = tgtHllType;
    this.curMode = curMode;
    oooFlag = (curMode == CurMode.SET) ? true : false;
  }

  /**
   * Copy constructor
   * @param that another HllSketchImpl
   */
  HllSketchImpl(final HllSketchImpl that) {
    lgConfigK = that.getLgConfigK();
    tgtHllType = that.getTgtHllType();
    curMode = that.getCurMode();
    oooFlag = that.isOutOfOrderFlag();
  }

  /**
   * Copy As constructor. Performs an isomorphic transformation.
   * @param that another HllSketchImpl
   * @param tgtHllType the new target Hll type
   */
  HllSketchImpl(final HllSketchImpl that, final TgtHllType tgtHllType) {
    lgConfigK = that.getLgConfigK();
    this.tgtHllType = tgtHllType;
    curMode = that.curMode;
    oooFlag = that.oooFlag;
  }

  abstract HllSketchImpl copy();

  abstract HllSketchImpl copyAs(TgtHllType tgtHllType);

  abstract HllSketchImpl couponUpdate(int coupon);

  abstract PairIterator getAuxIterator();

  abstract int getCouponCount(); //for test

  abstract int getCurMin();

  abstract int getCurrentSerializationBytes();

  CurMode getCurMode() {
    return curMode;
  }

  abstract double getEstimate();

  abstract double getCompositeEstimate();

  abstract double getHipAccum();

  abstract PairIterator getIterator();

  int getLgConfigK() {
    return lgConfigK;
  }

  abstract double getLowerBound(int numStdDev);

  abstract int getLgMaxCouponArrInts(); //for test

  abstract int getNumAtCurMin();

  TgtHllType getTgtHllType() {
    return tgtHllType;
  }

  abstract double getRelErr(int numStdDev);

  abstract double getRelErrFactor(int numStdDev);

  abstract double getUpperBound(int numStdDev);

  abstract boolean isEmpty();

  boolean isOutOfOrderFlag() {
    return oooFlag;
  }

  void putOutOfOrderFlag(final boolean oooFlag) {
    this.oooFlag = oooFlag;
  }

  abstract byte[] toCompactByteArray();

  abstract byte[] toUpdatableByteArray();

  static final void badPreambleState(final Memory mem) {
    throw new SketchesArgumentException("Possible Corruption, Invalid Preamble:"
        + PreambleUtil.toString(mem));
  }

}
