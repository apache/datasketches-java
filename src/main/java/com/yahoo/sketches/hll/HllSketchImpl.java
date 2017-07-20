/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

/**
 * The HllSketch implementation
 *
 * @author Lee Rhodes
 */
abstract class HllSketchImpl {

  abstract HllSketchImpl copy();

  abstract HllSketchImpl copyAs(TgtHllType tgtHllType);

  abstract HllSketchImpl couponUpdate(int coupon);

  abstract AuxHashMap getAuxHashMap();

  abstract PairIterator getAuxIterator();

  abstract int getCouponCount(); //for test

  abstract int[] getCouponIntArr();

  abstract int getCurMin();

  abstract CurMode getCurMode();

  abstract int getCompactSerializationBytes();

  abstract double getCompositeEstimate();

  abstract double getEstimate();

  abstract double getHipAccum();

  abstract byte[] getHllByteArr();

  abstract PairIterator getIterator();

  abstract double getKxQ0();

  abstract double getKxQ1();

  abstract int getLgConfigK();

  abstract int getLgCouponArrInts();

  abstract double getLowerBound(int numStdDev);

  abstract int getLgMaxCouponArrInts(); //for test

  abstract int getNumAtCurMin();

  abstract TgtHllType getTgtHllType();

  abstract double getRelErr(int numStdDev);

  abstract double getRelErrFactor(int numStdDev);

  abstract int getUpdatableSerializationBytes();

  abstract double getUpperBound(int numStdDev);

  abstract boolean isEmpty();

  abstract boolean isOutOfOrderFlag();

  abstract void putCouponCount(int couponCount);

  abstract void putOutOfOrderFlag(boolean oooFlag);

  abstract byte[] toCompactByteArray();

  abstract byte[] toUpdatableByteArray();

}
