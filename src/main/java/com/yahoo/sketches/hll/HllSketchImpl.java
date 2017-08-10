/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

/**
 * The Abstract HllSketch implementation
 *
 * @author Lee Rhodes
 */
abstract class HllSketchImpl {
  final int lgConfigK;
  final TgtHllType tgtHllType;
  final CurMode curMode;

  HllSketchImpl(final int lgConfigK, final TgtHllType tgtHllType, final CurMode curMode) {
    this.lgConfigK = lgConfigK;
    this.tgtHllType = tgtHllType;
    this.curMode = curMode;
  }

  abstract HllSketchImpl copy();

  abstract HllSketchImpl copyAs(TgtHllType tgtHllType);

  abstract HllSketchImpl couponUpdate(int coupon);

 CurMode getCurMode() {
   return curMode;
 }

  abstract int getCompactSerializationBytes();

  abstract double getCompositeEstimate();

  abstract double getEstimate();

  abstract PairIterator getIterator();

  int getLgConfigK() {
    return lgConfigK;
  }

  abstract double getLowerBound(int numStdDev);

  abstract int getMemArrStart();

  abstract int getPreInts();

  abstract double getRelErr(int numStdDev); //TODO ??

  abstract double getRelErrFactor(int numStdDev); //TODO ??

  TgtHllType getTgtHllType() {
    return tgtHllType;
  }

  abstract int getUpdatableSerializationBytes();

  abstract double getUpperBound(int numStdDev);

  abstract boolean isDirect();

  abstract boolean isEmpty();

  abstract boolean isOutOfOrderFlag();

  abstract void putOutOfOrderFlag(boolean oooFlag);

  abstract byte[] toCompactByteArray();

  abstract byte[] toUpdatableByteArray();

}
