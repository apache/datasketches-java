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

  abstract HllSketchImpl copy();

  abstract HllSketchImpl copyAs(TgtHllType tgtHllType);

  abstract HllSketchImpl couponUpdate(int coupon);

  abstract CurMode getCurMode();

  abstract int getCompactSerializationBytes();

  abstract double getCompositeEstimate();

  abstract double getEstimate();

  abstract PairIterator getIterator();

  abstract int getLgConfigK();

  abstract double getLowerBound(int numStdDev);

  abstract TgtHllType getTgtHllType();

  abstract double getRelErr(int numStdDev);

  abstract double getRelErrFactor(int numStdDev);

  abstract int getUpdatableSerializationBytes();

  abstract double getUpperBound(int numStdDev);

  abstract boolean isEmpty();

  abstract boolean isOutOfOrderFlag();

  abstract void putOutOfOrderFlag(boolean oooFlag);

  abstract byte[] toCompactByteArray();

  abstract byte[] toUpdatableByteArray();

}
