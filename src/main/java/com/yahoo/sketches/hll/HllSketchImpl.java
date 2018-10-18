/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;

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

  abstract PairIterator iterator();

  int getLgConfigK() {
    return lgConfigK;
  }

  abstract double getLowerBound(int numStdDev);

  abstract int getMemDataStart();

  abstract int getPreInts();

  TgtHllType getTgtHllType() {
    return tgtHllType;
  }

  abstract int getUpdatableSerializationBytes();

  abstract double getUpperBound(int numStdDev);

  abstract WritableMemory getWritableMemory();

  abstract boolean isCompact();

  abstract boolean isEmpty();

  abstract boolean isMemory();

  abstract boolean isOffHeap();

  abstract boolean isOutOfOrderFlag();

  abstract boolean isSameResource(Memory mem);

  abstract void putOutOfOrderFlag(boolean oooFlag);

  abstract HllSketchImpl reset();

  abstract byte[] toCompactByteArray();

  abstract byte[] toUpdatableByteArray();

}
