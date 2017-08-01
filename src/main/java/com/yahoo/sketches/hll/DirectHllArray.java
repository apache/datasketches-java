/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;

/**
 * @author Lee Rhodes
 */
class DirectHllArray extends AbstractHllArray {
  WritableMemory wmem;
  Memory mem;
  Object memObj;
  long memAdd;

  DirectHllArray(final WritableMemory wmem) {
    super();
    this.wmem = wmem;
    mem = wmem;
    memObj = wmem.getArray();
    memAdd = wmem.getCumulativeOffset(0L);
  }

  DirectHllArray(final Memory mem) {
    super();
    wmem = null;
    this.mem = mem;
    memObj = ((WritableMemory) mem).getArray();
    memAdd = mem.getCumulativeOffset(0L);
  }

  @Override
  HllSketchImpl copy() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  HllSketchImpl copyAs(final TgtHllType tgtHllType) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  HllSketchImpl couponUpdate(final int coupon) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  AuxHashMap getAuxHashMap() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  PairIterator getAuxIterator() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  int getCurMin() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  CurMode getCurMode() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  int getCompactSerializationBytes() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  double getCompositeEstimate() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  double getEstimate() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  double getHipAccum() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  byte[] getHllByteArr() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  PairIterator getIterator() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  double getKxQ0() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  double getKxQ1() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  int getLgConfigK() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  double getLowerBound(final int numStdDev) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  int getNumAtCurMin() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  TgtHllType getTgtHllType() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  double getRelErr(final int numStdDev) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  double getRelErrFactor(final int numStdDev) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  int getUpdatableSerializationBytes() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  double getUpperBound(final int numStdDev) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  boolean isDirect() {
    return mem.isDirect();
  }

  @Override
  boolean isEmpty() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  boolean isOutOfOrderFlag() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  void populateHllByteArrFromMem(final Memory srcMem, final int lenBytes) {
    // TODO Auto-generated method stub
  }

  @Override
  void populateMemFromHllByteArr(final WritableMemory dstWmem, final int lenBytes) {
    // TODO Auto-generated method stub
  }

  @Override
  void putOutOfOrderFlag(final boolean oooFlag) {
    // TODO Auto-generated method stub
  }

  @Override
  byte[] toCompactByteArray() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  byte[] toUpdatableByteArray() {
    // TODO Auto-generated method stub
    return null;
  }

}
