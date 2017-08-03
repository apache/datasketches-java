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
abstract class AbstractHllArray extends HllSketchImpl {

  abstract void addToHipAccum(double delta);

  abstract void decNumAtCurMin();

  abstract AuxHashMap getAuxHashMap();

  abstract PairIterator getAuxIterator();

  abstract int getCurMin();

  abstract double getHipAccum();

  abstract double getKxQ0();

  abstract double getKxQ1();

  abstract int getNumAtCurMin();

  abstract byte[] getHllByteArr();

  abstract int getHllByteArrBytes();

  abstract void populateHllByteArrFromMem(Memory srcMem, int lenBytes); //TODO ??

  abstract void populateMemFromHllByteArr(WritableMemory dstWmem, int lenBytes);

  abstract void putAuxHashMap(AuxHashMap auxHashMap);

  abstract void putCurMin(int curMin);

  abstract void putHipAccum(double hipAccum);

  abstract void putKxQ0(double kxq0);

  abstract void putKxQ1(double kxq1);

  abstract void putNumAtCurMin(int numAtCurMin);

  static final int hll6ByteArrBytes(final int lgConfigK) {
    final int numSlots = 1 << lgConfigK;
    return ((numSlots * 3) >>> 2) + 1;
  }
}
