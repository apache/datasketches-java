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

  abstract AuxHashMap getAuxHashMap();

  abstract PairIterator getAuxIterator();

  abstract int getCurMin();

  abstract double getHipAccum();

  abstract double getKxQ0();

  abstract double getKxQ1();

  abstract int getNumAtCurMin();

  abstract byte[] getHllByteArr();

  abstract void populateHllByteArrFromMem(Memory srcMem, int lenBytes); //TODO ??

  abstract void populateMemFromHllByteArr(WritableMemory dstWmem, int lenBytes);

}
