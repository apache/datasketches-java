/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import com.yahoo.sketches.SketchesStateException;

/**
 * @author Lee Rhodes
 */
interface AuxHashMap {

  AuxHashMap copy();

  int getAuxCount();

  int[] getAuxIntArr();

  int getCompactSizeBytes();

  PairIterator getIterator();

  int getLgAuxArrInts();

  int getUpdatableSizeBytes();

  boolean isMemory();

  boolean isOffHeap();

  /**
   * Adds the slotNo and value to the aux array.
   * @param slotNo the index from the HLL array
   * @param value the HLL value at the slotNo.
   * @throws SketchesStateException if this slotNo already exists in the aux array.
   */
  void mustAdd(int slotNo, int value);

  /**
   * Returns value given slotNo. If this fails an exception is thrown.
   * @param slotNo the index from the HLL array
   * @return value the HLL value at the slotNo
   * @throws SketchesStateException if valid slotNo and value is not found.
   */
  int mustFindValueFor(int slotNo);

  /**
   * Replaces the entry at slotNo with the given value.
   * @param slotNo the index from the HLL array
   * @param value the HLL value at the slotNo
   * @throws SketchesStateException if a valid slotNo, value is not found.
   */
  void mustReplace(int slotNo, int value);

}
