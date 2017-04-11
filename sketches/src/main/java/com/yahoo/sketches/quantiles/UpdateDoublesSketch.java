/*
 * Copyright 2017, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.quantiles;

import com.yahoo.memory.Memory;

/**
 * @author Jon Malkin
 */
public abstract class UpdateDoublesSketch extends DoublesSketch {
  UpdateDoublesSketch(final int k) {
    super(k);
  }

  /**
   * Updates this sketch with the given double data item
   *
   * @param dataItem an item from a stream of items.  NaNs are ignored.
   */
  public abstract void update(double dataItem);

  /**
   * Resets this sketch to the empty state, but retains the original value of k.
   */
  public abstract void reset();

  @Override
  boolean isCompact() {
    return false;
  }


  public static UpdateDoublesSketch heapify(final Memory srcMem) {
    return HeapUpdateDoublesSketch.heapifyInstance(srcMem);
  }

  public CompactDoublesSketch compact() {
    return compact(null);
  }

  /**
   * Returns a comapct version of this sketch. If passing in a Memory object, the compact sketch
   * will use that direct memory; otherwise, an on-heap sketch will be returned.
   * @param dstMem An optional target memory to hold the sketch.
   * @return A compact version of this sketch
   */
  public CompactDoublesSketch compact(final Memory dstMem) {
    if (dstMem == null) {
      return HeapCompactDoublesSketch.createFromUpdateSketch(this);
    } else {
      return DirectCompactDoublesSketch.createFromUpdateSketch(this, dstMem);
    }
  }

  //Puts

  /**
   * Puts the min value
   *
   * @param minValue the given min value
   */
  abstract void putMinValue(double minValue);

  /**
   * Puts the max value
   *
   * @param maxValue the given max value
   */
  abstract void putMaxValue(double maxValue);

  /**
   * Puts the value of <i>n</i>
   *
   * @param n the given value of <i>n</i>
   */
  abstract void putN(long n);

  /**
   * Puts the combined, non-compact buffer.
   *
   * @param combinedBuffer the combined buffer array
   */
  abstract void putCombinedBuffer(double[] combinedBuffer);

  /**
   * Puts the base buffer count
   *
   * @param baseBufCount the given base buffer count
   */
  abstract void putBaseBufferCount(int baseBufCount);

  /**
   * Puts the bit pattern
   *
   * @param bitPattern the given bit pattern
   */
  abstract void putBitPattern(long bitPattern);

  /**
   * Grows the combined buffer to the given spaceNeeded
   *
   * @param currentSpace the current allocated space
   * @param spaceNeeded  the space needed
   * @return the enlarged combined buffer with data from the original combined buffer.
   */
  abstract double[] growCombinedBuffer(int currentSpace, int spaceNeeded);
}
