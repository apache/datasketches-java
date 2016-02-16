/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.tuple;

import static com.yahoo.sketches.Util.DEFAULT_UPDATE_SEED;

import com.yahoo.sketches.memory.Memory;

/**
 * This is on-heap implementation
 */
public class HeapArrayOfDoublesUnion extends ArrayOfDoublesUnion {

  /**
   * Creates an instance of HeapArrayOfDoublesUnion
   * @param nomEntries Nominal number of entries. Forced to the nearest power of 2 greater than given value.
   * @param numValues Number of double values to keep for each key.
   */
  public HeapArrayOfDoublesUnion(int nomEntries, int numValues) {
    this(nomEntries, numValues, DEFAULT_UPDATE_SEED);
  }

  /**
   * Creates an instance of HeapArrayOfDoublesUnion with a custom seed
   * @param nomEntries Nominal number of entries. Forced to the nearest power of 2 greater than given value.
   * @param numValues Number of double values to keep for each key.
   * @param <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   */
  public HeapArrayOfDoublesUnion(int nomEntries, int numValues, long seed) {
    super(new HeapArrayOfDoublesQuickSelectSketch(nomEntries, 3, 1f, numValues, seed));
  }

  /**
   * This is to create an instance given a serialized form
   * @param mem <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   */
  public HeapArrayOfDoublesUnion(Memory mem) {
    this(mem, DEFAULT_UPDATE_SEED);
  }

  /**
   * This is to create an instance given a serialized form and a custom seed
   * @param mem <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   */
  public HeapArrayOfDoublesUnion(Memory mem, long seed) {
    super(new HeapArrayOfDoublesQuickSelectSketch(mem, seed));
  }

  @Override
  public void reset() {
    sketch_ = new HeapArrayOfDoublesQuickSelectSketch(nomEntries_, 3, 1f, numValues_, seed_);
    theta_ = sketch_.getThetaLong();
  }

}
