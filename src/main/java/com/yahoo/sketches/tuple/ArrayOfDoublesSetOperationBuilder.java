/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.tuple;

import static com.yahoo.sketches.Util.DEFAULT_UPDATE_SEED;

import com.yahoo.memory.WritableMemory;

/**
 * Builds set operations object for tuple sketches of type ArrayOfDoubles.
 */
public class ArrayOfDoublesSetOperationBuilder {

  private int nomEntries_;
  private int numValues_;
  private long seed_;

  private static final int DEFAULT_NOMINAL_ENTRIES = 4096;
  private static final int DEFAULT_NUMBER_OF_VALUES = 1;

  /**
   * Creates an instance of the builder with default parameters
   */
  public ArrayOfDoublesSetOperationBuilder() {
    nomEntries_ = DEFAULT_NOMINAL_ENTRIES;
    numValues_ = DEFAULT_NUMBER_OF_VALUES;
    seed_ = DEFAULT_UPDATE_SEED;
  }

  /**
   * This is to set the nominal number of entries.
   * @param nomEntries Nominal number of entries. Forced to the nearest power of 2 greater than 
   * given value.
   * @return this builder
   */
  public ArrayOfDoublesSetOperationBuilder setNominalEntries(final int nomEntries) {
    nomEntries_ = nomEntries;
    return this;
  }

  /**
   * This is to set the number of double values associated with each key
   * @param numValues number of double values
   * @return this builder
   */
  public ArrayOfDoublesSetOperationBuilder setNumberOfValues(final int numValues) {
    numValues_ = numValues;
    return this;
  }

  /**
   * Sets the long seed value that is required by the hashing function.
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   * @return this builder
   */
  public ArrayOfDoublesSetOperationBuilder setSeed(final long seed) {
    seed_ = seed;
    return this;
  }

  /**
   * Creates an instance of ArrayOfDoublesUnion based on the current configuration of the builder.
   * The new instance is allocated on the heap if the memory is not provided.
   * @return an instance of ArrayOfDoublesUnion
   */
  public ArrayOfDoublesUnion buildUnion() {
    return new HeapArrayOfDoublesUnion(nomEntries_, numValues_, seed_);
  }

  /**
   * Creates an instance of ArrayOfDoublesUnion based on the current configuration of the builder
   * and the given memory.
   * @param dstMem destination memory to be used by the sketch
   * @return an instance of ArrayOfDoublesUnion
   */
  public ArrayOfDoublesUnion buildUnion(final WritableMemory dstMem) {
    return new DirectArrayOfDoublesUnion(nomEntries_, numValues_, seed_, dstMem);
  }

  /**
   * Creates an instance of ArrayOfDoublesIntersection based on the current configuration of the 
   * builder.
   * The new instance is allocated on the heap if the memory is not provided.
   * The number of nominal entries is not relevant to this, so it is ignored.
   * @return an instance of ArrayOfDoublesIntersection
   */
  public ArrayOfDoublesIntersection buildIntersection() {
    return new HeapArrayOfDoublesIntersection(numValues_, seed_);
  }

  /**
   * Creates an instance of ArrayOfDoublesIntersection based on the current configuration of the 
   * builder.
   * The new instance is allocated on the heap if the memory is not provided.
   * The number of nominal entries is not relevant to this, so it is ignored.
   * @param dstMem destination memory to be used by the sketch
   * @return an instance of ArrayOfDoublesIntersection
   */
  public ArrayOfDoublesIntersection buildIntersection(final WritableMemory dstMem) {
    return new DirectArrayOfDoublesIntersection(numValues_, seed_, dstMem);
  }

  /**
   * Creates an instance of ArrayOfDoublesAnotB based on the current configuration of the builder.
   * The memory is not relevant to this, so it is ignored if set.
   * The number of nominal entries is not relevant to this, so it is ignored.
   * @return an instance of ArrayOfDoublesAnotB
   */
  public ArrayOfDoublesAnotB buildAnotB() {
    return new HeapArrayOfDoublesAnotB(numValues_, seed_);
  }

}
