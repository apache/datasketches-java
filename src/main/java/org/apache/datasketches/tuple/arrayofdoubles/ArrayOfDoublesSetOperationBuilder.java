/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.datasketches.tuple.arrayofdoubles;

import static org.apache.datasketches.Util.DEFAULT_UPDATE_SEED;

import org.apache.datasketches.memory.WritableMemory;

/**
 * Builds set operations object for tuple sketches of type ArrayOfDoubles.
 */
public class ArrayOfDoublesSetOperationBuilder {

  private int nomEntries_;
  private int numValues_;
  private long seed_;

  /**
   * Default Nominal Entries (a.k.a. K)
   */
  public static final int DEFAULT_NOMINAL_ENTRIES = 4096;

  /**
   * Default number of values
   */
  public static final int DEFAULT_NUMBER_OF_VALUES = 1;

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
