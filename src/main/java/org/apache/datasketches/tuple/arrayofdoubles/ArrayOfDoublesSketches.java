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

import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;

/**
 * Convenient static methods to instantiate tuple sketches of type ArrayOfDoubles.
 */
public final class ArrayOfDoublesSketches {

  /**
   * Heapify the given Memory as an ArrayOfDoublesSketch
   * @param mem the given Memory
   * @return an ArrayOfDoublesSketch
   */
  public static ArrayOfDoublesSketch heapifySketch(final Memory mem) {
    return heapifySketch(mem, DEFAULT_UPDATE_SEED);
  }

  /**
   * Heapify the given Memory and seed as a ArrayOfDoublesSketch
   * @param mem the given Memory
   * @param seed the given seed
   * @return an ArrayOfDoublesSketch
   */
  public static ArrayOfDoublesSketch heapifySketch(final Memory mem, final long seed) {
    return ArrayOfDoublesSketch.heapify(mem, seed);
  }

  /**
   * Heapify the given Memory as an ArrayOfDoublesUpdatableSketch
   * @param mem the given Memory
   * @return an ArrayOfDoublesUpdatableSketch
   */
  public static ArrayOfDoublesUpdatableSketch heapifyUpdatableSketch(final Memory mem) {
    return heapifyUpdatableSketch(mem, DEFAULT_UPDATE_SEED);
  }

  /**
   * Heapify the given Memory and seed as a ArrayOfDoublesUpdatableSketch
   * @param mem the given Memory
   * @param seed the given seed
   * @return an ArrayOfDoublesUpdatableSketch
   */
  public static ArrayOfDoublesUpdatableSketch heapifyUpdatableSketch(final Memory mem, final long seed) {
    return ArrayOfDoublesUpdatableSketch.heapify(mem, seed);
  }

  /**
   * Wrap the given Memory as an ArrayOfDoublesSketch
   * @param mem the given Memory
   * @return an ArrayOfDoublesSketch
   */
  public static ArrayOfDoublesSketch wrapSketch(final Memory mem) {
    return wrapSketch(mem, DEFAULT_UPDATE_SEED);
  }

  /**
   * Wrap the given Memory and seed as a ArrayOfDoublesSketch
   * @param mem the given Memory
   * @param seed the given seed
   * @return an ArrayOfDoublesSketch
   */
  public static ArrayOfDoublesSketch wrapSketch(final Memory mem, final long seed) {
    return ArrayOfDoublesSketch.wrap(mem, seed);
  }

  /**
   * Wrap the given WritableMemory as an ArrayOfDoublesUpdatableSketch
   * @param mem the given Memory
   * @return an ArrayOfDoublesUpdatableSketch
   */
  public static ArrayOfDoublesUpdatableSketch wrapUpdatableSketch(final WritableMemory mem) {
    return wrapUpdatableSketch(mem, DEFAULT_UPDATE_SEED);
  }

  /**
   * Wrap the given WritableMemory and seed as a ArrayOfDoublesUpdatableSketch
   * @param mem the given Memory
   * @param seed the given seed
   * @return an ArrayOfDoublesUpdatableSketch
   */
  public static ArrayOfDoublesUpdatableSketch wrapUpdatableSketch(final WritableMemory mem, final long seed) {
    return ArrayOfDoublesUpdatableSketch.wrap(mem, seed);
  }

  /**
   * Heapify the given Memory as an ArrayOfDoublesUnion
   * @param mem the given Memory
   * @return an ArrayOfDoublesUnion
   */
  public static ArrayOfDoublesUnion heapifyUnion(final Memory mem) {
    return heapifyUnion(mem, DEFAULT_UPDATE_SEED);
  }

  /**
   * Heapify the given Memory and seed as an ArrayOfDoublesUnion
   * @param mem the given Memory
   * @param seed the given seed
   * @return an ArrayOfDoublesUnion
   */
  public static ArrayOfDoublesUnion heapifyUnion(final Memory mem, final long seed) {
    return ArrayOfDoublesUnion.heapify(mem, seed);
  }

  /**
   * Wrap the given Memory as an ArrayOfDoublesUnion
   * @param mem the given Memory
   * @return an ArrayOfDoublesUnion
   */
  public static ArrayOfDoublesUnion wrapUnion(final Memory mem) {
    return wrapUnion(mem, DEFAULT_UPDATE_SEED);
  }

  /**
   * Wrap the given Memory and seed as an ArrayOfDoublesUnion
   * @param mem the given Memory
   * @param seed the given seed
   * @return an ArrayOfDoublesUnion
   */
  public static ArrayOfDoublesUnion wrapUnion(final Memory mem, final long seed) {
    return ArrayOfDoublesUnion.wrap(mem, seed);
  }

  /**
   * Wrap the given Memory as an ArrayOfDoublesUnion
   * @param mem the given Memory
   * @return an ArrayOfDoublesUnion
   */
  public static ArrayOfDoublesUnion wrapUnion(final WritableMemory mem) {
    return wrapUnion(mem, DEFAULT_UPDATE_SEED);
  }

  /**
   * Wrap the given Memory and seed as an ArrayOfDoublesUnion
   * @param mem the given Memory
   * @param seed the given seed
   * @return an ArrayOfDoublesUnion
   */
  public static ArrayOfDoublesUnion wrapUnion(final WritableMemory mem, final long seed) {
    return ArrayOfDoublesUnion.wrap(mem, seed);
  }

}
