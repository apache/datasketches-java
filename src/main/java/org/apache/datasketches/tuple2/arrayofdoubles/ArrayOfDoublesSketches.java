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

package org.apache.datasketches.tuple2.arrayofdoubles;

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.thetacommon2.ThetaUtil;

/**
 * Convenient static methods to instantiate tuple sketches of type ArrayOfDoubles.
 */
public final class ArrayOfDoublesSketches {

  /**
   * Heapify the given MemorySegment as an ArrayOfDoublesSketch
   * @param srcSeg the given source MemorySegment
   * @return an ArrayOfDoublesSketch
   */
  public static ArrayOfDoublesSketch heapifySketch(final MemorySegment srcSeg) {
    return heapifySketch(srcSeg, ThetaUtil.DEFAULT_UPDATE_SEED);
  }

  /**
   * Heapify the given MemorySegment and seed as a ArrayOfDoublesSketch
   * @param srcSeg the given source MemorySegment
   * @param seed the given seed
   * @return an ArrayOfDoublesSketch
   */
  public static ArrayOfDoublesSketch heapifySketch(final MemorySegment srcSeg, final long seed) {
    return ArrayOfDoublesSketch.heapify(srcSeg, seed);
  }

  /**
   * Heapify the given MemorySegment as an ArrayOfDoublesUpdatableSketch
   * @param srcSeg the given source MemorySegment
   * @return an ArrayOfDoublesUpdatableSketch
   */
  public static ArrayOfDoublesUpdatableSketch heapifyUpdatableSketch(final MemorySegment srcSeg) {
    return heapifyUpdatableSketch(srcSeg, ThetaUtil.DEFAULT_UPDATE_SEED);
  }

  /**
   * Heapify the given MemorySegment and seed as a ArrayOfDoublesUpdatableSketch
   * @param srcSeg the given source MemorySegment
   * @param seed the given seed
   * @return an ArrayOfDoublesUpdatableSketch
   */
  public static ArrayOfDoublesUpdatableSketch heapifyUpdatableSketch(final MemorySegment srcSeg, final long seed) {
    return ArrayOfDoublesUpdatableSketch.heapify(srcSeg, seed);
  }

  /**
   * Wrap the given MemorySegment as an ArrayOfDoublesSketch.
   * If the given source MemorySegment is read-only, the returned Union object will also be read-only.
   * @param srcSeg the given source MemorySegment
   * @return an ArrayOfDoublesSketch
   */
  public static ArrayOfDoublesSketch wrapSketch(final MemorySegment srcSeg) {
    return wrapSketch(srcSeg, ThetaUtil.DEFAULT_UPDATE_SEED);
  }

  /**
   * Wrap the given MemorySegment and seed as a ArrayOfDoublesSketch.
   * If the given source MemorySegment is read-only, the returned Union object will also be read-only.
   * @param srcSeg the given source MemorySegment
   * @param seed the given seed
   * @return an ArrayOfDoublesSketch
   */
  public static ArrayOfDoublesSketch wrapSketch(final MemorySegment srcSeg, final long seed) {
    return ArrayOfDoublesSketch.wrap(srcSeg, seed);
  }

  /**
   * Wrap the given MemorySegment as an ArrayOfDoublesUpdatableSketch.
   * If the given source MemorySegment is read-only, the returned Union object will also be read-only.
   * @param srcSeg the given source MemorySegment
   * @return an ArrayOfDoublesUpdatableSketch
   */
  public static ArrayOfDoublesUpdatableSketch wrapUpdatableSketch(final MemorySegment srcSeg) {
    return wrapUpdatableSketch(srcSeg, ThetaUtil.DEFAULT_UPDATE_SEED);
  }

  /**
   * Wrap the given MemorySegment and seed as a ArrayOfDoublesUpdatableSketch.
   * If the given source MemorySegment is read-only, the returned Union object will also be read-only.
   * @param srcSeg the given source MemorySegment
   * @param seed the given seed
   * @return an ArrayOfDoublesUpdatableSketch
   */
  public static ArrayOfDoublesUpdatableSketch wrapUpdatableSketch(final MemorySegment srcSeg, final long seed) {
    return ArrayOfDoublesUpdatableSketch.wrap(srcSeg, seed);
  }

  /**
   * Heapify the given MemorySegment as an ArrayOfDoublesUnion
   * @param srcSeg the given source MemorySegment
   * @return an ArrayOfDoublesUnion
   */
  public static ArrayOfDoublesUnion heapifyUnion(final MemorySegment srcSeg) {
    return heapifyUnion(srcSeg, ThetaUtil.DEFAULT_UPDATE_SEED);
  }

  /**
   * Heapify the given MemorySegment and seed as an ArrayOfDoublesUnion
   * @param srcSeg the given source MemorySegment
   * @param seed the given seed
   * @return an ArrayOfDoublesUnion
   */
  public static ArrayOfDoublesUnion heapifyUnion(final MemorySegment srcSeg, final long seed) {
    return ArrayOfDoublesUnion.heapify(srcSeg, seed);
  }

  /**
   * Wrap the given MemorySegment as an ArrayOfDoublesUnion
   * If the given source MemorySegment is read-only, the returned Union object will also be read-only.
   * @param srcSeg the given source MemorySegment
   * @return an ArrayOfDoublesUnion
   */
  public static ArrayOfDoublesUnion wrapUnion(final MemorySegment srcSeg) {
    return wrapUnion(srcSeg, ThetaUtil.DEFAULT_UPDATE_SEED);
  }

  /**
   * Wrap the given MemorySegment and seed as an ArrayOfDoublesUnion
   * If the given source MemorySegment is read-only, the returned Union object will also be read-only.
   * @param srcSeg the given source MemorySegment
   * @param seed the given seed
   * @return an ArrayOfDoublesUnion
   */
  public static ArrayOfDoublesUnion wrapUnion(final MemorySegment srcSeg, final long seed) {
    return ArrayOfDoublesUnion.wrap(srcSeg, seed);
  }

}
