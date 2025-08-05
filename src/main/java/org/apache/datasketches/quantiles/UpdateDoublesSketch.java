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

package org.apache.datasketches.quantiles;

import java.lang.foreign.MemorySegment;

/**
 * Extends DoubleSketch
 * @author Jon Malkin
 */
public abstract class UpdateDoublesSketch extends DoublesSketch {

  UpdateDoublesSketch(final int k) {
    super(k);
  }

  /**
   * Wrap this sketch around the given non-compact MemorySegment image of a DoublesSketch.
   *
   * @param srcSeg the given MemorySegment image of a DoublesSketch that may have data,
   * @return a sketch that wraps the given srcSeg
   */
  public static UpdateDoublesSketch wrap(final MemorySegment srcSeg) {
    return DirectUpdateDoublesSketch.wrapInstance(srcSeg, null);
  }

  /**
   * Factory heapify takes a compact sketch image in MemorySegment and instantiates an on-heap sketch.
   * The resulting sketch will not retain any link to the source MemorySegment.
   * @param srcSeg compact MemorySegment image of a sketch serialized by this sketch.
   * @return a heap-based sketch based on the given MemorySegment.
   */
  public static UpdateDoublesSketch heapify(final MemorySegment srcSeg) {
    return HeapUpdateDoublesSketch.heapifyInstance(srcSeg);
  }

  /**
   * Returns a CompactDoublesSketch of this class
   * @return a CompactDoublesSketch of this class
   */
  public CompactDoublesSketch compact() {
    return compact(null);
  }

  /**
   * Returns a compact version of this sketch. If passing in a MemorySegment object, the compact sketch
   * will use that direct MemorySegment; otherwise, an on-heap sketch will be returned.
   * @param dstSeg An optional target MemorySegment to hold the sketch.
   * @return A compact version of this sketch
   */
  public CompactDoublesSketch compact(final MemorySegment dstSeg) {
    if (dstSeg == null) {
      return HeapCompactDoublesSketch.createFromUpdateSketch(this);
    }
    return DirectCompactDoublesSketch.createFromUpdateSketch(this, dstSeg);
  }

  /**
   * Returns a copy of this sketch and then resets this sketch with the same value of <i>k</i>.
   * @return a copy of this sketch and then resets this sketch with the same value of <i>k</i>.
   */
  abstract UpdateDoublesSketch getSketchAndReset();

  /**
   * Grows the combined buffer to the given spaceNeeded
   *
   * @param currentSpace the current allocated space
   * @param spaceNeeded  the space needed
   * @return the enlarged combined buffer with data from the original combined buffer.
   */
  abstract double[] growCombinedBuffer(int currentSpace, int spaceNeeded);

  @Override
  boolean isCompact() {
    return false;
  }

  /**
   * Puts the minimum item
   *
   * @param minItem the given minimum item
   */
  abstract void putMinItem(double minItem);

  /**
   * Puts the max item
   *
   * @param maxItem the given maximum item
   */
  abstract void putMaxItem(double maxItem);

  /**
   * Puts the long <i>n</i>
   *
   * @param n the given long <i>n</i>
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

  @Override
  public abstract void update(double item);

}
