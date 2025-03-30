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

import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;

/**
 * Extends DoubleSketch
 * @author Jon Malkin
 */
public abstract class UpdateDoublesSketch extends DoublesSketch {

  UpdateDoublesSketch(final int k) {
    super(k);
  }

  /**
   * Wrap this sketch around the given non-compact Memory image of a DoublesSketch.
   *
   * @param srcMem the given Memory image of a DoublesSketch that may have data,
   * @return a sketch that wraps the given srcMem
   */
  public static UpdateDoublesSketch wrap(final WritableMemory srcMem) {
    return DirectUpdateDoublesSketch.wrapInstance(srcMem);
  }

  /**
   * Updates this sketch with the given double data item
   *
   * @param item an item from a stream of items.  NaNs are ignored.
   */
  @Override
  public abstract void update(double item);

  /**
   * Factory heapify takes a compact sketch image in Memory and instantiates an on-heap sketch.
   * The resulting sketch will not retain any link to the source Memory.
   * @param srcMem a compact Memory image of a sketch serialized by this sketch.
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @return a heap-based sketch based on the given Memory.
   */
  public static UpdateDoublesSketch heapify(final Memory srcMem) {
    return HeapUpdateDoublesSketch.heapifyInstance(srcMem);
  }

  /**
   * Returns a CompactDoublesSketch of this class
   * @return a CompactDoublesSketch of this class
   */
  public CompactDoublesSketch compact() {
    return compact(null);
  }

  /**
   * Returns a compact version of this sketch. If passing in a Memory object, the compact sketch
   * will use that direct memory; otherwise, an on-heap sketch will be returned.
   * @param dstMem An optional target memory to hold the sketch.
   * @return A compact version of this sketch
   */
  public CompactDoublesSketch compact(final WritableMemory dstMem) {
    if (dstMem == null) {
      return HeapCompactDoublesSketch.createFromUpdateSketch(this);
    }
    return DirectCompactDoublesSketch.createFromUpdateSketch(this, dstMem);
  }

  @Override
  boolean isCompact() {
    return false;
  }

  //Puts

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

  /**
   * Grows the combined buffer to the given spaceNeeded
   *
   * @param currentSpace the current allocated space
   * @param spaceNeeded  the space needed
   * @return the enlarged combined buffer with data from the original combined buffer.
   */
  abstract double[] growCombinedBuffer(int currentSpace, int spaceNeeded);
}
