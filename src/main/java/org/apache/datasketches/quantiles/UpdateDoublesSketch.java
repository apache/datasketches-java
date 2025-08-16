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

import org.apache.datasketches.common.MemorySegmentRequest;

/**
 * Extends DoubleSketch
 * @author Jon Malkin
 */
public abstract class UpdateDoublesSketch extends DoublesSketch {

  UpdateDoublesSketch(final int k) {
    super(k);
  }

  /**
   * Wrap a sketch around the given source MemorySegment containing sketch data that originated from this sketch.
   *
   * <p>The given MemorySegment must be writable and it must contain a <i>UpdateDoublesSketch</i>.
   * The sketch will be updated and managed totally within the MemorySegment. If the given source
   * MemorySegment is created off-heap, then all the management of the sketch's internal data will be off-heap as well.</p>
   *
   * <p><b>NOTE:</b>If during updating of the sketch the sketch requires more capacity than the given size of the MemorySegment, the sketch
   * will request more capacity using the {@link MemorySegmentRequest MemorySegmentRequest} interface. The default of this interface will
   * return a new MemorySegment on the heap.</p>
   *
   * @param srcSeg a MemorySegment that contains sketch data.
   * @return an instance of this sketch that wraps the given MemorySegment.
   */
  public static UpdateDoublesSketch wrap(final MemorySegment srcSeg) {
    return DirectUpdateDoublesSketch.wrapInstance(srcSeg, null);
  }

  /**
   * Wrap a sketch around the given source MemorySegment containing sketch data that originated from this sketch and including an
   * optional, user defined {@link MemorySegmentRequest MemorySegmentRequest}.
   *
   * <p>The given MemorySegment must be writable and it must contain a <i>UpdateDoublesSketch</i>.
   * The sketch will be updated and managed totally within the MemorySegment. If the given source
   * MemorySegment is created off-heap, then all the management of the sketch's internal data will be off-heap as well.</p>
   *
   * <p><b>NOTE:</b>If during updating of the sketch the sketch requires more capacity than the given size of the MemorySegment, the sketch
   * will request more capacity using the {@link MemorySegmentRequest MemorySegmentRequest} interface. The default of this interface will
   * return a new MemorySegment on the heap. It is up to the user to optionally extend this interface if more flexible
   * handling of requests for more capacity is required.</p>
   *
   * @param srcSeg a MemorySegment that contains sketch data.
   * @param mSegReq the MemorySegmentRequest used if the given MemorySegment needs to expand.
   * Otherwise, it can be null and the default MemorySegmentRequest will be used.
   * @return an instance of this sketch that wraps the given MemorySegment.
   */
  public static UpdateDoublesSketch wrap(final MemorySegment srcSeg, final MemorySegmentRequest mSegReq) {
    return DirectUpdateDoublesSketch.wrapInstance(srcSeg, mSegReq);
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
   * will use and load that MemorySegment; otherwise, an on-heap sketch will be returned.
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
   * Returns an on-heap copy of this sketch and then resets this sketch with the same value of <i>k</i>.
   * @return an on-heap copy of this sketch and then resets this sketch with the same value of <i>k</i>.
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
