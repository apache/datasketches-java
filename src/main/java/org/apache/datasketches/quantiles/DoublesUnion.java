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
import org.apache.datasketches.common.MemorySegmentStatus;

/**
 * The API for Union operations for quantiles DoublesSketches
 *
 * @author Lee Rhodes
 */
public abstract class DoublesUnion implements MemorySegmentStatus {

  /**
   * Returns a new UnionBuilder
   * @return a new UnionBuilder
   */
  public static DoublesUnionBuilder builder() {
    return new DoublesUnionBuilder();
  }

  /**
   * Returns a Heap Union object that has been initialized with the data from the given sketch.
   * @param sketch A DoublesSketch to be used as a source of data only and will not be modified.
   * @return a DoublesUnion object
   */
  public static DoublesUnion heapify(final DoublesSketch sketch) {
    return DoublesUnionImpl.heapifyInstance(sketch);
  }

  /**
   * Returns a Heap Union object that has been initialized with the data from the given MemorySegment that contains an
   * image of a sketch.
   *
   * @param srcSeg A MemorySegment image of a DoublesSketch to be used as a source of data and will not be modified.
   * @return a Union object
   */
  public static DoublesUnion heapify(final MemorySegment srcSeg) {
    return DoublesUnionImpl.heapifyInstance(srcSeg);
  }

  /**
   * Returns an updatable Union object that wraps the given MemorySegment that contains an image of a DoublesSketch.
   *
   * @param srcSeg A MemorySegment image of an updatable DoublesSketch to be used as the data structure for the union and will be modified.
   * @return a Union object
   */
  public static DoublesUnion wrap(final MemorySegment srcSeg) {
    return DoublesUnionImpl.wrapInstance(srcSeg, null);
  }

  /**
   * Returns an updatable Union object that wraps the given MemorySegment that contains an image of a DoublesSketch.
   *
   * @param srcSeg A MemorySegment sketch to be used as the data structure for the union and will be modified.
   * @param mSegReq the MemorySegmentRequest used if the given MemorySegment needs to expand.
   * Otherwise, it can be null and the default MemorySegmentRequest will be used.
   * @return a Union object
   */
  public static DoublesUnion wrap(final MemorySegment srcSeg, final MemorySegmentRequest mSegReq) {
    return DoublesUnionImpl.wrapInstance(srcSeg, mSegReq);
  }

  @Override
  public abstract boolean hasMemorySegment();

  @Override
  public abstract boolean isOffHeap();

  @Override
  public abstract boolean isSameResource(final MemorySegment that);

  /**
   * Returns true if this union is empty
   * @return true if this union is empty
   */
  public abstract boolean isEmpty();

  /**
   * Returns the configured <i>maxK</i> of this Union.
   * @return the configured <i>maxK</i> of this Union.
   */
  public abstract int getMaxK();

  /**
   * Returns the effective <i>k</i> of this Union.
   * @return the effective <i>k</i> of this Union.
   */
  public abstract int getEffectiveK();

  /**
   * Iterative union operation, which means this method can be repeatedly called.
   * Merges the given sketch into this union object.
   * The given sketch is not modified.
   * It is required that the ratio of the two K's be a power of 2.
   * This is easily satisfied if each of the K's are already a power of 2.
   * If the given sketch is null or empty it is ignored.
   *
   * <p>It is required that the results of the union operation, which can be obtained at any time,
   * is obtained from {@link #getResult() }.
   *
   * @param sketchIn the sketch to be merged into this one.
   */
  public abstract void union(DoublesSketch sketchIn);

  /**
   * Iterative union operation, which means this method can be repeatedly called.
   * Merges the given MemorySegment image of a DoublesSketch into this union object.
   * The given MemorySegment object is not modified and a link to it is not retained.
   * It is required that the ratio of the two K's be a power of 2.
   * This is easily satisfied if each of the K's are already a power of 2.
   * If the given sketch is null or empty it is ignored.
   *
   * <p>It is required that the results of the union operation, which can be obtained at any time,
   * is obtained from {@link #getResult() }.
   *
   * @param seg MemorySegment image of sketch to be merged
   */
  public abstract void union(MemorySegment seg);

  /**
   * Update this union with the given double (or float) data Item.
   *
   * @param dataItem The given double datum.
   */
  public abstract void update(double dataItem);

  /**
   * Gets the result of this Union as an UpdateDoublesSketch, which enables further update
   * operations on the resulting sketch. The Union state has not been changed, which allows
   * further union operations.
   *
   * @return the result of this Union operation
   */
  public abstract UpdateDoublesSketch getResult();

  /**
   * Places the result of this Union into the provided MemorySegment as an UpdateDoublesSketch,
   * which enables further update operations on the resulting sketch. The Union state has not
   * been changed, which allows further union operations.
   *
   * @param dstSeg the destination MemorySegment for the result
   * @param mSegReq the MemorySegmentRequest used if the given MemorySegment needs to expand.
   * Otherwise, it can be null and the default MemorySegmentRequest will be used.
   * @return the result of this Union operation
   */
  public abstract UpdateDoublesSketch getResult(MemorySegment dstSeg, MemorySegmentRequest mSegReq);

  /**
   * Gets the result of this Union  as an UpdateDoublesSketch, which enables further update
   * operations on the resulting sketch. The Union is reset to the virgin state.
   *
   * @return the result of this Union operation and reset.
   */
  public abstract UpdateDoublesSketch getResultAndReset();

  /**
   * Resets this Union to a virgin state.
   */
  public abstract void reset();

  /**
   * Serialize this union to a byte array. Result is an UpdateDoublesSketch, serialized in an
   * unordered, non-compact form. The resulting byte[] can be heapified or wrapped  as either a
   * sketch or a union.
   *
   * @return byte array of this union
   */
  public abstract byte[] toByteArray();

  /**
   * Returns summary information about the backing sketch.
   */
  @Override
  public abstract String toString();

  /**
   * Returns summary information about the backing sketch. Used for debugging.
   * @param sketchSummary if true includes sketch summary
   * @param dataDetail if true includes data detail
   * @return summary information about the sketch.
   */
  public abstract String toString(boolean sketchSummary, boolean dataDetail);

}
