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

package org.apache.datasketches.common;

import java.lang.foreign.MemorySegment;

/**
 * Methods for inquiring the status of a backing MemorySegment.
 */
public interface MemorySegmentStatus {

  /**
   * Returns true if this object's internal data is backed by a MemorySegment,
   * which may be on-heap or off-heap.
   * @return true if this object's internal data is backed by a MemorySegment.
   */
  boolean hasMemorySegment();

  /**
   * Returns true if this object's internal data is backed by an off-heap (direct or native)) MemorySegment.
   * @return true if this object's internal data is backed by an off-heap (direct or native)) MemorySegment.
   */
  boolean isOffHeap();

  /**
   * Returns true if an internally referenced MemorySegment refers to the same MemorySegment as <i>that</i>.
   * They can either have the same off-heap memory location and size, or refer to the same on-heap array object.
   *
   * <p>If both segment are off-heap, they both must have the same starting address and the same size.</p>
   *
   * <p>For on-heap segments, both segments must be based on or derived from the same array object and neither segment
   * can be read-only.</p>
   *
   * <p>Returns false if either argument is null;</p>
   *
   * @param that The given MemorySegment.
   * @return true if an internally referenced MemorySegment refers to the same MemorySegment as <i>that</i>.
   */
  boolean isSameResource(final MemorySegment that);

  /**
   * Returns true if the two given MemorySegments refer to the same backing resource,
   * which is either an off-heap memory address and size, or the same on-heap array object.
   *
   * <p>If both segment are off-heap, they both must have the same starting address and the same size.</p>
   *
   * <p>For on-heap segments, both segments must be based on or derived from the same array object and neither segment
   * can be read-only.</p>
   *
   * <p>Returns false if either argument is null;</p>
   *
   * @param seg1 The first given MemorySegment
   * @param seg2 The second given MemorySegment
   * @return true if both MemorySegments are determined to be the same backing memory.
   */
  static boolean isSameResource(final MemorySegment seg1, final MemorySegment seg2) {
    if ((seg1 == null) || (seg2 == null)) { return false; }
    if (!seg1.scope().isAlive() || !seg2.scope().isAlive()) {
      throw new IllegalArgumentException("Both arguments must be alive.");
    }
    final boolean seg1Native = seg1.isNative();
    final boolean seg2Native = seg2.isNative();
    if (seg1Native ^ seg2Native) { return false; }
    if (seg1Native && seg2Native) { //both off heap
      return (seg1.address() == seg2.address()) && (seg1.byteSize() == seg2.byteSize());
    }
    //both on heap
    if (seg1.isReadOnly() || seg2.isReadOnly()) {
      throw new IllegalArgumentException("Cannot determine 'isSameBackingMemory(..)' on heap if either MemorySegment is Read-only.");
    }
    return (seg1.heapBase().orElse(null) == seg2.heapBase().orElse(null));
  }

}
