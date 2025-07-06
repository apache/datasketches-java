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
import java.util.Optional;

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
   * Returns true if an internally referenced MemorySegment has the same backing resource as <i>that</i>,
   * or equivalently, if their two memory regions overlap.  This applies to both on-heap and off-heap MemorySegments.
   *
   * <p>This returns false if either segment is <i>null</i> or not alive.</p>
   *
   * <p><b>Note:</b> If both segments are on-heap and not read-only, it can be determined if they were derived from
   * the same backing memory (array).  However, this is not always possible off-heap. Because of this asymmetry, this definition
   * of "isSameResource" is confined to the existence of an overlap.</p>
   *
   * @param that The given MemorySegment.
   * @return true if an internally referenced MemorySegment has the same backing resource as <i>that</i>.
   */
  boolean isSameResource(final MemorySegment that);

  /**
   * Returns true if the two given MemorySegments have to the same backing resource, or equivalently,
   * if the two memory regions overlap.  This applies to both on-heap and off-heap MemorySegments.
   *
   * <p>This returns false if either segment is <i>null</i> or not alive.</p>
   *
   * <p><b>Note:</b> If both segments are on-heap and not read-only, it can be determined if they were derived from
   * the same backing memory (array).  However, this is not always possible off-heap. Because of this asymmetry, this definition
   * of "isSameResource" is confined to the existence of an overlap.</p>
   *
   * @param seg1 The first given MemorySegment
   * @param seg2 The second given MemorySegment
   * @return true if the two given MemorySegments have to the same backing resource.
   */
  static boolean isSameResource(final MemorySegment seg1, final MemorySegment seg2) {
    if ((seg1 == null) || (seg2 == null) || !seg1.scope().isAlive() || !seg2.scope().isAlive()) { return false; }
    final Optional<MemorySegment> opt = seg1.asOverlappingSlice(seg2);
    return opt.isPresent();
  }

}
