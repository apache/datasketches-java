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
  boolean isDirect();

  /**
   * Returns true if the backing MemorySegment of this object refers to the same MemorySegment of <i>that</i>.
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
   * @return true if the backing MemorySegment of this object hierarchy refers to the same MemorySegment of <i>that</i>.
   */
  boolean isSameResource(final MemorySegment that);

}
