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

import org.apache.datasketches.memory.Memory;

/**
 * Methods for inquiring the status of a backing Memory object.
 */
public interface MemoryStatus {

  /**
   * Returns true if this object's internal data is backed by a Memory object,
   * which may be on-heap or off-heap.
   * @return true if this object's internal data is backed by a Memory object.
   */
  default boolean hasMemory() { return false; }

  /**
   * Returns true if this object's internal data is backed by direct (off-heap) Memory.
   * @return true if this object's internal data is backed by direct (off-heap) Memory.
   */
  default boolean isDirect() { return false; }

  /**
   * Returns true if the backing resource of <i>this</i> is identical with the backing resource
   * of <i>that</i>. The capacities must be the same.  If <i>this</i> is a region,
   * the region offset must also be the same.
   *
   * @param that A different non-null and alive Memory object.
   * @return true if the backing resource of <i>this</i> is identical with the backing resource
   * of <i>that</i>.
   * @throws SketchesArgumentException if <i>that</i> is not alive (already closed).
   */
  default boolean isSameResource(final Memory that) { return false; }

}
