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

import java.lang.foreign.Arena;

import java.lang.foreign.MemorySegment;
import java.util.Enumeration;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This is an example of a possible implementation of the MemorySegmentRequest interface
 * where all requested segments are allocated off-heap.  A local ConcurrentHashMap tracks a newly created confined Arena
 * for every new MemorySegment allocated off-heap. This allows individual segments to be freed
 * immediately upon receiving the {@link #requestClose(MemorySegment) requestClose(MemorySegment)} call.
 */
public final class MemorySegmentRequestExample implements MemorySegmentRequest {
  private final ConcurrentHashMap<MemorySegment, Arena> map = new ConcurrentHashMap<>();

  /**
   * Request a new off-heap MemorySegment with the given <i>newByteSeze</i>.
   * An internal confined Arena is created to exclusively manage the new segment and it is associated
   * with the new segment with a ConcurrentHashMap.
   */
  @Override
  public synchronized MemorySegment request(final long newByteSize) {
    final Arena arena = Arena.ofConfined();
    final MemorySegment seg = arena.allocate(newByteSize);
    map.put(seg, arena);
    return seg;

  }

  @Override
  public synchronized void requestClose(final MemorySegment segKey) {
    final Arena arena = map.get(segKey);
    if (arena == null) { throw new SketchesArgumentException("Given MemorySegment key is not mapped to an Arena!"); }
    if (arena.scope().isAlive()) {
      arena.close();
      map.remove(segKey);
    }
  }

  /**
   * This cleans up any unclosed, off-heap MemorySegments.
   */
  public synchronized void cleanup() {
    for (final Enumeration<Arena> e = map.elements(); e.hasMoreElements();) {
      final Arena arena = e.nextElement();
      if (arena.scope().isAlive()) {
        arena.close();
      }
    }
  }

}
