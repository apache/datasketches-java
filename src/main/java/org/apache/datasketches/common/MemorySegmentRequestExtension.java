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
import java.util.Hashtable;

/**
 * This is just an example of a possible extension of the MemorySegmentRequest interface.
   * You may want to enable the println statements to track the state of the Hashtable.
 */
public final class MemorySegmentRequestExtension implements MemorySegmentRequest {
  private final Hashtable<MemorySegment, Arena> table = new Hashtable<>();

  @Override
  public synchronized MemorySegment request(final MemorySegment prevSeg, final long newByteSize) {
    if (prevSeg.isNative()) {
      final Arena arena = Arena.ofConfined();
      final MemorySegment seg = arena.allocate(newByteSize);
      table.put(seg, arena);  //System.out.println("Add");
      return seg;
    } else {
      if (newByteSize > Integer.MAX_VALUE) {
        throw new SketchesArgumentException("Requested byteSize is greater than Integer.MAX_VALUE.");
      }
      return MemorySegment.ofArray(new byte[(int)newByteSize]);
    }
  }

  @Override
  public synchronized void requestClose(final MemorySegment prevSeg) {
    final Arena arena = table.get(prevSeg);
    if ((arena != null) && arena.scope().isAlive()) {
      arena.close();
      table.remove(prevSeg); //System.out.println("Remove");
    } //else ignore
  }

  /**
   * This cleans up any unclosed off-heap MemorySegments.
   */
  public synchronized void cleanup() {
    for (final Enumeration<Arena> e = table.elements(); e.hasMoreElements();) {
      final Arena arena = e.nextElement();
      if (arena.scope().isAlive()) {
        arena.close(); //System.out.println("Closed a remaining Arena in the Hashtable");
      }
    }
  }

}
