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
 * This is a callback interface to provide a means to request a new MemorySegment of a specified size.
 *
 * @author Lee Rhodes
 */
public interface MemorySegmentRequest {

  /**
   * Request a new heap MemorySegment with the given <i>newByteSize</i>.
   * Because we do not have a reference to an Arena, the default here is to
   * allocate a new MemorySegment on the heap.  It is up to the user to override this as appropriate.
   * @param newByteSize The new <i>byteSize</i> being requested.
   * @return new MemorySegment with the requested <i>byteSize</i>.
   */
  default MemorySegment request(final long newByteSize) {
    if (newByteSize > Integer.MAX_VALUE) {
      throw new SketchesArgumentException("Requested size in bytes exceeds Integer.MAX_VALUE.");
    }
    return MemorySegment.ofArray(new byte[(int)newByteSize]);
  }

  /**
   * Request to close the given MemorySegment.
   * Because we do not have a reference to an Arena, the default here is to do nothing.
   * It is up to the user to override this as appropriate.
   * @param prevSeg the previous MemorySegment to be closed.
   */
  default void requestClose(final MemorySegment prevSeg) {
    //Because the default request goes on the heap, this default is a no-op
  }

  /**
   * This class implements the defaults
   */
  public static class Default implements MemorySegmentRequest {
    //A convenience class that creates the target for the static member DEFAULT.
  }

  /**
   * Create Default as static member.
   */
  Default DEFAULT = new Default();

}
