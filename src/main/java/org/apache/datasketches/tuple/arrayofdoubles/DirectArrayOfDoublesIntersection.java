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

package org.apache.datasketches.tuple.arrayofdoubles;

import java.lang.foreign.MemorySegment;

/**
 * Direct TupleIntersection operation for tuple sketches of type ArrayOfDoubles.
 *
 * <p>This implementation uses data in a given MemorySegment that is owned and managed by the caller.
 * This MemorySegment can be off-heap, which if managed properly will greatly reduce the need for
 * the JVM to perform garbage collection.</p>
 */
final class DirectArrayOfDoublesIntersection extends ArrayOfDoublesIntersection {

  private MemorySegment seg_;

  /**
   * Creates an instance of a DirectArrayOfDoublesIntersection with a custom update seed
   * @param numValues number of double values associated with each key
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   * @param dstSeg the destination MemorySegment
   */
  DirectArrayOfDoublesIntersection(final int numValues, final long seed, final MemorySegment dstSeg) {
    super(numValues, seed);
    seg_ = dstSeg;
  }

  @Override
  protected ArrayOfDoublesQuickSelectSketch createSketch(final int nomEntries, final int numValues,
      final long seed) {
    return new DirectArrayOfDoublesQuickSelectSketch(nomEntries, 0, 1f, numValues, seed, seg_);
  }

}
