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

package org.apache.datasketches.theta;

import org.apache.datasketches.memory.WritableMemory;

/**
 * An on-heap, compact, unordered, read-only sketch.
 *
 * @author Lee Rhodes
 */
final class HeapCompactUnorderedSketch extends HeapCompactSketch {

  /**
   * Constructs this sketch from correct, valid components.
   * @param cache in compact form
   * @param empty The correct <a href="{@docRoot}/resources/dictionary.html#empty">Empty</a>.
   * @param seedHash The correct
   * <a href="{@docRoot}/resources/dictionary.html#seedHash">Seed Hash</a>.
   * @param curCount correct value
   * @param thetaLong The correct
   * <a href="{@docRoot}/resources/dictionary.html#thetaLong">thetaLong</a>.
   */
  HeapCompactUnorderedSketch(final long[] cache, final boolean empty, final short seedHash,
      final int curCount, final long thetaLong) {
    super(cache, empty, seedHash, curCount, thetaLong);
  }

  //Sketch interface

  @Override //ordered, on-heap
  public CompactSketch compact() {
    return compact(true, null);
  }

  @Override
  public CompactSketch compact(final boolean dstOrdered, final WritableMemory dstMem) {
    return CompactOperations.componentsToCompact(
      getThetaLong(), getRetainedEntries(), getSeedHash(), isEmpty(),
      true, false, dstOrdered, dstMem, getCache());
  }

  @Override
  public byte[] toByteArray() {
    return toByteArray(false); //unordered
  }

  @Override
  public boolean isOrdered() {
    return false;
  }

}
