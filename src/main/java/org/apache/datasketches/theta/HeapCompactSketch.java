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

import static org.apache.datasketches.theta.PreambleUtil.COMPACT_FLAG_MASK;
import static org.apache.datasketches.theta.PreambleUtil.EMPTY_FLAG_MASK;
import static org.apache.datasketches.theta.PreambleUtil.ORDERED_FLAG_MASK;
import static org.apache.datasketches.theta.PreambleUtil.READ_ONLY_FLAG_MASK;

import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;

/**
 * Parent class of the Heap Compact Sketches.
 *
 * @author Lee Rhodes
 */
abstract class HeapCompactSketch extends CompactSketch {
  private final short seedHash_;
  private final boolean empty_;
  private final int curCount_;
  private final long thetaLong_;
  private final long[] cache_;
  private final int preLongs_;

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
  HeapCompactSketch(final long[] cache, final boolean empty, final short seedHash,
      final int curCount, final long thetaLong) {
    empty_ = empty;
    seedHash_ = seedHash;
    curCount_ = empty ? 0 : curCount;
    thetaLong_ = empty ? Long.MAX_VALUE : thetaLong;
    cache_ = cache;
    preLongs_ = computeCompactPreLongs(thetaLong, empty, curCount);
  }

  //Sketch

  @Override
  public int getCurrentBytes(final boolean compact) { //already compact; ignored
    return (preLongs_ + curCount_) << 3;
  }

  @Override
  public double getEstimate() {
    return Sketch.estimate(thetaLong_, curCount_);
  }

  @Override
  public int getRetainedEntries(final boolean valid) {
    return curCount_;
  }

  @Override
  public long getThetaLong() {
    return thetaLong_;
  }

  @Override
  public boolean hasMemory() {
    return false;
  }

  @Override
  public boolean isDirect() {
    return false;
  }

  @Override
  public boolean isEmpty() {
    return empty_;
  }

  @Override
  public HashIterator iterator() {
    return new HeapHashIterator(cache_, cache_.length, thetaLong_);
  }

  //restricted methods

  @Override
  long[] getCache() {
    return cache_;
  }

  @Override
  int getCurrentPreambleLongs(final boolean compact) { //already compact; ignored
    return preLongs_;
  }

  @Override
  Memory getMemory() {
    return null;
  }

  @Override
  short getSeedHash() {
    return seedHash_;
  }

  byte[] toByteArray(final boolean ordered) {
    final int bytes = getCurrentBytes(true);
    final byte[] byteArray = new byte[bytes];
    final WritableMemory dstMem = WritableMemory.wrap(byteArray);
    final int emptyBit = isEmpty() ? (byte) EMPTY_FLAG_MASK : 0;
    final int orderedBit = ordered ? (byte) ORDERED_FLAG_MASK : 0;
    final byte flags = (byte) (emptyBit |  READ_ONLY_FLAG_MASK | COMPACT_FLAG_MASK | orderedBit);
    final int preLongs = getCurrentPreambleLongs(true);
    loadCompactMemory(getCache(), getSeedHash(), getRetainedEntries(true), getThetaLong(),
        dstMem, flags, preLongs);
    return byteArray;
  }

}
