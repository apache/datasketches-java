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

import static org.apache.datasketches.theta.CompactOperations.checkIllegalCurCountAndEmpty;
import static org.apache.datasketches.theta.CompactOperations.componentsToCompact;
import static org.apache.datasketches.theta.CompactOperations.computeCompactPreLongs;
import static org.apache.datasketches.theta.CompactOperations.correctThetaOnCompact;
import static org.apache.datasketches.theta.CompactOperations.isSingleItem;
import static org.apache.datasketches.theta.CompactOperations.loadCompactMemory;
import static org.apache.datasketches.theta.PreambleUtil.COMPACT_FLAG_MASK;
import static org.apache.datasketches.theta.PreambleUtil.EMPTY_FLAG_MASK;
import static org.apache.datasketches.theta.PreambleUtil.ORDERED_FLAG_MASK;
import static org.apache.datasketches.theta.PreambleUtil.READ_ONLY_FLAG_MASK;
import static org.apache.datasketches.theta.PreambleUtil.SINGLEITEM_FLAG_MASK;

import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;

/**
 * Parent class of the Heap Compact Sketches.
 *
 * @author Lee Rhodes
 */
class HeapCompactSketch extends CompactSketch {
  private final long thetaLong_; //computed
  private final int curCount_;
  private final int preLongs_; //computed
  private final short seedHash_;
  private final boolean empty_;
  private final boolean ordered_;
  private final boolean singleItem_;
  private final long[] cache_;

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
      final int curCount, final long thetaLong, final boolean ordered) {
    seedHash_ = seedHash;
    curCount_ = curCount;
    empty_ = empty;
    ordered_ = ordered;
    cache_ = cache;
    //computed
    thetaLong_ = correctThetaOnCompact(empty, curCount, thetaLong);
    preLongs_ = computeCompactPreLongs(empty, curCount, thetaLong); //considers singleItem
    singleItem_ = isSingleItem(empty, curCount, thetaLong);
    checkIllegalCurCountAndEmpty(empty, curCount);
  }

  //Sketch

  @Override
  public CompactSketch compact(final boolean dstOrdered, final WritableMemory dstMem) {
    return componentsToCompact(getThetaLong(), getRetainedEntries(true), getSeedHash(), isEmpty(),
        true, ordered_, dstOrdered, dstMem, getCache().clone());
  }

  @Override
  public int getCurrentBytes() {
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
  public boolean isOrdered() {
    return ordered_;
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
  int getCompactPreambleLongs() {
    return preLongs_;
  }

  @Override
  int getCurrentPreambleLongs() { //already compact; ignored
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

  //use of Memory is convenient. The byteArray and Memory are loaded simulaneously.
  @Override
  public byte[] toByteArray() {
    final int bytes = getCurrentBytes();
    final byte[] byteArray = new byte[bytes];
    final WritableMemory dstMem = WritableMemory.wrap(byteArray);
    final int emptyBit = isEmpty() ? EMPTY_FLAG_MASK : 0;
    final int orderedBit = ordered_ ? ORDERED_FLAG_MASK : 0;
    final int singleItemBit = singleItem_ ? SINGLEITEM_FLAG_MASK : 0;
    final byte flags = (byte) (emptyBit |  READ_ONLY_FLAG_MASK | COMPACT_FLAG_MASK
        | orderedBit | singleItemBit);
    final int preLongs = getCompactPreambleLongs();
    loadCompactMemory(getCache(), getSeedHash(), getRetainedEntries(true), getThetaLong(),
        dstMem, flags, preLongs);
    return byteArray;
  }

}
