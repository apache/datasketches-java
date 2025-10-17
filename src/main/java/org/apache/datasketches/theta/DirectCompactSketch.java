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

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED;
import static org.apache.datasketches.theta.CompactOperations.checkIllegalCurCountAndEmpty;
import static org.apache.datasketches.theta.CompactOperations.segmentToCompact;
import static org.apache.datasketches.theta.PreambleUtil.ORDERED_FLAG_MASK;
import static org.apache.datasketches.theta.PreambleUtil.extractCurCount;
import static org.apache.datasketches.theta.PreambleUtil.extractFlags;
import static org.apache.datasketches.theta.PreambleUtil.extractSeedHash;
import static org.apache.datasketches.theta.PreambleUtil.extractThetaLong;
import static org.apache.datasketches.theta.SingleItemSketch.checkForSingleItem;

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.MemorySegmentStatus;
import org.apache.datasketches.common.Util;

/**
 * An off-heap (Direct), compact, read-only sketch. The internal hash array can be either ordered
 * or unordered. It is not empty, not a single item.
 *
 * <p>This sketch can only be associated with a Serialization Version 3 format binary image.</p>
 *
 * <p>This implementation uses data in a given MemorySegment that is owned and managed by the caller.
 * This MemorySegment can be off-heap, which if managed properly will greatly reduce the need for
 * the JVM to perform garbage collection.</p>
 *
 * @author Lee Rhodes
 */
class DirectCompactSketch extends CompactThetaSketch {
  final MemorySegment seg_;

  /**
   * Construct this sketch with the given MemorySegment.
   * @param seg (optional) Read-only MemorySegment object.
   */
  DirectCompactSketch(final MemorySegment seg) {
    seg_ = seg;
  }

  /**
   * Wraps the given MemorySegment, which must be a SerVer 3, CompactThetaSketch image.
   * Must check the validity of the MemorySegment before calling. The order bit must be set properly.
   * @param srcSeg the given MemorySegment
   * @param seedHash The update seedHash.
   * <a href="{@docRoot}/resources/dictionary.html#seedHash">See Seed Hash</a>.
   * @return this sketch
   */
  static DirectCompactSketch wrapInstance(final MemorySegment srcSeg, final short seedHash) {
    Util.checkSeedHashes((short) extractSeedHash(srcSeg), seedHash);
    return new DirectCompactSketch(srcSeg);
  }

  //ThetaSketch Overrides

  @Override
  public CompactThetaSketch compact(final boolean dstOrdered, final MemorySegment dstSeg) {
    return segmentToCompact(seg_, dstOrdered, dstSeg);
  }

  @Override
  public int getCurrentBytes() {
    if (checkForSingleItem(seg_)) { return 16; }
    final int preLongs = ThetaSketch.getPreambleLongs(seg_);
    final int curCount = (preLongs == 1) ? 0 : extractCurCount(seg_);
    return (preLongs + curCount) << 3;
  }

  @Override
  public int getRetainedEntries(final boolean valid) { //valid is only relevant for the AlphaSketch
    if (checkForSingleItem(seg_)) { return 1; }
    final int preLongs = ThetaSketch.getPreambleLongs(seg_);
    return (preLongs == 1) ? 0 : extractCurCount(seg_);
  }

  @Override
  public long getThetaLong() {
    final int preLongs = ThetaSketch.getPreambleLongs(seg_);
    return (preLongs > 2) ? extractThetaLong(seg_) : Long.MAX_VALUE;
  }

  @Override
  public boolean hasMemorySegment() {
    return (seg_ != null) && seg_.scope().isAlive();
  }

  @Override
  public boolean isOffHeap() {
    return hasMemorySegment() && seg_.isNative();
  }

  @Override
  public boolean isEmpty() {
    final boolean emptyFlag = PreambleUtil.isEmptyFlag(seg_);
    final long thetaLong = getThetaLong();
    final int curCount = getRetainedEntries(true);
    return emptyFlag || ((curCount == 0) && (thetaLong == Long.MAX_VALUE));
  }

  @Override
  public boolean isOrdered() {
    return (extractFlags(seg_) & ORDERED_FLAG_MASK) > 0;
  }

  @Override
  public boolean isSameResource(final MemorySegment that) {
    return hasMemorySegment() && MemorySegmentStatus.isSameResource(seg_, that);

  }

  @Override
  public HashIterator iterator() {
    return new MemorySegmentHashIterator(seg_, getRetainedEntries(true), getThetaLong());
  }

  @Override
  public byte[] toByteArray() {
    checkIllegalCurCountAndEmpty(isEmpty(), getRetainedEntries());
    final int outBytes = getCurrentBytes();
    final byte[] byteArrOut = new byte[outBytes];
    MemorySegment.copy(seg_, JAVA_BYTE, 0, byteArrOut, 0, outBytes);
    return byteArrOut;
  }

  //restricted methods

  @Override
  long[] getCache() {
    if (checkForSingleItem(seg_)) { return new long[] { seg_.get(JAVA_LONG_UNALIGNED, 8) }; }
    final int preLongs = ThetaSketch.getPreambleLongs(seg_);
    final int curCount = (preLongs == 1) ? 0 : extractCurCount(seg_);
    if (curCount > 0) {
      final long[] cache = new long[curCount];
      MemorySegment.copy(seg_, JAVA_LONG_UNALIGNED, preLongs << 3, cache, 0, curCount);
      return cache;
    }
    return new long[0];
  }

  @Override
  int getCompactPreambleLongs() {
    return ThetaSketch.getPreambleLongs(seg_);
  }

  @Override
  int getCurrentPreambleLongs() {
    return ThetaSketch.getPreambleLongs(seg_);
  }

  @Override
  MemorySegment getMemorySegment() {
    return seg_;
  }

  @Override
  short getSeedHash() {
    return (short) extractSeedHash(seg_);
  }
}
