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
import static org.apache.datasketches.theta.CompactOperations.memoryToCompact;
import static org.apache.datasketches.theta.PreambleUtil.ORDERED_FLAG_MASK;
import static org.apache.datasketches.theta.PreambleUtil.checkMemorySeedHash;
import static org.apache.datasketches.theta.PreambleUtil.extractCurCount;
import static org.apache.datasketches.theta.PreambleUtil.extractFlags;
import static org.apache.datasketches.theta.PreambleUtil.extractPreLongs;
import static org.apache.datasketches.theta.PreambleUtil.extractSeedHash;
import static org.apache.datasketches.theta.PreambleUtil.extractThetaLong;
import static org.apache.datasketches.theta.SingleItemSketch.otherCheckForSingleItem;

import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;

/**
 * An off-heap (Direct), compact, read-only sketch. The internal hash array can be either ordered
 * or unordered.
 *
 * <p>This sketch can only be associated with a Serialization Version 3 format binary image.</p>
 *
 * <p>This implementation uses data in a given Memory that is owned and managed by the caller.
 * This Memory can be off-heap, which if managed properly will greatly reduce the need for
 * the JVM to perform garbage collection.</p>
 *
 * @author Lee Rhodes
 */
class DirectCompactSketch extends CompactSketch {
  final Memory mem_;

  /**
   * Construct this sketch with the given memory.
   * @param mem Read-only Memory object with the order bit properly set.
   */
  DirectCompactSketch(final Memory mem) {
    mem_ = mem;
  }

  /**
   * Wraps the given Memory, which must be a SerVer 3, ordered, CompactSketch image.
   * Must check the validity of the Memory before calling. The order bit must be set properly.
   * @param srcMem <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param seed The update seed.
   * <a href="{@docRoot}/resources/dictionary.html#seed">See Update Hash Seed</a>.
   * @return this sketch
   */
  static DirectCompactSketch wrapInstance(final Memory srcMem, final long seed) {
    checkMemorySeedHash(srcMem, seed);
    return new DirectCompactSketch(srcMem);
  }

  //Sketch

  @Override
  public CompactSketch compact(final boolean dstOrdered, final WritableMemory dstMem) {
    return memoryToCompact(mem_, dstOrdered, dstMem);
  }

  @Override
  public int getCurrentBytes() {
    if (otherCheckForSingleItem(mem_)) { return 16; }
    final int preLongs = extractPreLongs(mem_);
    final int curCount = (preLongs == 1) ? 0 : extractCurCount(mem_);
    return (preLongs + curCount) << 3;
  }

  @Override
  public double getEstimate() {
    if (otherCheckForSingleItem(mem_)) { return 1; }
    final int preLongs = extractPreLongs(mem_);
    final int curCount = (preLongs == 1) ? 0 : extractCurCount(mem_);
    final long thetaLong = (preLongs > 2) ? extractThetaLong(mem_) : Long.MAX_VALUE;
    return Sketch.estimate(thetaLong, curCount);
  }

  @Override
  public int getRetainedEntries(final boolean valid) { //compact is always valid
    if (otherCheckForSingleItem(mem_)) { return 1; }
    final int preLongs = extractPreLongs(mem_);
    final int curCount = (preLongs == 1) ? 0 : extractCurCount(mem_);
    return curCount;
  }

  @Override
  public long getThetaLong() {
    final int preLongs = extractPreLongs(mem_);
    return (preLongs > 2) ? extractThetaLong(mem_) : Long.MAX_VALUE;
  }

  @Override
  public boolean hasMemory() {
    return true;
  }

  @Override
  public boolean isDirect() {
    return mem_.isDirect();
  }

  @Override
  public boolean isEmpty() {
    final boolean emptyFlag = PreambleUtil.isEmptyFlag(mem_);
    final long thetaLong = getThetaLong();
    final int curCount = getRetainedEntries(true);
    return emptyFlag || ((curCount == 0) && (thetaLong == Long.MAX_VALUE));
  }

  @Override
  public boolean isOrdered() {
    return (extractFlags(mem_) & ORDERED_FLAG_MASK) > 0;
  }

  @Override
  public boolean isSameResource(final Memory that) {
    return mem_.isSameResource(that);
  }

  @Override
  public HashIterator iterator() {
    return new MemoryHashIterator(mem_, getRetainedEntries(true), getThetaLong());
  }

  @Override
  public byte[] toByteArray() {
    final int curCount = getRetainedEntries(true);
    checkIllegalCurCountAndEmpty(isEmpty(), curCount);
    final int preLongs = extractPreLongs(mem_);
    final int outBytes = (curCount + preLongs) << 3;
    final byte[] byteArrOut = new byte[outBytes];
    mem_.getByteArray(0, byteArrOut, 0, outBytes);
    return byteArrOut;
  }

  //restricted methods

  @Override
  long[] getCache() {
    if (otherCheckForSingleItem(mem_)) { return new long[] { mem_.getLong(8) }; }
    final int preLongs = extractPreLongs(mem_);
    final int curCount = (preLongs == 1) ? 0 : extractCurCount(mem_);
    if (curCount > 0) {
      final long[] cache = new long[curCount];
      mem_.getLongArray(preLongs << 3, cache, 0, curCount);
      return cache;
    }
    return new long[0];
  }

  @Override
  int getCompactPreambleLongs() {
    return extractPreLongs(mem_);
  }

  @Override
  int getCurrentPreambleLongs() {
    return extractPreLongs(mem_);
  }

  @Override
  Memory getMemory() {
    return mem_;
  }

  @Override
  short getSeedHash() {
    return (short) extractSeedHash(mem_);
  }
}
