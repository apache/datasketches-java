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

import static org.apache.datasketches.theta.PreambleUtil.extractCurCount;
import static org.apache.datasketches.theta.PreambleUtil.extractPreLongs;
import static org.apache.datasketches.theta.PreambleUtil.extractSeedHash;
import static org.apache.datasketches.theta.PreambleUtil.extractThetaLong;
import static org.apache.datasketches.theta.SingleItemSketch.otherCheckForSingleItem;

import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;

/**
 * Parent class of the Direct Compact Sketches.
 *
 * @author Lee Rhodes
 */
abstract class DirectCompactSketch extends CompactSketch {
  final Memory mem_;

  DirectCompactSketch(final Memory mem) {
    mem_ = mem;
  }

  //Sketch

  @Override
  public CompactSketch compact() {
    return compact(true, null);
  }

  @Override
  public CompactSketch compact(final boolean dstOrdered, final WritableMemory dstMem) {
    return CompactOperations.memoryToCompact(mem_, dstOrdered, dstMem);
  }

  @Override
  public int getCurrentBytes(final boolean compact) { //compact is ignored here
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
  public boolean isSameResource(final Memory that) {
    return mem_.isSameResource(that);
  }

  @Override
  public HashIterator iterator() {
    return new MemoryHashIterator(mem_, getRetainedEntries(), getThetaLong());
  }

  @Override
  public byte[] toByteArray() {
    final int curCount = getRetainedEntries(true);
    Sketch.checkIllegalCurCountAndEmpty(isEmpty(), curCount);
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
  int getCurrentPreambleLongs(final boolean compact) { //already compact; ignore
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
