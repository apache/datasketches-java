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

package com.yahoo.sketches.theta;

import static com.yahoo.sketches.theta.PreambleUtil.extractCurCount;
import static com.yahoo.sketches.theta.PreambleUtil.extractPreLongs;
import static com.yahoo.sketches.theta.PreambleUtil.extractSeedHash;
import static com.yahoo.sketches.theta.PreambleUtil.extractThetaLong;

import com.yahoo.memory.Memory;

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
  //overidden by EmptyCompactSketch and SingleItemSketch
  @Override
  public int getCurrentBytes(final boolean compact) { //compact is ignored here
    final int preLongs = getCurrentPreambleLongs(true);
    //preLongs > 1
    final int curCount = extractCurCount(mem_);
    return (preLongs + curCount) << 3;
  }

  @Override
  public HashIterator iterator() {
    return new MemoryHashIterator(mem_, getRetainedEntries(), getThetaLong());
  }

  //overidden by EmptyCompactSketch and SingleItemSketch
  @Override
  public int getRetainedEntries(final boolean valid) { //compact is always valid
    //preLongs > 1
    return extractCurCount(mem_);
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
    return PreambleUtil.isEmpty(mem_);
  }

  @Override
  public boolean isSameResource(final Memory that) {
    return mem_.isSameResource(that);
  }

  @Override
  public byte[] toByteArray() {
    return
        compactMemoryToByteArray(mem_, getCurrentPreambleLongs(true), getRetainedEntries(true));
  }

  //restricted methods

  @Override
  long[] getCache() {
    final int curCount = getRetainedEntries(true);
    if (curCount > 0) {
      final long[] cache = new long[curCount];
      final int preLongs = getCurrentPreambleLongs(true);
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

  /**
   * Serializes a Memory based compact sketch to a byte array
   * @param srcMem the source Memory
   * @param preLongs current preamble longs
   * @param curCount the current valid count
   * @return this Direct, Compact sketch as a byte array
   */
  static byte[] compactMemoryToByteArray(final Memory srcMem, final int preLongs,
      final int curCount) {
    final int outBytes = (curCount + preLongs) << 3;
    final byte[] byteArrOut = new byte[outBytes];
    srcMem.getByteArray(0, byteArrOut, 0, outBytes); //copies the whole thing
    return byteArrOut;
  }

}
