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

import static com.yahoo.sketches.Util.checkSeedHashes;
import static com.yahoo.sketches.Util.computeSeedHash;
import static com.yahoo.sketches.theta.PreambleUtil.extractCurCount;
import static com.yahoo.sketches.theta.PreambleUtil.extractPreLongs;
import static com.yahoo.sketches.theta.PreambleUtil.extractSeedHash;
import static com.yahoo.sketches.theta.PreambleUtil.extractThetaLong;

import com.yahoo.memory.Memory;

/**
 * An on-heap, compact, read-only sketch.
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

  /**
   * Heapifies the given source Memory with seed
   * @param srcMem <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See Update Hash Seed</a>.
   * @return a CompactSketch
   */
  static CompactSketch heapifyInstance(final Memory srcMem, final long seed) {
    final short memSeedHash = (short) extractSeedHash(srcMem);
    final short computedSeedHash = computeSeedHash(seed);
    checkSeedHashes(memSeedHash, computedSeedHash);

    final int preLongs = extractPreLongs(srcMem); //must be > 1
    final boolean empty = PreambleUtil.isEmpty(srcMem); //checks for cap <= 8
    int curCount = 0;
    long thetaLong = Long.MAX_VALUE;
    long[] cache = new long[0];
    curCount = extractCurCount(srcMem);
    cache = new long[curCount];
    if (preLongs == 2) {
      srcMem.getLongArray(16, cache, 0, curCount);
    } else { //preLongs == 3
      srcMem.getLongArray(24, cache, 0, curCount);
      thetaLong = extractThetaLong(srcMem);
    }
    return new HeapCompactUnorderedSketch(cache, empty, memSeedHash, curCount, thetaLong);
  }

  /**
   * Constructs this sketch from correct, valid arguments.
   * @param cache in compact form
   * @param empty The correct <a href="{@docRoot}/resources/dictionary.html#empty">Empty</a>.
   * @param seedHash The correct
   * <a href="{@docRoot}/resources/dictionary.html#seedHash">Seed Hash</a>.
   * @param curCount correct value
   * @param thetaLong The correct
   * <a href="{@docRoot}/resources/dictionary.html#thetaLong">thetaLong</a>.
   * @return a CompactSketch
   */
  static CompactSketch compact(final long[] cache, final boolean empty,
      final short seedHash, final int curCount, final long thetaLong) {
    return new HeapCompactUnorderedSketch(cache, empty, seedHash, curCount, thetaLong);
  }

  //Sketch interface

  @Override
  public byte[] toByteArray() {
    return toByteArray(false);
  }

  //restricted methods

  @Override
  public boolean isOrdered() {
    return false;
  }

}
