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

import static org.apache.datasketches.theta.PreambleUtil.checkMemorySeedHash;
import static org.apache.datasketches.theta.PreambleUtil.extractCurCount;
import static org.apache.datasketches.theta.PreambleUtil.extractPreLongs;
import static org.apache.datasketches.theta.PreambleUtil.extractThetaLong;

import org.apache.datasketches.memory.Memory;

/**
 * An on-heap, compact, ordered, read-only sketch.
 *
 * @author Lee Rhodes
 */
final class HeapCompactOrderedSketch extends HeapCompactSketch {

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
  HeapCompactOrderedSketch(final long[] cache, final boolean empty, final short seedHash,
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
    final short memSeedHash = checkMemorySeedHash(srcMem, seed);
    final int preLongs = extractPreLongs(srcMem);
    final boolean empty = PreambleUtil.isEmpty(srcMem); //checks for cap <= 8
    long thetaLong = Long.MAX_VALUE;
    //preLongs == 1 handled before this method, so preLongs > 1
    final int curCount = extractCurCount(srcMem);
    final long[] cache = new long[curCount];
    if (preLongs == 2) {
      srcMem.getLongArray(16, cache, 0, curCount);
    } else { //preLongs == 3
      srcMem.getLongArray(24, cache, 0, curCount);
      thetaLong = extractThetaLong(srcMem);
    }
    return new HeapCompactOrderedSketch(cache, empty, memSeedHash, curCount, thetaLong);
  }

  /**
   * Constructs this sketch from correct, valid arguments.
   * @param cache in compact, ordered form
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
    return new HeapCompactOrderedSketch(cache, empty, seedHash, curCount, thetaLong);
  }

  //Sketch interface

  @Override
  public byte[] toByteArray() {
    return toByteArray(true);
  }

  @Override
  public boolean isOrdered() {
    return true;
  }

}
