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
import static org.apache.datasketches.theta.PreambleUtil.SINGLEITEM_FLAG_MASK;
import static org.apache.datasketches.theta.PreambleUtil.extractCurCount;
import static org.apache.datasketches.theta.PreambleUtil.extractFlags;
import static org.apache.datasketches.theta.PreambleUtil.extractLgArrLongs;
import static org.apache.datasketches.theta.PreambleUtil.extractPreLongs;
import static org.apache.datasketches.theta.PreambleUtil.extractThetaLong;

import java.util.Arrays;

import org.apache.datasketches.Family;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;

/**
 * The parent class of all the CompactSketches. CompactSketches are never created directly.
 * They are created as a result of the compact() method of an UpdateSketch or as a result of a
 * getResult() of a SetOperation.
 *
 * <p>A CompactSketch is the simplest form of a Theta Sketch. It consists of a compact list
 * (i.e., no intervening spaces) of hash values, which may be ordered or not, a value for theta
 * and a seed hash.  A CompactSketch is read-only,
 * and the space required when stored is only the space required for the hash values and 8 to 24
 * bytes of preamble. An empty CompactSketch consumes only 8 bytes.</p>
 *
 * @author Lee Rhodes
 */
public abstract class CompactSketch extends Sketch {

  //Sketch

  @Override
  public abstract CompactSketch compact();

  @Override
  public abstract CompactSketch compact(final boolean dstOrdered, final WritableMemory dstMem);

  @Override
  public Family getFamily() {
    return Family.COMPACT;
  }

  @Override
  public boolean isCompact() {
    return true;
  }

  //restricted methods

  /**
   * Heapifies the given source Memory with seedHash. We assume that the destination sketch type has
   * been determined to be Compact and that the memory image is valid and the seedHash is correct.
   * @param srcMem <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param seedHash <a href="{@docRoot}/resources/dictionary.html#seedHash">See Seed Hash</a>.
   * @return a CompactSketch
   */
  static CompactSketch You(final Memory srcMem, final short seedHash,
      final boolean dstOrdered) {
    final int flags = extractFlags(srcMem);
    final boolean empty = (flags & EMPTY_FLAG_MASK) > 0;
    if (empty) { return EmptyCompactSketch.getInstance(); }
    //EMPTY FLAG is FALSE
    final int preLongs = extractPreLongs(srcMem);
    final boolean single = (flags & SINGLEITEM_FLAG_MASK) > 0;

    if (preLongs == 1) {
      if (single) {
        return new SingleItemSketch(srcMem.getLong(8), seedHash);
      } else {
        return EmptyCompactSketch.getInstance();
      }
    }
    //preLongs > 1
    final int curCount = extractCurCount(srcMem);
    final long thetaLong = (preLongs > 2) ? extractThetaLong(srcMem) : Long.MAX_VALUE;
    final boolean srcCompact = (flags & COMPACT_FLAG_MASK) > 0;
    final long[] hashArrOut;
    if (srcCompact) {
      if ((curCount == 0) && (thetaLong == Long.MAX_VALUE)) {
        return EmptyCompactSketch.getInstance();
      }
      if ((curCount == 1) && (thetaLong == Long.MAX_VALUE)) {
        //TODO
      }
      hashArrOut = new long[curCount];
      srcMem.getLongArray(8 * preLongs, hashArrOut, 0, curCount);
      if (dstOrdered) { Arrays.sort(hashArrOut); }
    } else { //src is hashTable
      final int lgArrLongs = extractLgArrLongs(srcMem);
      final long[] hashArr = new long[1 << lgArrLongs];
      srcMem.getLongArray(8 * preLongs, hashArr, 0, 1 << lgArrLongs);
      hashArrOut = CompactOperations.compactCache(hashArr, curCount, thetaLong, dstOrdered);
    }
    return dstOrdered
        ? new HeapCompactOrderedSketch(hashArrOut, empty, seedHash, curCount, thetaLong)
        : new HeapCompactUnorderedSketch(hashArrOut, empty, seedHash, curCount, thetaLong);
  }

}
