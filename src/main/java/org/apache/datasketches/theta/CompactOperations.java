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
import static org.apache.datasketches.theta.PreambleUtil.SER_VER;
import static org.apache.datasketches.theta.PreambleUtil.SINGLEITEM_FLAG_MASK;
import static org.apache.datasketches.theta.PreambleUtil.extractCurCount;
import static org.apache.datasketches.theta.PreambleUtil.extractFamilyID;
import static org.apache.datasketches.theta.PreambleUtil.extractFlags;
import static org.apache.datasketches.theta.PreambleUtil.extractLgArrLongs;
import static org.apache.datasketches.theta.PreambleUtil.extractPreLongs;
import static org.apache.datasketches.theta.PreambleUtil.extractSeedHash;
import static org.apache.datasketches.theta.PreambleUtil.extractSerVer;
import static org.apache.datasketches.theta.PreambleUtil.extractThetaLong;
import static org.apache.datasketches.theta.PreambleUtil.insertCurCount;
import static org.apache.datasketches.theta.PreambleUtil.insertFamilyID;
import static org.apache.datasketches.theta.PreambleUtil.insertFlags;
import static org.apache.datasketches.theta.PreambleUtil.insertP;
import static org.apache.datasketches.theta.PreambleUtil.insertPreLongs;
import static org.apache.datasketches.theta.PreambleUtil.insertSeedHash;
import static org.apache.datasketches.theta.PreambleUtil.insertSerVer;
import static org.apache.datasketches.theta.PreambleUtil.insertThetaLong;

import java.util.Arrays;

import org.apache.datasketches.Family;
import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;

/**
 * @author Lee Rhodes
 */
final class CompactOperations {

  private CompactOperations() {}

  static CompactSketch componentsToCompact( //No error checking
      final long thetaLong,
      final int curCount,
      final short seedHash,
      final boolean srcCompact,
      boolean srcOrdered,
      final boolean dstOrdered,
      final WritableMemory dstMem,
      final long[] hashArr) //may not be compacted, ordered or unordered

  {
    final boolean direct = dstMem != null;
    final boolean empty = (curCount == 0) && (thetaLong == Long.MAX_VALUE);
    final boolean single = (curCount == 1) && (thetaLong == Long.MAX_VALUE);
    final long[] hashArrOut;
    if (!srcCompact) {
      hashArrOut = CompactOperations.compactCache(hashArr, curCount, thetaLong, dstOrdered);
      srcOrdered = true;
    } else {
      hashArrOut = hashArr;
    }
    if (!srcOrdered && dstOrdered && !empty && !single) {
      Arrays.sort(hashArrOut);
    }
    if (direct) {
      final int preLongs = computeCompactPreLongs(thetaLong, empty, curCount);
      int flags = READ_ONLY_FLAG_MASK | COMPACT_FLAG_MASK; //always LE
      flags |=  empty ? EMPTY_FLAG_MASK : 0;
      flags |= dstOrdered ? ORDERED_FLAG_MASK : 0;
      flags |= single ? SINGLEITEM_FLAG_MASK : 0;
      final Memory mem =
          loadCompactMemory(hashArr, seedHash, curCount, thetaLong, dstMem, (byte)flags, preLongs);
      if (dstOrdered) {
        return new DirectCompactOrderedSketch(mem);
      } else {
        return new DirectCompactUnorderedSketch(mem);
      }
    } else { //Heap
      if (empty) {
        return EmptyCompactSketch.getInstance();
      }
      if (single) {
        return new SingleItemSketch(hashArrOut[0], seedHash);
      }
      if (dstOrdered) {
        return new HeapCompactOrderedSketch(hashArrOut, empty, seedHash, curCount, thetaLong);
      } else {
        return new HeapCompactUnorderedSketch(hashArrOut, empty, seedHash, curCount, thetaLong);
      }
    }
  }

  @SuppressWarnings("unused")
  static CompactSketch memoryToCompact(
      final Memory srcMem,
      final boolean dstOrdered,
      final WritableMemory dstMem)
  {
    //extract Pre0 fields
    final int preLongs = extractPreLongs(srcMem);
    final int serVer = extractSerVer(srcMem);
    final int famId = extractFamilyID(srcMem);
    final int lgArrLongs = extractLgArrLongs(srcMem);
    final int flags = extractFlags(srcMem);
    final short seedHash = (short) extractSeedHash(srcMem);



    final int curCount = extractCurCount(srcMem);
    final long thetaLong = extractThetaLong(srcMem);

    final boolean empty = (flags & EMPTY_FLAG_MASK) > 0;
    final boolean srcCompact = (flags & COMPACT_FLAG_MASK) > 0;
    final boolean srcOrdered = (flags & ORDERED_FLAG_MASK) > 0;
    final boolean single = (flags & SINGLEITEM_FLAG_MASK) > 0;
    if (!srcOrdered) {

    }
    final long[] hashArr ;

    //do checks ...
    final boolean direct = dstMem != null;
    if (empty)  { assert (curCount == 0) && (thetaLong == Long.MAX_VALUE); }
    if (single) { assert (curCount == 1) && (thetaLong == Long.MAX_VALUE); }
    if (direct) {

    } else { //heap
      //dispatch empty and single
      //dispatch other
    }
    return null;
  }

  //All arguments must be valid and correct including flags.
  // Used as helper to create byte arrays as well as loading Memory for direct compact sketches
  static final Memory loadCompactMemory(
      final long[] compactHashArr,
      final short seedHash,
      final int curCount,
      final long thetaLong,
      final WritableMemory dstMem,
      final byte flags,
      final int preLongs)
  {
    assert (dstMem != null) && (compactHashArr != null);
    final int outLongs = preLongs + curCount;
    final int outBytes = outLongs << 3;
    final int dstBytes = (int) dstMem.getCapacity();
    if (outBytes > dstBytes) {
      throw new SketchesArgumentException("Insufficient Memory: " + dstBytes
        + ", Need: " + outBytes);
    }
    final byte famID = (byte) Family.COMPACT.getID();

    insertPreLongs(dstMem, preLongs); //RF not used = 0
    insertSerVer(dstMem, SER_VER);
    insertFamilyID(dstMem, famID);
    //ignore lgNomLongs, lgArrLongs bytes for compact sketches
    insertFlags(dstMem, flags);
    insertSeedHash(dstMem, seedHash);

    if ((preLongs == 1) && (curCount == 1)) { //singleItem, theta = 1.0
      dstMem.putLong(8, compactHashArr[0]);
      return dstMem;
    }
    if (preLongs > 1) {
      insertCurCount(dstMem, curCount);
      insertP(dstMem, (float) 1.0);
    }
    if (preLongs > 2) {
      insertThetaLong(dstMem, thetaLong);
    }
    if (curCount > 0) { //theta could be < 1.0.
      dstMem.putLongArray(preLongs << 3, compactHashArr, 0, curCount);
    }
    return dstMem; //curCount == 0, theta could be < 1.0
  }

  static final int computeCompactPreLongs(final long thetaLong, final boolean empty,
      final int curCount) {
    return (thetaLong < Long.MAX_VALUE) ? 3 : empty ? 1 : (curCount > 1) ? 2 : 1;
  }

  /**
   * Compact the given array. The source cache can be a hash table with interstitial zeros or
   * "dirty" values, which are hash values greater than theta. These can be generated by the
   * Alpha sketch.
   * @param srcCache anything
   * @param curCount must be correct
   * @param thetaLong The correct
   * <a href="{@docRoot}/resources/dictionary.html#thetaLong">thetaLong</a>.
   * @param dstOrdered true if output array must be sorted
   * @return the compacted array
   */
  static final long[] compactCache(final long[] srcCache, final int curCount,
      final long thetaLong, final boolean dstOrdered) {
    if (curCount == 0) {
      return new long[0];
    }
    final long[] cacheOut = new long[curCount];
    final int len = srcCache.length;
    int j = 0;
    for (int i = 0; i < len; i++) { //scan the full srcCache
      final long v = srcCache[i];
      if ((v <= 0L) || (v >= thetaLong) ) { continue; } //ignoring zeros or dirty values
      cacheOut[j++] = v;
    }
    assert curCount == j;
    if (dstOrdered && (curCount > 1)) {
      Arrays.sort(cacheOut);
    }
    return cacheOut;
  }




}

