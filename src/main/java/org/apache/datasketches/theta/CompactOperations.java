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
import static org.apache.datasketches.theta.PreambleUtil.LG_NOM_LONGS_BYTE;
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
import org.apache.datasketches.SketchesStateException;
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
      final boolean srcEmpty,
      final boolean srcCompact,
      final boolean srcOrdered,
      final boolean dstOrdered,
      final WritableMemory dstMem,
      final long[] hashArr) //may not be compacted, ordered or unordered, may be null
  {
    final boolean direct = dstMem != null;
    final boolean empty = srcEmpty || ((curCount == 0) && (thetaLong == Long.MAX_VALUE));
    final boolean single = (curCount == 1) && (thetaLong == Long.MAX_VALUE);
    final long[] hashArrOut;
    if (!srcCompact) {
      hashArrOut = CompactOperations.compactCache(hashArr, curCount, thetaLong, dstOrdered);
    } else {
      hashArrOut = hashArr;
    }
    if (!srcOrdered && dstOrdered && !empty && !single) {
      Arrays.sort(hashArrOut);
    }
    //Note: for empty or single we always output the ordered form.
    final boolean dstOrderedOut = (empty || single) ? true : dstOrdered;
    if (direct) {
      final int preLongs = computeCompactPreLongs(empty, curCount, thetaLong);
      int flags = READ_ONLY_FLAG_MASK | COMPACT_FLAG_MASK; //always LE
      flags |=  empty ? EMPTY_FLAG_MASK : 0;
      flags |= dstOrderedOut ? ORDERED_FLAG_MASK : 0;
      flags |= single ? SINGLEITEM_FLAG_MASK : 0;

      final Memory mem =
          loadCompactMemory(hashArrOut, seedHash, curCount, thetaLong, dstMem, (byte)flags, preLongs);
      return new DirectCompactSketch(mem);

    } else { //Heap
      if (empty) {
        return EmptyCompactSketch.getInstance();
      }
      if (single) {
        return new SingleItemSketch(hashArrOut[0], seedHash);
      }
      return new HeapCompactSketch(hashArrOut, empty, seedHash, curCount, thetaLong, dstOrderedOut);
    }
  }

  /**
   * Heapify or convert a source Theta Sketch Memory image into a heap or target Memory CompactSketch.
   * This assumes hashSeed is OK; serVer = 3.
   * @param srcMem the given input source Memory image
   * @param dstOrdered the desired ordering of the resulting CompactSketch
   * @param dstMem Used for the target CompactSketch if it is Direct.
   * @return a CompactSketch of the correct form.
   */
  @SuppressWarnings("unused")
  static CompactSketch memoryToCompact(
      final Memory srcMem,
      final boolean dstOrdered,
      final WritableMemory dstMem)
  {
    //extract Pre0 fields and Flags from srcMem
    final int srcPreLongs = extractPreLongs(srcMem);
    final int srcSerVer = extractSerVer(srcMem); //not used
    final int srcFamId = extractFamilyID(srcMem);
    final Family srcFamily = Family.idToFamily(srcFamId);
    final int srcLgArrLongs = extractLgArrLongs(srcMem);
    final int srcFlags = extractFlags(srcMem);
    final short srcSeedHash = (short) extractSeedHash(srcMem);

    //srcFlags
    final boolean srcReadOnlyFlag = (srcFlags & READ_ONLY_FLAG_MASK) > 0;
    final boolean srcEmptyFlag = (srcFlags & EMPTY_FLAG_MASK) > 0;
    final boolean srcCompactFlag = (srcFlags & COMPACT_FLAG_MASK) > 0;
    final boolean srcOrderedFlag = (srcFlags & ORDERED_FLAG_MASK) > 0;
    final boolean srcSingleFlag = (srcFlags & SINGLEITEM_FLAG_MASK) > 0;

    final boolean single = srcSingleFlag
        || SingleItemSketch.otherCheckForSingleItem(srcPreLongs, srcSerVer, srcFamId, srcFlags);

    //extract pre1 and pre2 fields
    final int curCount = single ? 1 : (srcPreLongs > 1) ? extractCurCount(srcMem) : 0;
    final long thetaLong = (srcPreLongs > 2) ? extractThetaLong(srcMem) : Long.MAX_VALUE;

    //do some basic checks ...
    if (srcEmptyFlag)  { assert (curCount == 0) && (thetaLong == Long.MAX_VALUE); }
    if (single) { assert (curCount == 1) && (thetaLong == Long.MAX_VALUE); }
    checkFamilyAndFlags(srcFamId, srcCompactFlag, srcReadOnlyFlag);

    //dispatch empty and single cases
    //Note: for empty and single we always output the ordered form.
    final boolean dstOrderedOut = (srcEmptyFlag || single) ? true : dstOrdered;
    if (srcEmptyFlag) {
      if (dstMem != null) {
        dstMem.putByteArray(0, EmptyCompactSketch.EMPTY_COMPACT_SKETCH_ARR, 0, 8);
        return new DirectCompactSketch(dstMem);
      } else {
        return EmptyCompactSketch.getInstance();
      }
    }
    if (single) {
      final long hash = srcMem.getLong(srcPreLongs << 3);
      final SingleItemSketch sis = new SingleItemSketch(hash, srcSeedHash);
      if (dstMem != null) {
        dstMem.putByteArray(0, sis.toByteArray(),0, 16);
        return new DirectCompactSketch(dstMem);
      } else { //heap
        return sis;
      }
    }

    //extract hashArr > 1
    final long[] hashArr;
    if (srcCompactFlag) {
      hashArr = new long[curCount];
      srcMem.getLongArray(srcPreLongs << 3, hashArr, 0, curCount);
    } else { //update sketch, thus hashTable form
      final int srcCacheLen = 1 << srcLgArrLongs;
      final long[] tempHashArr = new long[srcCacheLen];
      srcMem.getLongArray(srcPreLongs << 3, tempHashArr, 0, srcCacheLen);
      hashArr = compactCache(tempHashArr, curCount, thetaLong, dstOrderedOut);
    }

    final int flagsOut = READ_ONLY_FLAG_MASK | COMPACT_FLAG_MASK
                         | ((dstOrderedOut) ? ORDERED_FLAG_MASK : 0);

    //load the destination.
    if (dstMem != null) {
      final Memory tgtMem = loadCompactMemory(hashArr, srcSeedHash, curCount, thetaLong, dstMem,
          (byte)flagsOut, srcPreLongs);
      return new DirectCompactSketch(tgtMem);
    } else { //heap
      return new HeapCompactSketch(hashArr, srcEmptyFlag, srcSeedHash, curCount, thetaLong,
          dstOrderedOut);
    }
  }

  private static final void checkFamilyAndFlags(
      final int srcFamId,
      final boolean srcCompactFlag,
      final boolean srcReadOnlyFlag) {
    final Family srcFamily = Family.idToFamily(srcFamId);
    if (srcCompactFlag) {
      if ((srcFamily == Family.COMPACT) && srcReadOnlyFlag) { return; }
    } else {
      if (srcFamily == Family.ALPHA) { return; }
      if (srcFamily == Family.QUICKSELECT) { return; }
    }
    throw new SketchesArgumentException(
        "Possible Corruption: Family does not match flags: Family: "
            + srcFamily.toString()
            + ", Compact Flag: " + srcCompactFlag
            + ", ReadOnly Flag: " + srcReadOnlyFlag);
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

    //Caution: The following loads directly into Memory without creating a heap byte[] first,
    // which would act as a pre-clearing, initialization mechanism. So it is important to make sure
    // that all fields are initialized, even those that are not used by the CompactSketch.
    // Otherwise, uninitialized fields could be filled with off-heap garbage, which could cause
    // other problems downstream if those fields are not filtered out first.
    // As written below, all fields are initialized avoiding an extra copy.

    //The first 8 bytes (pre0)
    insertPreLongs(dstMem, preLongs); //RF not used = 0
    insertSerVer(dstMem, SER_VER);
    insertFamilyID(dstMem, famID);
    //The following initializes the lgNomLongs and lgArrLongs to 0.
    //They are not used in CompactSketches.
    dstMem.putShort(LG_NOM_LONGS_BYTE, (short)0);
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
    return dstMem; //if prelongs == 3 & curCount == 0, theta could be < 1.0.
  }

  /**
   * Copies then compacts, cleans, and may sort the resulting array.
   * The source cache can be a hash table with interstitial zeros or
   * "dirty" values, which are hash values greater than theta.
   * These can be generated by the Alpha sketch.
   * @param srcCache anything
   * @param curCount must be correct
   * @param thetaLong The correct
   * <a href="{@docRoot}/resources/dictionary.html#thetaLong">thetaLong</a>.
   * @param dstOrdered true if output array must be sorted
   * @return the compacted array.
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
    if (j < curCount) {
      throw new SketchesStateException(
          "Possible Corruption: curCount parameter is incorrect.");
    }
    if (dstOrdered && (curCount > 1)) {
      Arrays.sort(cacheOut);
    }
    return cacheOut;
  }

  /*
   * The truth table for empty, curCount and theta when compacting is as follows:
   * <pre>
   * Num Theta CurCount Empty State    Comments
   *  0    1.0     0      T     OK     The Normal Empty State
   *  1    1.0     0      F   Internal This can result from an intersection of two exact, disjoint sets,
   *                                   or AnotB of two exact, identical sets. There is no probability
   *                                   distribution, so change to empty. Return {Th = 1.0, 0, T}.
   *                                   This is handled in SetOperation.createCompactSketch().
   *  2    1.0    !0      T   Error    Empty=T and curCount !0 should never co-exist.
   *                                   This is checked in all compacting operations.
   *  3    1.0    !0      F     OK     This corresponds to a sketch in exact mode
   *  4   <1.0     0      T   Internal This can be an initial UpdateSketch state if p < 1.0,
   *                                   so change theta to 1.0. Return {Th = 1.0, 0, T}.
   *                                   This is handled in UpdateSketch.compact() and toByteArray().
   *  5   <1.0     0      F     OK     This can result from set operations
   *  6   <1.0    !0      T   Error    Empty=T and curCount !0 should never co-exist.
   *                                   This is checked in all compacting operations.
   *  7   <1.0    !0      F     OK     This corresponds to a sketch in estimation mode
   * </pre>
   * #4 is handled by <i>correctThetaOnCompat(boolean, int)</i> (below).
   * #2 & #6 handled by <i>checkIllegalCurCountAndEmpty(boolean, int)</i>
   */

  /**
   * This corrects a temporary anomalous condition where compact() is called on an UpdateSketch
   * that was initialized with p < 1.0 and update() was never called.  In this case Theta < 1.0,
   * curCount = 0, and empty = true.  The correction is to change Theta to 1.0, which makes the
   * returning sketch empty. This should only be used in the compaction or serialization of an
   * UpdateSketch.
   * @param empty the given empty state
   * @param curCount the given curCount
   * @param thetaLong the given thetaLong
   * @return thetaLong
   */
  static final long correctThetaOnCompact(final boolean empty, final int curCount,
      final long thetaLong) { //handles #4 above
    return (empty && (curCount == 0)) ? Long.MAX_VALUE : thetaLong;
  }

  /**
   * This checks for the illegal condition where curCount > 0 and the state of
   * empty = true.  This check can be used anywhere a sketch is returned or a sketch is created
   * from complete arguments.
   * @param empty the given empty state
   * @param curCount the given current count
   */ //This handles #2 and #6 above
  static final void checkIllegalCurCountAndEmpty(final boolean empty, final int curCount) {
    if (empty && (curCount != 0)) { //this handles #2 and #6 above
      throw new SketchesStateException("Illegal State: Empty=true and Current Count != 0.");
    }
  }

  /**
   * This compute number of preamble longs for a compact sketch based on <i>empty</i>,
   * <i>curCount</i> and <i>thetaLong</i>.
   * This also accommodates for EmptyCompactSketch and SingleItemSketch.
   * @param empty The given empty state
   * @param curCount The given current count (retained entries)
   * @param thetaLong the current thetaLong
   * @return the number of preamble longs
   */
  static final int computeCompactPreLongs(final boolean empty, final int curCount,
      final long thetaLong) {
    return (thetaLong < Long.MAX_VALUE) ? 3 : empty ? 1 : (curCount > 1) ? 2 : 1;
  }

  /**
   * This checks for the singleItem Compact Sketch.
   * @param empty the given empty state
   * @param curCount the given curCount
   * @param thetaLong the given thetaLong
   * @return true if notEmpty, curCount = 1 and theta = 1.0;
   */
  static final boolean isSingleItem(final boolean empty, final int curCount,
      final long thetaLong) {
    return !empty && (curCount == 1) && (thetaLong == Long.MAX_VALUE);
  }
}

