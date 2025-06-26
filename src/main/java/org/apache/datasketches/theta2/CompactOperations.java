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

package org.apache.datasketches.theta2;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_SHORT_UNALIGNED;
import static org.apache.datasketches.theta2.PreambleUtil.COMPACT_FLAG_MASK;
import static org.apache.datasketches.theta2.PreambleUtil.EMPTY_FLAG_MASK;
import static org.apache.datasketches.theta2.PreambleUtil.LG_NOM_LONGS_BYTE;
import static org.apache.datasketches.theta2.PreambleUtil.ORDERED_FLAG_MASK;
import static org.apache.datasketches.theta2.PreambleUtil.READ_ONLY_FLAG_MASK;
import static org.apache.datasketches.theta2.PreambleUtil.SER_VER;
import static org.apache.datasketches.theta2.PreambleUtil.SINGLEITEM_FLAG_MASK;
import static org.apache.datasketches.theta2.PreambleUtil.extractCurCount;
import static org.apache.datasketches.theta2.PreambleUtil.extractFamilyID;
import static org.apache.datasketches.theta2.PreambleUtil.extractFlags;
import static org.apache.datasketches.theta2.PreambleUtil.extractLgArrLongs;
import static org.apache.datasketches.theta2.PreambleUtil.extractPreLongs;
import static org.apache.datasketches.theta2.PreambleUtil.extractSeedHash;
import static org.apache.datasketches.theta2.PreambleUtil.extractSerVer;
import static org.apache.datasketches.theta2.PreambleUtil.extractThetaLong;
import static org.apache.datasketches.theta2.PreambleUtil.insertCurCount;
import static org.apache.datasketches.theta2.PreambleUtil.insertFamilyID;
import static org.apache.datasketches.theta2.PreambleUtil.insertFlags;
import static org.apache.datasketches.theta2.PreambleUtil.insertP;
import static org.apache.datasketches.theta2.PreambleUtil.insertPreLongs;
import static org.apache.datasketches.theta2.PreambleUtil.insertSeedHash;
import static org.apache.datasketches.theta2.PreambleUtil.insertSerVer;
import static org.apache.datasketches.theta2.PreambleUtil.insertThetaLong;

import java.lang.foreign.MemorySegment;
import java.util.Arrays;

import org.apache.datasketches.common.Family;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.SketchesStateException;

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
      final MemorySegment dstWSeg,
      final long[] hashArr) //may not be compacted, ordered or unordered, may be null
  {
    final boolean direct = dstWSeg != null;
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

      final MemorySegment seg =
          loadCompactMemorySegment(hashArrOut, seedHash, curCount, thetaLong, dstWSeg, (byte)flags, preLongs);
      return new DirectCompactSketch(seg);

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
   * Heapify or convert a source Theta Sketch MemorySegment image into a heap or target MemorySegment CompactSketch.
   * This assumes hashSeed is OK; serVer = 3.
   * @param srcSeg the given input source MemorySegment image. Can be Read Only.
   * @param dstOrdered the desired ordering of the resulting CompactSketch
   * @param dstWSeg Used for the target CompactSketch if it is MemorySegment-based. Must be Writable.
   * @return a CompactSketch of the correct form.
   */
  @SuppressWarnings("unused")
  static CompactSketch segmentToCompact(
      final MemorySegment srcSeg,
      final boolean dstOrdered,
      final MemorySegment dstWSeg)
  {
    //extract Pre0 fields and Flags from srcMem
    final int srcPreLongs = extractPreLongs(srcSeg);
    final int srcSerVer = extractSerVer(srcSeg); //not used
    final int srcFamId = extractFamilyID(srcSeg);
    final int srcLgArrLongs = extractLgArrLongs(srcSeg);
    final int srcFlags = extractFlags(srcSeg);
    final short srcSeedHash = (short) extractSeedHash(srcSeg);

    //srcFlags
    final boolean srcReadOnlyFlag = (srcFlags & READ_ONLY_FLAG_MASK) > 0;
    final boolean srcEmptyFlag = (srcFlags & EMPTY_FLAG_MASK) > 0;
    final boolean srcCompactFlag = (srcFlags & COMPACT_FLAG_MASK) > 0;
    final boolean srcOrderedFlag = (srcFlags & ORDERED_FLAG_MASK) > 0;
    final boolean srcSingleFlag = (srcFlags & SINGLEITEM_FLAG_MASK) > 0;

    final boolean single = srcSingleFlag
        || SingleItemSketch.otherCheckForSingleItem(srcPreLongs, srcSerVer, srcFamId, srcFlags);

    //extract pre1 and pre2 fields
    final int curCount = single ? 1 : (srcPreLongs > 1) ? extractCurCount(srcSeg) : 0;
    final long thetaLong = (srcPreLongs > 2) ? extractThetaLong(srcSeg) : Long.MAX_VALUE;

    //do some basic checks ...
    if (srcEmptyFlag)  { assert (curCount == 0) && (thetaLong == Long.MAX_VALUE); }
    if (single) { assert (curCount == 1) && (thetaLong == Long.MAX_VALUE); }
    checkFamilyAndFlags(srcFamId, srcCompactFlag, srcReadOnlyFlag);

    //dispatch empty and single cases
    //Note: for empty and single we always output the ordered form.
    final boolean dstOrderedOut = (srcEmptyFlag || single) ? true : dstOrdered;
    if (srcEmptyFlag) {
      if (dstWSeg != null) {
        MemorySegment.copy(EmptyCompactSketch.EMPTY_COMPACT_SKETCH_ARR, 0, dstWSeg, JAVA_BYTE, 0, 8);
        return new DirectCompactSketch(dstWSeg);
      } else {
        return EmptyCompactSketch.getInstance();
      }
    }
    if (single) {
      final long hash = srcSeg.get(JAVA_LONG_UNALIGNED, srcPreLongs << 3);
      final SingleItemSketch sis = new SingleItemSketch(hash, srcSeedHash);
      if (dstWSeg != null) {
        MemorySegment.copy(sis.toByteArray(), 0, dstWSeg, JAVA_BYTE, 0, 16);
        return new DirectCompactSketch(dstWSeg);
      } else { //heap
        return sis;
      }
    }

    //extract hashArr > 1
    final long[] hashArr;
    if (srcCompactFlag) {
      hashArr = new long[curCount];
      MemorySegment.copy(srcSeg, JAVA_LONG_UNALIGNED, srcPreLongs << 3, hashArr, 0, curCount);
    } else { //update sketch, thus hashTable form
      final int srcCacheLen = 1 << srcLgArrLongs;
      final long[] tempHashArr = new long[srcCacheLen];
      MemorySegment.copy(srcSeg, JAVA_LONG_UNALIGNED, srcPreLongs << 3, tempHashArr, 0, srcCacheLen);
      hashArr = compactCache(tempHashArr, curCount, thetaLong, dstOrderedOut);
    }

    final int flagsOut = READ_ONLY_FLAG_MASK | COMPACT_FLAG_MASK
                         | ((dstOrderedOut) ? ORDERED_FLAG_MASK : 0);

    //load the destination.
    if (dstWSeg != null) {
      final MemorySegment tgtSeg = loadCompactMemorySegment(hashArr, srcSeedHash, curCount, thetaLong, dstWSeg,
          (byte)flagsOut, srcPreLongs);
      return new DirectCompactSketch(tgtSeg);
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
  // Used as helper to create byte arrays as well as loading MemorySegment for direct compact sketches
  //Input must be writable, return can be Read Only
  static final MemorySegment loadCompactMemorySegment(
      final long[] compactHashArr,
      final short seedHash,
      final int curCount,
      final long thetaLong,
      final MemorySegment dstWSeg,
      final byte flags,
      final int preLongs)
  {
    assert (dstWSeg != null) && (compactHashArr != null);
    final int outLongs = preLongs + curCount;
    final int outBytes = outLongs << 3;
    final int dstBytes = (int) dstWSeg.byteSize();
    if (outBytes > dstBytes) {
      throw new SketchesArgumentException("Insufficient Space in MemorySegment: " + dstBytes
        + ", Need: " + outBytes);
    }
    final byte famID = (byte) Family.COMPACT.getID();

    //Caution: The following loads directly into a MemorySegment without creating a heap byte[] first,
    // which would act as a pre-clearing, initialization mechanism. So it is important to make sure
    // that all fields are initialized, even those that are not used by the CompactSketch.
    // Otherwise, uninitialized fields could be filled with off-heap garbage, which could cause
    // other problems downstream if those fields are not filtered out first.
    // As written below, all fields are initialized avoiding an extra copy.

    //The first 8 bytes (pre0)
    insertPreLongs(dstWSeg, preLongs); //RF not used = 0
    insertSerVer(dstWSeg, SER_VER);
    insertFamilyID(dstWSeg, famID);
    //The following initializes the lgNomLongs and lgArrLongs to 0.
    //They are not used in CompactSketches.
    dstWSeg.set(JAVA_SHORT_UNALIGNED, LG_NOM_LONGS_BYTE, (short)0);
    insertFlags(dstWSeg, flags);
    insertSeedHash(dstWSeg, seedHash);

    if ((preLongs == 1) && (curCount == 1)) { //singleItem, theta = 1.0
      dstWSeg.set(JAVA_LONG_UNALIGNED, 8, compactHashArr[0]);
      return dstWSeg;
    }
    if (preLongs > 1) {
      insertCurCount(dstWSeg, curCount);
      insertP(dstWSeg, (float) 1.0);
    }
    if (preLongs > 2) {
      insertThetaLong(dstWSeg, thetaLong);
    }
    if (curCount > 0) { //theta could be < 1.0.
      //dstWSeg.putLongArray(preLongs << 3, compactHashArr, 0, curCount);
      MemorySegment.copy(compactHashArr, 0, dstWSeg, JAVA_LONG_UNALIGNED, preLongs << 3, curCount);
    }
    return dstWSeg; //if prelongs == 3 & curCount == 0, theta could be < 1.0. This can be RO
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
   * Num Theta CurCount Empty State    Name, Comments
   *  0    1.0     0      T     OK     EMPTY: The Normal Empty State
   *  1    1.0     0      F   Internal This can occur internally as the result of an intersection of two exact,
   *                                   disjoint sets, or AnotB of two exact, identical sets. There is no probability
   *                                   distribution, so this is converted internally to EMPTY {1.0, 0, T}.
   *                                   This is handled in SetOperation.createCompactSketch().
   *  2    1.0    !0      T   Error    Empty=T and curCount !0 should never coexist.
   *                                   This is checked in all compacting operations.
   *  3    1.0    !0      F     OK     EXACT: This corresponds to a sketch in exact mode
   *  4   <1.0     0      T   Internal This can be an initial UpdateSketch state if p < 1.0,
   *                                   so change theta to 1.0. Return {Th = 1.0, 0, T}.
   *                                   This is handled in UpdateSketch.compact() and toByteArray().
   *  5   <1.0     0      F     OK     This can result from set operations
   *  6   <1.0    !0      T   Error    Empty=T and curCount !0 should never coexist.
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

