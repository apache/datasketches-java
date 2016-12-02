/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.theta;

import static com.yahoo.sketches.Family.stringToFamily;
import static com.yahoo.sketches.theta.PreambleUtil.SER_VER;
import static com.yahoo.sketches.theta.PreambleUtil.insertCurCount;
import static com.yahoo.sketches.theta.PreambleUtil.insertFamilyID;
import static com.yahoo.sketches.theta.PreambleUtil.insertFlags;
import static com.yahoo.sketches.theta.PreambleUtil.insertP;
import static com.yahoo.sketches.theta.PreambleUtil.insertPreLongs;
import static com.yahoo.sketches.theta.PreambleUtil.insertSeedHash;
import static com.yahoo.sketches.theta.PreambleUtil.insertSerVer;
import static com.yahoo.sketches.theta.PreambleUtil.insertThetaLong;

import java.util.Arrays;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.SketchesArgumentException;

/**
 * The parent class of all the CompactSketches. CompactSketches are never created directly.
 * They are created as a result of the compact() method of an UpdateSketch or as a result of a
 * getResult() of a SetOperation.
 *
 * <p>A CompactSketch is the simplist form of a Theta Sketch. It consists of a compact list
 * (i.e., no intervening spaces) of hash values, which may be ordered or not, a value for theta
 * and a seed hash.  A CompactSketch is read-only,
 * and the space required when stored is only the space required for the hash values and 8 to 24
 * bytes of preamble. An empty CompactSketch consumes only 8 bytes.</p>
 *
 * @author Lee Rhodes
 */
public abstract class CompactSketch extends Sketch {
  private final short seedHash_;
  private final boolean empty_;
  private final int curCount_;
  private final long thetaLong_;

  CompactSketch(final boolean empty, final short seedHash, final int curCount, final long thetaLong) {
    empty_ = empty;
    seedHash_ = seedHash;
    curCount_ = curCount;
    thetaLong_ = thetaLong;
  }

  //Sketch

  @Override
  public int getRetainedEntries(final boolean valid) {
    return curCount_;
  }

  @Override
  public boolean isEmpty() {
    return empty_;
  }

  @Override
  public boolean isEstimationMode() {
    return Sketch.estMode(getThetaLong(), isEmpty());
  }

  @Override
  public Family getFamily() {
    return Family.COMPACT;
  }

  @Override
  public boolean isCompact() {
    return true;
  }

  //restricted methods

  @Override
  short getSeedHash() {
    return seedHash_;
  }

  @Override
  long getThetaLong() {
    return thetaLong_;
  }

  @Override
  int getPreambleLongs() {
    return compactPreambleLongs(getThetaLong(), isEmpty());
  }

  /**
   * Compact the given array.
   * @param srcCache anything
   * @param curCount must be correct
   * @param thetaLong The correct <a href="{@docRoot}/resources/dictionary.html#thetaLong">thetaLong</a>.
   * @param dstOrdered true if output array must be sorted
   * @return the compacted array
   */
  static final long[] compactCache(
      final long[] srcCache, final int curCount, final long thetaLong, final boolean dstOrdered) {
    if (curCount == 0) {
      return new long[0];
    }
    final long[] cacheOut = new long[curCount];
    final int len = srcCache.length;
    int j = 0;
    for (int i = 0; i < len; i++) {
      final long v = srcCache[i];
      if ((v <= 0L) || (v >= thetaLong) ) { continue; }
      cacheOut[j++] = v;
    }
    assert curCount == j;
    if (dstOrdered) {
      Arrays.sort(cacheOut);
    }
    return cacheOut;
  }

  /**
   * Compact first 2^lgArrLongs of given array
   * @param srcCache anything
   * @param lgArrLongs The correct
   * <a href="{@docRoot}/resources/dictionary.html#lgArrLongs">lgArrLongs</a>.
   * @param curCount must be correct
   * @param thetaLong The correct
   * <a href="{@docRoot}/resources/dictionary.html#thetaLong">thetaLong</a>.
   * @param dstOrdered true if output array must be sorted
   * @return the compacted array
   */
  static final long[] compactCachePart(final long[] srcCache, final int lgArrLongs,
      final int curCount, final long thetaLong, final boolean dstOrdered) {
    if (curCount == 0) {
      return new long[0];
    }
    final long[] cacheOut = new long[curCount];
    final int len = 1 << lgArrLongs;
    int j = 0;
    for (int i = 0; i < len; i++) {
      final long v = srcCache[i];
      if ((v <= 0L) || (v >= thetaLong) ) { continue; }
      cacheOut[j++] = v;
    }
    assert curCount == j;
    if (dstOrdered) {
      Arrays.sort(cacheOut);
    }
    return cacheOut;
  }

  static final CompactSketch createCompactSketch(final long[] compactCache, final boolean empty,
      final short seedHash, final int curCount, final long thetaLong, final boolean dstOrdered,
      final Memory dstMem) {
    CompactSketch sketchOut = null;
    final int sw = (dstOrdered ? 2 : 0) | ((dstMem != null) ? 1 : 0);
    switch (sw) {
      case 0: { //dst not ordered, dstMem == null
        sketchOut = new HeapCompactSketch(compactCache, empty, seedHash, curCount, thetaLong);
        break;
      }
      case 1: { //dst not ordered, dstMem == valid
        sketchOut =
            new DirectCompactSketch(compactCache, empty, seedHash, curCount, thetaLong, dstMem);
        break;
      }
      case 2: { //dst ordered, dstMem == null
        sketchOut =
            new HeapCompactOrderedSketch(compactCache, empty, seedHash, curCount, thetaLong);
        break;
      }
      case 3: { //dst ordered, dstMem == valid
        sketchOut =
        new DirectCompactOrderedSketch(compactCache, empty, seedHash, curCount, thetaLong, dstMem);
        break;
      }
      //default: //This cannot happen and cannot be tested
    }
    return sketchOut;
  }

  static final Memory loadCompactMemory(final long[] compactCache, final boolean empty,
      final short seedHash, final int curCount, final long thetaLong, final Memory dstMem,
      final byte flags) {
    final int preLongs = compactPreambleLongs(thetaLong, empty);
    final int outLongs = preLongs + curCount;
    final int outBytes = outLongs << 3;
    final int dstBytes = (int) dstMem.getCapacity();
    if (outBytes > dstBytes) {
      throw new SketchesArgumentException("Insufficient Memory: " + dstBytes
        + ", Need: " + outBytes);
    }
    final byte famID = (byte) stringToFamily("Compact").getID();

    final Object memObj = dstMem.array(); //may be null
    final long memAdd = dstMem.getCumulativeOffset(0L);

    insertPreLongs(memObj, memAdd, preLongs); //RF not used = 0
    insertSerVer(memObj, memAdd, SER_VER);
    insertFamilyID(memObj, memAdd, famID);
    //ignore lgNomLongs, lgArrLongs bytes for compact sketches
    insertFlags(memObj, memAdd, flags);
    insertSeedHash(memObj, memAdd, seedHash);

    if (preLongs > 1) {
      insertCurCount(memObj, memAdd, curCount);
      insertP(memObj, memAdd, (float) 1.0);
    }
    if (preLongs > 2) {
      insertThetaLong(memObj, memAdd, thetaLong);
    }
    if ((compactCache != null) && (curCount > 0)) {
      dstMem.putLongArray(preLongs << 3, compactCache, 0, curCount);
    }
    return dstMem;
  }

}
