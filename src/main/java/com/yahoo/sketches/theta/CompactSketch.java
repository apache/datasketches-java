/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.theta;

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
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.SketchesArgumentException;

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
  public CompactSketch compact() { return this; }

  @Override
  public CompactSketch compact(final boolean dstOrdered, final WritableMemory dstMem) {
    return this;
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

  /**
   * Compact the given array. The source cache can be a hash table with interstitial zeros or
   * "dirty" values.
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

  //compactCache and dstMem must be valid
  static final Memory loadCompactMemory(final long[] compactCache, final short seedHash,
      final int curCount, final long thetaLong, final WritableMemory dstMem,
      final byte flags, final int preLongs) {

    assert (dstMem != null) && (compactCache != null);
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

    if ((preLongs == 1) && (curCount == 1)) { //singleItem
      dstMem.putLong(8, compactCache[0]);
      return dstMem;
    }
    if (preLongs > 1) {
      insertCurCount(dstMem, curCount);
      insertP(dstMem, (float) 1.0);
    }
    if (preLongs > 2) {
      insertThetaLong(dstMem, thetaLong);
    }
    if (curCount > 0) {
      dstMem.putLongArray(preLongs << 3, compactCache, 0, curCount);
    }
    return dstMem;
  }

}
