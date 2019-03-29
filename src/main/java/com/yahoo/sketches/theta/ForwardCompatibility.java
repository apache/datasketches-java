/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.theta;

import static com.yahoo.sketches.theta.PreambleUtil.EMPTY_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.FLAGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.PREAMBLE_LONGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.RETAINED_ENTRIES_INT;
import static com.yahoo.sketches.theta.PreambleUtil.SEED_HASH_SHORT;
import static com.yahoo.sketches.theta.PreambleUtil.THETA_LONG;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.Util;

/**
 * Used to convert older serialization versions 1 and 2 to version 3.  The Serialization
 * Version is the version of the sketch binary image format and should not be confused with the
 * version number of the Open Source DataSketches Library.
 *
 * @author Lee Rhodes
 */
final class ForwardCompatibility {

  /**
   * Convert a serialization version (SerVer) 1 sketch to a SerVer 3 HeapCompactOrderedSketch.
   * Note: SerVer 1 sketches always have metadata-longs of 3 and are always stored
   * in a compact ordered form, but with 3 different sketch types.  All SerVer 1 sketches will
   * be converted to a SerVer 3, HeapCompactOrderedSketch.
   *
   * @param srcMem the image of a SerVer 1 sketch
   *
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See Update Hash Seed</a>.
   * The seed used for building the sketch image in srcMem.
   * Note: SerVer 1 sketches do not have the concept of the SeedHash, so the seed provided here
   * MUST be the actual seed that was used when the SerVer 1 sketches were built.
   * @return a SerVer 3 HeapCompactOrderedSketch.
   */
  static final CompactSketch heapify1to3(final Memory srcMem, final long seed) {
    final int memCap = (int) srcMem.getCapacity();

    final short seedHash = Util.computeSeedHash(seed);

    if (memCap <= 24) { //return empty
      return HeapCompactOrderedSketch
          .compact(new long[0], true, seedHash, 0, Long.MAX_VALUE);
    }

    final int curCount = srcMem.getInt(RETAINED_ENTRIES_INT);

    final int mdLongs = 3;
    final int reqBytesIn = (curCount + mdLongs) << 3;
    validateInputSize(reqBytesIn, memCap);

    final long thetaLong = srcMem.getLong(THETA_LONG);

    final long[] compactOrderedCache = new long[curCount];
    srcMem.getLongArray(24, compactOrderedCache, 0, curCount);
    return HeapCompactOrderedSketch
        .compact(compactOrderedCache, false, seedHash, curCount, thetaLong);
  }

  /**
   * Convert a serialization version (SerVer) 2 sketch to a SerVer 3 HeapCompactOrderedSketch.
   * Note: SerVer 2 sketches can have metadata-longs of 1,2 or 3 and are always stored
   * in a compact ordered form, but with 4 different sketch types.
   * @param srcMem the image of a SerVer 1 sketch
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See Update Hash Seed</a>.
   * The seed used for building the sketch image in srcMem
   * @return a SerVer 3 HeapCompactOrderedSketch
   */
  static final CompactSketch heapify2to3(final Memory srcMem, final long seed) {
    final int memCap = (int) srcMem.getCapacity();

    final short seedHash = Util.computeSeedHash(seed);
    final short memSeedHash = srcMem.getShort(SEED_HASH_SHORT);
    Util.checkSeedHashes(seedHash, memSeedHash);

    if (memCap == 8) { //return empty, theta = 1.0
      return HeapCompactOrderedSketch
          .compact(new long[0], true, seedHash, 0, Long.MAX_VALUE);
    }

    final int curCount = srcMem.getInt(RETAINED_ENTRIES_INT);
    //Note: curCount could be zero and theta < 1.0 and be empty or not-empty.

    final int mdLongs = srcMem.getByte(PREAMBLE_LONGS_BYTE) & 0X3F; //either 2 or 3
    final int reqBytesIn = (curCount + mdLongs) << 3;
    validateInputSize(reqBytesIn, memCap);

    final long thetaLong = (mdLongs < 3) ? Long.MAX_VALUE : srcMem.getLong(THETA_LONG);
    boolean empty = (srcMem.getByte(FLAGS_BYTE) & EMPTY_FLAG_MASK) != 0;
    empty = (curCount == 0) && (thetaLong == Long.MAX_VALUE); //force true
    final long[] compactOrderedCache = new long[curCount];
    srcMem.getLongArray(mdLongs << 3, compactOrderedCache, 0, curCount);
    return HeapCompactOrderedSketch
        .compact(compactOrderedCache, empty, seedHash, curCount, thetaLong);
  }

  private static final void validateInputSize(final int reqBytesIn, final int memCap) {
    if (reqBytesIn > memCap) {
      throw new SketchesArgumentException(
        "Input Memory or byte[] size is too small: Required Bytes: " + reqBytesIn
          + ", bytesIn: " + memCap);
    }
  }

}
