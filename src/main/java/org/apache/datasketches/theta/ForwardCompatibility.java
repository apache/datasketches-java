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
import static org.apache.datasketches.theta.PreambleUtil.extractFamilyID;
import static org.apache.datasketches.theta.PreambleUtil.extractPreLongs;
import static org.apache.datasketches.theta.PreambleUtil.extractThetaLong;

import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.Util;
import org.apache.datasketches.memory.Memory;

/**
 * Used to convert older serialization versions 1 and 2 to version 3.  The Serialization
 * Version is the version of the sketch binary image format and should not be confused with the
 * version number of the Open Source DataSketches Library.
 *
 * @author Lee Rhodes
 */
final class ForwardCompatibility {

  /**
   * Convert a serialization version (SerVer) 1 sketch (~Feb 2014) to a SerVer 3 sketch.
   * Note: SerVer 1 sketches always have (metadata) preamble-longs of 3 and are always stored
   * in a compact ordered form, but with 3 different sketch types.  All SerVer 1 sketches will
   * be converted to a SerVer 3 sketches. There is no concept of p-sampling, no empty bit.
   *
   * @param srcMem the image of a SerVer 1 sketch
   *
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See Update Hash Seed</a>.
   * The seed used for building the sketch image in srcMem.
   * Note: SerVer 1 sketches do not have the concept of the SeedHash, so the seed provided here
   * MUST be the actual seed that was used when the SerVer 1 sketches were built.
   * @return a SerVer 3 {@link CompactSketch}.
   */
  static final CompactSketch heapify1to3(final Memory srcMem, final long seed) {
    final short seedHash = Util.computeSeedHash(seed);
    return heapify1to3(srcMem, seedHash);
  }

  /**
   * Convert a serialization version (SerVer) 1 sketch (~Feb 2014) to a SerVer 3 sketch.
   * Note: SerVer 1 sketches always have (metadata) preamble-longs of 3 and are always stored
   * in a compact ordered form, but with 3 different sketch types.  All SerVer 1 sketches will
   * be converted to a SerVer 3 sketches. There is no concept of p-sampling, no empty bit.
   *
   * @param srcMem the image of a SerVer 1 sketch
   *
   * @param seedHash <a href="{@docRoot}/resources/dictionary.html#seedHash">See Seed Hash</a>.
   * The seedHash that matches the seedHash of the original seed used to construct the sketch.
   * Note: SerVer 1 sketches do not have the concept of the SeedHash, so the seedHash provided here
   * MUST be derived from the actual seed that was used when the SerVer 1 sketches were built.
   * @return a SerVer 3 {@link CompactSketch}.
   */
  static final CompactSketch heapify1to3(final Memory srcMem, final short seedHash) {
    final int memCap = (int) srcMem.getCapacity();
    final int preLongs = extractPreLongs(srcMem); //always 3 for serVer 1
    if (preLongs != 3) {
      throw new SketchesArgumentException("PreLongs must be 3 for SerVer 1: " + preLongs);
    }
    final int familyId = extractFamilyID(srcMem); //1,2,3,4
    if ((familyId < 1) || (familyId > 3)) {
      throw new SketchesArgumentException("Family (Sketch Type) must be 1 to 3: " + familyId);
    }
    final int curCount = extractCurCount(srcMem);
    final long thetaLong = extractThetaLong(srcMem);
    final boolean empty = (curCount == 0) && (thetaLong == Long.MAX_VALUE);

    if (empty || (memCap <= 24)) { //return empty
      return EmptyCompactSketch.getInstance();
    }

    final int reqCap = (curCount + preLongs) << 3;
    validateInputSize(reqCap, memCap);

    if ((thetaLong == Long.MAX_VALUE) && (curCount == 1)) {
        final long hash = srcMem.getLong(preLongs << 3);
        return new SingleItemSketch(hash, seedHash);
    }
    //theta < 1.0 and/or curCount > 1

    final long[] compactOrderedCache = new long[curCount];
    srcMem.getLongArray(preLongs << 3, compactOrderedCache, 0, curCount);
    return new HeapCompactSketch(compactOrderedCache, false, seedHash, curCount, thetaLong, true);
  }

  /**
   * Convert a serialization version (SerVer) 2 sketch to a SerVer 3 HeapCompactOrderedSketch.
   * Note: SerVer 2 sketches can have metadata-longs of 1,2 or 3 and are always stored
   * in a compact ordered form (not as a hash table), but with 4 different sketch types.
   * @param srcMem the image of a SerVer 2 sketch
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See Update Hash Seed</a>.
   * The seed used for building the sketch image in srcMem
   * @return a SerVer 3 HeapCompactOrderedSketch
   */
  static final CompactSketch heapify2to3(final Memory srcMem, final long seed) {
    final short seedHash = checkMemorySeedHash(srcMem, seed);
    final int memCap = (int) srcMem.getCapacity();
    final int preLongs = extractPreLongs(srcMem); //1,2 or 3
    final int familyId = extractFamilyID(srcMem); //1,2,3,4
    if ((familyId < 1) || (familyId > 4)) {
      throw new SketchesArgumentException("Family (Sketch Type) must be 1 to 4: " + familyId);
    }
    int reqBytesIn = 8;
    int curCount = 0;
    long thetaLong = Long.MAX_VALUE;
    if (preLongs == 1) {
      reqBytesIn = 8;
      validateInputSize(reqBytesIn, memCap);
      return EmptyCompactSketch.getInstance();
    }
    if (preLongs == 2) { //includes pre0 + count, no theta (== 1.0)
      reqBytesIn = preLongs << 3;
      validateInputSize(reqBytesIn, memCap);
      curCount = extractCurCount(srcMem);
      if (curCount == 0) {
        return EmptyCompactSketch.getInstance();
      }
      if (curCount == 1) {
        reqBytesIn = (preLongs + 1) << 3;
        validateInputSize(reqBytesIn, memCap);
        final long hash = srcMem.getLong(preLongs << 3);
        return new SingleItemSketch(hash, seed);
      }
      //curCount > 1
      reqBytesIn = (curCount + preLongs) << 3;
      validateInputSize(reqBytesIn, memCap);
      final long[] compactOrderedCache = new long[curCount];
      srcMem.getLongArray(preLongs << 3, compactOrderedCache, 0, curCount);
      return new HeapCompactSketch(compactOrderedCache, false, seedHash, curCount, thetaLong,true);
    }
    if (preLongs == 3) { //pre0 + count + theta
      reqBytesIn = (preLongs) << 3; //
      validateInputSize(reqBytesIn, memCap);
      curCount = extractCurCount(srcMem);
      thetaLong = extractThetaLong(srcMem);
      if ((curCount == 0) && (thetaLong == Long.MAX_VALUE)) {
        return EmptyCompactSketch.getInstance();
      }
      if ((curCount == 1) && (thetaLong == Long.MAX_VALUE)) {
        reqBytesIn = (preLongs + 1) << 3;
        validateInputSize(reqBytesIn, memCap);
        final long hash = srcMem.getLong(preLongs << 3);
        return new SingleItemSketch(hash, seed);
      }
      //curCount > 1 and/or theta < 1.0
      reqBytesIn = (curCount + preLongs) << 3;
      validateInputSize(reqBytesIn, memCap);
      final long[] compactOrderedCache = new long[curCount];
      srcMem.getLongArray(preLongs << 3, compactOrderedCache, 0, curCount);
      return new HeapCompactSketch(compactOrderedCache, false, seedHash, curCount, thetaLong, true);
    }
    throw new SketchesArgumentException("PreLongs must be 1,2, or 3: " + preLongs);
  }

  private static final void validateInputSize(final int reqBytesIn, final int memCap) {
    if (reqBytesIn > memCap) {
      throw new SketchesArgumentException(
        "Input Memory or byte[] size is too small: Required Bytes: " + reqBytesIn
          + ", bytesIn: " + memCap);
    }
  }

}
