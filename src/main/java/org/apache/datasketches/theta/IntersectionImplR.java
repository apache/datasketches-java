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

import static org.apache.datasketches.Util.MIN_LG_ARR_LONGS;
import static org.apache.datasketches.Util.floorPowerOf2;
import static org.apache.datasketches.theta.PreambleUtil.EMPTY_FLAG_MASK;
import static org.apache.datasketches.theta.PreambleUtil.FAMILY_BYTE;
import static org.apache.datasketches.theta.PreambleUtil.FLAGS_BYTE;
import static org.apache.datasketches.theta.PreambleUtil.LG_ARR_LONGS_BYTE;
import static org.apache.datasketches.theta.PreambleUtil.LG_NOM_LONGS_BYTE;
import static org.apache.datasketches.theta.PreambleUtil.PREAMBLE_LONGS_BYTE;
import static org.apache.datasketches.theta.PreambleUtil.P_FLOAT;
import static org.apache.datasketches.theta.PreambleUtil.RETAINED_ENTRIES_INT;
import static org.apache.datasketches.theta.PreambleUtil.SEED_HASH_SHORT;
import static org.apache.datasketches.theta.PreambleUtil.SER_VER;
import static org.apache.datasketches.theta.PreambleUtil.SER_VER_BYTE;
import static org.apache.datasketches.theta.PreambleUtil.THETA_LONG;

import java.util.Arrays;

import org.apache.datasketches.Family;
import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.SketchesReadOnlyException;
import org.apache.datasketches.SketchesStateException;
import org.apache.datasketches.Util;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;

/**
 * Intersection operation for Theta Sketches.
 *
 * <p>This implementation uses data either on-heap or off-heap in a given Memory
 * that is owned and managed by the caller.
 * The off-heap Memory, which if managed properly, will greatly reduce the need for
 * the JVM to perform garbage collection.</p>
 *
 * @author Lee Rhodes
 * @author Kevin Lang
 */
class IntersectionImplR extends Intersection {
  protected final short seedHash_;
  protected final WritableMemory mem_;

  //Note: Intersection does not use lgNomLongs or k, per se.
  protected int lgArrLongs_; //current size of hash table
  protected int curCount_; //curCount of HT, if < 0 means Universal Set (US) is true
  protected long thetaLong_;
  protected boolean empty_;

  protected long[] hashTable_ = null;  //HT => Data.  Only used On Heap
  protected int maxLgArrLongs_ = 0; //max size of hash table. Only used Off Heap

  IntersectionImplR(final WritableMemory mem, final long seed, final boolean newMem) {
    mem_ = mem;
    if (mem != null) {
      if (newMem) {
        seedHash_ = computeSeedHash(seed);
        mem_.putShort(SEED_HASH_SHORT, seedHash_);
      } else {
        seedHash_ = mem_.getShort(SEED_HASH_SHORT);
        Util.checkSeedHashes(seedHash_, computeSeedHash(seed)); //check for seed hash conflict
      }
    } else {
      seedHash_ = computeSeedHash(seed);
    }
  }

  IntersectionImplR(final short seedHash) {
    seedHash_ = seedHash;
    mem_ = null;
    lgArrLongs_ = 0;
    curCount_ = -1;
    thetaLong_ = Long.MAX_VALUE;
    empty_ = false;
    hashTable_ = null;
  }

  /**
   * Wrap an Intersection target around the given source Memory containing intersection data.
   * @param srcMem The source Memory image.
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   * @return an IntersectionImplR that wraps a read-only Intersection image referenced by srcMem
   */
  static IntersectionImplR wrapInstance(final Memory srcMem, final long seed) {
    final IntersectionImplR impl = new IntersectionImplR((WritableMemory) srcMem, seed, false);
    return internalWrapInstance(srcMem, impl);
  }

  static IntersectionImplR internalWrapInstance(final Memory srcMem, final IntersectionImplR impl) {
    //Get Preamble
    //Note: Intersection does not use lgNomLongs (or k), per se.
    //seedHash loaded and checked in constructor
    final int preLongsMem = srcMem.getByte(PREAMBLE_LONGS_BYTE) & 0X3F;
    final int serVer = srcMem.getByte(SER_VER_BYTE) & 0XFF;
    final int famID = srcMem.getByte(FAMILY_BYTE) & 0XFF;
    final int lgArrLongs = srcMem.getByte(LG_ARR_LONGS_BYTE) & 0XFF;
    final int flags = srcMem.getByte(FLAGS_BYTE) & 0XFF;
    final boolean empty = (flags & EMPTY_FLAG_MASK) > 0;

    //Checks
    if (preLongsMem != CONST_PREAMBLE_LONGS) {
      throw new SketchesArgumentException(
          "Memory PreambleLongs must equal " + CONST_PREAMBLE_LONGS + ": " + preLongsMem);
    }

    if (serVer != SER_VER) {
      throw new SketchesArgumentException("Serialization Version must equal " + SER_VER);
    }

    Family.INTERSECTION.checkFamilyID(famID);

    final int curCount = srcMem.getInt(RETAINED_ENTRIES_INT);
    final long thetaLong = srcMem.getLong(THETA_LONG);

    if (empty) {
      if (curCount != 0) {
        throw new SketchesArgumentException(
            "srcMem empty state inconsistent with curCount: " + empty + "," + curCount);
      }
      //empty = true AND curCount_ = 0: OK
    } //else empty = false, curCount could be anything

    //Initialize
    impl.lgArrLongs_ = lgArrLongs;
    impl.curCount_ = curCount;
    impl.thetaLong_ = thetaLong;
    impl.empty_ = empty;
    impl.maxLgArrLongs_ = checkMaxLgArrLongs(srcMem); //Only Off Heap, check for min size
    return impl;
  }

  @Override
  public CompactSketch getResult(final boolean dstOrdered, final WritableMemory dstMem) {
    if (curCount_ < 0) {
      throw new SketchesStateException(
          "Calling getResult() with no intervening intersections would represent the infinite set, "
          + "which is not a legal result.");
    }
    long[] compactCacheR;

    if (curCount_ == 0) {
      compactCacheR = new long[0];
      return CompactOperations.componentsToCompact(
          thetaLong_, curCount_, seedHash_, empty_, true, false, dstOrdered, dstMem, compactCacheR);
    }
    //else curCount > 0
    final long[] hashTable;
    if (mem_ != null) {
      final int htLen = 1 << lgArrLongs_;
      hashTable = new long[htLen];
      mem_.getLongArray(CONST_PREAMBLE_LONGS << 3, hashTable, 0, htLen);
    } else {
      hashTable = hashTable_;
    }
    compactCacheR = compactCachePart(hashTable, lgArrLongs_, curCount_, thetaLong_, dstOrdered);

    //Create the CompactSketch
    return CompactOperations.componentsToCompact(
        thetaLong_, curCount_, seedHash_, empty_, true, dstOrdered, dstOrdered, dstMem, compactCacheR);
  }

  @Override
  public CompactSketch getResult() {
    return getResult(true, null);
  }

  /**
   * Gets the number of retained entries from this operation. If negative, it is interpreted
   * as the infinite <i>Universal Set</i>.
   */
  @Override
  int getRetainedEntries(final boolean valid) {
    return curCount_;
  }

  @Override
  public boolean hasResult() {
    return (mem_ != null) ? mem_.getInt(RETAINED_ENTRIES_INT) >= 0 : curCount_ >= 0;
  }

  @Override
  boolean isEmpty() {
    return empty_;
  }

  @Override
  public boolean isSameResource(final Memory that) {
    return (mem_ != null) ? mem_.isSameResource(that) : false;
  }

  @Override
  public void reset() {
    throw new SketchesReadOnlyException();
  }

  @Override
  public byte[] toByteArray() {
    final int preBytes = CONST_PREAMBLE_LONGS << 3;
    final int dataBytes = (curCount_ > 0) ? 8 << lgArrLongs_ : 0;
    final byte[] byteArrOut = new byte[preBytes + dataBytes];
    if (mem_ != null) {
      mem_.getByteArray(0, byteArrOut, 0, preBytes + dataBytes);
    }
    else {
      final WritableMemory memOut = WritableMemory.wrap(byteArrOut);

      //preamble
      memOut.putByte(PREAMBLE_LONGS_BYTE, (byte) CONST_PREAMBLE_LONGS); //RF not used = 0
      memOut.putByte(SER_VER_BYTE, (byte) SER_VER);
      memOut.putByte(FAMILY_BYTE, (byte) Family.INTERSECTION.getID());
      memOut.putByte(LG_NOM_LONGS_BYTE, (byte) 0); //not used
      memOut.putByte(LG_ARR_LONGS_BYTE, (byte) lgArrLongs_);
      if (empty_) {
        memOut.setBits(FLAGS_BYTE, (byte) EMPTY_FLAG_MASK);
      }
      else {
        memOut.clearBits(FLAGS_BYTE, (byte) EMPTY_FLAG_MASK);
      }
      memOut.putShort(SEED_HASH_SHORT, seedHash_);
      memOut.putInt(RETAINED_ENTRIES_INT, curCount_);
      memOut.putFloat(P_FLOAT, (float) 1.0);
      memOut.putLong(THETA_LONG, thetaLong_);

      //data
      if (curCount_ > 0) {
        memOut.putLongArray(preBytes, hashTable_, 0, 1 << lgArrLongs_);
      }
    }
    return byteArrOut;
  }

  @Override
  public void update(final Sketch sketchIn) {
    throw new SketchesReadOnlyException();
  }

  @Override
  public CompactSketch intersect(final Sketch a, final Sketch b, final boolean dstOrdered,
     final WritableMemory dstMem) {
    throw new SketchesReadOnlyException();
  }

  //restricted

  @Override
  long[] getCache() {
    if (mem_ == null) {
      return (hashTable_ != null) ? hashTable_ : new long[0];
    }
    //Direct
    final int arrLongs = 1 << lgArrLongs_;
    final long[] outArr = new long[arrLongs];
    mem_.getLongArray(CONST_PREAMBLE_LONGS << 3, outArr, 0, arrLongs);
    return outArr;
  }

  @Override
  short getSeedHash() {
    return seedHash_;
  }

  @Override
  long getThetaLong() {
    return thetaLong_;
  }

  /**
   * Returns the correct maximum lgArrLongs given the capacity of the Memory. Checks that the
   * capacity is large enough for the minimum sized hash table.
   * @param dstMem the given Memory
   * @return the correct maximum lgArrLongs given the capacity of the Memory
   */
  static final int checkMaxLgArrLongs(final Memory dstMem) {
    final int preBytes = CONST_PREAMBLE_LONGS << 3;
    final long cap = dstMem.getCapacity();
    final int maxLgArrLongs =
        Integer.numberOfTrailingZeros(floorPowerOf2((int)(cap - preBytes)) >>> 3);
    if (maxLgArrLongs < MIN_LG_ARR_LONGS) {
      throw new SketchesArgumentException(
        "dstMem not large enough for minimum sized hash table: " + cap);
    }
    return maxLgArrLongs;
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
   */ //Only used in IntersectionImplR
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

}
