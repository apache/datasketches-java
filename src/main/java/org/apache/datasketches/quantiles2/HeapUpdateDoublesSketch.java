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

package org.apache.datasketches.quantiles2;

import static java.lang.foreign.ValueLayout.JAVA_DOUBLE_UNALIGNED;
import static org.apache.datasketches.quantiles2.ClassicUtil.MIN_K;
import static org.apache.datasketches.quantiles2.ClassicUtil.checkFamilyID;
import static org.apache.datasketches.quantiles2.ClassicUtil.checkHeapFlags;
import static org.apache.datasketches.quantiles2.ClassicUtil.computeBaseBufferItems;
import static org.apache.datasketches.quantiles2.ClassicUtil.computeBitPattern;
import static org.apache.datasketches.quantiles2.ClassicUtil.computeCombinedBufferItemCapacity;
import static org.apache.datasketches.quantiles2.ClassicUtil.computeNumLevelsNeeded;
import static org.apache.datasketches.quantiles2.ClassicUtil.computeRetainedItems;
import static org.apache.datasketches.quantiles2.PreambleUtil.COMPACT_FLAG_MASK;
import static org.apache.datasketches.quantiles2.PreambleUtil.EMPTY_FLAG_MASK;
import static org.apache.datasketches.quantiles2.PreambleUtil.MAX_DOUBLE;
import static org.apache.datasketches.quantiles2.PreambleUtil.MIN_DOUBLE;
import static org.apache.datasketches.quantiles2.PreambleUtil.extractFamilyID;
import static org.apache.datasketches.quantiles2.PreambleUtil.extractFlags;
import static org.apache.datasketches.quantiles2.PreambleUtil.extractK;
import static org.apache.datasketches.quantiles2.PreambleUtil.extractN;
import static org.apache.datasketches.quantiles2.PreambleUtil.extractPreLongs;
import static org.apache.datasketches.quantiles2.PreambleUtil.extractSerVer;

import java.lang.foreign.MemorySegment;
import java.util.Arrays;

import org.apache.datasketches.common.Family;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.quantilescommon.QuantilesAPI;

/**
 * Implements the DoublesSketch on the Java heap.
 *
 * @author Lee Rhodes
 * @author Jon Malkin
 */
final class HeapUpdateDoublesSketch extends UpdateDoublesSketch {
  static final int MIN_HEAP_DOUBLES_SER_VER = 1;

  /**
   * The smallest item ever seen in the stream.
   */
  private double minItem_;

  /**
   * The largest item ever seen in the stream.
   */
  private double maxItem_;

  /**
   * The total count of items seen.
   */
  private long n_;

  /**
   * Number of items currently in base buffer.
   *
   * <p>Count = N % (2*K)</p>
   */
  private int baseBufferCount_;

  /**
   * Active levels expressed as a bit pattern.
   *
   * <p>Pattern = N / (2 * K)</p>
   */
  private long bitPattern_;

  /**
   * This single array contains the base buffer plus all levels some of which may not be used,
   * i.e, is in non-compact form.
   * A level is of size K and is either full and sorted, or not used. A "not used" buffer may have
   * garbage. Whether a level buffer used or not is indicated by the bitPattern_.
   * The base buffer has length 2*K but might not be full and isn't necessarily sorted.
   * The base buffer precedes the level buffers. This buffer does not include the min, max items.
   *
   * <p>The levels arrays require quite a bit of explanation, which we defer until later.</p>
   */
  private double[] combinedBuffer_;

  //**CONSTRUCTORS**********************************************************
  private HeapUpdateDoublesSketch(final int k) {
    super(k); //Checks k
  }

  /**
   * Obtains a new on-heap instance of a DoublesSketch.
   *
   * @param k Parameter that controls space usage of sketch and accuracy of estimates.
   * Must be greater than 1 and less than 65536 and a power of 2.
   * @return a HeapUpdateDoublesSketch
   */
  static HeapUpdateDoublesSketch newInstance(final int k) {
    final HeapUpdateDoublesSketch huds = new HeapUpdateDoublesSketch(k);
    final int baseBufAlloc = 2 * Math.min(MIN_K, k); //the min is important
    huds.n_ = 0;
    huds.combinedBuffer_ = new double[baseBufAlloc];
    huds.baseBufferCount_ = 0;
    huds.bitPattern_ = 0;
    huds.minItem_ = Double.NaN;
    huds.maxItem_ = Double.NaN;
    return huds;
  }

  /**
   * Heapifies the given srcSeg, which must be a MemorySegment image of a DoublesSketch and may have data.
   *
   * @param srcSeg a MemorySegment image of a sketch, which may be in compact or not compact form.
   * @return a DoublesSketch on the Java heap.
   */
  static HeapUpdateDoublesSketch heapifyInstance(final MemorySegment srcSeg) {
    final long segCapBytes = srcSeg.byteSize();
    if (segCapBytes < 8) {
      throw new SketchesArgumentException("Source MemorySegment too small: " + segCapBytes + " < 8");
    }

    final int preLongs = extractPreLongs(srcSeg);
    final int serVer = extractSerVer(srcSeg);
    final int familyID = extractFamilyID(srcSeg);
    final int flags = extractFlags(srcSeg);
    final int k = extractK(srcSeg);

    final boolean empty = (flags & EMPTY_FLAG_MASK) > 0; //Preamble flags empty state
    final long n = empty ? 0 : extractN(srcSeg);

    //VALIDITY CHECKS
    DoublesUtil.checkDoublesSerVer(serVer, MIN_HEAP_DOUBLES_SER_VER);
    checkHeapFlags(flags);
    checkPreLongsFlagsSerVer(flags, serVer, preLongs);
    checkFamilyID(familyID);

    final HeapUpdateDoublesSketch huds = newInstance(k); //checks k
    if (empty) { return huds; }

    //Not empty, must have valid preamble + min, max, n.
    //Forward compatibility from SerVer = 1 :
    final boolean srcIsCompact = (serVer == 2) || ((flags & COMPACT_FLAG_MASK) > 0);

    checkHeapSegCapacity(k, n, srcIsCompact, serVer, segCapBytes);

    //set class members by computing them
    huds.n_ = n;
    final int combBufCap = computeCombinedBufferItemCapacity(k, n);
    huds.baseBufferCount_ = computeBaseBufferItems(k, n);
    huds.bitPattern_ = computeBitPattern(k, n);
    //Extract min, max, data from srcSeg into Combined Buffer
    huds.srcMemorySegmentToCombinedBuffer(srcSeg, serVer, srcIsCompact, combBufCap);
    return huds;
  }

  @Override
  public double getMaxItem() {
    if (isEmpty()) { throw new IllegalArgumentException(QuantilesAPI.EMPTY_MSG); }
    return maxItem_;
  }

  @Override
  public double getMinItem() {
    if (isEmpty()) { throw new IllegalArgumentException(QuantilesAPI.EMPTY_MSG); }
    return minItem_;
  }

  @Override
  public long getN() {
    return n_;
  }

  @Override
  public boolean hasMemorySegment() {
    return false;
  }

  @Override
  public boolean isOffHeap() {
    return false;
  }

  @Override
  public boolean isReadOnly() {
    return false;
  }

  @Override
  public boolean isSameResource(final MemorySegment that) {
    return false;
  }

  @Override
  public void reset() {
    n_ = 0;
    final int combinedBufferItemCapacity = 2 * Math.min(MIN_K, k_); //min is important
    combinedBuffer_ = new double[combinedBufferItemCapacity];
    baseBufferCount_ = 0;
    bitPattern_ = 0;
    minItem_ = Double.NaN;
    maxItem_ = Double.NaN;
  }

  @Override
  public void update(final double dataItem) {
    if (Double.isNaN(dataItem)) { return; }

    if (n_ == 0) {
      putMaxItem(dataItem);
      putMinItem(dataItem);
    } else {
      if (dataItem > getMaxItem()) { putMaxItem(dataItem); }
      if (dataItem < getMinItem()) { putMinItem(dataItem); }
    }

    //don't increment n_ and baseBufferCount_ yet
    final int curBBCount = baseBufferCount_;
    final int newBBCount = curBBCount + 1;
    final long newN = n_ + 1;

    final int combBufItemCap = combinedBuffer_.length;
    if (newBBCount > combBufItemCap) {
      growBaseBuffer(); //only changes combinedBuffer when it is only a base buffer
    }

    //put the new item in the base buffer
    combinedBuffer_[curBBCount] = dataItem;

    if (newBBCount == (k_ << 1)) { //Propagate

      // make sure there will be enough space (levels) for the propagation
      final int spaceNeeded = DoublesUpdateImpl.getRequiredItemCapacity(k_, newN);

      if (spaceNeeded > combBufItemCap) {
        // copies base buffer plus old levels, adds space for new level
        growCombinedBuffer(combBufItemCap, spaceNeeded);
      }

      // sort only the (full) base buffer via accessor which modifies the underlying base buffer,
      // then use as one of the inputs to propagate-carry
      final DoublesSketchAccessor bbAccessor = DoublesSketchAccessor.wrap(this, true);
      bbAccessor.sort();

      final long newBitPattern = DoublesUpdateImpl.inPlacePropagateCarry(
              0, // starting level
              null,
              bbAccessor,
              true,
              k_,
              DoublesSketchAccessor.wrap(this, true),
              bitPattern_
      );

      assert newBitPattern == computeBitPattern(k_, newN); // internal consistency check
      assert newBitPattern == (bitPattern_ + 1);

      bitPattern_ = newBitPattern;
      baseBufferCount_ = 0;
    } else {
      //bitPattern unchanged
      baseBufferCount_ = newBBCount;
    }
    n_ = newN;
    doublesSV = null;
  }

  /**
   * Loads the Combined Buffer, min and max from the given source MemorySegment.
   * The resulting Combined Buffer is always in non-compact form and must be pre-allocated.
   * @param srcSeg the given source MemorySegment
   * @param serVer the serialization version of the source
   * @param srcIsCompact true if the given source MemorySegment is in compact form
   * @param combBufCap total items for the combined buffer (size in doubles)
   */
  private void srcMemorySegmentToCombinedBuffer(final MemorySegment srcSeg, final int serVer,
                                         final boolean srcIsCompact, final int combBufCap) {
    final int preLongs = 2;
    final int extra = (serVer == 1) ? 3 : 2; // space for min and max quantiles, buf alloc (SerVer 1)
    final int preBytes = (preLongs + extra) << 3;
    final int bbCnt = baseBufferCount_;
    final int k = getK();
    final long n = getN();
    final double[] combinedBuffer = new double[combBufCap]; //always non-compact
    //Load min, max
    putMinItem(srcSeg.get(JAVA_DOUBLE_UNALIGNED, MIN_DOUBLE));
    putMaxItem(srcSeg.get(JAVA_DOUBLE_UNALIGNED, MAX_DOUBLE));

    if (srcIsCompact) {
      //Load base buffer
      MemorySegment.copy(srcSeg, JAVA_DOUBLE_UNALIGNED, preBytes, combinedBuffer, 0, bbCnt);

      //Load levels from compact srcSeg
      long bitPattern = bitPattern_;
      if (bitPattern != 0) {
        long segOffset = preBytes + (bbCnt << 3);
        int combBufOffset = 2 * k;
        while (bitPattern != 0L) {
          if ((bitPattern & 1L) > 0L) {
            MemorySegment.copy(srcSeg, JAVA_DOUBLE_UNALIGNED, segOffset, combinedBuffer, combBufOffset, k);
            segOffset += (k << 3); //bytes, increment compactly
          }
          combBufOffset += k; //doubles, increment every level
          bitPattern >>>= 1;
        }

      }
    } else { //srcSeg not compact
      final int levels = computeNumLevelsNeeded(k, n);
      final int totItems = (levels == 0) ? bbCnt : (2 + levels) * k;
      MemorySegment.copy(srcSeg, JAVA_DOUBLE_UNALIGNED, preBytes, combinedBuffer, 0, totItems);
    }
    putCombinedBuffer(combinedBuffer);
  }

  //Restricted overrides
  //Gets

  @Override
  int getBaseBufferCount() {
    return baseBufferCount_;
  }

  @Override
  int getCombinedBufferItemCapacity() {
    return combinedBuffer_.length;
  }

  @Override
  double[] getCombinedBuffer() {
    return combinedBuffer_;
  }

  @Override
  long getBitPattern() {
    return bitPattern_;
  }

  @Override
  MemorySegment getMemorySegment() {
    return null;
  }

  //Puts

  @Override
  void putMinItem(final double minItem) {
    minItem_ = minItem;
  }

  @Override
  void putMaxItem(final double maxItem) {
    maxItem_ = maxItem;
  }

  @Override
  void putN(final long n) {
    n_ = n;
  }

  @Override
  void putCombinedBuffer(final double[] combinedBuffer) {
    combinedBuffer_ = combinedBuffer;
  }

  @Override
  void putBaseBufferCount(final int baseBufferCount) {
    baseBufferCount_ = baseBufferCount;
  }

  @Override
  void putBitPattern(final long bitPattern) {
    bitPattern_ = bitPattern;
  }

  @Override //the returned array is not always used
  double[] growCombinedBuffer(final int currentSpace, final int spaceNeeded) {
    combinedBuffer_ = Arrays.copyOf(combinedBuffer_, spaceNeeded);
    return combinedBuffer_;
  }

  /**
   * This is only used for on-heap sketches, and grows the Base Buffer by factors of 2 until it
   * reaches the maximum size of 2 * k. It is only called when there are no levels above the
   * Base Buffer.
   */
  //important: n has not been incremented yet
  private void growBaseBuffer() {
    final int oldSize = combinedBuffer_.length;
    assert oldSize < (2 * k_);
    final double[] baseBuffer = combinedBuffer_;
    final int newSize = 2 * Math.max(Math.min(k_, oldSize), MIN_K);
    combinedBuffer_ = Arrays.copyOf(baseBuffer, newSize);
  }

  static void checkPreLongsFlagsSerVer(final int flags, final int serVer, final int preLongs) {
    final boolean empty = (flags & EMPTY_FLAG_MASK) > 0;
    final boolean compact = (flags & COMPACT_FLAG_MASK) > 0;

    final int sw = (compact ? 1 : 0) + (2 * (empty ? 1 : 0)) + (4 * (serVer & 0xF))
        + (32 * (preLongs & 0x3F));
    boolean valid = true;
    switch (sw) { //These are the valid cases.
      case 38  : break; //!compact,  empty, serVer = 1, preLongs = 1; always stored as not compact
      case 164 : break; //!compact, !empty, serVer = 1, preLongs = 5; always stored as not compact
      case 42  : break; //!compact,  empty, serVer = 2, preLongs = 1; always stored as compact
      case 72  : break; //!compact, !empty, serVer = 2, preLongs = 2; always stored as compact
      case 47  : break; // compact,  empty, serVer = 3, preLongs = 1;
      case 46  : break; //!compact,  empty, serVer = 3, preLongs = 1;
      case 79  : break; // compact,  empty, serVer = 3, preLongs = 2;
      case 78  : break; //!compact,  empty, serVer = 3, preLongs = 2;
      case 77  : break; // compact, !empty, serVer = 3, preLongs = 2;
      case 76  : break; //!compact, !empty, serVer = 3, preLongs = 2;
      default : //all other cases are invalid
        valid = false;
    }

    if (!valid) {
      throw new SketchesArgumentException("Possible corruption. Inconsistent state: "
          + "PreambleLongs = " + preLongs + ", empty = " + empty + ", SerVer = " + serVer
          + ", Compact = " + compact);
    }
  }

  /**
   * Checks the validity of the heap MemorySegment capacity assuming n, k and the compact state.
   * @param k the given k
   * @param n the given n
   * @param compact true if MemorySegment is in compact form
   * @param serVer serialization version of the source
   * @param segCapBytes the current MemorySegment capacity in bytes
   */
  static void checkHeapSegCapacity(final int k, final long n, final boolean compact,
                                   final int serVer, final long segCapBytes) {
    final int metaPre = Family.QUANTILES.getMaxPreLongs() + ((serVer == 1) ? 3 : 2);
    final int retainedItems = computeRetainedItems(k, n);
    final int reqBufBytes;
    if (compact) {
      reqBufBytes = (metaPre + retainedItems) << 3;
    } else { //not compact
      final int totLevels = computeNumLevelsNeeded(k, n);
      reqBufBytes = (totLevels == 0)
          ? (metaPre + retainedItems) << 3
          : (metaPre + ((2 + totLevels) * k)) << 3;
    }
    if (segCapBytes < reqBufBytes) {
      throw new SketchesArgumentException("Possible corruption: MemorySegment capacity too small: "
          + segCapBytes + " < " + reqBufBytes);
    }
  }

}
