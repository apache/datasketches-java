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

package org.apache.datasketches.quantiles;

import static org.apache.datasketches.quantiles.ClassicUtil.MIN_K;
import static org.apache.datasketches.quantiles.ClassicUtil.checkFamilyID;
import static org.apache.datasketches.quantiles.ClassicUtil.checkHeapFlags;
import static org.apache.datasketches.quantiles.ClassicUtil.computeBaseBufferItems;
import static org.apache.datasketches.quantiles.ClassicUtil.computeBitPattern;
import static org.apache.datasketches.quantiles.ClassicUtil.computeCombinedBufferItemCapacity;
import static org.apache.datasketches.quantiles.ClassicUtil.computeNumLevelsNeeded;
import static org.apache.datasketches.quantiles.ClassicUtil.computeRetainedItems;
import static org.apache.datasketches.quantiles.PreambleUtil.COMPACT_FLAG_MASK;
import static org.apache.datasketches.quantiles.PreambleUtil.EMPTY_FLAG_MASK;
import static org.apache.datasketches.quantiles.PreambleUtil.MAX_DOUBLE;
import static org.apache.datasketches.quantiles.PreambleUtil.MIN_DOUBLE;
import static org.apache.datasketches.quantiles.PreambleUtil.extractFamilyID;
import static org.apache.datasketches.quantiles.PreambleUtil.extractFlags;
import static org.apache.datasketches.quantiles.PreambleUtil.extractK;
import static org.apache.datasketches.quantiles.PreambleUtil.extractN;
import static org.apache.datasketches.quantiles.PreambleUtil.extractPreLongs;
import static org.apache.datasketches.quantiles.PreambleUtil.extractSerVer;

import java.util.Arrays;

import org.apache.datasketches.common.Family;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;
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
    final HeapUpdateDoublesSketch hqs = new HeapUpdateDoublesSketch(k);
    final int baseBufAlloc = 2 * Math.min(MIN_K, k); //the min is important
    hqs.n_ = 0;
    hqs.combinedBuffer_ = new double[baseBufAlloc];
    hqs.baseBufferCount_ = 0;
    hqs.bitPattern_ = 0;
    hqs.minItem_ = Double.NaN;
    hqs.maxItem_ = Double.NaN;
    return hqs;
  }

  /**
   * Heapifies the given srcMem, which must be a Memory image of a DoublesSketch and may have data.
   *
   * @param srcMem a Memory image of a sketch, which may be in compact or not compact form.
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @return a DoublesSketch on the Java heap.
   */
  static HeapUpdateDoublesSketch heapifyInstance(final Memory srcMem) {
    final long memCapBytes = srcMem.getCapacity();
    if (memCapBytes < 8) {
      throw new SketchesArgumentException("Source Memory too small: " + memCapBytes + " < 8");
    }

    final int preLongs = extractPreLongs(srcMem);
    final int serVer = extractSerVer(srcMem);
    final int familyID = extractFamilyID(srcMem);
    final int flags = extractFlags(srcMem);
    final int k = extractK(srcMem);

    final boolean empty = (flags & EMPTY_FLAG_MASK) > 0; //Preamble flags empty state
    final long n = empty ? 0 : extractN(srcMem);

    //VALIDITY CHECKS
    DoublesUtil.checkDoublesSerVer(serVer, MIN_HEAP_DOUBLES_SER_VER);
    checkHeapFlags(flags);
    checkPreLongsFlagsSerVer(flags, serVer, preLongs);
    checkFamilyID(familyID);

    final HeapUpdateDoublesSketch hds = newInstance(k); //checks k
    if (empty) { return hds; }

    //Not empty, must have valid preamble + min, max, n.
    //Forward compatibility from SerVer = 1 :
    final boolean srcIsCompact = (serVer == 2) | ((flags & COMPACT_FLAG_MASK) > 0);

    checkHeapMemCapacity(k, n, srcIsCompact, serVer, memCapBytes);

    //set class members by computing them
    hds.n_ = n;
    final int combBufCap = computeCombinedBufferItemCapacity(k, n);
    hds.baseBufferCount_ = computeBaseBufferItems(k, n);
    hds.bitPattern_ = computeBitPattern(k, n);
    //Extract min, max, data from srcMem into Combined Buffer
    hds.srcMemoryToCombinedBuffer(srcMem, serVer, srcIsCompact, combBufCap);
    return hds;
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
  public boolean hasMemory() {
    return false;
  }

  @Override
  public boolean isDirect() {
    return false;
  }

  @Override
  public boolean isReadOnly() {
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
    classicQdsSV = null;
  }

  @Override
  public void updateMultipleIdentical(final double dataItem, final int count) {
    for (int i = 0; i < count; i++) {
      update(dataItem);
    }
  }

  /**
   * Loads the Combined Buffer, min and max from the given source Memory.
   * The resulting Combined Buffer is always in non-compact form and must be pre-allocated.
   * @param srcMem the given source Memory
   * @param serVer the serialization version of the source
   * @param srcIsCompact true if the given source Memory is in compact form
   * @param combBufCap total items for the combined buffer (size in doubles)
   */
  private void srcMemoryToCombinedBuffer(final Memory srcMem, final int serVer,
                                         final boolean srcIsCompact, final int combBufCap) {
    final int preLongs = 2;
    final int extra = (serVer == 1) ? 3 : 2; // space for min and max quantiles, buf alloc (SerVer 1)
    final int preBytes = (preLongs + extra) << 3;
    final int bbCnt = baseBufferCount_;
    final int k = getK();
    final long n = getN();
    final double[] combinedBuffer = new double[combBufCap]; //always non-compact
    //Load min, max
    putMinItem(srcMem.getDouble(MIN_DOUBLE));
    putMaxItem(srcMem.getDouble(MAX_DOUBLE));

    if (srcIsCompact) {
      //Load base buffer
      srcMem.getDoubleArray(preBytes, combinedBuffer, 0, bbCnt);

      //Load levels from compact srcMem
      long bitPattern = bitPattern_;
      if (bitPattern != 0) {
        long memOffset = preBytes + (bbCnt << 3);
        int combBufOffset = 2 * k;
        while (bitPattern != 0L) {
          if ((bitPattern & 1L) > 0L) {
            srcMem.getDoubleArray(memOffset, combinedBuffer, combBufOffset, k);
            memOffset += (k << 3); //bytes, increment compactly
          }
          combBufOffset += k; //doubles, increment every level
          bitPattern >>>= 1;
        }

      }
    } else { //srcMem not compact
      final int levels = computeNumLevelsNeeded(k, n);
      final int totItems = (levels == 0) ? bbCnt : (2 + levels) * k;
      srcMem.getDoubleArray(preBytes, combinedBuffer, 0, totItems);
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
  WritableMemory getMemory() {
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
   * Checks the validity of the heap memory capacity assuming n, k and the compact state.
   * @param k the given k
   * @param n the given n
   * @param compact true if memory is in compact form
   * @param serVer serialization version of the source
   * @param memCapBytes the current memory capacity in bytes
   */
  static void checkHeapMemCapacity(final int k, final long n, final boolean compact,
                                   final int serVer, final long memCapBytes) {
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
    if (memCapBytes < reqBufBytes) {
      throw new SketchesArgumentException("Possible corruption: Memory capacity too small: "
          + memCapBytes + " < " + reqBufBytes);
    }
  }

}
