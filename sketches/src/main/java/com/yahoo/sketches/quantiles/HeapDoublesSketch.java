/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.quantiles;

import static com.yahoo.sketches.quantiles.PreambleUtil.COMPACT_FLAG_MASK;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractFamilyID;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractFlags;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractK;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractMaxDouble;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractMinDouble;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractN;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractPreLongs;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractSerVer;
import static com.yahoo.sketches.quantiles.Util.computeBaseBufferItems;
import static com.yahoo.sketches.quantiles.Util.computeBitPattern;
import static com.yahoo.sketches.quantiles.Util.computeCombinedBufferItemCapacity;

import java.util.Arrays;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.SketchesArgumentException;

/**
 * Implements the DoublesSketch on the Java heap.
 *
 * @author Lee Rhodes
 */
final class HeapDoublesSketch extends DoublesSketch {

  /**
   * The smallest value ever seen in the stream.
   */
  private double minValue_;

  /**
   * The largest value ever seen in the stream.
   */
  private double maxValue_;

  /**
   * The total count of items seen.
   */
  private long n_;

  /**
   * Number of samples currently in base buffer.
   *
   * <p>Count = N % (2*K)
   */
  private int baseBufferCount_;

  /**
   * Active levels expressed as a bit pattern.
   *
   * <p>Pattern = N / (2 * K)
   */
  private long bitPattern_;

  /**
   * In the initial on-heap version, equals combinedBuffer_.length.
   * May differ in later versions that grow space more aggressively.
   * Also, in the off-heap version, combinedBuffer_ won't be a java array,
   * so it won't know its own length.
   */
  private int combinedBufferItemCapacity_;

  /**
   * This single array contains the base buffer plus all levels some of which may not be used,
   * i.e, is in non-compact form.
   * A level is of size K and is either full and sorted, or not used. A "not used" buffer may have
   * garbage. Whether a level buffer used or not is indicated by the bitPattern_.
   * The base buffer has length 2*K but might not be full and isn't necessarily sorted.
   * The base buffer precedes the level buffers. This buffer does not include the min, max values.
   *
   * <p>The levels arrays require quite a bit of explanation, which we defer until later.</p>
   */
  private double[] combinedBuffer_;

  //**CONSTRUCTORS**********************************************************
  private HeapDoublesSketch(final int k) {
    super(k);
  }

  /**
   * Obtains a new instance of a DoublesSketch.
   *
   * @param k Parameter that controls space usage of sketch and accuracy of estimates.
   * Must be greater than 1 and less than 65536 and a power of 2.
   * @return a HeapQuantileSketch
   */
  static HeapDoublesSketch newInstance(final int k) {
    final HeapDoublesSketch hqs = new HeapDoublesSketch(k);
    final int bufAlloc = Math.min(Util.MIN_BASE_BUF_SIZE, 2 * k); //the min is important
    hqs.n_ = 0;
    hqs.combinedBufferItemCapacity_ = bufAlloc;
    hqs.combinedBuffer_ = new double[bufAlloc];
    hqs.baseBufferCount_ = 0;
    hqs.bitPattern_ = 0;
    hqs.minValue_ = Double.POSITIVE_INFINITY;
    hqs.maxValue_ = Double.NEGATIVE_INFINITY;
    return hqs;
  }

  /**
   * Heapifies the given srcMem, which must be a Memory image of a DoublesSketch
   * @param srcMem a Memory image of a sketch.
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @return a DoublesSketch on the Java heap.
   */
  static HeapDoublesSketch heapifyInstance(final Memory srcMem) {
    final long memCapBytes = srcMem.getCapacity();
    if (memCapBytes < 8) {
      throw new SketchesArgumentException("Source Memory too small: " + memCapBytes + " < 8");
    }
    final Object memObj = srcMem.array(); //may be null
    final long memAdd = srcMem.getCumulativeOffset(0L);

    //Extract the preamble first 8 bytes
    final int preLongs = extractPreLongs(memObj, memAdd);
    final int serVer = extractSerVer(memObj, memAdd);
    final int familyID = extractFamilyID(memObj, memAdd);
    final int flags = extractFlags(memObj, memAdd);
    final int k = extractK(memObj, memAdd);

    //VALIDITY CHECKS
    DoublesUtil.checkDoublesSerVer(serVer);

    final boolean empty = Util.checkPreLongsFlagsCap(preLongs, flags, memCapBytes);
    Util.checkFamilyID(familyID);

    final HeapDoublesSketch hds = newInstance(k); //checks k
    if (empty) { return hds; }

    //Not empty, must have valid preamble + min, max, n.
    //Forward compatibility from SerVer = 2 :
    final boolean compact = (serVer == 2) | ((flags & COMPACT_FLAG_MASK) > 0);

    final long n = extractN(memObj, memAdd); //Second 8 bytes of preamble
    DoublesUtil.checkMemCapacity(k, n, compact, memCapBytes);

    //set class members by computing them
    hds.n_ = n;
    hds.combinedBufferItemCapacity_ = computeCombinedBufferItemCapacity(k, n, true);
    hds.baseBufferCount_ = computeBaseBufferItems(k, n);
    hds.bitPattern_ = computeBitPattern(k, n);
    hds.combinedBuffer_ = new double[hds.combinedBufferItemCapacity_];

    //Extract min, max, data from srcMem into Combined Buffer
    hds.srcMemoryToCombinedBuffer(compact, srcMem);
    return hds;
  }

  @Override
  public void update(final double dataItem) {
    if (Double.isNaN(dataItem)) { return; }
    final double maxValue = getMaxValue();
    final double minValue = getMinValue();

    if (dataItem > maxValue) { putMaxValue(dataItem); }
    if (dataItem < minValue) { putMinValue(dataItem); }

    //don't increment n_ and baseBufferCount- yet
    final int curBBCount = baseBufferCount_;
    final int newBBCount = curBBCount + 1;
    final long newN = n_ + 1;

    if (newBBCount > combinedBufferItemCapacity_) {
      growBaseBuffer(); //only changes combinedBuffer,  combinedBufferItemCapacity
    }

    //put the new item in the base buffer
    combinedBuffer_[curBBCount] = dataItem;

    if (newBBCount == 2 * k_) { //Propogate

      // make sure there will be enough levels for the propagation
      final int spaceNeeded = DoublesUpdateImpl.maybeGrowLevels(newN, k_);

      if (spaceNeeded > combinedBufferItemCapacity_) {
        // copies base buffer plus old levels, adds space for new level
        growCombinedBuffer(spaceNeeded);
      }

      // note that combinedBuffer_ is after the possible resizing above
      Arrays.sort(combinedBuffer_, 0, k_ << 1); //sort only the BB portion, which is full

      final long newBitPattern = DoublesUpdateImpl.inPlacePropagateCarry(
          0,               //starting level
          null,            //sizeKbuf,   not needed here
          0,               //sizeKStart, not needed here
          combinedBuffer_, //size2Kbuf, the base buffer = the Combined Buffer, possibly resized
          0,               //size2KStart
          true,            //doUpdateVersion
          k_,
          combinedBuffer_, //the base buffer = the Combined Buffer, possibly resized
          bitPattern_      //current bitPattern prior to updating n
      );
      assert newBitPattern == computeBitPattern(k_, newN); // internal consistency check

      bitPattern_ = newBitPattern;
      baseBufferCount_ = 0;
    } else {
      //bitPattern unchanged
      baseBufferCount_ = newBBCount;
    }
    n_ = newN;
  }

  @Override
  public int getK() {
    return k_;
  }

  @Override
  public long getN() {
    return n_;
  }

  @Override
  public boolean isEmpty() {
    return (n_ == 0);
  }

  @Override
  public double getMinValue() {
    return minValue_;
  }

  @Override
  public double getMaxValue() {
    return maxValue_;
  }

  @Override
  public void reset() {
    n_ = 0;
    combinedBufferItemCapacity_ = Math.min(Util.MIN_BASE_BUF_SIZE, 2 * k_); //the min is important
    combinedBuffer_ = new double[combinedBufferItemCapacity_];
    baseBufferCount_ = 0;
    bitPattern_ = 0;
    minValue_ = Double.POSITIVE_INFINITY;
    maxValue_ = Double.NEGATIVE_INFINITY;
  }

  /**
   * Loads the Combined Buffer, min and max from the given source Memory.
   * The Combined Buffer is always in non-compact form and must be pre-allocated.
   * @param compact true if the given source Memory is in compact form
   * @param srcMem the given source Memory
   */
  private void srcMemoryToCombinedBuffer(final boolean compact, final Memory srcMem) {
    final Object memArr = srcMem.array(); //may be null
    final long memAdd = srcMem.getCumulativeOffset(0L);

    final int preLongs = 2;
    final int extra = 2; // space for min and max values
    final int preBytes = (preLongs + extra) << 3;
    final int bbCnt = baseBufferCount_;
    final int k = getK();
    final long n = getN();
    final double[] combinedBuffer = getCombinedBuffer();
    //Load min, max
    putMinValue(extractMinDouble(memArr, memAdd));
    putMaxValue(extractMaxDouble(memArr, memAdd));

    if (compact) {
      //Load base buffer
      srcMem.getDoubleArray(preBytes, combinedBuffer, 0, bbCnt);

      //Load levels from compact srcMem
      long bits = bitPattern_;
      if (bits != 0) {
        long memOffset = preBytes + (bbCnt << 3);
        int combBufOffset = 2 * k;
        while (bits != 0L) {
          if ((bits & 1L) > 0L) {
            srcMem.getDoubleArray(memOffset, combinedBuffer, combBufOffset, k);
            memOffset += (k << 3); //bytes, increment compactly
          }
          combBufOffset += k; //doubles, increment every level
          bits >>>= 1;
        }
      }
    } else { //srcMem not compact
      final int levels = Util.computeNumLevelsNeeded(k, n);
      final int totItems = (levels == 0) ? bbCnt : (2 + levels) * k;
      srcMem.getDoubleArray(preBytes, combinedBuffer, 0, totItems);
    }
  }

  //Restricted overrides
  //Gets

  @Override
  int getBaseBufferCount() {
    return baseBufferCount_;
  }

  @Override
  int getCombinedBufferItemCapacity() {
    return combinedBufferItemCapacity_;
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
  Memory getMemory() {
    return null;
  }

  //Puts

  @Override
  void putMinValue(final double minValue) {
    minValue_ = minValue;
  }

  @Override
  void putMaxValue(final double maxValue) {
    maxValue_ = maxValue;
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
  void putCombinedBufferItemCapacity(final int combinedBufferItemCapacity) {
    combinedBufferItemCapacity_ = combinedBufferItemCapacity;
  }

  @Override
  void putBaseBufferCount(final int baseBufferCount) {
    baseBufferCount_ = baseBufferCount;
  }

  @Override
  void putBitPattern(final long bitPattern) {
    bitPattern_ = bitPattern;
  }

  @Override
  double[] growCombinedBuffer(final int spaceNeeded) {
    combinedBuffer_ = Arrays.copyOf(combinedBuffer_, spaceNeeded);
    combinedBufferItemCapacity_ = spaceNeeded;
    return combinedBuffer_;
  }

  /**
   * This is only used for on-heap sketches, and grows the Base Buffer by factors of 2 until it
   * reaches the maximum size of 2 * k. It is only called when there are no levels above the
   * Base Buffer.
   *
   * @param sketch the given sketch.
   */
  //important: n has not been incremented yet
  private final void growBaseBuffer() {
    final double[] baseBuffer = combinedBuffer_;
    final int oldSize = combinedBufferItemCapacity_;
    assert oldSize < 2 * k_;
    final int newSize = Math.max(Math.min(2 * k_, 2 * oldSize), Util.MIN_BASE_BUF_SIZE);
    combinedBufferItemCapacity_ = newSize;
    combinedBuffer_ = Arrays.copyOf(baseBuffer, newSize);
  }

} // End of class HeapDoublesSketch
