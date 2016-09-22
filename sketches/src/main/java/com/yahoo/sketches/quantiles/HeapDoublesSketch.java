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
import static com.yahoo.sketches.quantiles.PreambleUtil.extractSerDeId;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractSerVer;
import static com.yahoo.sketches.quantiles.Util.computeBaseBufferItems;
import static com.yahoo.sketches.quantiles.Util.computeBitPattern;
import static com.yahoo.sketches.quantiles.Util.computeExpandedCombinedBufferItemCapacity;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.SketchesArgumentException;

/**
 * Implements the DoublesSketch on the Java heap.
 * 
 * @author Kevin Lang
 * @author Lee Rhodes
 */
final class HeapDoublesSketch extends DoublesSketch {
  
  /**
   * The smallest value ever seen in the stream.
   */
  double minValue_;

  /**
   * The largest value ever seen in the stream.
   */
  double maxValue_;

  /**
   * The total count of items seen.
   */
  long n_;

  /**
   * Number of samples currently in base buffer.
   * 
   * <p>Count = N % (2*K)
   */
  int baseBufferCount_;

  /**
   * Active levels expressed as a bit pattern.
   * 
   * <p>Pattern = N / (2 * K)
   */
  long bitPattern_;
  
  /**
   * In the initial on-heap version, equals combinedBuffer_.length.
   * May differ in later versions that grow space more aggressively.
   * Also, in the off-heap version, combinedBuffer_ won't be a java array,
   * so it won't know its own length.
   */
  int combinedBufferItemCapacity_;
  
  /**
   * This single array contains the base buffer plus all levels some of which may not be used.
   * A level is of size K and is either full and sorted, or not used. A "not used" buffer may have
   * garbage. Whether a level buffer used or not is indicated by the bitPattern_.
   * The base buffer has length 2*K but might not be full and isn't necessarily sorted.
   * The base buffer precedes the level buffers. 
   * 
   * The levels arrays require quite a bit of explanation, which we defer until later.
   */
  double[] combinedBuffer_;

  //**CONSTRUCTORS**********************************************************
  private HeapDoublesSketch(int k) {
    super(k);
  }
  
  /**
   * Obtains a new instance of a DoublesSketch.
   * 
   * @param k Parameter that controls space usage of sketch and accuracy of estimates. 
   * Must be greater than 2 and less than 65536 and a power of 2.
   * @return a HeapQuantileSketch
   */
  static HeapDoublesSketch newInstance(int k) {
    HeapDoublesSketch hqs = new HeapDoublesSketch(k);
    int bufAlloc = Math.min(Util.MIN_BASE_BUF_SIZE, 2 * k); //the min is important
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
  static HeapDoublesSketch heapifyInstance(Memory srcMem) {
    long memCapBytes = srcMem.getCapacity();
    if (memCapBytes < 8) {
      throw new SketchesArgumentException("Source Memory too small: " + memCapBytes + " < 8");
    }
    long cumOffset = srcMem.getCumulativeOffset(0L);
    Object memArr = srcMem.array(); //may be null
    
    //Extract the preamble first 8 bytes 
    int preLongs = extractPreLongs(memArr, cumOffset);
    int serVer = extractSerVer(memArr, cumOffset);
    int familyID = extractFamilyID(memArr, cumOffset);
    int flags = extractFlags(memArr, cumOffset);
    int k = extractK(memArr, cumOffset);
    short serDeId = extractSerDeId(memArr, cumOffset);

    //VALIDITY CHECKS
    DoublesUtil.checkDoublesSerVer(serVer);
    
    if (serDeId != ARRAY_OF_DOUBLES_SERDE_ID) {
      throw new SketchesArgumentException(
      "Possible Corruption: serDeId incorrect: " + serDeId + " != " + ARRAY_OF_DOUBLES_SERDE_ID);
    }
    boolean empty = Util.checkPreLongsFlagsCap(preLongs, flags, memCapBytes);
    Util.checkFamilyID(familyID);

    HeapDoublesSketch hds = newInstance(k); //checks k
    if (empty) { return hds; }
    
    //Not empty, must have valid preamble + min, max, n.
    //Forward compatibility from SerVer = 2 :
    boolean compact = (serVer == 2) | ((flags & COMPACT_FLAG_MASK) > 0);

    long n = extractN(memArr, cumOffset); //Second 8 bytes of preamble
    DoublesUtil.checkMemCapacity(k, n, compact, memCapBytes);

    //set class members by computing them
    hds.n_ = n;
    hds.combinedBufferItemCapacity_ = computeExpandedCombinedBufferItemCapacity(k, n);
    hds.baseBufferCount_ = computeBaseBufferItems(k, n);
    hds.bitPattern_ = computeBitPattern(k, n);
    hds.combinedBuffer_ = new double[hds.combinedBufferItemCapacity_];
    
    //Extract min, max, data from srcMem into Combined Buffer
    hds.srcMemoryToCombinedBuffer(compact, srcMem);
    return hds;
  }

  @Override
  public void update(double dataItem) {
    // this method only uses the base buffer part of the combined buffer
    if (Double.isNaN(dataItem)) return;
    double maxValue = getMaxValue();
    double minValue = getMinValue();
    
    if (dataItem > maxValue) { putMaxValue(dataItem); }
    if (dataItem < minValue) { putMinValue(dataItem); }

//    int baseBufferCount = getBaseBufferCount();
//    int combinedBufferItemCapacity = getCombinedBufferItemCapacity();

    if (baseBufferCount_ + 1 > combinedBufferItemCapacity_) {
      DoublesUpdateImpl.growBaseBuffer(this);
    }
//    baseBufferCount++;
//    putBaseBufferCount(baseBufferCount);
//    //put the new item in the base buffer
//    combinedBuffer_[baseBufferCount] = dataItem;
    combinedBuffer_[baseBufferCount_++] = dataItem;
    n_++;
    if (baseBufferCount_ == 2 * k_) {
      DoublesUpdateImpl.processFullBaseBuffer(this);
    }
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
  private void srcMemoryToCombinedBuffer(boolean compact, Memory srcMem) {
    final int preLongs = 2;
    final int extra = 2; // space for min and max values
    final int preBytes = (preLongs + extra) << 3;
    long cumOffset = srcMem.getCumulativeOffset(0L);
    Object memArr = srcMem.array(); //may be null
    int bbCnt = baseBufferCount_;
    int k = getK();
    long n = getN();
    double[] combinedBuffer = getCombinedBuffer();
    //Load min, max
    putMinValue(extractMinDouble(memArr, cumOffset));
    putMaxValue(extractMaxDouble(memArr, cumOffset));
    
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
      int levels = Util.computeNumLevelsNeeded(k, n);
      int totItems = (levels == 0) ? bbCnt : (2 + levels) * k;
      srcMem.getDoubleArray(preBytes, combinedBuffer, 0, totItems);
    }
  }
  
  /**
   * From an existing sketch, this creates a new heap sketch that can have a smaller value of K.
   * The original sketch is not modified.
   * 
   * @param smallerK the new sketch's value of K that must be smaller than this value of K.
   * It is required that this.getK() = smallerK * 2^(nonnegative integer).
   * @return the new sketch.
   */
  @Override
  public DoublesSketch downSample(int smallerK) {
    HeapDoublesSketch oldSketch = this;
    HeapDoublesSketch newSketch = HeapDoublesSketch.newInstance(smallerK);
    DoublesMergeImpl.downSamplingMergeInto(oldSketch, newSketch);
    return newSketch;
  }
  
  @Override
  public void putMemory(Memory dstMem, boolean sort) {
    byte[] byteArr = toByteArray(sort);
    int arrLen = byteArr.length;
    long memCap = dstMem.getCapacity();
    if (memCap < arrLen) {
      throw new SketchesArgumentException(
          "Destination Memory not large enough: " + memCap + " < " + arrLen);
    }
    dstMem.putByteArray(0, byteArr, 0, arrLen);
  }
  
  //Restricted overrides
  
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
  void putCombinedBuffer(double[] combinedBuffer) {
    combinedBuffer_ = combinedBuffer;
  }
  
  @Override
  void putMinValue(double minValue) {
    minValue_ = minValue;
  }
  
  @Override
  void putMaxValue(double maxValue) {
    maxValue_ = maxValue;
  }
  
  @Override
  void putN(long n) {
    n_ = n;
  }
  
  @Override
  void putCombinedBufferItemCapacity(int combBufItemCap) {
    combinedBufferItemCapacity_ = combBufItemCap;
  }
  
  @Override
  void putBaseBufferCount(int baseBufferCount) {
    baseBufferCount_ = baseBufferCount;
  }
  
  @Override
  void putBitPattern(long bitPattern) {
    bitPattern_ = bitPattern;
  }
  
  @Override
  Memory getMemory() {
    return null;
  }
  
} // End of class HeapDoublesSketch
