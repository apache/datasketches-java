/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.quantiles;

import static com.yahoo.sketches.quantiles.PreambleUtil.COMPACT_FLAG_MASK;
import static com.yahoo.sketches.quantiles.PreambleUtil.EMPTY_FLAG_MASK;
import static com.yahoo.sketches.quantiles.PreambleUtil.ORDERED_FLAG_MASK;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractFamilyID;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractFlags;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractK;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractMaxDouble;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractMinDouble;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractN;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractPreLongs;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractSerDeId;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractSerVer;
import static com.yahoo.sketches.quantiles.PreambleUtil.insertFamilyID;
import static com.yahoo.sketches.quantiles.PreambleUtil.insertFlags;
import static com.yahoo.sketches.quantiles.PreambleUtil.insertK;
import static com.yahoo.sketches.quantiles.PreambleUtil.insertMaxDouble;
import static com.yahoo.sketches.quantiles.PreambleUtil.insertMinDouble;
import static com.yahoo.sketches.quantiles.PreambleUtil.insertN;
import static com.yahoo.sketches.quantiles.PreambleUtil.insertPreLongs;
import static com.yahoo.sketches.quantiles.PreambleUtil.insertSerDeId;
import static com.yahoo.sketches.quantiles.PreambleUtil.insertSerVer;
import static com.yahoo.sketches.quantiles.Util.computeBaseBufferItems;
import static com.yahoo.sketches.quantiles.Util.computeBitPattern;
import static com.yahoo.sketches.quantiles.Util.computeExpandedCombinedBufferItemCapacity;

import java.util.Arrays;

import com.yahoo.memory.Memory;
import com.yahoo.memory.NativeMemory;
import com.yahoo.sketches.Family;
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
   * In the initial on-heap version, equals combinedBuffer_.length.
   * May differ in later versions that grow space more aggressively.
   * Also, in the off-heap version, combinedBuffer_ won't even be a java array,
   * so it won't know its own length.
   */
  int combinedBufferItemCapacity_;

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
      throw new SketchesArgumentException("Memory too small: " + memCapBytes);
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

    HeapDoublesSketch hqs = newInstance(k); //checks k
    if (empty) return hqs;
    
    //Not empty, must have valid preamble + min, max, n.
    //Forward compatibility from SerVer = 2 :
    boolean compact = (serVer == 2) | ((flags & COMPACT_FLAG_MASK) > 0);

    long n = extractN(memArr, cumOffset); //Second 8 bytes of preamble
    DoublesUtil.checkMemCapacity(k, n, compact, memCapBytes);

    //set class members by computing them
    hqs.n_ = n;
    hqs.combinedBufferItemCapacity_ = computeExpandedCombinedBufferItemCapacity(k, n);
    hqs.baseBufferCount_ = computeBaseBufferItems(k, n);
    hqs.bitPattern_ = computeBitPattern(k, n);
    hqs.combinedBuffer_ = new double[hqs.combinedBufferItemCapacity_];
    
    //Extract min, max, data from srcMem into Combined Buffer
    hqs.srcMemoryToCombinedBuffer(compact, srcMem);
    return hqs;
  }

  /**
   * Returns a copy of the given sketch
   * @param sketch the given sketch
   * @return a copy of the given sketch
   */
  static HeapDoublesSketch copy(DoublesSketch sketch) {
    HeapDoublesSketch qsCopy;
    qsCopy = HeapDoublesSketch.newInstance(sketch.getK());
    qsCopy.n_ = sketch.getN();
    qsCopy.minValue_ = sketch.getMinValue();
    qsCopy.maxValue_ = sketch.getMaxValue();
    qsCopy.combinedBufferItemCapacity_ = sketch.getCombinedBufferItemCapacity();
    qsCopy.baseBufferCount_ = sketch.getBaseBufferCount();
    qsCopy.bitPattern_ = sketch.getBitPattern();
    double[] combBuf = sketch.getCombinedBuffer();
    qsCopy.combinedBuffer_ = Arrays.copyOf(combBuf, combBuf.length);
    return qsCopy;
  }

  @Override
  public void update(double dataItem) {
    // this method only uses the base buffer part of the combined buffer
    if (Double.isNaN(dataItem)) return;

    if (dataItem > maxValue_) { maxValue_ = dataItem; }
    if (dataItem < minValue_) { minValue_ = dataItem; }

    if (baseBufferCount_ + 1 > combinedBufferItemCapacity_) {
      DoublesUtil.growBaseBuffer(this);
    }
    combinedBuffer_[baseBufferCount_++] = dataItem;
    n_++;
    if (baseBufferCount_ == 2 * k_) {
      DoublesUtil.processFullBaseBuffer(this);
    }
  }

  @Override
  public double getQuantile(double fraction) {
    if ((fraction < 0.0) || (fraction > 1.0)) {
      throw new SketchesArgumentException("Fraction cannot be less than zero or greater than 1.0");
    }
    if      (fraction == 0.0) { return minValue_; }
    else if (fraction == 1.0) { return maxValue_; }
    else {
      DoublesAuxiliary aux = this.constructAuxiliary();
      return aux.getQuantile(fraction);
    }
  }

  @Override
  public double[] getQuantiles(double[] fractions) {
    Util.validateFractions(fractions);
    DoublesAuxiliary aux = null;
    double[] answers = new double[fractions.length];
    for (int i = 0; i < fractions.length; i++) {
      double fraction = fractions[i];
      if      (fraction == 0.0) { answers[i] = minValue_; }
      else if (fraction == 1.0) { answers[i] = maxValue_; }
      else {
        if (aux == null) {
          aux = this.constructAuxiliary();
        }
        answers[i] = aux.getQuantile(fraction);
      }
    }
    return answers;
  }

  @Override
  public double[] getPMF(double[] splitPoints) {
    return getPMFOrCDF(splitPoints, false);
  }

  @Override
  public double[] getCDF(double[] splitPoints) {
    return getPMFOrCDF(splitPoints, true);
  }

  private double[] getPMFOrCDF(double[] splitPoints, boolean isCDF) {
    long[] counters = DoublesUtil.internalBuildHistogram(splitPoints, this);
    int numCounters = counters.length;
    double[] result = new double[numCounters];
    double n = n_;
    long subtotal = 0;
    if (isCDF) {
      for (int j = 0; j < numCounters; j++) {
        long count = counters[j];
        subtotal += count;
        result[j] = subtotal / n; //normalize by n
      }
    } else { // PMF
      for (int j = 0; j < numCounters; j++) {
        long count = counters[j];
        subtotal += count;
        result[j] = count / n; //normalize by n
      }
    }
    assert subtotal == n; //internal consistency check
    return result;
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
  
  @Override
  public byte[] toByteArray(boolean ordered, boolean compact) {
    boolean empty = isEmpty();

    int flags = (empty ? EMPTY_FLAG_MASK : 0) 
        | (ordered ? ORDERED_FLAG_MASK : 0) 
        | (compact ? COMPACT_FLAG_MASK : 0);
    
    if (empty) {
      byte[] outByteArr = new byte[Long.BYTES];
      Memory memOut = new NativeMemory(outByteArr);
      long cumOffset = memOut.getCumulativeOffset(0L);
      int preLongs = 1;
      insertPre0(outByteArr, cumOffset, preLongs, flags, k_);
      return outByteArr;
    }
    //not empty
    return combinedBufferToByteArray(ordered, compact);
  }
  
  /**
   * Loads the Combined Buffer, min and max from the given source Memory. 
   * The Combined Buffer is always in non-compact form and must be pre-allocated.
   * @param compact true if the given Memory is in compact form
   * @param srcMem the given source Memory
   */
  private void srcMemoryToCombinedBuffer(boolean compact, Memory srcMem) {
    final int preLongs = 2;
    final int extra = 2; // space for min and max values
    final int preBytes = (preLongs + extra) << 3;
    long cumOffset = srcMem.getCumulativeOffset(0L);
    Object memArr = srcMem.array(); //may be null
    int bbCnt = baseBufferCount_;
    
    
    //Load min, max
    minValue_ = extractMinDouble(memArr, cumOffset);
    maxValue_ = extractMaxDouble(memArr, cumOffset);
    
    if (compact) {
      //Load base buffer
      srcMem.getDoubleArray(preBytes, combinedBuffer_, 0, bbCnt);
      
      //Load levels from compact srcMem
      long bits = bitPattern_;
      if (bits != 0) {
        long memOffset = preBytes + (bbCnt << 3);
        int combBufOffset = 2 * k_;
        while (bits != 0L) {
          if ((bits & 1L) > 0L) {
            srcMem.getDoubleArray(memOffset, combinedBuffer_, combBufOffset, k_);
            memOffset += (k_ << 3); //bytes, increment compactly
          }
          combBufOffset += k_; //doubles, increment every level
          bits >>>= 1;
        }
      }
    } else { //srcMem not compact
      int levels = Util.computeNumLevelsNeeded(k_, n_);
      int totItems = (levels == 0) ? bbCnt : (2 + levels) * k_;
      srcMem.getDoubleArray(preBytes, combinedBuffer_, 0, totItems);
    }
  }
  
  /**
   * Returns a byte array, including preamble, min, max and data extracted from the Combined Buffer.
   * @param ordered true if the desired form of the resulting array has the base buffer sorted.
   * @param compact true if the desired form of the resulting array is in compact form.
   * @return a byte array, including preamble, min, max and data extracted from the Combined Buffer.
   */
  private byte[] combinedBufferToByteArray(boolean ordered, boolean compact) {
    final int preLongs = 2;
    final int extra = 2; // extra space for min and max values
    int preBytes = (preLongs + extra) << 3;
    int flags = (ordered ? ORDERED_FLAG_MASK : 0) | (compact ? COMPACT_FLAG_MASK : 0);
    double[] bbItemsArr = null;
    
    final int bbCnt = Util.computeBaseBufferItems(k_, n_);
    if (bbCnt > 0) {
      bbItemsArr = new double[bbCnt];
      System.arraycopy(combinedBuffer_, 0, bbItemsArr, 0, bbCnt);
      if (ordered) { Arrays.sort(bbItemsArr); }
    }
    byte[] outByteArr = null;

    if (compact) {
      final int retainedItems = getRetainedItems();
      int outBytes = (retainedItems << 3) + preBytes;
      outByteArr = new byte[outBytes];
      
      Memory memOut = new NativeMemory(outByteArr);
      long cumOffset = memOut.getCumulativeOffset(0L);
      
      //insert preamble, min, max
      insertPre0(outByteArr, cumOffset, preLongs, flags, k_);
      insertN(outByteArr, cumOffset, n_);
      insertMinDouble(outByteArr, cumOffset, minValue_);
      insertMaxDouble(outByteArr, cumOffset, maxValue_);
      
      //insert base buffer
      if (bbCnt > 0) {
        memOut.putDoubleArray(preBytes, bbItemsArr, 0, bbCnt);
      }
      //insert levels into compact dstMem (and array)
      long bits = bitPattern_;
      if (bits != 0) {
        long memOffset = preBytes + (baseBufferCount_ << 3); //bytes
        int combBufOffset = 2 * k_; //doubles
        while (bits != 0L) {
          if ((bits & 1L) > 0L) {
            memOut.putDoubleArray(memOffset, combinedBuffer_, combBufOffset, k_);
            memOffset += (k_ << 3); //bytes, increment compactly
          }
          combBufOffset += k_; //doubles, increment every level
          bits >>>= 1;
        }
      }

    } else { //not compact
      final int totLevels = Util.computeNumLevelsNeeded(k_, n_);
      int outBytes = (totLevels == 0)
          ? (bbCnt << 3) + preBytes
          : (((2 + totLevels) * k_) << 3)  + preBytes;
      outByteArr = new byte[outBytes];
      
      Memory memOut = new NativeMemory(outByteArr);
      long cumOffset = memOut.getCumulativeOffset(0L);
      
      //insert preamble, min, max
      insertPre0(outByteArr, cumOffset, preLongs, flags, k_);
      insertN(outByteArr, cumOffset, n_);
      insertMinDouble(outByteArr, cumOffset, minValue_);
      insertMaxDouble(outByteArr, cumOffset, maxValue_);
      
      //insert base buffer
      if (bbCnt > 0) {
        memOut.putDoubleArray(preBytes, bbItemsArr, 0, bbCnt);
      }
      //insert levels
      if (totLevels > 0) {
        long memOffset = preBytes + ((2L * k_) << 3);
        int combBufOffset = 2 * k_;
        memOut.putDoubleArray(memOffset, combinedBuffer_, combBufOffset, totLevels * k_);
      }
    }
    return outByteArr;
  }
  
  private static final void insertPre0(byte[] outArr, long cumOffset, int preLongs, int flags, 
      int k) {
    insertPreLongs(outArr, cumOffset, preLongs);
    insertSerVer(outArr, cumOffset, DoublesUtil.DOUBLES_SER_VER);
    insertFamilyID(outArr, cumOffset, Family.QUANTILES.getID());
    insertFlags(outArr, cumOffset, flags);
    insertK(outArr, cumOffset, k);
    insertSerDeId(outArr, cumOffset, ARRAY_OF_DOUBLES_SERDE_ID);
  }
  
  @Override
  public String toString(boolean sketchSummary, boolean dataDetail) {
    return DoublesUtil.toString(sketchSummary, dataDetail, this);
  }
  
  @Override
  public DoublesSketch downSample(int newK) {
    HeapDoublesSketch oldSketch = this;
    HeapDoublesSketch newSketch = HeapDoublesSketch.newInstance(newK);
    DoublesUtil.downSamplingMergeInto(oldSketch, newSketch);
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
  
//  @Override
//  long getBitPattern() {
//    return bitPattern_;
//  }

  @Override
  double[] getCombinedBuffer() {
    return combinedBuffer_;
  }
  
} // End of class HeapDoublesSketch
