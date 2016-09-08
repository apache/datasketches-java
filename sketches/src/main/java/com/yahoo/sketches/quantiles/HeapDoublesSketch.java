/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.quantiles;

import static com.yahoo.sketches.quantiles.PreambleUtil.EMPTY_FLAG_MASK;
import static com.yahoo.sketches.quantiles.PreambleUtil.MAX_DOUBLE;
import static com.yahoo.sketches.quantiles.PreambleUtil.MIN_DOUBLE;
import static com.yahoo.sketches.quantiles.PreambleUtil.N_LONG;
import static com.yahoo.sketches.quantiles.PreambleUtil.SER_VER;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractFamilyID;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractFlags;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractK;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractPreLongs;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractSerDeId;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractSerVer;
import static com.yahoo.sketches.quantiles.PreambleUtil.insertFamilyID;
import static com.yahoo.sketches.quantiles.PreambleUtil.insertFlags;
import static com.yahoo.sketches.quantiles.PreambleUtil.insertK;
import static com.yahoo.sketches.quantiles.PreambleUtil.insertPreLongs;
import static com.yahoo.sketches.quantiles.PreambleUtil.insertSerDeId;
import static com.yahoo.sketches.quantiles.PreambleUtil.insertSerVer;
import static com.yahoo.sketches.quantiles.Util.computeBaseBufferItems;
import static com.yahoo.sketches.quantiles.Util.computeBitPattern;
import static com.yahoo.sketches.quantiles.Util.computeCombBufItemCapacity;
import static com.yahoo.sketches.quantiles.Util.computeRetainedItems;

import java.util.Arrays;

import com.yahoo.memory.Memory;
import com.yahoo.memory.NativeMemory;
import com.yahoo.sketches.ArrayOfDoublesSerDe;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.SketchesArgumentException;

/**
 * Implements the DoublesSketch on the Java heap.
 * 
 * @author Kevin Lang
 * @author Lee Rhodes
 */
final class HeapDoublesSketch extends DoublesSketch {

  private static final short ARRAY_OF_DOUBLES_SERDE_ID = new ArrayOfDoublesSerDe().getId();
  /**
   * The smallest value ever seen in the stream.
   */
  double minValue_;

  /**
   * The largest value ever seen in the stream.
   */
  double maxValue_;

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
  static HeapDoublesSketch getInstance(int k) {
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
  static HeapDoublesSketch getInstance(Memory srcMem) {
    long memCapBytes = srcMem.getCapacity();
    if (memCapBytes < Long.BYTES) {
      throw new SketchesArgumentException("Memory too small: " + memCapBytes);
    }
    long pre0 = srcMem.getLong(0);
    int preambleLongs = extractPreLongs(pre0);
    int serVer = extractSerVer(pre0);
    int familyID = extractFamilyID(pre0);
    int flags = extractFlags(pre0);
    int k = extractK(pre0);
    short serDeId = extractSerDeId(pre0);

    if (serDeId != ARRAY_OF_DOUBLES_SERDE_ID) {
      throw new SketchesArgumentException(
      "Possible Corruption: serDeId incorrect: " + serDeId + " != " + ARRAY_OF_DOUBLES_SERDE_ID);
    }

    boolean empty = Util.checkPreLongsFlagsCap(preambleLongs, flags, memCapBytes);
    Util.checkFamilyID(familyID);
    Util.checkSerVer(serVer);

    HeapDoublesSketch hqs = getInstance(k); //checks k

    if (empty) return hqs;

    //Not empty, must have valid preamble + min, max

    long n = srcMem.getLong(PreambleUtil.N_LONG);
    int retainedItems = computeRetainedItems(k, n);
    Util.checkMemCapacity(retainedItems, memCapBytes);

    //set class members
    hqs.n_ = n;
    hqs.combinedBufferItemCapacity_ = computeCombBufItemCapacity(k, n);
    hqs.baseBufferCount_ = computeBaseBufferItems(k, n);
    hqs.bitPattern_ = computeBitPattern(k, n);
    hqs.combinedBuffer_ = new double[hqs.combinedBufferItemCapacity_];

    int srcMemItemsOffsetBytes = preambleLongs * Long.BYTES;
    hqs.minValue_ = srcMem.getDouble(srcMemItemsOffsetBytes);
    srcMemItemsOffsetBytes += Double.BYTES;
    hqs.maxValue_ = srcMem.getDouble(srcMemItemsOffsetBytes);
    srcMemItemsOffsetBytes += Double.BYTES;

    //load Base Buffer
    srcMem.getDoubleArray(srcMemItemsOffsetBytes, hqs.combinedBuffer_, 0, hqs.baseBufferCount_);
    srcMemItemsOffsetBytes += hqs.baseBufferCount_ * Double.BYTES;

    long bits = computeBitPattern(k, n);
    if (bits == 0) return hqs;
    int levelBytes = k * Double.BYTES;
    for (int level = 0; bits != 0L; level++, bits >>>= 1) {
      if ((bits & 1L) > 0L) {
        srcMem.getDoubleArray(srcMemItemsOffsetBytes, hqs.combinedBuffer_, (2 + level) * k, k);
        srcMemItemsOffsetBytes += levelBytes;
      }
    }
    return hqs;
  }

  /**
   * Returns a copy of the given sketch, which may be either Direct or on-heap
   * @param sketch the given sketch
   * @return a copy of the given sketch, which may be either Direct or on-heap
   */
  static HeapDoublesSketch copy(DoublesSketch sketch) {
    HeapDoublesSketch qsCopy;
    qsCopy = HeapDoublesSketch.getInstance(sketch.getK());
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
    DoublesAuxiliary aux = null; //
    double[] answers = new double[fractions.length];
    for (int i = 0; i < fractions.length; i++) {
      double fraction = fractions[i];
      if      (fraction == 0.0) { answers[i] = minValue_; }
      else if (fraction == 1.0) { answers[i] = maxValue_; }
      else {
        if (aux == null) aux = this.constructAuxiliary();
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
  public byte[] toByteArray(boolean sort) {
    int preLongs, arrLongs, flags;
    boolean empty = isEmpty();
    
    if (empty) {
      preLongs = 1;
      arrLongs = 1;
      flags = EMPTY_FLAG_MASK;
    }
    else {
      preLongs = 2;
      arrLongs = preLongs + 2 + Util.computeRetainedItems(k_, n_); // 2 for min and max values
      flags = 0;
    }
    //build prelong 0
    long pre0 = 0L;
    pre0 = insertPreLongs(preLongs, pre0);
    pre0 = insertSerVer(SER_VER, pre0);
    pre0 = insertFamilyID(Family.QUANTILES.getID(), pre0);
    //other flags: bigEndian = false
    pre0 = insertFlags(flags, pre0);
    pre0 = insertK(k_, pre0);
    pre0 = insertSerDeId(ARRAY_OF_DOUBLES_SERDE_ID, pre0);
    
    byte[] outArr = new byte[arrLongs << 3];
    Memory memOut = new NativeMemory(outArr);
    if (empty) {
      memOut.putLong(0, pre0);
      return outArr;
    }
    //insert preamble + min and max
    memOut.putLong(0, pre0);
    memOut.putLong(N_LONG, n_);
    memOut.putDouble(MIN_DOUBLE, minValue_);
    memOut.putDouble(MAX_DOUBLE, maxValue_);
    //insert BaseBuffer
    int bbItems = computeBaseBufferItems(k_, n_);
    int offsetBytes = (preLongs + 2) << 3;
    if ((bbItems < 2 * k_) && (bbItems > 0)) {
      if (sort)  {
        Arrays.sort(combinedBuffer_, 0, bbItems);
      }
      memOut.putDoubleArray(offsetBytes , combinedBuffer_, 0, bbItems);
      offsetBytes += Double.BYTES * bbItems;
    }
    //insert levels
    long bits = computeBitPattern(k_, n_);
    for (int level = 0; bits != 0L; level++, bits >>>= 1) {
      if ((bits & 1L) > 0L) {
        memOut.putDoubleArray(offsetBytes, combinedBuffer_, (2 + level) * k_, k_);
        offsetBytes += k_ * Double.BYTES;
      }
    }
    return outArr;
  }
  
  @Override
  public String toString(boolean sketchSummary, boolean dataDetail) {
    return DoublesUtil.toString(sketchSummary, dataDetail, this);
  }
  
  @Override
  public DoublesSketch downSample(int newK) {
    HeapDoublesSketch oldSketch = this;
    HeapDoublesSketch newSketch = HeapDoublesSketch.getInstance(newK);
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
  
  //Other restricted
  
  /**
   * Returns the Auxiliary data structure which is only used for getQuantile() and getQuantiles() 
   * queries.
   * @return the Auxiliary data structure
   */
  DoublesAuxiliary constructAuxiliary() {
    return new DoublesAuxiliary( this );
  }
  
} // End of class HeapDoublesSketch
