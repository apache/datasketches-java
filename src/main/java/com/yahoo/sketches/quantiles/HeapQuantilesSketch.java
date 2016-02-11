/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.quantiles;

import static com.yahoo.sketches.quantiles.PreambleUtil.EMPTY_FLAG_MASK;
import static com.yahoo.sketches.quantiles.PreambleUtil.SER_VER;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractFamilyID;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractFlags;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractK;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractPreLongs;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractSeed;
import static com.yahoo.sketches.quantiles.PreambleUtil.extractSerVer;
import static com.yahoo.sketches.quantiles.PreambleUtil.insertFamilyID;
import static com.yahoo.sketches.quantiles.PreambleUtil.insertFlags;
import static com.yahoo.sketches.quantiles.PreambleUtil.insertK;
import static com.yahoo.sketches.quantiles.PreambleUtil.insertPreLongs;
import static com.yahoo.sketches.quantiles.PreambleUtil.insertSerVer;
import static com.yahoo.sketches.quantiles.Util.computeBaseBufferCount;
import static com.yahoo.sketches.quantiles.Util.computeBitPattern;

import java.util.Arrays;

import com.yahoo.sketches.Family;
import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.memory.NativeMemory;

/**
 * Implements the QuantilesSketch on the Java heap.
 * 
 * @author Kevin Lang
 * @author Lee Rhodes
 */
class HeapQuantilesSketch extends QuantilesSketch {
  
  /**
   * Total number of data items in the stream so far. (Uniqueness plays no role in these sketches).
   */
  long n_;

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
  int combinedBufferAllocatedCount_;

  /**
   * Number of samples currently in base buffer.
   * 
   * Count = N % (2*K)
   */
  int baseBufferCount_; 

  /**
   * Active levels expressed as a bit pattern.
   * 
   * Pattern = N / (2 * K)
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
  private HeapQuantilesSketch(int k, short seed) {
    super(k, seed);
    if (seed != 0) QuantilesSketch.rand.setSeed(seed);
  }
  
  /**
   * Obtains an instance of a QuantileSketch of double elements.
   * 
   * @param k Parameter that controls space usage of sketch and accuracy of estimates. 
   * Must be greater than 0 and less than 65536.
   * It is recommended that <i>k</i> be a power of 2 to enable merging of sketches with
   * different values of <i>k</i>.
   * @param seed if zero, it is ignored and the random generator is not changed,
   * otherwise it will be used to set the seed of the random generator.
   * @return a HeapQuantileSketch
   */
  static HeapQuantilesSketch getInstance(int k, short seed) {
    HeapQuantilesSketch hqs = new HeapQuantilesSketch(k, seed);
    int bufAlloc = Math.min(MIN_BASE_BUF_SIZE,2*k); //the min is important
    hqs.n_ = 0;
    hqs.combinedBufferAllocatedCount_ = bufAlloc;
    hqs.combinedBuffer_ = new double[bufAlloc];
    hqs.baseBufferCount_ = 0;
    hqs.bitPattern_ = 0;
    hqs.minValue_ = java.lang.Double.POSITIVE_INFINITY;
    hqs.maxValue_ = java.lang.Double.NEGATIVE_INFINITY;
    return hqs;
  }
  
  /**
   * Heapifies the given srcMem, which must be a Memory image of a QuantilesSketch
   * @param srcMem a Memory image of a sketch.
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @return a QuantilesSketch on the Java heap.
   */
  static HeapQuantilesSketch getInstance(Memory srcMem) {
    long memCapBytes = srcMem.getCapacity();
    if (memCapBytes < 8) {
      throw new IllegalArgumentException("Memory too small: "+memCapBytes);
    }
    long pre0 = srcMem.getLong(0);
    int preambleLongs = extractPreLongs(pre0);
    int serVer = extractSerVer(pre0);
    int familyID = extractFamilyID(pre0);
    int flags = extractFlags(pre0);
    int k = extractK(pre0);
    short seed = (short)extractSeed(pre0);
    
    boolean empty = Util.checkPreLongsFlagsCap(preambleLongs, flags, memCapBytes);
    Util.checkFamilyID(familyID);
    Util.checkSerVer(serVer);
    
    HeapQuantilesSketch hqs = new HeapQuantilesSketch(k, seed);
    
    if (empty) return hqs;
    
    //Not empty, must have valid preamble
    long[] remainderPreArr = new long[4];
    srcMem.getLongArray(8, remainderPreArr, 0, 4);
    
    long n = remainderPreArr[0];
    double minValue = Double.longBitsToDouble(remainderPreArr[1]);
    double maxValue = Double.longBitsToDouble(remainderPreArr[2]);
    int memBufAlloc = (int) remainderPreArr[3];
    
    Util.checkBufAllocAndCap(k, n, memBufAlloc, memCapBytes);
    
    //set class members
    hqs.n_ = n;
    hqs.combinedBufferAllocatedCount_ = memBufAlloc;
    hqs.minValue_ = minValue;
    hqs.maxValue_ = maxValue;
    hqs.baseBufferCount_ = computeBaseBufferCount(k, n);
    hqs.bitPattern_ = computeBitPattern(k, n);
    hqs.combinedBuffer_ = new double[memBufAlloc];
    srcMem.getDoubleArray(40, hqs.combinedBuffer_, 0, memBufAlloc);
    
    return hqs;
  }
  
  /**
   * Returns a copy of the given sketch, which may be either Direct or on-heap
   * @param sketch the given sketch
   * @return a copy of the given sketch, which may be either Direct or on-heap
   */
  static HeapQuantilesSketch copy(QuantilesSketch sketch) {
    HeapQuantilesSketch qsCopy; 
    qsCopy = HeapQuantilesSketch.getInstance(sketch.getK(), sketch.getSeed());
    qsCopy.n_ = sketch.getN();
    qsCopy.minValue_ = sketch.getMinValue();
    qsCopy.maxValue_ = sketch.getMaxValue();
    qsCopy.combinedBufferAllocatedCount_ = sketch.getCombinedBufferAllocatedCount();
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

    if (baseBufferCount_+1 > combinedBufferAllocatedCount_) {
      Util.growBaseBuffer(this);
    } 
    combinedBuffer_[baseBufferCount_++] = dataItem;
    n_++;
    if (baseBufferCount_ == 2*k_) {
      Util.processFullBaseBuffer(this);
    }
  }

  @Override
  public double getQuantile(double fraction) {
    if ((fraction < 0.0) || (fraction > 1.0)) {
      throw new IllegalArgumentException("Fraction cannot be less than zero or greater than 1.0");
    }
    if      (fraction == 0.0) { return minValue_; }
    else if (fraction == 1.0) { return maxValue_; }
    else {
      Auxiliary aux = this.constructAuxiliary();
      return aux.getQuantile(fraction);
    }
  }

  @Override
  public double[] getQuantiles(double[] fractions) {
    Util.validateSequential(fractions);
    Auxiliary aux = null; //
    double[] answers = new double[fractions.length];
    for (int i = 0; i < fractions.length; i++) {
      double fraction = fractions[i];
      if ((fraction < 0.0) || (fraction > 1.0)) {
        throw new IllegalArgumentException("Fraction cannot be less than zero or greater than 1.0");
      }
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
    long[] counters = Util.internalBuildHistogram(splitPoints, this);
    int numCounters = counters.length;
    double[] result = new double[numCounters];
    double n = n_;
    long subtotal = 0;
    for (int j = 0; j < numCounters; j++) { 
      long count = counters[j];
      subtotal += count;
      result[j] = count / n; //normalize by n
    }
    assert subtotal == n; //internal consistency check
    return result;
  }

  @Override
  public double[] getCDF(double[] splitPoints) {
    long[] counters = Util.internalBuildHistogram(splitPoints, this);
    int numCounters = counters.length;
    double[] result = new double[numCounters];
    double n = n_;
    long subtotal = 0;
    for (int j = 0; j < numCounters; j++) { 
      long count = counters[j];
      subtotal += count;
      result[j] = subtotal / n; //normalize by n
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
  public long getN() { 
    return n_; 
  }
  
  @Override
  public short getSeed() {
    return seed_;
  }
  
  @Override
  public void reset() {
    n_ = 0;
    combinedBufferAllocatedCount_ = Math.min(MIN_BASE_BUF_SIZE,2*k_); //the min is important
    combinedBuffer_ = new double[combinedBufferAllocatedCount_];
    baseBufferCount_ = 0;
    bitPattern_ = 0;
    minValue_ = java.lang.Double.POSITIVE_INFINITY;
    maxValue_ = java.lang.Double.NEGATIVE_INFINITY;
  }

  @Override
  public byte[] toByteArray() {
    int preLongs, arrLongs, flags;
    boolean empty = isEmpty();
    
    if (empty) {
      preLongs = 1;
      arrLongs = 1;
      flags = EMPTY_FLAG_MASK;
    }
    else {
      preLongs = 5;
      arrLongs = preLongs + combinedBuffer_.length;
      flags = 0;
    }
    byte[] outArr = new byte[arrLongs << 3];
    NativeMemory mem = new NativeMemory(outArr);
    
    //build first prelong
    long pre0 = 0L;
    pre0 = insertPreLongs(preLongs, pre0);
    pre0 = insertSerVer(SER_VER, pre0);
    pre0 = insertFamilyID(Family.QUANTILES.getID(), pre0);
    //other flags: bigEndian = readOnly = compact = ordered = false
    pre0 = insertFlags(flags, pre0);
    pre0 = insertK(k_, pre0);
    
    if (empty) {
      mem.putLong(0, pre0);
    }
    else {
      long[] preArr = new long[5];
      preArr[0] = pre0;
      preArr[1] = n_;
      preArr[2] = Double.doubleToLongBits(minValue_);
      preArr[3] = Double.doubleToLongBits(maxValue_);
      preArr[4] = combinedBufferAllocatedCount_;
      mem.putLongArray(0, preArr, 0, 5);
      mem.putDoubleArray(40, combinedBuffer_, 0, combinedBuffer_.length);
    }
    return outArr;
  }
  
  @Override
  public String toString(boolean sketchSummary, boolean dataDetail) {
    return Util.toString(sketchSummary, dataDetail, this);
  }
  
  @Override
  public QuantilesSketch downSample(int newK) {
    HeapQuantilesSketch oldSketch = this;
    HeapQuantilesSketch newSketch = HeapQuantilesSketch.getInstance(newK, QuantilesSketch.DEFAULT_SEED);
    Util.downSamplingMergeInto(oldSketch, newSketch);
    return newSketch;
  }
  
  @Override
  public void putMemory(Memory dstMem) {
    byte[] byteArr = toByteArray();
    int arrLen = byteArr.length;
    long memCap = dstMem.getCapacity();
    if (memCap < arrLen) {
      throw new IllegalArgumentException(
          "Destination Memory not large enough: "+memCap +" < "+arrLen);
    }
    dstMem.putByteArray(0, byteArr, 0, arrLen);
  }
  
  //Restricted overrides
  
  @Override
  int getBaseBufferCount() {
    return baseBufferCount_;
  }
  
  @Override
  int getCombinedBufferAllocatedCount() {
    return combinedBufferAllocatedCount_;
  }
  
  @Override
  long getBitPattern() {
    return bitPattern_;
  }

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
  Auxiliary constructAuxiliary() {
    return new Auxiliary( this );
  }
  
} // End of class HeapQuantilesSketch
