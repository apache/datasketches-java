/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the 
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.quantiles;

import static com.yahoo.sketches.quantiles.DoublesUtil.checkMemCapacity;
import static com.yahoo.sketches.quantiles.PreambleUtil.COMPACT_FLAG_MASK;
import static com.yahoo.sketches.quantiles.PreambleUtil.EMPTY_FLAG_MASK;
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

import com.yahoo.memory.Memory;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.SketchesStateException;

/**
 * Implements the DoublesSketch off-heap.
 * 
 * @author Kevin Lang
 * @author Lee Rhodes
 */
@SuppressWarnings("unused")
public class DirectDoublesSketch extends DoublesSketch {
  private static final int DIRECT_PRE_LONGS = 4; //includes min and max values
  private Memory mem_;
  private long cumOffset_; //
  private Object memArr_;
  
  
  //**CONSTRUCTORS**********************************************************  
  private DirectDoublesSketch(int k) {
    super(k);
  }
  
  static DirectDoublesSketch newInstance(int k, Memory dstMem) {
    DirectDoublesSketch dds = new DirectDoublesSketch(k);
    long memCap = dstMem.getCapacity();
    long minCap = (DIRECT_PRE_LONGS + 2 * k) << 3; //require at least full base buffer
    if (memCap < minCap) {
      throw new SketchesArgumentException(
          "Destination Memory too small: " + memCap + " < " + minCap);
    }
    long cumOffset = dstMem.getCumulativeOffset(0L);
    Object memArr = dstMem.array();
    
    //init dstMem
    insertPreLongs(memArr, cumOffset, 2);
    insertSerVer(memArr, cumOffset, DoublesUtil.DOUBLES_SER_VER);
    insertFamilyID(memArr, cumOffset, Family.QUANTILES.getID());
    int flags = EMPTY_FLAG_MASK; //empty
    insertFlags(memArr, cumOffset, flags);
    insertK(memArr, cumOffset, k);
    insertSerDeId(memArr, cumOffset, DoublesSketch.ARRAY_OF_DOUBLES_SERDE_ID);
    insertN(memArr, cumOffset, 0L);
    insertMinDouble(memArr, cumOffset, Double.POSITIVE_INFINITY);
    insertMaxDouble(memArr, cumOffset, Double.NEGATIVE_INFINITY);
    
    dds.mem_ = dstMem;
    dds.cumOffset_ = dds.mem_.getCumulativeOffset(0L);
    dds.memArr_ = dds.mem_.array();
    return dds;
  }

  static DirectDoublesSketch wrapInstance(Memory srcMem) {
    long memCapBytes = srcMem.getCapacity();
    if (memCapBytes < 8) { //initially require enough for the first long
      throw new SketchesArgumentException(
          "Destination Memory too small: " + memCapBytes + " < 8");
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
    
    boolean compact = (serVer == 2) | ((flags & COMPACT_FLAG_MASK) > 0);
    if (compact) {
      throw new SketchesArgumentException("Compact Memory is not supported for Direct.");
    }
    DirectDoublesSketch dds = new DirectDoublesSketch(k);
    if (empty) { return dds; }
    
    //check if srcMem has required capacity given k, n, compact, memCapBytes
    long n = extractN(memArr, cumOffset);
    checkMemCapacity(k, n, compact, memCapBytes);

    return dds;
  }
  
  @Override
  public void update(double dataItem) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public double getQuantile(double fraction) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public double[] getQuantiles(double[] fractions) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int getK() {
    return extractK(memArr_, cumOffset_);
  }

  @Override
  public long getN() {
    return extractN(memArr_, cumOffset_);
  }
  
  @Override
  public boolean isEmpty() {
    return (extractFlags(memArr_, cumOffset_) & EMPTY_FLAG_MASK) > 0;
  }
  
  @Override
  void putMinValue(double minValue) {
    insertMinDouble(memArr_, cumOffset_, minValue);
  }
  
  @Override
  public double getMinValue() {
    return extractMinDouble(memArr_, cumOffset_);
  }

  @Override
  void putMaxValue(double maxValue) {
    insertMaxDouble(memArr_, cumOffset_, maxValue);
  }
  
  @Override
  public double getMaxValue() {
    return extractMaxDouble(memArr_, cumOffset_);
  }

  @Override
  public void reset() {
    insertN(memArr_, cumOffset_, 0L);
    insertMinDouble(memArr_, cumOffset_, Double.POSITIVE_INFINITY);
    insertMaxDouble(memArr_, cumOffset_, Double.NEGATIVE_INFINITY);
    insertFlags(memArr_, cumOffset_, EMPTY_FLAG_MASK); //not compact, not ordered
  }
  
  //TODO This needs rethinking

  @Override
  public byte[] toByteArray(boolean ordered, boolean compact) {
    int preLongs = extractPreLongs(memArr_, cumOffset_);
    boolean empty = (extractFlags(memArr_, cumOffset_) & EMPTY_FLAG_MASK) > 0;
    if (empty ^ (preLongs == 1)) {
      throw new SketchesStateException(
          "Inconsistent state of Empty and PreLongs: Empty: " + empty + ", PreLongs: " + preLongs);
    }
    if (empty) {
      byte[] out = new byte[8];
      mem_.getByteArray(0, out, 0, 8);
      return out;
    }
    int k = extractK(memArr_, cumOffset_);
    long n = extractN(memArr_, cumOffset_);
    
    //TODO
    
    return null;
  }

  @Override
  public DoublesSketch downSample(int smallerK) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void putMemory(Memory dstMem, boolean sort) {
    // TODO Auto-generated method stub
    
  }

  @Override
  int getBaseBufferCount() {
    return Util.computeBaseBufferItems(getK(), getN());
  }

  @Override
  int getCombinedBufferItemCapacity() {
    return Util.computeExpandedCombinedBufferItemCapacity(getK(), getN());
  }

  @Override
  double[] getCombinedBuffer() {
    // TODO Auto-generated method stub
    return null;
  }
  
  @Override
  void putCombinedBuffer(double[] combinedBuffer) {
    mem_.putDoubleArray(32, combinedBuffer, 0, combinedBuffer.length);
  }
  
  @Override
  void putCombinedBufferItemCapacity(int combBufItemCap) {
    //intentionally a no-op
  }
  
  //Other restricted
  
  /**
   * Returns the current item capacity of the combined, non-compact, data buffer 
   * given <i>k</i> and <i>n</i>. The base buffer is always allocated at full size.
   * 
   * @param k sketch parameter. This determines the accuracy of the sketch and the 
   * size of the updatable data structure, which is a function of <i>k</i> and <i>n</i>.
   * 
   * @param n The number of items in the input stream
   * @return the current item capacity of the combined data buffer
   */
  private static final int computeDirectCombBufItemCapacity(int k, long n) {
    int totLevels = Util.computeNumLevelsNeeded(k, n);
    return (2 + totLevels) * k; //base buffer always allocated at full size.
  }

  @Override
  void putN(long n) {
    // TODO Auto-generated method stub
    
  }

  @Override
  void putBaseBufferCount(int baseBufCount) {
    // TODO Auto-generated method stub
    
  }

  @Override
  void putBitPattern(long bitPattern) {
    // TODO Auto-generated method stub
    
  }

  @Override
  Memory getMemory() {
    // TODO Auto-generated method stub
    return null;
  }
  
}
