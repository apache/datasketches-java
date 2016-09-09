/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the 
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.quantiles;

import static com.yahoo.memory.UnsafeUtil.unsafe;
import static com.yahoo.sketches.quantiles.PreambleUtil.*;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.SketchesArgumentException;

/**
 * Implements the DoublesSketch off-heap.
 * 
 * @author Kevin Lang
 * @author Lee Rhodes
 */
@SuppressWarnings("unused")
public class DirectDoublesSketch extends DoublesSketch {

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
    long minCap = 32 + (k << 3);
    if (memCap < minCap) {
      throw new SketchesArgumentException(
          "Destination Memory too small: " + memCap + " < " + minCap);
    }
    long cumOffset = dstMem.getCumulativeOffset(0L);
    Object memArr = dstMem.array();
    
    //init dstMem
    insertPreLongs(memArr, cumOffset, 2);
    insertSerVer(memArr, cumOffset, SER_VER);
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
    return null;
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
  public double[] getPMF(double[] splitPoints) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public double[] getCDF(double[] splitPoints) {
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
  
  void putMinValue(double minValue) {
    insertMinDouble(memArr_, cumOffset_, minValue);
  }
  
  @Override
  public double getMinValue() {
    return extractMinDouble(memArr_, cumOffset_);
  }

  void putMaxValue(double maxValue) {
    insertMaxDouble(memArr_, cumOffset_, maxValue);
  }
  
  @Override
  public double getMaxValue() {
    return extractMaxDouble(memArr_, cumOffset_);
  }

  @Override
  public void reset() {
    // TODO Auto-generated method stub
    
  }

  @Override
  public byte[] toByteArray(boolean sort) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String toString(boolean sketchSummary, boolean dataDetail) {
    // TODO Auto-generated method stub
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
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  int getCombinedBufferItemCapacity() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  double[] getCombinedBuffer() {
    // TODO Auto-generated method stub
    return null;
  }
  
}
