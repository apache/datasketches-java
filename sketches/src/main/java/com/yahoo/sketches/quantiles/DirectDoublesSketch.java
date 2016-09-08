/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the 
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.quantiles;

import static com.yahoo.memory.UnsafeUtil.unsafe;
import static com.yahoo.sketches.quantiles.PreambleUtil.MAX_DOUBLE;
import static com.yahoo.sketches.quantiles.PreambleUtil.MIN_DOUBLE;
import static com.yahoo.sketches.quantiles.PreambleUtil.N_LONG;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.ArrayOfDoublesSerDe;

/**
 * Implements the DoublesSketch off-heap.
 * 
 * @author Kevin Lang
 * @author Lee Rhodes
 */
@SuppressWarnings("unused")
public class DirectDoublesSketch extends DoublesSketch {

  private static final short ARRAY_OF_DOUBLES_SERDE_ID = new ArrayOfDoublesSerDe().getId();
  
  private Memory mem_;
  private long cumOffset_;
  private boolean direct_;
  
  //**CONSTRUCTORS**********************************************************  
  private DirectDoublesSketch(int k) {
    super(k);
  }
  
  static DirectDoublesSketch getInstance(int k, Memory dstMem) {
    DirectDoublesSketch dds = new DirectDoublesSketch(k);
    dds.mem_ = dstMem;
    dds.cumOffset_ = dds.mem_.getCumulativeOffset(0L);
    dds.direct_ = dds.mem_.isDirect();
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
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public long getN() {
    return mem_.getLong(N_LONG);
  }
  
  void putMinValue(double minValue) {
    if (direct_) {
      unsafe.putDouble(cumOffset_ + MIN_DOUBLE, minValue);
    } else {
      mem_.putDouble(MIN_DOUBLE, minValue);
    }
  }
  
  @Override
  public double getMinValue() {
    return (direct_)
        ? unsafe.getDouble(cumOffset_ + MIN_DOUBLE)
        : mem_.getDouble(MIN_DOUBLE);
  }

  void putMaxValue(double maxValue) {
    if (direct_) {
      unsafe.putDouble(cumOffset_ + MAX_DOUBLE, maxValue);
    } else {
      mem_.putDouble(MAX_DOUBLE, maxValue);
    }
  }
  
  @Override
  public double getMaxValue() {
    return (direct_) 
        ? unsafe.getDouble(cumOffset_ + MAX_DOUBLE)
        : mem_.getDouble(MAX_DOUBLE);
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
