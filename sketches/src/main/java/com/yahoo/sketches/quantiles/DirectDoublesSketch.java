/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the 
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.quantiles;

import static com.yahoo.sketches.quantiles.PreambleUtil.MAX_DOUBLE;
import static com.yahoo.sketches.quantiles.PreambleUtil.MIN_DOUBLE;

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
  
  //**CONSTRUCTORS**********************************************************  
  private DirectDoublesSketch(int k) {
    super(k);
  }
  
  static DirectDoublesSketch getInstance(int k, Memory dstMem) {
    DirectDoublesSketch dqs = new DirectDoublesSketch(k);
    dqs.mem_ = dstMem;
    return dqs;
  }
  
  
  
  
  private void putMinValue(double minValue) {
    mem_.putDouble(MIN_DOUBLE, minValue);
  }
  
  private void putMaxValue(double maxValue) {
    mem_.putDouble(MAX_DOUBLE,  maxValue);
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
  public double getMinValue() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public double getMaxValue() {
    // TODO Auto-generated method stub
    return 0;
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
