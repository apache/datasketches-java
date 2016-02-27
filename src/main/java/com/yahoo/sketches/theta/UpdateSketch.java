/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.theta;

import static com.yahoo.sketches.hash.MurmurHash3.hash;
import static com.yahoo.sketches.theta.UpdateReturnState.RejectedNullOrEmpty;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.yahoo.sketches.ResizeFactor;
import com.yahoo.sketches.memory.Memory;

/**
 * The parent class for the  Update Sketch families, such as QuickSelect and Alpha.  
 * The primary task of an Upeate Sketch is to consider datums presented via the update() methods 
 * for inclusion in its internal cache. This is the sketch building process. 
 * 
 * @author Lee Rhodes 
 */
public abstract class UpdateSketch extends Sketch {
  
  UpdateSketch() {}
  
  //Sketch interface
  
  @Override
  public abstract boolean isEmpty();
  
  @Override
  public boolean isCompact() {
    return false;
  }
  
  @Override
  public boolean isOrdered() {
    return false;
  }
  
  //UpdateSketch interface
  
  /**
   * Returns a new builder
   *
   * @return a new builder
   */
  public static final UpdateSketchBuilder builder() {
    return new UpdateSketchBuilder();
  }
  
  /**
   * Resets this sketch back to a virgin empty state.
   */
  public abstract void reset();
  
  /**
   * Convert this UpdateSketch to a CompactSketch in the chosen form.
   * <p>This compacting process converts the hash table form of an UpdateSketch to
   * a simple list of the valid hash values from the hash table.  Any hash values equal to or
   * greater than theta will be discarded.  The number of valid values remaining in the
   * Compact Sketch depends on a number of factors, but may be larger or smaller than 
   * <i>Nominal Entries</i> (or <i>k</i>). It will never exceed 2<i>k</i>.  If it is critical
   * to always limit the size to no more than <i>k</i>, then <i>rebuild()</i> should be called
   * on the UpdateSketch prior to this.
   * 
   * @param dstOrdered 
   * <a href="{@docRoot}/resources/dictionary.html#dstOrdered">See Destination Ordered</a>
   * 
   * @param dstMem 
   * <a href="{@docRoot}/resources/dictionary.html#dstMem">See Destination Memory</a>.
   * 
   * @return this sketch as a CompactSketch in the chosen form
   */
  public CompactSketch compact(boolean dstOrdered, Memory dstMem) {
    CompactSketch sketchOut = null;
    int sw = (dstOrdered? 2:0) | ((dstMem != null)? 1:0);
    switch (sw) {
      case 0: { //dst not ordered, dstMem == null 
        sketchOut = new HeapCompactSketch(this);
        break;
      }
      case 1: { //dst not ordered, dstMem == valid
        sketchOut = new DirectCompactSketch(this, dstMem);
        break;
      }
      case 2: { //dst ordered, dstMem == null
        sketchOut = new HeapCompactOrderedSketch(this);
        break;
      }
      case 3: { //dst ordered, dstMem == valid        
        sketchOut = new DirectCompactOrderedSketch(this, dstMem);
        break;
      }
    }
    return sketchOut;
  }
  
  /**
   * Converts this UpdateSketch to an ordered CompactSketch on the Java heap.
   * @return this sketch as an ordered CompactSketch on the Java heap.
   */
  public CompactSketch compact() {
    return compact(true, null);
  }
  
  /**
   * Rebuilds the hash table to remove dirty values or to reduce the size
   * to nominal entries.
   * @return this sketch
   */
  public abstract UpdateSketch rebuild();
  
  /**
   * Returns the configured ResizeFactor
   * @return the configured ResizeFactor
   */
  public abstract ResizeFactor getResizeFactor();
  
  /**
   * Present this sketch with a long.
   * 
   * @param datum The given long datum.
   * @return 
   * <a href="{@docRoot}/resources/dictionary.html#updateReturnState">See Update Return State</a>
   */
  public UpdateReturnState update(long datum) {
    long[] data = { datum };
    return hashUpdate(hash(data, getSeed())[0] >>> 1);
  }
  
  /**
   * Present this sketch with the given double (or float) datum. 
   * The double will be converted to a long using Double.doubleToLongBits(datum), 
   * which normalizes all NaN values to a single NaN representation. 
   * Plus and minus zero will be normalized to plus zero. 
   * The special floating-point values NaN and +/- Infinity are treated as distinct.
   * 
   * @param datum The given double datum.
   * @return 
   * <a href="{@docRoot}/resources/dictionary.html#updateReturnState">See Update Return State</a>
   */
  public UpdateReturnState update(double datum) {
    double d = (datum == 0.0) ? 0.0 : datum; // canonicalize -0.0, 0.0
    long[] data = { Double.doubleToLongBits(d) };// canonicalize all NaN forms
    return hashUpdate(hash(data, getSeed())[0] >>> 1);
  }
  
  /**
   * Present this sketch with the given String. 
   * The string is converted to a byte array using UTF8 encoding. 
   * If the string is null or empty no update attempt is made and the method returns.
   * 
   * @param datum The given String.
   * @return 
   * <a href="{@docRoot}/resources/dictionary.html#updateReturnState">See Update Return State</a>
   */
  public UpdateReturnState update(String datum) {
    if (datum == null || datum.isEmpty()) {
      return RejectedNullOrEmpty; 
    }
    byte[] data = datum.getBytes(UTF_8);
    return hashUpdate(hash(data, getSeed())[0] >>> 1);
  }
  
  /**
   * Present this sketch with the given byte array. 
   * If the byte array is null or empty no update attempt is made and the method returns.
   * 
   * @param data The given byte array.
   * @return 
   * <a href="{@docRoot}/resources/dictionary.html#updateReturnState">See Update Return State</a>
   */
  public UpdateReturnState update(byte[] data) {
    if ((data == null) || (data.length == 0)) {
      return RejectedNullOrEmpty;
    }
    return hashUpdate(hash(data, getSeed())[0] >>> 1);
  }
  
  /**
   * Present this sketch with the given integer array. 
   * If the integer array is null or empty no update attempt is made and the method returns.
   * 
   * @param data The given int array.
   * @return 
   * <a href="{@docRoot}/resources/dictionary.html#updateReturnState">See Update Return State</a>
   */
  public UpdateReturnState update(int[] data) {
    if ((data == null) || (data.length == 0)) {
      return RejectedNullOrEmpty;
    }
    return hashUpdate(hash(data, getSeed())[0] >>> 1);
  }
  
  /**
   * Present this sketch with the given long array. 
   * If the long array is null or empty no update attempt is made and the method returns.
   * 
   * @param data The given long array.
   * @return 
   * <a href="{@docRoot}/resources/dictionary.html#updateReturnState">See Update Return State</a>
   */
  public UpdateReturnState update(long[] data) {
    if ((data == null) || (data.length == 0)) {
      return RejectedNullOrEmpty;
    }
    return hashUpdate(hash(data, getSeed())[0] >>> 1);
  }
  
  //restricted methods
  
  /**
   * All potential updates converge here.
   * <p>Don't ever call this unless you really know what you are doing!</p>
   * 
   * @param hash the given input hash value.  A hash of zero or Long.MAX_VALUE is ignored. 
   * A negative hash value will throw an exception.
   * @return <a href="{@docRoot}/resources/dictionary.html#updateReturnState">See Update Return State</a>
   */
  abstract UpdateReturnState hashUpdate(long hash);
  
  /**
   * Gets the Log base 2 of the current size of the internal cache
   * @return the Log base 2 of the current size of the internal cache
   */
  abstract int getLgArrLongs();
  
  /**
   * Gets the Log base 2 of the configured nominal entries
   * @return the Log base 2 of the configured nominal entries
   */
  abstract int getLgNomLongs();
  
  /**
   * Gets the Log base 2 of the Resize Factor
   * @return the Log base 2 of the Resize Factor
   */
  abstract int getLgResizeFactor();
  
  /**
   * Gets the configured sampling probability, <i>p</i>. 
   * <a href="{@docRoot}/resources/dictionary.html#p">See Sampling Probability, <i>p</i></a>
   * @return the sampling probability, <i>p</i>
   */
  abstract float getP();
  
  /**
   * Gets the configured seed
   * @return the configured seed
   */
  abstract long getSeed();
  
  /**
   * Returns true if the internal cache contains "dirty" values that are greater than or equal
   * to thetaLong.
   * @return true if the internal cache is dirty.
   */
  abstract boolean isDirty();
  
  //static methods
  
  /**
   * Gets the smallest allowed exponent of 2 that it is a sub-multiple of the target by zero, 
   * one or more resize factors.
   * 
   * @param lgTarget Log2 of the target size
   * @param rf <a href="{@docRoot}/resources/dictionary.html#resizeFactor">See Resize Factor</a>
   * @param lgMin Log2 of the minimum allowed starting size
   * @return The Log-base 2 of the starting size
   */
  static final int startingSubMultiple(int lgTarget, ResizeFactor rf, int lgMin) {
    int lgRR = rf.lg();
    return (lgTarget <= lgMin)? lgMin : (lgRR == 0)? lgTarget : (lgTarget - lgMin) % lgRR + lgMin;
  }
  
}
