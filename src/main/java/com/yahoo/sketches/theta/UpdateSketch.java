/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.theta;

import static com.yahoo.sketches.Util.DEFAULT_NOMINAL_ENTRIES;
import static com.yahoo.sketches.Util.DEFAULT_UPDATE_SEED;
import static com.yahoo.sketches.Util.checkIfPowerOf2;
import static com.yahoo.sketches.hash.MurmurHash3.hash;
import static com.yahoo.sketches.theta.UpdateReturnState.RejectedNullOrEmpty;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.yahoo.sketches.Family;
import com.yahoo.sketches.memory.Memory;

/**
 * The parent class for all Update Sketches, such as QuickSelect and Alpha.  The primary task of an
 * Upeate Sketch is to consider datums presented via the update() methods for inclusion in its
 * internal cache. This is the sketch building process. 
 * 
 * @author Lee Rhodes 
 */
public abstract class UpdateSketch extends Sketch {
  
  UpdateSketch() {}
  
  //Sketch
  
  @Override
  public abstract boolean isEmpty();
  
  //UpdateSketch
  
  /**
   * Makes a new builder
   *
   * @return a new builder
   */
  public static final Builder builder() {
    return new Builder();
  }
  
  /**
   * For building a new UpdateSketch.
   */
  public static class Builder {
    private int bLgNomLongs;
    private long bSeed;
    private float bP;
    private ResizeFactor bRF;
    private Family bFam;
    private Memory bMem;
    
    Builder() {
      bLgNomLongs = Integer.numberOfTrailingZeros(DEFAULT_NOMINAL_ENTRIES);
      bSeed = DEFAULT_UPDATE_SEED;
      bP = (float) 1.0;
      bRF = ResizeFactor.X8;
      bFam = Family.QUICKSELECT;
      bMem = null;
    }
    
    /**
     * Sets the long seed value that is require by the hashing function.
     * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
     * @return this Builder
     */
    public Builder setSeed(long seed) {
      bSeed = seed;
      return this;
    }
    
    /**
     * Sets the upfront uniform sampling probability, <i>p</i>
     * @param p <a href="{@docRoot}/resources/dictionary.html#p">See Sampling Probability, <i>p</i></a>
     * @return this Builder
     */
    public Builder setP(float p) {
      if ((p <= 0.0) || (p > 1.0)) {
        throw new IllegalArgumentException("p must be > 0 and <= 1.0: "+p);
      }
      bP = p;
      return this;
    }
    
    /**
     * Sets the cache Resize Factor
     * @param rf <a href="{@docRoot}/resources/dictionary.html#resizeFactor">See Resize Factor</a>
     * @return this Builder
     */
    public Builder setResizeFactor(ResizeFactor rf) {
      bRF = rf;
      return this;
    }
    
    /**
     * Set the Family.  
     * @param family the family for this builder
     * @return this Builder
     */
    public Builder setFamily(Family family) {
      this.bFam = family;
      return this;
    }
    
    /**
     * Initialize the specified backing Memory store.  
     * Note: this cannot be used with the Alpha Family of sketches.
     * @param mem  <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
     * @return this Builder
     */
    public Builder initMemory(Memory mem) {
      bMem = mem;
      return this;
    }
    
    /**
     * Returns a configured UpdateSketch with the
     * <a href="{@docRoot}/resources/dictionary.html#defaultNomEntries">Default Nominal Entres</a>
     * @return an UpdateSketch
     */
    public UpdateSketch build() {
      return build(DEFAULT_NOMINAL_ENTRIES);
    }
    
    /**
     * Returns a configured UpdateSketch with the given
     * <a href="{@docRoot}/resources/dictionary.html#nomEntries">Nominal Entres</a>
     * @param nomEntries <a href="{@docRoot}/resources/dictionary.html#nomEntries">Nominal Entres</a>
     * @return an UpdateSketch
     */
    public UpdateSketch build(int nomEntries) {
      checkIfPowerOf2(nomEntries, "nomEntries");
      bLgNomLongs = Integer.numberOfTrailingZeros(nomEntries);
      UpdateSketch sketch = null;
      switch (bFam) {
        case ALPHA: {
          if (bMem == null) {
            sketch = new HeapAlphaSketch(bLgNomLongs, bSeed, bP, bRF);
          } 
          else {
            throw new IllegalArgumentException("AlphaSketch cannot be made Direct to Memory.");
          }
          break;
        }
        case QUICKSELECT: {
          if (bMem == null) {
            sketch = new HeapQuickSelectSketch(bLgNomLongs, bSeed, bP, bRF, false);
          } 
          else {
            sketch = 
              new DirectQuickSelectSketch(bLgNomLongs, bSeed, bP, bMem, false);
          }
          break;
        }
        default: throw new IllegalArgumentException(
            "Given Family cannot be built as a Sketch: "+bFam.toString());
        
      }
      return sketch;
    }
  }
  
  /**
   * Resets this sketch back to a virgin empty state.
   */
  public abstract void reset();
  
  //Updatable
  
  /**
   * Convert this UpdateSketch to a CompactSketch in the chosen form.
   * <p>This compacting process converts the hash table form of an UpdateSketch to
   * a simple list of the valid hash values from the hash table.  Any hash values equal to or
   * greater than theta will be discarded.  The number of valid values remaining in the
   * Compact Sketch depends on a number of factors, but may be larger or smaller than 
   * <i>Nominal Entries</i> (or <i>k</i>).  It will never exceed 2<i>k</i>.  If it is critical
   * to always limit the size to no more than <i>k</i>, then <i>rebuild()</i> should be called
   * on the UpdateSketch prior to this.
   * 
   * @param dstOrdered if true, the destination cache should be ordered
   * @param dstMem if valid, and large enough the returned sketch will be backed by this Memory.
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>.
   * @return this sketch as a compact sketch
   * @throws IllegalArgumentException if destination Memory is not large enough.
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
   * Rebuilds the hash table to remove dirty values or to reduce the size
   * to nominal entries.
   * @return this sketch
   */
  public abstract UpdateSketch rebuild();
  
  
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
  
  //UpdateInternal Defined here with Javadocs
  
  /**
   * All potential updates converge here.
   * <p>Don't ever call this unless you really know what you are doing!</p>
   * 
   * @param hash the given input hash value
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
   * Gets the value of theta as a double with a value between zero and one
   * @return the value of theta as a double
   */
  abstract double getTheta();
  
  /**
   * Returns true if the internal cache contains "dirty" values that are greater than or equal
   * to thetaLong.
   * @return true if the internal cache is dirty.
   */
  abstract boolean isDirty();
  
  //SetArgument
  
  @Override
  public boolean isOrdered() {
    return false;
  }
  
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