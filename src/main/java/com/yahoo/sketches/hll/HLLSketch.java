/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.hll;

import static com.yahoo.sketches.Util.LS;
import static com.yahoo.sketches.Util.checkIfPowerOf2;
import static com.yahoo.sketches.hash.MurmurHash3.hash;
import static com.yahoo.sketches.hll.HLLRegression.HLL_LG_HIBINS;
import static com.yahoo.sketches.hll.HLLRegression.HLL_LG_LOBINS;
import static com.yahoo.sketches.hll.HLLRegression.regress;
import static java.lang.Math.log;
import static java.lang.Math.sqrt;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.Serializable;

/**
 * This sketch is designed partially on the algorithm described by the Flajolet paper but implements
 * two additional algorithms to smooth out the distortions in the error distribution.
 * 
 * @author Lee Rhodes
 */
public class HLLSketch implements Serializable, Cloneable {
  private static final long serialVersionUID = 1L;
  private static final double BETA_M = Math.sqrt((3.0 * Math.log(2.0)) - 1.0);
  private static final double[] ALPHA_ARR = new double[HLL_LG_HIBINS + 1];
  private final int bins_;
  private final int lgBins_;
  private final int maxCacheSize_;
  private int empties_; //tracks the # of remaining empty bins
  private double alpha_;
  //At any point in time exactly one of binArr_ and cache_ is null.
  private byte[] binArr_; //The bin array
  private HLLCache cache_; //The very low range cache

  static {
    //load correction factors from 2^4 to 2^HLL_LG_HIBINS
    for (int b = 7; b <= HLL_LG_HIBINS; b++ ) {
      int m = 1 << b;
      ALPHA_ARR[b] = 0.7213 / (1.0 + (1.079 / m));
    }
    ALPHA_ARR[4] = 0.673;
    ALPHA_ARR[5] = 0.697;
    ALPHA_ARR[6] = 0.709;
  }

  /**
   * Constructor that uses a default lowRangeCacheSize obtained from HllRegression.
   * 
   * @param bins Powers of 2 within the range defined by HllRegression.
   */
  public HLLSketch(int bins) {
    this(bins, chooseCache(bins));
  }

  /**
   * Constructor to set custom lowRangeCacheSize primarily for testing.
   * 
   * @param bins Powers of 2 from 1024 to 1048576, inclusive
   * @param lowRangeCacheSize Powers of 2 &le; bins/8.
   */
  public HLLSketch(int bins, int lowRangeCacheSize) {
    checkIfPowerOf2(bins, "bins");
    bins_ = bins;
    lgBins_ = Integer.numberOfTrailingZeros(bins);
    HLLSketch.checkBounds(lgBins_, HLL_LG_LOBINS, HLL_LG_HIBINS, "LgBins");

    checkIfPowerOf2(lowRangeCacheSize, "LowRangeCacheSize");
    if (lowRangeCacheSize > (bins / 8)) {
      throw new IllegalArgumentException("lowRangeCacheSize must be <= # bins/8: "
          + lowRangeCacheSize);
    }
    maxCacheSize_ = lowRangeCacheSize;
    cache_ = new HLLCache(lowRangeCacheSize);

    alpha_ = ALPHA_ARR[lgBins_];
    empties_ = bins;
  }

  /**
   * Selects the optimum cache size based on the configured bin array size.
   * 
   * @param bins the configured bin array size
   * @return optimum cache size.
   */
  private static final int chooseCache(int bins) {
    return (bins < 8192) ? 128 : (bins == 8192) ? 256 : 512;
  }

  @Override
  public HLLSketch clone() {
    HLLSketch out = null;
    try {
      out = (HLLSketch) super.clone();
      if (cache_ != null) {
        out.cache_ = this.cache_.clone();
      } 
      else {
        out.cache_ = null;
        out.binArr_ = this.binArr_.clone();
      }
    } 
    catch (CloneNotSupportedException e) {
      //should not happen
    }
    return out;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    double est = getEstimate();
    double var = getVariance();
    double stdDev = (var == 0.0) ? 0.0 : sqrt(var);
    double stdErr = (est == 0.0) ? 0.0 : stdDev / est;
    sb
        .append(LS).append(" ===HLLSketch SUMMARY:").append(LS).append("  Bins (m)        : ")
        .append(bins_).append(LS).append("  Active Bins     : ").append(bins_ - empties_)
        .append(LS).append("  Cache Count     : ")
        .append(((cache_ != null) ? cache_.getCurrentCount() : 0)).append(LS)
        .append("  Estimate        : ").append(getEstimate()).append(LS)
        .append("  Variance Bound  : ").append(getVariance()).append(LS)
        .append("  Std Dev Bound   : ").append(stdDev).append(LS).append("  Std Err Bound   : ")
        .append(stdErr).append(LS).append(" ===END OF SUMMARY").append(LS);
    return sb.toString();
  }

  /**
   * Returns a formatted view of the HLL Sketch 
   * @return a formatted view of the HLL Sketch 
   */
  public String getSketchDetail() {
    StringBuilder sb = new StringBuilder();
    sb.append("====START OF HLLSketch LISTING====").append(LS).append(getDataDetail());
    sb.append(toString());
    sb.append("====END OF LISTING====").append(LS);
    return sb.toString();
  }

  /**
   * Returns a formatted view of the data of the HLL Sketch
   * @return a formatted view of the data of the HLL Sketch
   */
  public String getDataDetail() {
    StringBuilder sb = new StringBuilder();
    sb.append(" ===DATA DETAIL LIST").append(LS);
    int columns = 32;
    if (cache_ != null) {
      sb.append(cache_.toString());
    } 
    else {
      int rows = bins_ / columns;
      for (int i = 0; i < rows; i++ ) {
        sb.append(" Bin: ").append(right(Integer.toString(i * columns), 7, ' ')).append(": ");
        for (int j = 0; j < columns; j++ ) {
          int b = (binArr_[(i * columns) + j] & 0xFF);
          String s = right(Integer.toString(b), 2, ' ');
          sb.append(s);
          if ((j < (columns - 1)) || (i < (rows - 1))) {
            sb.append(',');
          }
        }
        sb.append(LS);
      }
    }
    sb.append(" ===END OF LIST").append(LS);
    return sb.toString();
  }

  /**
   * Convenience method that returns a new HLL sketch with the same configuration as this one
   * @return a new HLL sketch with the same configuration as this one
   */
  public HLLSketch getNewSketch() {
    return new HLLSketch(bins_, maxCacheSize_);
  }

  /**
   * Gets approximate memory footprint size in bytes. This includes:
   * <ul>
   * <li>Class + 3 ints + 1 double = 40</li>
   * <li>cacheArr (if active) = cacheArr.getMemorySize()</li>
   * <li>binArr (if active) = 16 + (N/8)*8 + ( (N%8 &gt; 0)? 8 : 0)
   * </ul>
   * @return the memory size in bytes.
   */
  public int getMemorySize() {
    int cacheSz = (cache_ == null) ? 8 : cache_.getMemorySize();
    int n = bins_;
    int binArrSz = (binArr_ == null) ? 8 : 24 + ((n / 8) * 8);
    return 32 + cacheSz + binArrSz;
  }

  /**
   * Gets size in bytes of retained data. When the cache is active, it is the current count of the
   * cache times eight. Once the binArray becomes active the retained data size is <i>bins</i>
   * @return size of retained data in bytes.
   */
  public int getDataSize() {
    int cacheSz = (cache_ == null) ? 0 : cache_.getCurrentCount() * 8;
    int binArrSz = (binArr_ == null) ? 0 : bins_;
    return cacheSz + binArrSz;
  }

  /**
   * Returns true if the given sketch is merge compatible with this one. This is true if and only
   * if:
   * <ul>
   * <li><i>that</i> is an instance of HLLSketch</li>
   * <li>The same hash function is used for both (the default)</li>
   * <li>The configured values of <i>bins</i> match</li>
   * <li>The configured values of <i>cacheSize</i> match</li>
   * </ul>
   * 
   * @param that Another HLLSketch.
   * @return true if that is merge compatible.
   */
  public boolean isMergeCompatible(HLLSketch that) {
    return ((this.bins_ == that.bins_) && (this.maxCacheSize_ == that.maxCacheSize_));
  }

  /**
   * Returns this sketch merged with the given one
   * @param that2 the given sketch
   * @return this sketch merged with the given one
   */
  public HLLSketch merge(final HLLSketch that2) {
    if (that2 == null) {
      throw new IllegalArgumentException("Argument cannot be null.");
    }
    if ( !this.isMergeCompatible(that2)) {
      throw new IllegalArgumentException("Sketches are not merge compatible.");
    }
    int emptyState = ((this.isEmpty()) ? 1 : 0) | ((that2.isEmpty()) ? 2 : 0);
    switch (emptyState) {
      case 0: { //both are non-empty, 
        break;
      }
      case 1: { //this is empty, that not empty
        if (that2.cache_ != null) { //that cache is not empty
          this.cache_ = that2.cache_.clone();
        } 
        else {
          this.binArr_ = that2.getBinArr(); //deep clone
          this.cache_ = null;
        }
        this.empties_ = that2.empties_;
        this.alpha_ = that2.alpha_;
        return this;
      }
      case 2: { //this not empty, that is empty
        return this;
      }
      case 3: { //both are empty, return this
        return this;
      }
    }
    //Both sketches have valid entries (i.e., not empty)
    int cacheState = (((this.cache_ == null) ? 1 : 0) | ((that2.cache_ == null) ? 2 : 0));
    switch (cacheState) {
      case 0: { //both cache_ are valid; both binArr are null
        int thatCount = that2.cache_.getCurrentCount();
        for (int i = 0; i < thatCount; i++ ) {
          updateSwitch(that2.cache_.get(i));
        }
        return this;
      }
      case 1: { //this cache_ is null; that cache_ is valid
        //this binArr is valid, that binArr is null
        int thatCount = that2.cache_.getCurrentCount();
        for (int i = 0; i < thatCount; i++ ) {
          updateSwitch(that2.cache_.get(i));
        }
        return this;
      }
      case 2: { //this cache_ is valid; that cache_ is null
        //this binArr is null;  that binArr is valid
        dumpCacheToBinArr();
        break; //merge that binArr to this binArr
      }
      case 3: { //both cache_ are null; both binArr are valid
        break;
      }
    }
    // merge that binArr to this binArr
    for (int i = bins_; i-- > 0;) {
      int thisBin = this.binArr_[i] & 0xFF;
      int thatBin = that2.binArr_[i] & 0xFF;
      if (thatBin > thisBin) {
        this.binArr_[i] = (byte) (thatBin & 0xFF);
        if (thisBin == 0) {
          empties_-- ;
        }
      }
    }
    return this;
  }

  /**
   * Update with a long
   * @param datum as a long
   * @return this updated sketch
   */
  public HLLSketch update(long datum) {
    long[] data = new long[1];
    data[0] = datum;
    return update(data); //update(long[])
  }

  /**
   * Update with a double
   * @param datum as a double
   * @return this updated sketch
   */
  public HLLSketch update(double datum) {
    long[] data = new long[1];
    double d = (datum == 0.0) ? 0.0 : datum; //canonicalize -0.0, 0.0
    data[0] = Double.doubleToLongBits(d); //canonicalize all NaN forms
    return update(data); //update(long[])
  }

  /**
   * Update with a String
   * @param datum as a String
   * @return this updated sketch
   */
  public HLLSketch update(String datum) {
    if ((datum == null) || datum.isEmpty()) {
      return this;
    }
    byte[] data = datum.getBytes(UTF_8);
    return update(data); //update(byte[])
  }

  /**
   * Update with a byte array
   * @param data as a byte array
   * @return this updated sketch
   */
  public HLLSketch update(byte[] data) {
    if ((data == null) || (data.length == 0)) {
      return this;
    }
    updateSwitch(hash(data, 9001)[0]);
    return this;
  }
  
  /**
   * Update with an int array
   * @param data as an int array
   * @return this updated sketch
   */
  public HLLSketch update(int[] data) {
    if ((data == null) || (data.length == 0)) {
      return this;
    }
    updateSwitch(hash(data, 9001)[0]);
    return this;
  }

  /**
   * Update with a long array
   * @param data as a long array
   * @return this updated sketch
   */
  public HLLSketch update(long[] data) {
    if ((data == null) || (data.length == 0)) {
      return this;
    }
    updateSwitch(hash(data, 9001)[0]);
    return this;
  }

  /**
   * Update switch. The update methods converge here. This decides whether to update the cache or
   * the binArray.
   * 
   * @param hash Incoming 64-bit hash value.
   */
  private void updateSwitch(long hash) {
    if (cache_ != null) {
      int count = cache_.getCurrentCount();
      if (count < maxCacheSize_) {
        cache_.updateCache(hash);
        return;
      }
      // cache >= cacheSize
      dumpCacheToBinArr();
    } //cache is null
    updateAbin(hash);
  }

  /**
   * Special function that creates the binArr, transfers local cache to the new binArr and nulls the
   * cache.
   */
  private void dumpCacheToBinArr() {
    //dump my cache to a new local binArr
    binArr_ = new byte[bins_];
    int count = cache_.getCurrentCount();
    for (int i = 0; i < count; i++ ) {
      updateAbin(cache_.get(i));
    }
    cache_ = null;
  }

  /**
   * The bin address is selected from the top lg(bins) bits of the given 64-bit hash. The candidate
   * value to be presented to a bin is the number of leading zeros +1 of the lower (64-lg(bins))
   * bits. The maximum value that can stored in a bin is <i>64 - AddBits</i>, where AddBits is the
   * smallest allowed number of address bits. If this is 10, than the maximum bin value is 54, which
   * requires only 6 bits. The upper 2 bits of a bin are not used. A bin value of zero represents an
   * empty bin.
   * 
   * @param hash Incoming 64-bit hash value.
   */
  private void updateAbin(long hash) {
    int binsM1 = bins_ - 1;
    int lgBins = lgBins_;
    int binIdx = (int) ((hash >>> (64 - lgBins)) & binsM1);
    long h = (hash << lgBins) | binsM1;
    int numZerosPls1 = Long.numberOfLeadingZeros(h) + 1;
    byte curVal = binArr_[binIdx];
    if (numZerosPls1 > curVal) {
      binArr_[binIdx] = (byte) numZerosPls1; //update the bin
      if (curVal == 0) {
        empties_-- ;
      }
    }
  }

  /**
   * Returns the estimated variance based on the internal state of this sketch
   * @return the estimated variance based on the internal state of this sketch
   */
  public double getVariance() {
    if (cache_ != null) {
      return 0.0;
    }
    double seb = BETA_M / Math.sqrt(bins_); //est SE bound
    double est = getEstimate();
    double sd = seb * est;
    return sd * sd;
  }

  /**
   * Return true if this sketch is empty
   * @return true if this sketch is empty
   */
  public boolean isEmpty() {
    return (cache_ != null) ? cache_.isEmpty() : (bins_ == empties_);
  }

  /**
   * Return true if this sketch is equal to the given sketch
   * @param that2 the given sketch
   * @return true if this sketch is equal to the given sketch
   */
  public boolean equalTo(HLLSketch that2) {
    if (this == that2) {
      return true; //same instance
    }
    if (this.bins_ != that2.bins_) {
      return false;
    }
    if (this.maxCacheSize_ != that2.maxCacheSize_) {
      return false;
    }
    if (cache_ != null) {
      return this.cache_.equalTo(that2.cache_);
    }
    if (this.empties_ != that2.empties_) {
      return false;
    }
    for (int i = this.bins_; i-- > 0;) {
      if (this.binArr_[i] != that2.binArr_[i]) {
        return false;
      }
    }
    return true;
  }

  /**
   * Computes the composite estimate from 4 different estimators. The raw HLL estimator, a
   * regression algorithm, the Poisson approximation algorithm (aka the Low Range Estimator) and the
   * initial cache algorithm.
   * @return estimate
   */
  public double getEstimate() {
    if (cache_ != null) {
      return cache_.getCurrentCount();
    }
    double regressEst = regress(lgBins_, getRawHllEstimate());
    return (regressEst <= 0.0) ? getPoissonEstimate(bins_, empties_) : regressEst;
  }

  /**
   * Gets the estimate according to the Flajolet HLL paper where a switchover occurs to the Poisson
   * approximation algorithm at counts below <i>2.5*bins</i>. Used only for characterization and
   * testing.
   * @return the Flajolet estimate.
   */
  public double getFlajoletEstimate() {
    double hllEst = getRawHllEstimate();
    return ((empties_ > 0) && (hllEst <= (2.5 * bins_)))
        ? getPoissonEstimate(bins_, empties_)
        : hllEst;
  }

  /**
   * Gets the Hyper-Log Log Estimate without any switchover to any other algorithm. If the initial
   * cache is valid this returns a value of <i>alpha * bins</i>. Used by getEstimate(),
   * getFlajoletEstimate() and for characterization and testing.
   * @return the raw HLL estimate.
   */
  public double getRawHllEstimate() {
    if (cache_ != null) {
      return alpha_ * bins_;
    }
    double denom = 0.0;
    for (int j = bins_; j-- > 0;) {
      int lzP1 = binArr_[j]; //# of leading zeros+1
      double twoToTheLzP1 = 1L << lzP1;
      denom += 1.0 / twoToTheLzP1; //2^-lzP1
    }
    double harmonicMean = bins_ / denom;
    return alpha_ * bins_ * harmonicMean;
  }

  /**
   * Flajolet's paper recommends using the Poisson approximation equation for estimating uniques
   * where the number of uniques is less than <i>2.5*bins</i>. This equation is <i>bins *
   * ln(bins/empties)</i>, where <i>empties</i> is the number of empty bins. In this implementation
   * this estimator is used when the estimated number of uniques is less than <i>bins</i> and
   * greater than the size of the initial cache. This method will return a Double.POSITIVE_INFINITY
   * if empties == 0 and a zero if bins == empties, which is the case if the initial cache is
   * active.
   * @return the Poisson estimate.
   */
  public double getPoissonEstimate() {
    return getPoissonEstimate(bins_, empties_);
  }

  /**
   * Flajolet's paper recommends using the Poisson approximation equation for estimating uniques
   * where the number of uniques is less than <i>2.5*bins</i>. This equation is <i>bins *
   * ln(bins/empties)</i>, where <i>empties</i> is the number of empty bins. In this implementation
   * this estimator is used when the estimated number of uniques is less than <i>bins</i> and
   * greater than the size of the initial cache. This method will return a Double.POSITIVE_INFINITY
   * if empties == 0 and a zero if bins == empties, which is when the initial cache is active. This
   * is the private static target for the dynamic method.
   * @param bins the configured number of bins
   * @param empties the current number of empties.
   * @return the Poisson estimate.
   */
  private static double getPoissonEstimate(int bins, int empties) {
    return (bins == empties) ? 0.0 : bins * log(((double) bins) / empties);
  }

  /**
   * Gets the count of the very low range cache.
   * @return the cache count.
   */
  public double getCacheCount() {
    return (cache_ != null) ? (double) cache_.getCurrentCount() : 0;
  }

  /**
   * Gets the number of empty bins from the bin array. If the very low range cache is active this
   * will return empties = bins.
   * @return the number of empty bins.
   */
  public int getEmpties() {
    return empties_;
  }

  /**
   * Gets the configured <i>bins</i> value.
   * @return the number of configured bins.
   */
  public int getBins() {
    return bins_;
  }

  /**
   * Gets the configured <i>cacheSize</i> value.
   * @return the configured cache size.
   */
  public int getCacheSize() {
    return maxCacheSize_;
  }

  /**
   * Gets a deep clone of the internal bin array. Used for merging, characterization and testing.
   * @return a deep clone of the bin array.
   */
  public byte[] getBinArr() {
    return (binArr_ == null) ? null : binArr_.clone();
  }

  /**
   * Checks that <i>v</i> is within the given bounds. The <i>name</i> is used in the exception text
   * if it is thrown.
   * 
   * @param v Given value to test.
   * @param minValue the lower bound.
   * @param maxValue the upper bound.
   * @param name The name of the value.
   */
  public static void checkBounds(
      final int v, final int minValue, final int maxValue, final String name) {
    if ((v >= minValue) && (v <= maxValue)) {
      return;
    }
    throw new IllegalArgumentException(name + " must be >= " + minValue + " and <= " + maxValue
        + ": " + v);
  }

  static String right(String arg, int colwidth, char pad) {
    if (colwidth < 1) {
      return "";
    }
    String s = (arg == null) ? "" : arg;
    int slen = s.length();
    char[] buf = new char[colwidth];
    int pads = colwidth - slen;
    if (pads > 0) {
      s.getChars(0, slen, buf, pads);
      for (int i = pads; i-- > 0;) {
        buf[i] = pad;
      }
    } 
    else { //string is longer than colwidth
      pads = -pads;
      s.getChars(pads, slen, buf, 0);
    }
    return new String(buf);
  }
}
