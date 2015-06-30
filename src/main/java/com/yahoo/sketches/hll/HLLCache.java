/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.hll;

import static com.yahoo.sketches.Util.LS;
import static com.yahoo.sketches.Util.checkIfPowerOf2;

import java.io.Serializable;

/**
 * This is the early cache for the CountUniqueHLLSketch.
 * 
 * @author Lee Rhodes
 */
class HLLCache implements Cloneable, Serializable {
  private static final long serialVersionUID = 1L;
  private final int k_; //max cache array size
  //mutable values
  private long[] cacheArr_ = new long[1]; //value of zero 
  private int curCount_ = 0; //current count of values in cache

  /**
   * Constructor for a new HLLCache.
   * 
   * @param k The maximum size of the array
   */
  HLLCache(final int k) {
    k_ = k;
  }

  /**
   * Internal constructor used by test.
   * 
   * @param k The max size of the cache.
   * @param cacheArr Use to prepopulate the cache array.
   * @param curCount Use to set the current count.
   */
  HLLCache(final int k, long[] cacheArr, int curCount) {
    k_ = k;
    checkIfPowerOf2(k, "k");
    if (cacheArr == null) {
      cacheArr_ = new long[1];
      curCount_ = 0;
    } 
    else { //cacheArr is valid
      //k >= curArrSize >= curCount >= 0
      int curArrSize = cacheArr.length;
      checkIfPowerOf2(curArrSize, "cacheArr.length");
      if (curCount > 0) {
        if (curCount > curArrSize) {
          throw new IllegalArgumentException("Current Count can never be > Cache Array length");
        }
        if (curArrSize > k) {
          throw new IllegalArgumentException("Current Array size can never > k : " + curArrSize);
        }
      } 
      else if (curCount < 0) {
        throw new IllegalArgumentException("Current count can never be negative: " + curCount);
      }
      cacheArr_ = cacheArr;
      curCount_ = curCount;
    }
  }

  /**
   * Used by HLLSketch.
   */
  @Override
  public HLLCache clone() {
    HLLCache cache = null;
    try {
      cache = (HLLCache) super.clone();
    } 
    catch (CloneNotSupportedException e) {
      //should not happen
    }
    return cache;
  }

  /**
   * Used by HLLSketch.
   */
  @Override
  public String toString() {
    return toString(true);
  }

  /**
   * This version of toString enables pretty output using decimal or hex values. Decimal primarily
   * used in testing.
   * 
   * @param hexMode If false, output is decimal.
   * @return pretty output
   */
  public String toString(final boolean hexMode) {
    StringBuilder sb = new StringBuilder();
    sb
        .append("   Cache Limit         : ").append(k_).append(LS)
        .append("   Cache Current Size  : ").append(cacheArr_.length).append(LS)
        .append("   Cache Count         : ").append(curCount_).append(LS)
        .append("   Cache Array       : 0 to ").append(curCount_ - 1).append(" =").append(LS)
        .append("    {");
    String s;
    long v;
    for (int i = 0; i < curCount_; i++ ) {
      if ((i != 0) && ((i % 4) == 0)) {
        sb.append(LS).append("     ");
      }
      v = cacheArr_[i];
      s = (hexMode) ? Long.toHexString(v) : Long.toString(v);
      sb.append(s).append((i < (curCount_ - 1)) ? ", " : "}");
    }
    sb.append(LS);
    return sb.toString();
  }

  /**
   * Used by HLLSketch and testing.
   * 
   * @param that The other HLLCache
   * @return true if equal
   */
  public boolean equalTo(final HLLCache that) {
    //Check k
    if (this.k_ != that.k_) {
      return false;
    }
    //Check curCount
    if (this.curCount_ != that.curCount_) {
      return false;
    }

    long[] thisCacheArr = this.cacheArr_;
    long[] thatCacheArr = that.cacheArr_;
    //Check arr sizes
    int thisLen = thisCacheArr.length;
    if (thisLen != thatCacheArr.length) {
      return false;
    }
    //Check all contents
    for (int i = thisLen; i-- > 0;) {
      if (thisCacheArr[i] != thatCacheArr[i]) {
        return false;
      }
    }
    return true;
  }

  /**
   * Gets the current count of values currently in this cache. Used by HLLSketch.
   * @return the current count.
   */
  public int getCurrentCount() {
    return curCount_;
  }

  /**
   * Returns true if this cache is empty. Used by HLLSketch.
   * @return true if empty.
   */
  public boolean isEmpty() {
    return (curCount_ == 0);
  }

  /**
   * Gets approximate memory footprint size in bytes.<br>
   * Estimate: 16 + 2*4 +24 + 8*cacheArr.length. Used by HLLSketch.
   * @return memory size
   */
  public int getMemorySize() {
    return 16 + (2 * 4) + 24 + (8 * cacheArr_.length);
  }

  /**
   * Gets the <i>i</i>th entry from the cache. This does not check for out-of-bounds! Used by
   * HLLSketch.
   * 
   * @param i Index into cache array.
   * @return value at given index i.
   */
  public long get(final int i) {
    return cacheArr_[i];
  }

  /**
   * Updates this cache with candidate long value. Uses getInsertIndex() to compute the insertion
   * point and then modifies this cache state as necessary. This method assumes that 0 &le; curCount
   * &le; curArrSize &le; k. Used by HLLSketch.
   * 
   * @param x The candidate value to be inserted.
   * @return True if cache was modified
   */
  public boolean updateCache(final long x) {

    //Rule 1a: cannot accept a zero hash
    if (x == 0L) {
      return false;
    }

    int insertIdx = HLLCache.getInsertIndex(x, cacheArr_);

    //Rule 1c: discard duplicates
    if (insertIdx == -1) {
      return false; //duplicate
    }

    int curArrSize = cacheArr_.length;
    int curCount = curCount_;
    boolean arrFull = curCount == curArrSize;
    boolean curArrSzEqK = (curArrSize == k_);
    boolean xAboveTopValue = (insertIdx == curCount);
    boolean cacheMod = false;

    int sw = (arrFull ? 4 : 0) | (xAboveTopValue ? 2 : 0) | (curArrSzEqK ? 1 : 0);
    //Note: Case 5 and 7 should never happen as they are filtered out
    // by the HLLSketch.
    switch (sw) { //should compile to a tableswitch byte code
    //For these two cases, the cache is not full and the insert point
    //  is below the top value. This requires an internal move-up.
      case 0: //Rule 2: !arrFull, !xAboveTopValue, !curArrSzEqK
      case 1: { //Rule 2: !arrFull, !xAboveTopValue,  curArrSzEqK 
        System.arraycopy(cacheArr_, insertIdx, cacheArr_, insertIdx + 1, curCount - insertIdx);
      }

      //For these two cases, the cache is not full and the insert point 
      //  is available without a move-up.
      //$FALL-THROUGH$
      case 2: //Rule 2: !arrFull,  xAboveTopValue, !curArrSzEqK
      case 3: { //Rule 2: !arrFull,  xAboveTopValue,  curArrSzEqK
        cacheArr_[insertIdx] = x;
        curCount_++ ;
        cacheMod = true;
        break;
      }

      //For these two cases the cacheArr is full and has not yet reached k.
      // So it is safe to double the cache size and insert.
      case 4: //Rule 3:  arrFull, !xAboveTopValue, !curArrSzEqK
      case 6: { //Rule 3:  arrFull,  xAboveTopValue, !curArrSzEqK
        cacheArr_ = doubleAndInsert(x, insertIdx, curArrSize, cacheArr_);
        curCount_++ ;
        cacheMod = true;
        break;
      }
    } //end switch
    return cacheMod;
  }

  /**
   * <p>
   * Computes the insert array index based on an unsigned long input value and the given long[]
   * cache array, which must already be ordered as unsigned longs. The size of the cache array must
   * be a power of 2. The minimum cache size is 1, and its value may be zero. Values above the
   * largest value in the cache must be zero.
   * </p>
   * <p>
   * The performance of this algorithm is <i>O(lg(arraySize))</i>.
   * <ul>
   * <li>InsertIdx is:
   * <ul>
   * <li>-1 if a duplicate,</li>
   * <li>curArrSize if x &gt; curArrSize-1,</li>
   * <li>index of the lowest empty slot</li>
   * <li>index where insert needs to be made.</li>
   * </ul>
   * </li>
   * </ul>
   * 
   * @param x Input hash value used to compute the insert index. Cannot == 0.
   * @param cacheArr The given cache array
   * @return The insert index
   */
  public static int getInsertIndex(final long x, final long[] cacheArr) {
    final int curArrSz = cacheArr.length;
    int ptrPls1 = curArrSz; //search pointer start. Always idx +1.
    long curV = cacheArr[curArrSz - 1]; //start at top of cacheArr
    int comp = compareUnsignedZeroHigh(x, curV);
    if (comp == 0) {
      return -1; //duplicate
    }
    if (comp == 1) {
      return curArrSz; //x > top index value
    }
    boolean down = true; //x < curV (or curV == 0) 
    //println("Outer Down");
    int lgSkArrSz = Integer.numberOfTrailingZeros(curArrSz);
    for (int level = 1; level <= lgSkArrSz; level++ ) {
      if (down) {
        ptrPls1 -= (curArrSz >>> level);
      } 
      else {
        ptrPls1 += (curArrSz >>> level);
      }
      curV = cacheArr[ptrPls1 - 1];
      if (x == curV) {
        return -1; //duplicate
      }
      down = (compareUnsignedZeroHigh(x, curV) == -1);
      //if (down) println("Loop Down"); else println("Loop Up");
    }
    return (down) ? ptrPls1 - 1 : ptrPls1;
  }

  /**
   * Compares two unsigned longs, however, this treats a binary 0 as greater than all other values.
   * 
   * @param ulongA unsigned long A
   * @param ulongB unsigned long B
   * @return (ulongA &lt; ulongB)? -1: (ulongA &lt; ulongB)? 1 : 0
   */
  public static int compareUnsignedZeroHigh(final long ulongA, final long ulongB) {
    if (ulongA == ulongB) {
      return 0;
    }
    if (ulongA == 0L) {
      return 1;
    }
    if (ulongB == 0L) {
      return -1;
    }
    //compare first 63 bits
    long a = ulongA >>> 1;
    long b = ulongB >>> 1;
    if (a < b) {
      return -1;
    }
    if (a > b) {
      return 1;
    }
    //still here? it must be the last bit
    a = ulongA & 1L;
    b = ulongB & 1L;
    return (int) (a - b);
  }

  /**
   * The cache is full and must be doubled in size to accommodate the new value, which may not be a
   * duplicate.
   * 
   * @param x Input value. Cannot be zero.
   * @param insertIdx Must be &ge; 0.
   * @param curArrSz Source array size, which is full and equal to current count.
   * @param cacheArr The source array.
   * @return the new larger cache array.
   */
  private final static long[] doubleAndInsert(long x, int insertIdx, int curArrSz, long[] cacheArr) {
    long[] newCacheArr = new long[curArrSz * 2];
    int sw = (insertIdx == 0) ? 0 //Insert at 0
        : (insertIdx == curArrSz) ? 2 //Insert at top  
            : 1; //Insert in between
    switch (sw) {
      case 0: { //Insert at 0
        System.arraycopy(cacheArr, 0, newCacheArr, 1, curArrSz - insertIdx);
        break;
      }
      case 1: { //Insert in between
        System.arraycopy(cacheArr, 0, newCacheArr, 0, insertIdx);
        System.arraycopy(cacheArr, insertIdx, newCacheArr, insertIdx + 1, curArrSz - insertIdx);
        break;
      }
      case 2: { //Insert at top
        System.arraycopy(cacheArr, 0, newCacheArr, 0, insertIdx);
        break;
      }
    }
    newCacheArr[insertIdx] = x;
    return newCacheArr;
  }
}
