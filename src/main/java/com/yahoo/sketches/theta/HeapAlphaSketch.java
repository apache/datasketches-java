/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.theta;

import static com.yahoo.sketches.HashOperations.STRIDE_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.BIG_ENDIAN_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.COMPACT_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.EMPTY_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.MAX_THETA_LONG_AS_DOUBLE;
import static com.yahoo.sketches.theta.PreambleUtil.ORDERED_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.READ_ONLY_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.SER_VER;
import static com.yahoo.sketches.theta.PreambleUtil.extractCurCount;
import static com.yahoo.sketches.theta.PreambleUtil.extractFamilyID;
import static com.yahoo.sketches.theta.PreambleUtil.extractFlags;
import static com.yahoo.sketches.theta.PreambleUtil.extractLgArrLongs;
import static com.yahoo.sketches.theta.PreambleUtil.extractLgNomLongs;
import static com.yahoo.sketches.theta.PreambleUtil.extractP;
import static com.yahoo.sketches.theta.PreambleUtil.extractPreLongs;
import static com.yahoo.sketches.theta.PreambleUtil.extractResizeFactor;
import static com.yahoo.sketches.theta.PreambleUtil.extractSeedHash;
import static com.yahoo.sketches.theta.PreambleUtil.extractSerVer;
import static com.yahoo.sketches.theta.PreambleUtil.getMemBytes;
import static com.yahoo.sketches.theta.UpdateReturnState.InsertedCountIncremented;
import static com.yahoo.sketches.theta.UpdateReturnState.InsertedCountNotIncremented;
import static com.yahoo.sketches.theta.UpdateReturnState.RejectedDuplicate;
import static com.yahoo.sketches.theta.UpdateReturnState.RejectedOverTheta;
import static com.yahoo.sketches.Util.*;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.sqrt;

import com.yahoo.sketches.Family;
import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.HashOperations;
import com.yahoo.sketches.ResizeFactor;
import com.yahoo.sketches.Util;

/**
 * This sketch uses the 
 * <a href="{@docRoot}/resources/dictionary.html#thetaSketch">Theta Sketch Framework</a>
 * and the 
 * <a href="{@docRoot}/resources/dictionary.html#alphaTCF">Alpha TCF</a>
 * with a single cache.
 * 
 * @author Lee Rhodes
 * @author Kevin Lang
 */
class HeapAlphaSketch extends HeapUpdateSketch {
  private static final int ALPHA_MIN_LG_NOM_LONGS = 9; //The smallest Log2 nom entries allowed => 512.
  private final double alpha_;  // computed from lgNomLongs
  private final long split1_;   // computed from alpha and p
  
  private int lgArrLongs_;
  private int hashTableThreshold_;  //never serialized
  private int curCount_ = 0;
  private long thetaLong_;
  private boolean empty_ = true;
  
  private long[] cache_;
  private boolean dirty_ = false;
  
  private HeapAlphaSketch(int lgNomLongs, long seed, float p, ResizeFactor rf, 
      double alpha, long split1) {
    super(lgNomLongs, seed, p, rf);
    alpha_ = alpha;
    split1_ = split1;
  }
  
  /**
   * Get a new sketch instance on the java heap. 
   * 
   * @param lgNomLongs <a href="{@docRoot}/resources/dictionary.html#lgNomLongs">See lgNomLongs</a>
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See Update Hash Seed</a>
   * @param p <a href="{@docRoot}/resources/dictionary.html#p">See Sampling Probability, <i>p</i></a>
   * @param rf <a href="{@docRoot}/resources/dictionary.html#resizeFactor">See Resize Factor</a>
   * @return instance of this sketch
   */
  static HeapAlphaSketch getInstance(int lgNomLongs, long seed, float p, ResizeFactor rf) {
    
    if (lgNomLongs < ALPHA_MIN_LG_NOM_LONGS) throw new IllegalArgumentException(
        "This sketch requires a minimum nominal entries of "+(1 << ALPHA_MIN_LG_NOM_LONGS));
    
    double nomLongs = (1L << lgNomLongs);
    double alpha = nomLongs / (nomLongs + 1.0);
    long split1 = (long) ((p * (alpha + 1.0)/2.0) * MAX_THETA_LONG_AS_DOUBLE);
    
    HeapAlphaSketch has = new HeapAlphaSketch(lgNomLongs, seed, p, rf, alpha, split1);
    
    int lgArrLongs = startingSubMultiple(lgNomLongs+1, rf, MIN_LG_ARR_LONGS);
    has.lgArrLongs_ = lgArrLongs;
    has.hashTableThreshold_ = setHashTableThreshold(lgNomLongs, lgArrLongs);
    has.curCount_ = 0; 
    has.thetaLong_ = (long)(p * MAX_THETA_LONG_AS_DOUBLE);
    has.empty_ = true; //other flags: bigEndian = readOnly = compact = ordered = false; 
    has.cache_ = new long[1 << lgArrLongs];
    return has;
  }
  
  /**
   * Heapify a sketch from a Memory object containing sketch data. 
   * @param srcMem The source Memory object.
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   * @return instance of this sketch
   */
  static HeapAlphaSketch getInstance(Memory srcMem, long seed) {
    long[] preArr = new long[3];
    srcMem.getLongArray(0, preArr, 0, 3); //extract the preamble
    long long0 = preArr[0];
    int preambleLongs = extractPreLongs(long0);                           //byte 0
    ResizeFactor myRF = ResizeFactor.getRF(extractResizeFactor(long0));   //byte 0
    int serVer = extractSerVer(long0);                                    //byte 1
    int familyID = extractFamilyID(long0);                                //byte 2
    int lgNomLongs = extractLgNomLongs(long0);                            //byte 3
    int lgArrLongs = extractLgArrLongs(long0);                            //byte 4
    int flags = extractFlags(long0);                                      //byte 5
    short seedHash = (short)extractSeedHash(long0);                       //byte 6,7
    long long1 = preArr[1];
    int curCount = extractCurCount(long1);                                //bytes 8-11
    float p = extractP(long1);                                            //bytes 12-15
    long thetaLong = preArr[2];                                           //bytes 16-23
    
    Family family = Family.idToFamily(familyID);
    if (family.equals(Family.ALPHA)) {
      if (preambleLongs != Family.ALPHA.getMinPreLongs()) {
        throw new IllegalArgumentException(
            "Possible corruption: Invalid PreambleLongs value for ALPHA: " +preambleLongs);
      }
    }
    else {
      throw new IllegalArgumentException(
          "Possible corruption: Invalid Family: " + family.toString());
    }
    
    if (serVer != SER_VER) {
      throw new IllegalArgumentException(
          "Possible corruption: Invalid Serialization Version: "+serVer);
    }
    
    int flagsMask = ORDERED_FLAG_MASK | COMPACT_FLAG_MASK | READ_ONLY_FLAG_MASK | BIG_ENDIAN_FLAG_MASK;
    if ((flags & flagsMask) > 0) {
      throw new IllegalArgumentException(
          "Possible corruption: Input srcMem cannot be: big-endian, compact, ordered, or read-only");
    }
    
    Util.checkSeedHashes(seedHash, Util.computeSeedHash(seed));
    
    long curCapBytes = srcMem.getCapacity();
    int minReqBytes = getMemBytes(lgArrLongs, preambleLongs);
    if (curCapBytes < minReqBytes) {
      throw new IllegalArgumentException(
          "Possible corruption: Current Memory size < min required size: " + 
              curCapBytes + " < " + minReqBytes);
    }
    
    double theta = thetaLong/MAX_THETA_LONG_AS_DOUBLE;
    if ((lgArrLongs <= lgNomLongs) && (theta < p) ) {
      throw new IllegalArgumentException(
        "Possible corruption: Theta cannot be < p and lgArrLongs <= lgNomLongs. "+
            lgArrLongs + " <= " + lgNomLongs + ", Theta: "+theta + ", p: " + p);
    }
    
    double nomLongs = (1L << lgNomLongs);
    double alpha = nomLongs / (nomLongs + 1.0);
    long split1 = (long) ((p * (alpha + 1.0)/2.0) * MAX_THETA_LONG_AS_DOUBLE);
    
    HeapAlphaSketch has = new HeapAlphaSketch(lgNomLongs, seed, p, myRF, alpha, split1);
    has.lgArrLongs_ = lgArrLongs;
    has.hashTableThreshold_ = setHashTableThreshold(lgNomLongs, lgArrLongs);
    has.curCount_ = curCount;
    has.thetaLong_ = thetaLong;
    has.empty_ = (flags & EMPTY_FLAG_MASK) > 0;
    has.cache_ = new long[1 << lgArrLongs];
    srcMem.getLongArray(preambleLongs << 3, has.cache_, 0, 1 << lgArrLongs);  //read in as hash table
    return has;
  }
  
  //Sketch
  
  @Override
  public double getEstimate() {
    if (isEstimationMode()) {
      int curCount = getRetainedEntries(true);
      double theta = getTheta();
      return (thetaLong_ > split1_)? curCount / theta : (1 << lgNomLongs_) / theta;      
    } 
    return curCount_;
  }
  
  @Override
  public double getLowerBound(int numStdDev) {
    if ((numStdDev < 1) || (numStdDev > 3)) { 
      throw new IllegalArgumentException("numStdDev can only be the values 1, 2 or 3.");
    }
    double lb;
    if (isEstimationMode()) {
      int validCount = getRetainedEntries(true);
      if (validCount > 0) {
        double est = getEstimate();
        double var = getVariance(1<<lgNomLongs_, getP(), alpha_, getTheta(), validCount);
        lb = est - numStdDev * sqrt(var);
        lb = max(lb, 0.0);
      } 
      else {
        lb = 0.0; 
      }
    } 
    else {
      lb = curCount_;
    }
    return lb;
  }
  
  @Override
  public int getRetainedEntries(boolean valid) {
    if (curCount_ > 0) {
      if (valid && isDirty()) {
        int curCount = HashOperations.countPart(getCache(), getLgArrLongs(), getThetaLong());
        return curCount;
      }
    }
    return curCount_;
  }
  
  @Override
  public double getUpperBound(int numStdDev) {
    if ((numStdDev < 1) || (numStdDev > 3)) { 
      throw new IllegalArgumentException("numStdDev can only be the values 1, 2 or 3.");
    }
    if (isEstimationMode()) {
      double var = getVariance(1<<lgNomLongs_, getP(), alpha_, getTheta(), getRetainedEntries(true));
      return getEstimate() + numStdDev * sqrt(var);
    }
    return curCount_;
  }
  
  @Override
  public boolean isEmpty() {
    return empty_;
  }
  
  @Override
  public byte[] toByteArray() {
    return toByteArray(Family.ALPHA.getMinPreLongs(), (byte) Family.ALPHA.getID());
  }
  
  @Override
  public Family getFamily() {
    return Family.ALPHA;
  }
  
  //UpdateSketch
  
  @Override
  public UpdateSketch rebuild() {
    if (isDirty()) {
      rebuildDirty();
    }
    return this;
  }
  
  @Override
  public final void reset() {
    int lgArrLongs = startingSubMultiple(lgNomLongs_+1, getResizeFactor(), MIN_LG_ARR_LONGS);
    if (lgArrLongs == lgArrLongs_) {
      int arrLongs = cache_.length;
      assert (1 << lgArrLongs_) == arrLongs; 
      java.util.Arrays.fill(cache_, 0L);
    } 
    else {
      cache_ = new long[1 << lgArrLongs];
      lgArrLongs_ = lgArrLongs;
    }
    hashTableThreshold_ = setHashTableThreshold(lgNomLongs_, lgArrLongs_);
    empty_ = true;
    curCount_ = 0;
    thetaLong_ =  (long)(getP() * MAX_THETA_LONG_AS_DOUBLE);
    dirty_ = false;
  }  
  
  //restricted methods
  
  @Override
  int getPreambleLongs() {
    return Family.ALPHA.getMinPreLongs();
  }
  
  //SetArgument "interface"
  
  @Override
  long getThetaLong() {
    return thetaLong_;
  }
  
  @Override
  boolean isDirty() {
    return dirty_;
  }
  
  @Override
  long[] getCache() {
    return cache_;
  }
  
  //UpdateInternal interface
  
  @Override
  int getLgArrLongs() {
    return lgArrLongs_;
  }
  
  @Override
  UpdateReturnState hashUpdate(long hash) {
    HashOperations.checkHashCorruption(hash);
    empty_ = false;
    
    //The over-theta test
    if (HashOperations.continueCondition(thetaLong_, hash)) { 
      // very very unlikely that hash == Long.MAX_VALUE. It is ignored just as zero is ignored.
      return RejectedOverTheta; //signal that hash was rejected due to theta. 
    }
    
    //The duplicate/inserted tests
    if (dirty_) { //may have dirty values, must be at tgt size
      return enhancedHashInsert(cache_, hash);
    } 
    
    //NOT dirty, the other duplicate or inserted test
    if (HashOperations.hashSearchOrInsert(cache_, lgArrLongs_, hash) >= 0) {
      return UpdateReturnState.RejectedDuplicate;
    }
    //insertion occurred, must increment
    curCount_++;
    int r = (thetaLong_ > split1_)? 0 : 1;  //are we in sketch mode? (i.e., seen k+1 inserts?)
    if (r == 0) { //not yet sketch mode (has not seen k+1 inserts), but could be sampling
      if (curCount_ > (1<<lgNomLongs_)) { // > k
        //Reached the k+1 insert. Must be at tgt size or larger.
        //Transition to Sketch Mode. Happens only once.
        //Decrement theta, make dirty, don't bother check size, already not-empty.
        thetaLong_ = (long) (thetaLong_ * alpha_);
        dirty_ = true; //now may have dirty values
      } 
      else { 
        //inserts (not entries!) <= k. It may not be at tgt size.
        //Check size, don't decrement theta. cnt already ++, empty_ already false;
        if (curCount_ > hashTableThreshold_) {
          resizeClean(); //not dirty, not at tgt size.
        }
      }
    } 
    else { //r > 0: sketch mode and not dirty (e.g., after a rebuild).
      //dec theta, make dirty, cnt already ++, must be at tgt size or larger. check for rebuild
      assert (lgArrLongs_ > lgNomLongs_) : "lgArr: " + lgArrLongs_ + ", lgNom: " + lgNomLongs_;
      thetaLong_ = (long) (thetaLong_ * alpha_); //decrement theta
      dirty_ = true; //now may have dirty values
      if (curCount_ > hashTableThreshold_) {
        rebuildDirty(); // at tgt size and maybe dirty
      }
    }
    return UpdateReturnState.InsertedCountIncremented;
  }
  
  /**
   * Enhanced Knuth-style Open Addressing, Double Hash insert. 
   * The insertion process will overwrite an already existing, dirty (over-theta) value if one is 
   * found in the search.  
   * If an empty cell is found first, it will be inserted normally.  
   * 
   * @param hashTable the hash table to insert into
   * @param hash must not be 0. If not a duplicate, it will be inserted into the hash array
   * @return <a href="{@docRoot}/resources/dictionary.html#updateReturnState">See Update Return State</a>
   */
  private final UpdateReturnState enhancedHashInsert(long[] hashTable, long hash) {
    int arrayMask = (1 << lgArrLongs_) - 1; // arrayLongs -1
    // make odd and independent of curProbe:
    int stride = (2 * (int) ((hash >> lgArrLongs_) & STRIDE_MASK)) + 1;
    int curProbe = (int) (hash & arrayMask);
    long curTableHash = hashTable[curProbe];
    
    // This is the enhanced part
    // Search for duplicate or zero, or opportunity to replace garbage.
    while ((curTableHash != hash) && (curTableHash != 0)) {
      // curHash is not a duplicate and not zero
      
      if (curTableHash >= thetaLong_) { // curTableHash is garbage, do enhanced insert
        int rememberPos = curProbe; // remember its position.
        // Now we must make sure there are no duplicates in this search path, 
        //   so we keep searching
        curProbe = (curProbe + stride) & arrayMask; // move forward
        curTableHash = hashTable[curProbe];
        while ((curTableHash != hash) && (curTableHash != 0)) {
          curProbe = (curProbe + stride) & arrayMask;
          curTableHash = hashTable[curProbe];
        }
        // curTableHash is a duplicate or zero
        if (curTableHash == hash) {
          return RejectedDuplicate; // duplicate, just return
        }
        assert (curTableHash == 0); // must be zero
        // Now that we know there are no duplicates we can
        // go back and insert at first garbage value position
        hashTable[rememberPos] = hash;
        thetaLong_ = (long) (thetaLong_ * alpha_); //decrement theta
        dirty_ = true; //the decremented theta could have produced a new dirty value
        return InsertedCountNotIncremented;
      }
      
      // curTableHash was not a duplicate, not zero, and NOT garbage,
      // so we keep searching
      assert (curTableHash < thetaLong_);
      curProbe = (curProbe + stride) & arrayMask;
      curTableHash = hashTable[curProbe];
      // end of Enhanced insert
    } // end while and search
    
    // curTableHash is a duplicate or zero and NOT garbage
    if (curTableHash == hash) {
      return RejectedDuplicate; // duplicate, just return
    }
    // must be zero, so insert and increment
    assert (curTableHash == 0);
    hashTable[curProbe] = hash;
    thetaLong_ = (long) (thetaLong_ * alpha_); //decrement theta
    dirty_ = true; //the decremented theta could have produced a new dirty value
    if (++curCount_ > hashTableThreshold_) {
      rebuildDirty(); //at tgt size and maybe dirty
    }
    return InsertedCountIncremented;
  }
  
  //At tgt size or greater
  //Checks for rare lockup condition
  // Used by hashUpdate(), rebuild()
  private final void rebuildDirty() {
    int curCountBefore = curCount_;
    forceRebuildDirtyCache(); //changes curCount_ only
    if (curCountBefore == curCount_) {
      //clean but unsuccessful at reducing count, must take drastic measures, very rare.
      forceResizeCleanCache(1);
    }
  }
  
  //curCount > hashTableThreshold
  //Checks for rare lockup condition
  // Used by hashUpdate()
  private final void resizeClean() {
    //must resize, but are we at tgt size?
    int lgTgtLongs = lgNomLongs_ + 1;
    if (lgTgtLongs > lgArrLongs_) {
      //not yet at tgt size
      ResizeFactor rf = getResizeFactor();
      int lgDeltaLongs = lgTgtLongs - lgArrLongs_; //must be > 0
      int lgResizeFactor = max(min(rf.lg(), lgDeltaLongs), 1); //rf_.lg() could be 0
      forceResizeCleanCache(lgResizeFactor); 
    } 
    else {
      //at tgt size or larger, no dirty values, must take drastic measures, very rare.
      forceResizeCleanCache(1);
    }
  }
  
  //Force resize. Changes lgArrLongs_ only. Theta doesn't change, count doesn't change.
  // Used by rebuildDirty(), resizeClean()
  private final void forceResizeCleanCache(int lgResizeFactor) {
    assert (!dirty_); // Should never be dirty before a resize.
    lgArrLongs_ += lgResizeFactor; // new tgt size
    long[] tgtArr = new long[1 << lgArrLongs_];
    int newCount = HashOperations.hashArrayInsert(cache_, tgtArr, lgArrLongs_, thetaLong_);
    assert (curCount_ == newCount);
    curCount_ = newCount;
    cache_ = tgtArr;
    hashTableThreshold_ = setHashTableThreshold(lgNomLongs_, lgArrLongs_);
  }
  
  //Cache stays the same size. Must be dirty. Theta doesn't change, count will change.
  // Used by rebuildDirtyAtTgtSize()
  private final void forceRebuildDirtyCache() {
    long[] tgtArr = new long[1 << lgArrLongs_];
    curCount_ = HashOperations.hashArrayInsert(cache_, tgtArr, lgArrLongs_, thetaLong_);
    cache_ = tgtArr;
    dirty_ = false;
    //hashTableThreshold stays the same
  }
  
  // @formatter:off
  /**
   * Computes an estimate of the error variance based on Historic Inverse Probability (HIP)
   * estimators.  See Cohen: All-Distances Sketches, Revisited: HIP Estimators for Massive Graph
   * Analysis, Nov 2014.
   * <pre>
   * Table of sketch states and how Upper and Lower Bounds are computed
   * 
   * Theta P    Count  Empty  EstMode Est   UB  LB   Comments
   * 1.0   1.0  0      T      F       0     0   0    Empty Sketch-mode only sketch
   * 1.0   1.0  N      F      F       N     N   N    Degenrate Sketch-mode only sketch
   * &lt;1.0  1.0  -      F      T       est   HIP HIP  Normal Sketch-mode only sketch
   *  P    &lt;1.0 0      T      F       0     0   0    Virgin sampling sketch
   *  P    &lt;1.0 N      F      T       est   HIP HIP  Degenerate sampling sketch
   *  &lt;P   &lt;1.0 N      F      T       est   HIP HIP  Sampling sketch also in sketch-mode
   * </pre>
   * @param k alias for nominal entries.
   * @param p <a href="{@docRoot}/resources/dictionary.html#p">See Sampling Probability, <i>p</i></a>.
   * @param alpha the value of alpha for this sketch
   * @param theta <a href="{@docRoot}/resources/dictionary.html#theta">See <i>theta</i></a>.
   * @param count the current valid count.
   * @return the variance.
   */
  // @formatter:on
  private static final double getVariance(double k, double p, double alpha, double theta, 
      int count) {
    double kPlus1 = k+1.0;
    double y = 1.0/p;
    double ySq = y*y;
    double ySqMinusY = ySq - y;
    int r = getR(theta, alpha, p);
    double result;
    if (r == 0) {
      result = count*ySqMinusY;
    } 
    else if (r == 1) {
      result = kPlus1*ySqMinusY; //term1
    } 
    else { //r > 1
      double b = 1.0/alpha;
      double bSq = b*b;
      double x = p/theta;
      double xSq = x*x;
      double term1 = kPlus1*ySqMinusY;
      double term2 = y/(1.0 - bSq);
      double term3 = (y*bSq - y*xSq - b - bSq + x + x*b);
      result = term1 + term2 * term3;
    }
    double term4 = (1-theta)/(theta*theta);
    return result + term4;
  }
  
  /**
   * Computes whether there have been 0, 1, or 2 or more actual insertions into the cache in a 
   * numerically safe way. 
   * @param theta <a href="{@docRoot}/resources/dictionary.html#theta">See Theta</a>.
   * @param alpha internal computed value alpha.
   * @param p <a href="{@docRoot}/resources/dictionary.html#p">See Sampling Probability, <i>p</i></a>.
   * @return R.
   */
  private static final int getR(double theta, double alpha, double p) {
    double split1 = p * (alpha + 1.0)/2.0;
    if (theta > split1) return 0;
    if (theta > (alpha * split1)) return 1;
    return 2;
  }
  
  /**
   * Returns the cardinality limit given the current size of the hash table array.
   * 
   * @param lgNomLongs <a href="{@docRoot}/resources/dictionary.html#lgNomLongs">See lgNomLongs</a>.
   * @param lgArrLongs <a href="{@docRoot}/resources/dictionary.html#lgArrLongs">See lgArrLongs</a>.
   * @return the hash table threshold
   */
  private static final int setHashTableThreshold(final int lgNomLongs, final int lgArrLongs) {
    double fraction = (lgArrLongs <= lgNomLongs) ? RESIZE_THRESHOLD : REBUILD_THRESHOLD;    
    return (int) Math.floor(fraction * (1 << lgArrLongs));
  }

}
