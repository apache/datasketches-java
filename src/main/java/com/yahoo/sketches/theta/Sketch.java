/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.theta;

import static com.yahoo.sketches.Family.idToFamily;
import static com.yahoo.sketches.Util.DEFAULT_UPDATE_SEED;
import static com.yahoo.sketches.Util.LS;
import static com.yahoo.sketches.Util.checkIfPowerOf2;
import static com.yahoo.sketches.Util.zeroPad;
import static com.yahoo.sketches.theta.PreambleUtil.COMPACT_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.FAMILY_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.FLAGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.MAX_THETA_LONG_AS_DOUBLE;
import static com.yahoo.sketches.theta.PreambleUtil.ORDERED_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.SER_VER_BYTE;

import com.yahoo.sketches.BinomialBounds;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.memory.Memory;

/**
 * The top-level class for all sketches. This class is never constructed directly. 
 * Use the UpdateSketch.builder() methods to create UpdateSketches.
 * 
 * @author Lee Rhodes 
 */
public abstract class Sketch {

  static final int DEFAULT_LG_RESIZE_FACTOR = 3;   //Unique to Heap
  
  Sketch() {}
  
  //Sketch, defined here with Javadocs
  
  /**
   * Gets the unique count estimate.
   * @return the sketch's best estimate of the cardinality of the input stream.
   */
  public double getEstimate() {
    return Sketch.estimate(getThetaLong(), getRetainedEntries(true), isEmpty());
  }
  
  /**
   * Gets the approximate lower error bound given the specified number of Standard Deviations. 
   * This will return getEstimate() if isEmpty() is true.
   * 
   * @param numStdDev 
   * <a href="{@docRoot}/resources/dictionary.html#numStdDev">See Number of Standard Deviations</a>
   * @return the lower bound.
   */
  public double getLowerBound(int numStdDev) {
    return Sketch.lowerBound(numStdDev, getThetaLong(), getRetainedEntries(true), isEmpty());
  }
  
  /**
   * Returns the number of entries that have been retained by the sketch.
   * @param valid if true, returns the number of valid entries, which are less than theta and used
   * for estimation.
   * Otherwise, return the number of all entries, valid or not, that are currently in the internal 
   * sketch cache.
   * @return the number of valid retained entries
   */
  public abstract int getRetainedEntries(boolean valid);
  
  /**
   * Gets the value of theta as a double with a value between zero and one
   * @return the value of theta as a double
   */
  public double getTheta() {
    return getThetaLong() / MAX_THETA_LONG_AS_DOUBLE;
  }
  
  /**
   * Gets the approximate upper error bound given the specified number of Standard Deviations. 
   * This will return getEstimate() if isEmpty() is true.
   * 
   * @param numStdDev
   * <a href="{@docRoot}/resources/dictionary.html#numStdDev">See Number of Standard Deviations</a>
   * @return the upper bound.
   */
  public double getUpperBound(int numStdDev) {
    return Sketch.upperBound(numStdDev, getThetaLong(), getRetainedEntries(true), isEmpty());
  }
  
  /**
   * <a href="{@docRoot}/resources/dictionary.html#empty">See Empty</a>
   * @return true if empty.
   */
  public abstract boolean isEmpty();
  
  /**
   * Returns true if the sketch is Estimation Mode (as opposed to Exact Mode).
   * This is true if theta &lt; 1.0 AND isEmpty() is false.
   * @return true if the sketch is in estimation mode.
   */
  public boolean isEstimationMode() {
    return Sketch.estMode(getThetaLong(), isEmpty());
  }
  
  /**
   * Serialize this sketch to a byte array form. 
   * @return byte array of this sketch
   */
  public abstract byte[] toByteArray();
  
  /**
   * Returns a human readable summary of the sketch.  This method is equivalent to the parameterized
   * call:<br>
   * <i>Sketch.toString(sketch, true, false, 8, true);</i>
   * @return summary
   */
  @Override
  public String toString() {
    return toString(true, false, 8, true);
  }
  
  /**
   * Gets a human readable listing of contents and summary of the given sketch. 
   * This can be a very long string.  If this sketch is in a "dirty" state there
   * may be values in the dataDetail view that are &ge; theta.
   * 
   * @param sketchSummary If true the sketch summary will be output at the end.
   * @param dataDetail If true, includes all valid hash values in the sketch.
   * @param width The number of columns of hash values. Default is 8.
   * @param hexMode If true, hashes will be output in hex.
   * @return The result string, which can be very long.
   */
  public String toString(boolean sketchSummary, boolean dataDetail, int width, boolean hexMode) {
    StringBuilder sb = new StringBuilder();
    
    long[] cache = getCache();
    int nomLongs = 0;
    int arrLongs = cache.length;
    long seed = 0;
    float p = 0;
    int rf = 0;
    //int preLongs = getPreambleLongs();
    boolean updateSketch = (this instanceof UpdateSketch);
    
    //boolean direct = isDirect();
    long thetaLong = this.getThetaLong();
    int curCount = this.getRetainedEntries(true);
    
    if (updateSketch) {
      UpdateSketch uis = (UpdateSketch)this;
      nomLongs = 1 << uis.getLgNomLongs();
      seed = uis.getSeed();
      arrLongs = 1 << uis.getLgArrLongs();
      p = uis.getP();
      rf = 1<<uis.getLgResizeFactor();
    } 
    
    if (dataDetail) {
      int w = (width > 0) ? width : 8; // default is 8 wide
      if (curCount > 0) {
        sb.append("### SKETCH DATA DETAIL");
        for (int i = 0, j = 0; i < arrLongs; i++ ) {
          long h;
          h = cache[i];
          if ((h <= 0) || (h >= thetaLong)) {
            continue;
          }
          if ((j % w) == 0) {
            sb.append(LS).append(String.format("   %6d", (j + 1)));
          }
          if (hexMode) {
            sb.append(" "+zeroPad(Long.toHexString(h), 16)+",");
          } 
          else {
            sb.append(String.format(" %20d,", h));
          }
          j++ ;
        }
        sb.append(LS).append("### END DATA DETAIL").append(LS + LS);
      }
    }
    
    if (sketchSummary) {
      double thetaDbl = thetaLong / MAX_THETA_LONG_AS_DOUBLE;
      String thetaHex = zeroPad(Long.toHexString(thetaLong), 16);
      String thisSimpleName = this.getClass().getSimpleName();
      short seedHash = this.getSeedHash();
      
      sb.append(LS);
      sb.append("### ").append(thisSimpleName).append(" SUMMARY: ").append(LS);
      if (updateSketch) sb.append("   Nominal Entries (k)     : ").append(nomLongs).append(LS);
      sb.append("   Estimate                : ").append(getEstimate()).append(LS);
      sb.append("   Upper Bound, 95% conf   : ").append(getUpperBound(2)).append(LS);
      sb.append("   Lower Bound, 95% conf   : ").append(getLowerBound(2)).append(LS);
      if (updateSketch) sb.append("   p                       : ").append(p).append(LS);
      sb.append("   Theta (double)          : ").append(thetaDbl).append(LS);
      sb.append("   Theta (long)            : ").append(thetaLong).append(LS);
      sb.append("   Theta (long, hex        : ").append(thetaHex).append(LS);
      sb.append("   EstMode?                : ").append(isEstimationMode()).append(LS);
      sb.append("   Empty?                  : ").append(isEmpty()).append(LS);
      if (updateSketch) sb.append("   Resize Factor           : ").append(rf).append(LS);
      sb.append("   Array Size Entries      : ").append(arrLongs).append(LS);
      sb.append("   Retained Entries        : ").append(curCount).append(LS);
      if (updateSketch) sb.append("   Update Seed             : ").append(Long.toString(seed)).append(LS);
      sb.append("   Seed Hash               : ").append(Integer.toHexString(seedHash)).append(LS);
      sb.append("### END SKETCH SUMMARY").append(LS);
  
    }
    return sb.toString();
  }
  
  //public static methods
  
  /**
   * Heapify takes the sketch image in Memory and instantiates an on-heap 
   * Sketch using the
   * <a href="{@docRoot}/resources/dictionary.html#defaultUpdateSeed">Default Update Seed</a>.
   * The resulting sketch will not retain any link to the source Memory. 
   * @param srcMem an image of a Sketch where the image seed hash matches the default seed hash.
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @return a Heap-based Sketch from the given Memory
   */
  public static Sketch heapify(Memory srcMem) {
    return heapify(srcMem, DEFAULT_UPDATE_SEED);
  }
  
  /**
   * Heapify takes the sketch image in Memory and instantiates an on-heap 
   * Sketch using the given seed.
   * The resulting sketch will not retain any link to the source Memory.
   * @param srcMem an image of a Sketch where the image seed hash matches the given seed hash.
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See Update Hash Seed</a>.
   * Compact sketches do not use the seed parameter.
   * @return a Heap-based Sketch from the given Memory
   */
  public static Sketch heapify(Memory srcMem, long seed) {
    int serVer = srcMem.getByte(SER_VER_BYTE);
    if (serVer == 3) {
      byte famID = srcMem.getByte(FAMILY_BYTE);
      boolean ordered = srcMem.isAnyBitsSet(FLAGS_BYTE, (byte) ORDERED_FLAG_MASK);
      return constructHeapSketch(famID, ordered, srcMem, seed);
    }
    if (serVer == 1) {
      return ForwardCompatibility.heapify1to3(srcMem, seed);
    }
    if (serVer == 2) {
      return ForwardCompatibility.heapify2to3(srcMem, seed);
    }
    throw new IllegalArgumentException("Unknown Serialization Version: "+serVer);
  }
  
  /**
   * Wrap takes the sketch image in Memory and refers to it directly. There is no data copying onto
   * the java heap.  Only "Direct" sketches that have been explicity stored as direct objects can
   * be wrapped.  This method assumes the 
   * <a href="{@docRoot}/resources/dictionary.html#defaultUpdateSeed">Default Update Seed</a>.
   * @param srcMem an image of a Sketch where the image seed hash matches the default seed hash.
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @return a Sketch backed by the given Memory
   */
  public static Sketch wrap(Memory srcMem) {
    return wrap(srcMem, DEFAULT_UPDATE_SEED);
  }

  /**
   * Wrap takes the sketch image in Memory and refers to it directly. There is no data copying onto
   * the java heap.  Only "Direct" sketches that have been explicity stored as direct objects can
   * be wrapped.
   * @param srcMem an image of a Sketch where the image seed hash matches the given seed hash.
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See Update Hash Seed</a>.
   * Compact sketches do not use the seed parameter.
   * @return a UpdateSketch backed by the given Memory
   */
  public static Sketch wrap(Memory srcMem, long seed) { 
    int serVer = srcMem.getByte(SER_VER_BYTE);
    if (serVer < 3) {
      throw new IllegalArgumentException("Sketch cannot wrap Serialization Versions 1 or 2.");
    }
    byte famID = srcMem.getByte(FAMILY_BYTE);
    boolean ordered = srcMem.isAnyBitsSet(FLAGS_BYTE, (byte) ORDERED_FLAG_MASK);
    return constructDirectSketch(famID, ordered, srcMem, seed);
  }
  
  //Sizing methods
  
  /**
   * Returns the number of storage bytes required for this Sketch in its current state.
   * @param compact if true, returns the bytes required for compact form. 
   * If this sketch is already in compact form this parameter is ignored.
   * @return the number of storage bytes required for this sketch
   */
  public int getCurrentBytes(boolean compact) {
    int preBytes = getCurrentPreambleLongs(compact) << 3;
    int dataBytes = getCurrentDataLongs(compact) << 3;
    return preBytes + dataBytes;
  }
  
  /**
   * Returns the maximum number of storage bytes required for a CompactSketch with the given  
   * number of actual entries.
   * @param numberOfEntries the actual number of entries stored with the CompactSketch. 
   * @return the maximum number of storage bytes required for a CompactSketch with the given number
   * of entries. 
   */
  public static int getMaxCompactSketchBytes(int numberOfEntries) {
    return (numberOfEntries << 3) + (Family.COMPACT.getMaxPreLongs() << 3);
  }
  
  /**
   * Returns the maximum number of storage bytes required for an UpdateSketch with the given  
   * number of nominal entries (power of 2).
   * @param nomEntries must be a power of 2 
   * @return the maximum number of storage bytes required for a UpdateSketch with the given 
   * nomEntries
   */
  public static int getMaxUpdateSketchBytes(int nomEntries) {
    checkIfPowerOf2(nomEntries,"nomEntries");
    return (nomEntries << 4) + (Family.QUICKSELECT.getMaxPreLongs() << 3);
  }
  
  /**
   * Returns the serialization version from the given Memory
   * @param mem the sketch Memory
   * @return the serialization version from the Memory
   */
  public static int getSerializationVersion(Memory mem) {
    return mem.getByte(SER_VER_BYTE);
  }
  
  /**
   * Returns true if this sketch is in compact form.
   * @return true if this sketch is in compact form.
   */
  public abstract boolean isCompact();
  
  /**
   * Returns true if internal cache is ordered
   * @return true if internal cache is ordered
   */
  public abstract boolean isOrdered();
  
  /**
   * Returns true if this sketch is Direct to Memory
   * @return true if this sektch is Direct to Memory
   */
  public abstract boolean isDirect();
  
  //Restricted methods
  //DATA
  
  final int getCurrentDataLongs(boolean compact) {
    int longs;
    if ((this instanceof CompactSketch) || compact) {
      longs = getRetainedEntries(true);
    } 
    else { //must be update sketch
      longs = (1 << ((UpdateSketch)this).getLgArrLongs());
    }
    return longs;
  }
  
  //PREAMBLE
  
  final int getCurrentPreambleLongs(boolean compact) {
    return compact? compactPreambleLongs(getThetaLong(), isEmpty()) : getPreambleLongs();
  }
  
  final static int compactPreambleLongs(long thetaLong, boolean empty) {
    return (thetaLong < Long.MAX_VALUE)? 3 : empty? 1 : 2;
  }
  
  //CHECK PREAMBLE
  
  /**
   * Returns preamble longs if stored in current state
   * @return preamble longs if stored in current state
   */
  abstract int getPreambleLongs();
  
  /**
   * Gets the 16-bit seed hash
   * @return the seed hash
   */
  abstract short getSeedHash();
  
  /**
   * Gets the value of theta as a long
   * @return the value of theta as a long
   */
  abstract long getThetaLong();
  
  /**
   * Gets the internal cache array.
   * @return the internal cache array.
   */
  abstract long[] getCache();
  
  /**
   * Gets the <a href="{@docRoot}/resources/dictionary.html#mem">Memory</a> 
   * if available, otherwise returns null.
   * @return the backing Memory or null.
   */
  abstract Memory getMemory();

  
  /**
   * Returns true if given Family id is one of the theta sketches
   * @param id the given Family id
   * @return true if given Family id is one of the theta sketches
   */
  static boolean isValidSketchID(int id) {
  return (id == Family.ALPHA.getID())       ||
         (id == Family.QUICKSELECT.getID()) ||
         (id == Family.COMPACT.getID());
  }

  static final boolean estMode(long thetaLong, boolean empty) {
    return (thetaLong < Long.MAX_VALUE) && !empty;
  }
  
  static final double estimate(long thetaLong, int curCount, boolean empty) {
    if (estMode(thetaLong, empty)) {
      double theta = thetaLong / MAX_THETA_LONG_AS_DOUBLE;
      return curCount / theta;
    } 
    return curCount;
  }
  
  static final double lowerBound(int numStdDev, long thetaLong, int curCount, boolean empty) {
    if ((numStdDev < 1) || (numStdDev > 3)) {
      throw new IllegalArgumentException("numStdDev can only be the values 1, 2 or 3: "+numStdDev);
    }
    double theta = thetaLong / MAX_THETA_LONG_AS_DOUBLE;
    return BinomialBounds.getLowerBound(curCount, theta, numStdDev, empty);
  }
  
  static final double upperBound(int numStdDev, long thetaLong, int curCount, boolean empty) {
    if  ((numStdDev < 1) || (numStdDev > 3))  {
      throw new IllegalArgumentException("numStdDev can only be the values 1, 2 or 3:"+numStdDev);
    }
    double theta = thetaLong / MAX_THETA_LONG_AS_DOUBLE;
    return BinomialBounds.getUpperBound(curCount, theta, numStdDev, empty);
  }
  
  /**
   * Instantiates a Direct Sketch that wraps Memory.
   * @param famID the Family ID
   * @param ordered true if the sketch is of the Compact family and ordered
   * @param srcMem <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See Update Hash Seed</a>. 
   * The seed required to instantiate a non-compact sketch.
   * @return a Sketch
   */
  private static final Sketch constructDirectSketch(byte famID, boolean ordered, Memory srcMem, long seed) {
    boolean compact = srcMem.isAnyBitsSet(FLAGS_BYTE, (byte) COMPACT_FLAG_MASK);
    Family family = idToFamily(famID);
    switch(family) {
      case QUICKSELECT: {
        return DirectQuickSelectSketch.getInstance(srcMem, seed);
      }
      case COMPACT: {
        if(!compact) {
          throw new IllegalArgumentException("Corrupted " + family + " image: must be compact");
        }
        return ordered ? new DirectCompactOrderedSketch(srcMem) : new DirectCompactSketch(srcMem);
      }
      default: {
        throw new IllegalArgumentException("Sketch cannot wrap family: " + family + " as a Sketch");
      }
    }
  }
  
  /**
   * Instantiates a Heap Sketch from Memory.
   * @param famID the Family ID
   * @param ordered true if the sketch is of the Compact family and ordered
   * @param srcMem <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See Update Hash Seed</a>. 
   * The seed required to instantiate a non-compact sketch.
   * @return a Sketch
   */
  private static final Sketch constructHeapSketch(byte famID, boolean ordered, Memory srcMem, long seed) {
    boolean compact = srcMem.isAnyBitsSet(FLAGS_BYTE, (byte) COMPACT_FLAG_MASK);
    Family family = idToFamily(famID);
    switch(family) {
      case ALPHA: {
        if (compact) {
          throw new IllegalArgumentException("Corrupted " + family + " image: cannot be compact");
        }
        return new HeapAlphaSketch(srcMem, seed);
      }
      case QUICKSELECT: {
        if (compact) {
          throw new IllegalArgumentException("Corrupted " + family + " image: cannot be compact");
        }
        return new HeapQuickSelectSketch(srcMem, seed);
      }
      case COMPACT: {
        if(!compact) {
          throw new IllegalArgumentException("Corrupted " + family + " image: must be compact");
        }
        return ordered ? new HeapCompactOrderedSketch(srcMem) : new HeapCompactSketch(srcMem);
      }
      default: {
        throw new IllegalArgumentException("Sketch cannot heapify family: " + family + " as a Sketch");
      }
    }
  }
  
}
