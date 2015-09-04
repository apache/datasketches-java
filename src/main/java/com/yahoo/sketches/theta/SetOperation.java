/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.theta;

import static com.yahoo.sketches.Family.idToFamily;
import static com.yahoo.sketches.Util.DEFAULT_NOMINAL_ENTRIES;
import static com.yahoo.sketches.Util.DEFAULT_UPDATE_SEED;
import static com.yahoo.sketches.Util.ceilingPowerOf2;
import static com.yahoo.sketches.Util.checkIfPowerOf2;
import static com.yahoo.sketches.theta.PreambleUtil.FAMILY_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.SER_VER_BYTE;
import static java.lang.Math.max;

import com.yahoo.sketches.Family;
import com.yahoo.sketches.memory.Memory;

/**
 * The parent API for all Set Operations
 * 
 * @author Lee Rhodes
 */
public class SetOperation {
  static final int MIN_LG_ARR_LONGS = 5;
  public static final int MIN_LG_NOM_LONGS = 4;
  static final double RESIZE_THRESHOLD = 15.0/16.0;
  static final int CONST_PREAMBLE_LONGS = 3;
  
  SetOperation() {}
  
  /**
   * Makes a new builder
   *
   * @return a new builder
   */
  public static final Builder builder() {
    return new Builder();
  }
  
  /**
   * For building a new SetOperation.
   */
  public static class Builder {
    private int bLgNomLongs;
    private long bSeed;
    private float bP;
    private ResizeFactor bRF;
    private Memory bMem;
    
    Builder() {
      bLgNomLongs = Integer.numberOfTrailingZeros(DEFAULT_NOMINAL_ENTRIES);
      bSeed = DEFAULT_UPDATE_SEED;
      bP = (float) 1.0;
      bRF = ResizeFactor.X8;
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
     * Sets the upfront uniform sampling probability, <i>p</i>. Although this functionality is
     * implemented for Unions only, it rarely makes sense to use it. The proper use of upfront
     * sampling is when building the sketches.
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
     * Sets the backing Memory store. 
     * @param mem  <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
     * @return this Builder
     */
    public Builder setMemory(Memory mem) {
      bMem = mem;
      return this;
    }
    
    /**
     * Returns a configured SetOperation with the given
     * <a href="{@docRoot}/resources/dictionary.html#defaultNomEntries">Default Nominal Entries</a>
     * @param family the chosen SetOperation family
     * @return an SetOperation
     */
    public SetOperation build(Family family) {
      return build(DEFAULT_NOMINAL_ENTRIES, family);
    }  
    
    /**
     * Returns a configured SetOperation with the given
     * <a href="{@docRoot}/resources/dictionary.html#nomEntries">Nominal Entries</a> and Family.
     * @param nomEntries <a href="{@docRoot}/resources/dictionary.html#nomEntries">Nominal Entres</a>
     * @param family build this SetOperation family
     * @return a SetOperation
     */
    public SetOperation build(int nomEntries, Family family) {
      checkIfPowerOf2(nomEntries, "nomEntries");
      bLgNomLongs = Integer.numberOfTrailingZeros(nomEntries);
      SetOperation setOp = null;
      switch (family) {
        case UNION: {
          if (bMem == null) {
            setOp = new HeapUnion(bLgNomLongs, bSeed, bP, bRF);
          } 
          else {
            setOp = new DirectUnion(bLgNomLongs, bSeed, bP, bMem);
          }
          break;
        }
        case INTERSECTION: {
          if (bMem == null) {
            setOp = new HeapIntersection(bLgNomLongs, bSeed);
          } 
          else {
            setOp = new DirectIntersection(bLgNomLongs, bSeed, bMem);
          }
          break;
        }
        case A_NOT_B: {
          if (bMem == null) {
            setOp = new HeapAnotB(bSeed);
          } 
          else throw new IllegalArgumentException(
              "AnotB is a stateless operation and cannot be persisted.");
          break;
        }
        default: 
          throw new IllegalArgumentException(
              "Given Family cannot be built as a SetOperation: "+family.toString());
      }
      return setOp;
    }

    /**
     * Convenience method, returns a configured SetOperation Union with
     * <a href="{@docRoot}/resources/dictionary.html#defaultNomEntries">Default Nominal Entries</a>
     * @return a Union object
     */
    public Union buildUnion() {
      return (Union) build(Family.UNION);
    }

    /**
     * Convenience method, returns a configured SetOperation Union with the given
     * <a href="{@docRoot}/resources/dictionary.html#nomEntries">Nominal Entries</a>.
     * @param nomEntries <a href="{@docRoot}/resources/dictionary.html#nomEntries">Nominal Entres</a>
     * @return a Union object
     */
    public Union buildUnion(int nomEntries) {
      return (Union) build(nomEntries, Family.UNION);
    }

    /**
     * Convenience method, returns a configured SetOperation Intersection with
     * <a href="{@docRoot}/resources/dictionary.html#defaultNomEntries">Default Nominal Entries</a>
     * @return an Intersection object
     */
    public Intersection buildIntersection() {
      return (Intersection) build(Family.INTERSECTION);
    }

    /**
     * Convenience method, returns a configured SetOperation Intersection with the given
     * <a href="{@docRoot}/resources/dictionary.html#nomEntries">Nominal Entries</a>.
     * @param nomEntries <a href="{@docRoot}/resources/dictionary.html#nomEntries">Nominal Entres</a>
     * @return an Intersection object
     */
    public Intersection buildIntersection(int nomEntries) {
      return (Intersection) build(nomEntries, Family.INTERSECTION);
    }

    /**
     * Convenience method, returns a configured SetOperation ANotB with
     * <a href="{@docRoot}/resources/dictionary.html#defaultNomEntries">Default Nominal Entries</a>
     * @return an ANotB object
     */
    public AnotB buildANotB() {
      return (AnotB) build(Family.A_NOT_B);
    }

    /**
     * Convenience method, returns a configured SetOperation ANotB with the given
     * <a href="{@docRoot}/resources/dictionary.html#nomEntries">Nominal Entries</a>.
     * @param nomEntries <a href="{@docRoot}/resources/dictionary.html#nomEntries">Nominal Entres</a>
     * @return an ANotB object
     */
    public AnotB buildANotB(int nomEntries) {
      return (AnotB) build(nomEntries, Family.A_NOT_B);
    }
  }
  
  /**
   * Heapify takes the SetOperations image in Memory and instantiates an on-heap 
   * SetOperation using the
   * <a href="{@docRoot}/resources/dictionary.html#defaultUpdateSeed">Default Update Seed</a>.
   * The resulting SetOperation will not retain any link to the source Memory. 
   * @param srcMem an image of a SetOperation.
   * @return a Heap-based Sketch from the given Memory
   * @throws IllegalArgumentException if given image does not match a known
   * Sketch implementation, or if the assumed default seed does not match the image seed hash.
   */
  public static SetOperation heapify(Memory srcMem) {
    return heapify(srcMem, DEFAULT_UPDATE_SEED);
  }

  /**
   * Heapify takes the SetOperation image in Memory and instantiates an on-heap 
   * SetOperation using the 
   * given seed.
   * The resulting SetOperation will not retain any link to the source Memory.
   * @param srcMem an image of a SetOperation.
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See Update Hash Seed</a>.
   * @return a Heap-based SetOperation from the given Memory
   * @throws IllegalArgumentException if given image does not match a known
   * SetOperation implementation, or if the assumed default seed does not match the image seed hash.
   */
  public static SetOperation heapify(Memory srcMem, long seed) {
    byte famID = srcMem.getByte(FAMILY_BYTE);
    Family family = idToFamily(famID);
    switch(family) {
      case UNION : {
        return new HeapUnion(srcMem, seed);
      }
      case INTERSECTION : {
        return new HeapIntersection(srcMem, seed);
      }
      default: {
        throw new IllegalArgumentException("SetOperation cannot heapify family: "+family.toString());
      }
    }
  }

  /**
   * Wrap takes the SetOperation image in Memory and refers to it directly. 
   * There is no data copying onto the java heap.  
   * Only "Direct" SetOperations that have been explicity stored as direct can be wrapped.  
   * This method assumes the 
   * <a href="{@docRoot}/resources/dictionary.html#defaultUpdateSeed">Default Update Seed</a>.
   * @param srcMem an image of a SetOperation
   * @return a SetOperation backed by the given Memory
   * @throws IllegalArgumentException if given image does not match a known
   * SetOperation implementation, or if the assumed default seed does not match the image seed hash.
   */
  public static SetOperation wrap(Memory srcMem) {
    return wrap(srcMem, DEFAULT_UPDATE_SEED);
  }

  /**
   * Wrap takes the SetOperation image in Memory and refers to it directly. 
   * There is no data copying onto the java heap.  
   * Only "Direct" SetOperations that have been explicity stored as direct can be wrapped.
   * @param srcMem an image of a SetOperation.
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See Update Hash Seed</a>.
   * @return a SetOperation backed by the given Memory
   * @throws IllegalArgumentException if given image does not match a known
   * SetOperation implementation, or if the assumed default seed does not match the image seed hash.
   */
  public static SetOperation wrap(Memory srcMem, long seed) {
    byte famID = srcMem.getByte(FAMILY_BYTE);
    Family family = idToFamily(famID);
    int serVer = srcMem.getByte(SER_VER_BYTE);
    if (serVer != 3) throw new IllegalArgumentException("SerVer must be 3: "+serVer);
    switch(family) {
      case UNION : {
        return new DirectUnion(srcMem, seed);
      }
      case INTERSECTION : {
        return new DirectIntersection(srcMem, seed);
      }
      default:
        throw new IllegalArgumentException("SetOperation cannot wrap family: "+family.toString());
    }
  }

  //Sizing methods
  /**
   * Returns the maximum required storage bytes given a nomEntries parameter for Union operations
   * @param nomEntries <a href="{@docRoot}/resources/dictionary.html#nomEntries">Nominal Entres</a>
   * @return the maximum required storage bytes given a nomEntries parameter
   */
  public static int getMaxUnionBytes (int nomEntries) {
    checkIfPowerOf2(nomEntries, "Nominal Entries");
    return (nomEntries << 4) + (Family.UNION.getMaxPreLongs() << 3);
  }
  
  /**
   * Returns the maximum required storage bytes given a nomEntries parameter for Intersection 
   * operations
   * @param nomEntries <a href="{@docRoot}/resources/dictionary.html#nomEntries">Nominal Entres</a>
   * @return the maximum required storage bytes given a nomEntries parameter
   */
  public static int getMaxIntersectionBytes (int nomEntries) {
    checkIfPowerOf2(nomEntries, "Nominal Entries");
    return (nomEntries << 4) + (Family.INTERSECTION.getMaxPreLongs() << 3);
  }
  
  static short computeSeedHash(long seed) {
    return PreambleUtil.computeSeedHash(seed);
  }
  
  static final int computeLgArrLongsFromCount(final int count) {
    int upperCount = (int) Math.ceil(count / RESIZE_THRESHOLD);
    int arrLongs = max(ceilingPowerOf2(upperCount), 1 << MIN_LG_ARR_LONGS);
    int newLgArrLongs = Integer.numberOfTrailingZeros(arrLongs);
    return newLgArrLongs;
  }
  
  static final int computeMinLgArrLongsFromCount(final int count, int curLgArrLongs) {
    int upperCount = (int) Math.ceil(count / RESIZE_THRESHOLD);
    int arrLongs = max(ceilingPowerOf2(upperCount), 1 << MIN_LG_ARR_LONGS);
    int newLgArrLongs = Integer.numberOfTrailingZeros(arrLongs);
    if (newLgArrLongs > curLgArrLongs) throw new IllegalArgumentException(
        "Input sketch too large for allocated memory.");
    return newLgArrLongs;
  }
  
}