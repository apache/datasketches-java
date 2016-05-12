/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.theta;

import static com.yahoo.sketches.Family.idToFamily;
import static com.yahoo.sketches.Util.DEFAULT_UPDATE_SEED;
import static com.yahoo.sketches.Util.ceilingPowerOf2;
import static com.yahoo.sketches.theta.PreambleUtil.FAMILY_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.SER_VER_BYTE;
import static com.yahoo.sketches.Util.*;
import static java.lang.Math.max;

import com.yahoo.sketches.Family;
import com.yahoo.sketches.Util;
import com.yahoo.sketches.memory.Memory;

/**
 * The parent API for all Set Operations
 * 
 * @author Lee Rhodes
 */
public abstract class SetOperation {
  static final int CONST_PREAMBLE_LONGS = 3;
  
  SetOperation() {}
  
  /**
   * Makes a new builder
   *
   * @return a new builder
   */
  public static final SetOperationBuilder builder() {
    return new SetOperationBuilder();
  }
  
  /**
   * Heapify takes the SetOperations image in Memory and instantiates an on-heap 
   * SetOperation using the
   * <a href="{@docRoot}/resources/dictionary.html#defaultUpdateSeed">Default Update Seed</a>.
   * The resulting SetOperation will not retain any link to the source Memory. 
   * @param srcMem an image of a SetOperation where the image seed hash matches the default seed hash.
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @return a Heap-based SetOperation from the given Memory
   */
  public static SetOperation heapify(Memory srcMem) {
    return heapify(srcMem, DEFAULT_UPDATE_SEED);
  }

  /**
   * Heapify takes the SetOperation image in Memory and instantiates an on-heap 
   * SetOperation using the given seed.
   * The resulting SetOperation will not retain any link to the source Memory.
   * @param srcMem an image of a SetOperation where the hash of the given seed matches the image seed hash.
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See Update Hash Seed</a>.
   * @return a Heap-based SetOperation from the given Memory
   */
  public static SetOperation heapify(Memory srcMem, long seed) {
    byte famID = srcMem.getByte(FAMILY_BYTE);
    Family family = idToFamily(famID);
    switch(family) {
      case UNION : {
        return UnionImpl.heapifyInstance(srcMem, seed);
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
   * @param srcMem an image of a SetOperation where the image seed hash matches the default seed hash.
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @return a SetOperation backed by the given Memory
   */
  public static SetOperation wrap(Memory srcMem) {
    return wrap(srcMem, DEFAULT_UPDATE_SEED);
  }

  /**
   * Wrap takes the SetOperation image in Memory and refers to it directly. 
   * There is no data copying onto the java heap.  
   * Only "Direct" SetOperations that have been explicity stored as direct can be wrapped.
   * @param srcMem an image of a SetOperation where the hash of the given seed matches the image seed hash.
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See Update Hash Seed</a>.
   * @return a SetOperation backed by the given Memory
   */
  public static SetOperation wrap(Memory srcMem, long seed) {
    byte famID = srcMem.getByte(FAMILY_BYTE);
    Family family = idToFamily(famID);
    int serVer = srcMem.getByte(SER_VER_BYTE);
    if (serVer != 3) throw new IllegalArgumentException("SerVer must be 3: "+serVer);
    switch(family) {
      case UNION : {
        return UnionImpl.wrapInstance(srcMem, seed);
      }
      case INTERSECTION : {
        return new DirectIntersection(srcMem, seed);
      }
      default:
        throw new IllegalArgumentException("SetOperation cannot wrap family: "+family.toString());
    }
  }

  /**
   * Returns the maximum required storage bytes given a nomEntries parameter for Union operations
   * @param nomEntries <a href="{@docRoot}/resources/dictionary.html#nomEntries">Nominal Entres</a>
   * This will become the ceiling power of 2 if it is not.
   * @return the maximum required storage bytes given a nomEntries parameter
   */
  public static int getMaxUnionBytes(int nomEntries) {
    int nomEnt = ceilingPowerOf2(nomEntries);
    return (nomEnt << 4) + (Family.UNION.getMaxPreLongs() << 3);
  }
  
  /**
   * Returns the maximum required storage bytes given a nomEntries parameter for Intersection 
   * operations
   * @param nomEntries <a href="{@docRoot}/resources/dictionary.html#nomEntries">Nominal Entres</a>
   * This will become the ceiling power of 2 if it is not.
   * @return the maximum required storage bytes given a nomEntries parameter
   */
  public static int getMaxIntersectionBytes(int nomEntries) {
    int nomEnt = ceilingPowerOf2(nomEntries);
    int bytes = (nomEnt << 4) + (Family.INTERSECTION.getMaxPreLongs() << 3);
    return bytes;
  }
  
  public abstract Family getFamily();
  
  //restricted
  
  static short computeSeedHash(long seed) {
    return Util.computeSeedHash(seed);
  }
  
  //Used by intersection and AnotB
  static final int computeMinLgArrLongsFromCount(final int count) {
    int upperCount = (int) Math.ceil(count / REBUILD_THRESHOLD);
    int arrLongs = max(ceilingPowerOf2(upperCount), 1 << MIN_LG_ARR_LONGS);
    int newLgArrLongs = Integer.numberOfTrailingZeros(arrLongs);
    return newLgArrLongs;
  }
  
  /**
   * Returns true if given Family id is one of the set operations
   * @param id the given Family id
   * @return true if given Family id is one of the set operations
   */
  static boolean isValidSetOpID(int id) {
    int loID = Family.UNION.getID();
    int hiID = Family.A_NOT_B.getID();
    return ((hiID - id) | (id - loID)) >= 0;
  }
}
