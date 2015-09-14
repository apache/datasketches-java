/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.theta;

import static com.yahoo.sketches.theta.PreambleUtil.*;

import com.yahoo.sketches.Family;
import com.yahoo.sketches.memory.Memory;


/**
 * This class brings together the common sketch and set operation creation methods and
 * the public static methods into one place.
 * 
 * @author Lee Rhodes
 */
public final class Sketches {
  
  private Sketches() {}
  
  /**
   * Ref: {@link UpdateSketchBuilder UpdateSketchBuilder}
   * @return {@link UpdateSketchBuilder UpdateSketchBuilder}
   */
  public static UpdateSketchBuilder updateSketchBuilder() {
    return new UpdateSketchBuilder();
  }
  
  /**
   * Ref: {@link Sketch#heapify(Memory) Sketch.heapify(Memory)}
   * @param srcMem <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @return {@link Sketch Sketch}
   */
  public static Sketch heapifySketch(Memory srcMem) {
    return Sketch.heapify(srcMem);
  }
  
  /**
   * Ref: {@link Sketch#heapify(Memory, long) Sketch.heapify(Memory, long)}
   * @param srcMem <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See Seed</a>
   * @return {@link Sketch Sketch}
   */
  public static Sketch heapifySketch(Memory srcMem, long seed) {
    return Sketch.heapify(srcMem, seed);
  }
  
  /**
   * Ref: {@link Sketch#wrap(Memory) Sketch.heapify(Memory)}
   * @param srcMem <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @return {@link Sketch Sketch}
   */
  public static Sketch wrapSketch(Memory srcMem) {
    return Sketch.wrap(srcMem);
  }
  
  /**
   * Ref: {@link Sketch#wrap(Memory, long) Sketch.wrap(Memory, long)}
   * @param srcMem <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See Seed</a>
   * @return {@link Sketch Sketch}
   */
  public static Sketch wrapSketch(Memory srcMem, long seed) {
    return Sketch.wrap(srcMem, seed);
  }
  
  /**
   * Ref: {@link SetOperationBuilder SetOperationBuilder}
   * @return {@link SetOperationBuilder SetOperationBuilder}
   */
  public static SetOperationBuilder setOperationBuilder() {
    return new SetOperationBuilder();
  }
  
  /**
   * Ref: {@link SetOperation#heapify(Memory) SetOperation.heapify(Memory)}
   * @param srcMem <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @return {@link SetOperation SetOperation}
   */
  public static SetOperation heapifySetOperation(Memory srcMem) {
    return SetOperation.heapify(srcMem);
  }
  
  /**
   * Ref: {@link SetOperation#heapify(Memory, long) SetOperation.heapify(Memory, long)}
   * @param srcMem <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See Seed</a>
   * @return {@link SetOperation SetOperation}
   */
  public static SetOperation heapifySetOperation(Memory srcMem, long seed) {
    return SetOperation.heapify(srcMem, seed);
  }
  
  /**
   * Ref: {@link SetOperation#wrap(Memory) SetOperation.wrap(Memory)}
   * @param srcMem <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @return {@link SetOperation SetOperation}
   */
  public static SetOperation wrapSetOperation(Memory srcMem) {
    return SetOperation.wrap(srcMem);
  }

  /**
   * Convenience method, calls {@link SetOperation#wrap(Memory)} and casts the result to a Union
   * @param srcMem <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @return a Union backed by the given Memory
   * @throws IllegalArgumentException if given image does not match a known
   * SetOperation implementation, or if the assumed default seed does not match the image seed hash.
   */
  public static Union wrapUnion(Memory srcMem) {
    return (Union) SetOperation.wrap(srcMem);
  }

  /**
   * Convenience method, calls {@link SetOperation#wrap(Memory)} and casts the result to a Intersection
   * @param srcMem <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @return a Intersection backed by the given Memory
   * @throws IllegalArgumentException if given image does not match a known
   * SetOperation implementation, or if the assumed default seed does not match the image seed hash.
   */
  public static Intersection wrapIntersection(Memory srcMem) {
    return (Intersection) SetOperation.wrap(srcMem);
  }

  /**
   * Ref: {@link SetOperation#wrap(Memory, long) SetOperation.wrap(Memory, long)}
   * @param srcMem <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See Seed</a>
   * @return {@link SetOperation SetOperation}
   */
  public static SetOperation wrapSetOperation(Memory srcMem, long seed) {
    return SetOperation.wrap(srcMem, seed);
  }
  
  //Get size methods, etc
  
  /**
   * Ref: {@link Sketch#getMaxCompactSketchBytes(int)}
   * @param numberOfEntries  {@link Sketch#getMaxCompactSketchBytes(int)}
   * @return {@link Sketch#getMaxCompactSketchBytes(int)}
   */
  public static int getMaxCompactSketchBytes(int numberOfEntries) {
    return Sketch.getMaxCompactSketchBytes(numberOfEntries);
  }
  
  /**
   * Ref: {@link Sketch#getMaxUpdateSketchBytes(int)}
   * @param nomEntries {@link Sketch#getMaxUpdateSketchBytes(int)}
   * @return {@link Sketch#getMaxUpdateSketchBytes(int)}
   */
  public static int getMaxUpdateSketchBytes(int nomEntries) {
    return Sketch.getMaxUpdateSketchBytes(nomEntries);
  }
  
  /**
   * Ref: {@link Sketch#getSerializationVersion(Memory)}
   * @param srcMem <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @return {@link Sketch#getSerializationVersion(Memory)}
   */
  public static int getSerializationVersion(Memory srcMem) {
    return Sketch.getSerializationVersion(srcMem);
  }
  /**
   * Ref: {@link SetOperation#getMaxUnionBytes(int)}
   * @param nomEntries {@link SetOperation#getMaxUnionBytes(int)}
   * @return {@link SetOperation#getMaxUnionBytes(int)}
   */
  public static int getMaxUnionBytes(int nomEntries) {
    return SetOperation.getMaxUnionBytes(nomEntries);
  }
  
  /**
   * Ref: {@link SetOperation#getMaxIntersectionBytes(int)}
   * @param nomEntries {@link SetOperation#getMaxIntersectionBytes(int)}
   * @return {@link SetOperation#getMaxIntersectionBytes(int)}
   */
  public static int getMaxIntersectionBytes(int nomEntries) {
    return SetOperation.getMaxIntersectionBytes(nomEntries);
  }
  
  //Get estimates and bounds from Memory
  
  /**
   * Gets the unique count estimate.
   * @param srcMem <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @return the sketch's best estimate of the cardinality of the input stream.
   * @throws IllegalArgumentException if given memory is not from a valid ThetaSketch.
   */
  public static double getEstimate(Memory srcMem) {
    checkIfValidThetaSketch(srcMem);
    return Sketch.estimate(getThetaLong(srcMem), getRetainedEntries(srcMem), getEmpty(srcMem));
  }
  
  /**
   * Gets the approximate upper error bound given the specified number of Standard Deviations. 
   * This will return getEstimate() if isEmpty() is true.
   * 
   * @param numStdDev
   * <a href="{@docRoot}/resources/dictionary.html#numStdDev">See Number of Standard Deviations</a>
   * @param srcMem <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @return the upper bound.
   */
  public static double getUpperBound(int numStdDev, Memory srcMem) {
    return Sketch.upperBound(numStdDev, getThetaLong(srcMem), getRetainedEntries(srcMem), getEmpty(srcMem));
  }
  
  /**
   * Gets the approximate lower error bound given the specified number of Standard Deviations. 
   * This will return getEstimate() if isEmpty() is true.
   * 
   * @param numStdDev
   * <a href="{@docRoot}/resources/dictionary.html#numStdDev">See Number of Standard Deviations</a>
   * @param srcMem <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @return the lower bound.
   */
  public static double getLowerBound(int numStdDev, Memory srcMem) {
    return Sketch.lowerBound(numStdDev, getThetaLong(srcMem), getRetainedEntries(srcMem), getEmpty(srcMem));
  }
  
  //Restricted static methods
  
  static int getPreambleLongs(Memory srcMem) {
    return srcMem.getByte(PREAMBLE_LONGS_BYTE) & 0X3F; //for SerVer 1,2,3
  }
  
  static int getRetainedEntries(Memory srcMem) {
    int preLongs = getPreambleLongs(srcMem);
    return (preLongs == 1)? 0: srcMem.getInt(RETAINED_ENTRIES_INT); //for SerVer 1,2,3
  }
  
  static long getThetaLong(Memory srcMem) {
    int preLongs = getPreambleLongs(srcMem);
    return (preLongs < 3)? Long.MAX_VALUE : srcMem.getLong(THETA_LONG); //for SerVer 1,2,3
  }
  
  static boolean getEmpty(Memory srcMem) {
    int serVer = srcMem.getByte(SER_VER_BYTE);
    if (serVer == 1) {
      return ((getThetaLong(srcMem) == Long.MAX_VALUE) && (getRetainedEntries(srcMem) == 0));
    }
    return srcMem.isAnyBitsSet(FLAGS_BYTE, (byte) EMPTY_FLAG_MASK); //for SerVer 2 & 3
  }
  
  static void checkIfValidThetaSketch(Memory srcMem) {
    int fam = srcMem.getByte(FAMILY_BYTE);
    if (!Family.isValidSketchID(fam)) {
     throw new IllegalArgumentException("Source Memory not a valid Sketch. Family: "+
       Family.idToFamily(fam).toString()); 
    }
  }
}