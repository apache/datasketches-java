/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.theta;

import static com.yahoo.sketches.theta.PreambleUtil.EMPTY_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.FAMILY_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.FLAGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.PREAMBLE_LONGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.RETAINED_ENTRIES_INT;
import static com.yahoo.sketches.theta.PreambleUtil.SER_VER_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.THETA_LONG;

import com.yahoo.sketches.Family;
import com.yahoo.sketches.SketchesArgumentException;
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
   * @param srcMem Ref: {@link Sketch#heapify(Memory) Sketch.heapify(Memory)} {@code srcMem}
   * @return {@link Sketch Sketch}
   */
  public static Sketch heapifySketch(Memory srcMem) {
    return Sketch.heapify(srcMem);
  }
  
  /**
   * Ref: {@link Sketch#heapify(Memory, long) Sketch.heapify(Memory, long)}
   * @param srcMem Ref: {@link Sketch#heapify(Memory, long) Sketch.heapify(Memory, long)} {@code srcMem}
   * @param seed Ref: {@link Sketch#heapify(Memory, long) Sketch.heapify(Memory, long)} {@code seed}
   * @return {@link Sketch Sketch}
   */
  public static Sketch heapifySketch(Memory srcMem, long seed) {
    return Sketch.heapify(srcMem, seed);
  }
  
  /**
   * Ref: {@link Sketch#wrap(Memory) Sketch.heapify(Memory)}
   * @param srcMem Ref: {@link Sketch#wrap(Memory) Sketch.heapify(Memory)} {@code srcMem}
   * @return {@link Sketch Sketch}
   */
  public static Sketch wrapSketch(Memory srcMem) {
    return Sketch.wrap(srcMem);
  }
  
  /**
   * Ref: {@link Sketch#wrap(Memory, long) Sketch.wrap(Memory, long)}
   * @param srcMem Ref: {@link Sketch#wrap(Memory, long) Sketch.wrap(Memory, long)} {@code srcMem}
   * @param seed Ref: {@link Sketch#wrap(Memory, long) Sketch.wrap(Memory, long)} {@code seed}
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
   * @param srcMem Ref: {@link SetOperation#heapify(Memory) SetOperation.heapify(Memory)} {@code srcMem}
   * @return {@link SetOperation SetOperation}
   */
  public static SetOperation heapifySetOperation(Memory srcMem) {
    return SetOperation.heapify(srcMem);
  }
  
  /**
   * Ref: {@link SetOperation#heapify(Memory, long) SetOperation.heapify(Memory, long)}
   * @param srcMem Ref: {@link SetOperation#heapify(Memory, long) SetOperation.heapify(Memory, long)}
   * {@code srcMem} 
   * @param seed Ref: {@link SetOperation#heapify(Memory, long) SetOperation.heapify(Memory, long)}
   * {@code seed}
   * @return {@link SetOperation SetOperation}
   */
  public static SetOperation heapifySetOperation(Memory srcMem, long seed) {
    return SetOperation.heapify(srcMem, seed);
  }
  
  /**
   * Ref: {@link SetOperation#wrap(Memory) SetOperation.wrap(Memory)}
   * @param srcMem Ref: {@link SetOperation#wrap(Memory) SetOperation.wrap(Memory)} {@code srcMem}
   * @return {@link SetOperation SetOperation}
   */
  public static SetOperation wrapSetOperation(Memory srcMem) {
    return SetOperation.wrap(srcMem);
  }

  /**
   * Convenience method, calls {@link SetOperation#wrap(Memory)} and casts the result to a Union
   * @param srcMem Ref: {@link SetOperation#wrap(Memory)} {@code srcMem}
   * @return a Union backed by the given Memory
   */
  public static Union wrapUnion(Memory srcMem) {
    return (Union) SetOperation.wrap(srcMem);
  }

  /**
   * Convenience method, calls {@link SetOperation#wrap(Memory)} and casts the result to a Intersection
   * @param srcMem Ref: {@link SetOperation#wrap(Memory)} {@code srcMem}
   * @return a Intersection backed by the given Memory
   */
  public static Intersection wrapIntersection(Memory srcMem) {
    return (Intersection) SetOperation.wrap(srcMem);
  }

  /**
   * Ref: {@link SetOperation#wrap(Memory, long) SetOperation.wrap(Memory, long)}
   * @param srcMem Ref: {@link SetOperation#wrap(Memory, long) SetOperation.wrap(Memory, long)}
   * {@code srcMem}
   * @param seed Ref: {@link SetOperation#wrap(Memory, long) SetOperation.wrap(Memory, long)}
   * {@code seed}
   * @return {@link SetOperation SetOperation}
   */
  public static SetOperation wrapSetOperation(Memory srcMem, long seed) {
    return SetOperation.wrap(srcMem, seed);
  }
  
  //Get size methods, etc
  
  /**
   * Ref: {@link Sketch#getMaxCompactSketchBytes(int)}
   * @param numberOfEntries  Ref: {@link Sketch#getMaxCompactSketchBytes(int)} 
   * {@code numberOfEntries}
   * @return Ref: {@link Sketch#getMaxCompactSketchBytes(int)}
   */
  public static int getMaxCompactSketchBytes(int numberOfEntries) {
    return Sketch.getMaxCompactSketchBytes(numberOfEntries);
  }
  
  /**
   * Ref: {@link Sketch#getMaxUpdateSketchBytes(int)}
   * @param nomEntries Ref: {@link Sketch#getMaxUpdateSketchBytes(int)} {@code nomEntries}
   * @return Ref: {@link Sketch#getMaxUpdateSketchBytes(int)}
   */
  public static int getMaxUpdateSketchBytes(int nomEntries) {
    return Sketch.getMaxUpdateSketchBytes(nomEntries);
  }
  
  /**
   * Ref: {@link Sketch#getSerializationVersion(Memory)}
   * @param srcMem Ref: {@link Sketch#getSerializationVersion(Memory)} {@code srcMem}
   * @return Ref: {@link Sketch#getSerializationVersion(Memory)}
   */
  public static int getSerializationVersion(Memory srcMem) {
    return Sketch.getSerializationVersion(srcMem);
  }
  
  /**
   * Ref: {@link SetOperation#getMaxUnionBytes(int)}
   * @param nomEntries Ref: {@link SetOperation#getMaxUnionBytes(int)} {@code nomEntries}
   * @return Ref: {@link SetOperation#getMaxUnionBytes(int)}
   */
  public static int getMaxUnionBytes(int nomEntries) {
    return SetOperation.getMaxUnionBytes(nomEntries);
  }
  
  /**
   * Ref: {@link SetOperation#getMaxIntersectionBytes(int)}
   * @param nomEntries Ref: {@link SetOperation#getMaxIntersectionBytes(int)} {@code nomEntries}
   * @return Ref: {@link SetOperation#getMaxIntersectionBytes(int)}
   */
  public static int getMaxIntersectionBytes(int nomEntries) {
    return SetOperation.getMaxIntersectionBytes(nomEntries);
  }
  
  //Get estimates and bounds from Memory
  
  /**
   * Gets the unique count estimate from a valid memory image of a Sketch
   * @param srcMem <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @return the sketch's best estimate of the cardinality of the input stream.
   */
  public static double getEstimate(Memory srcMem) {
    checkIfValidThetaSketch(srcMem);
    return Sketch.estimate(getThetaLong(srcMem), getRetainedEntries(srcMem), getEmpty(srcMem));
  }
  
  /**
   * Gets the approximate upper error bound from a valid memory image of a Sketch 
   * given the specified number of Standard Deviations. 
   * This will return getEstimate() if isEmpty() is true.
   * 
   * @param numStdDev
   * <a href="{@docRoot}/resources/dictionary.html#numStdDev">See Number of Standard Deviations</a>
   * @param srcMem 
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @return the upper bound.
   */
  public static double getUpperBound(int numStdDev, Memory srcMem) {
    return Sketch.upperBound(getRetainedEntries(srcMem), getThetaLong(srcMem), numStdDev, getEmpty(srcMem));
  }
  
  /**
   * Gets the approximate lower error bound from a valid memory image of a Sketch
   * given the specified number of Standard Deviations. 
   * This will return getEstimate() if isEmpty() is true.
   * 
   * @param numStdDev
   * <a href="{@docRoot}/resources/dictionary.html#numStdDev">See Number of Standard Deviations</a>
   * @param srcMem <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @return the lower bound.
   */
  public static double getLowerBound(int numStdDev, Memory srcMem) {
    return Sketch.lowerBound(getRetainedEntries(srcMem), getThetaLong(srcMem), numStdDev, getEmpty(srcMem));
  }
  
  //Restricted static methods
  
  static int getPreambleLongs(Memory srcMem) {
    return srcMem.getByte(PREAMBLE_LONGS_BYTE) & 0X3F; //for SerVer 1,2,3
  }
  
  static int getRetainedEntries(Memory srcMem) {
    int preLongs = getPreambleLongs(srcMem);
    return (preLongs == 1) ? 0 : srcMem.getInt(RETAINED_ENTRIES_INT); //for SerVer 1,2,3
  }
  
  static long getThetaLong(Memory srcMem) {
    int preLongs = getPreambleLongs(srcMem);
    return (preLongs < 3) ? Long.MAX_VALUE : srcMem.getLong(THETA_LONG); //for SerVer 1,2,3
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
    if (!Sketch.isValidSketchID(fam)) {
     throw new SketchesArgumentException("Source Memory not a valid Sketch. Family: " 
         + Family.idToFamily(fam).toString()); 
    }
  }
}
