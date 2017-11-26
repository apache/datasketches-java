/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.theta;

import static com.yahoo.sketches.Util.DEFAULT_UPDATE_SEED;
import static com.yahoo.sketches.theta.PreambleUtil.EMPTY_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.FAMILY_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.FLAGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.PREAMBLE_LONGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.RETAINED_ENTRIES_INT;
import static com.yahoo.sketches.theta.PreambleUtil.SER_VER_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.THETA_LONG;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.SketchesArgumentException;

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
  public static Sketch heapifySketch(final Memory srcMem) {
    return Sketch.heapify(srcMem);
  }

  /**
   * Ref: {@link Sketch#heapify(Memory, long) Sketch.heapify(Memory, long)}
   * @param srcMem Ref: {@link Sketch#heapify(Memory, long) Sketch.heapify(Memory, long)} {@code srcMem}
   * @param seed Ref: {@link Sketch#heapify(Memory, long) Sketch.heapify(Memory, long)} {@code seed}
   * @return {@link Sketch Sketch}
   */
  public static Sketch heapifySketch(final Memory srcMem, final long seed) {
    return Sketch.heapify(srcMem, seed);
  }

  /**
   * Ref: {@link UpdateSketch#heapify(Memory) UpdateSketch.heapify(Memory)}
   * @param srcMem Ref: {@link UpdateSketch#heapify(Memory) UpdateSketch.heapify(Memory)} {@code srcMem}
   * @return {@link UpdateSketch UpdateSketch}
   */
  public static UpdateSketch heapifyUpdateSketch(final Memory srcMem) {
    return UpdateSketch.heapify(srcMem);
  }

  /**
   * Ref: {@link UpdateSketch#heapify(Memory, long) UpdateSketch.heapify(Memory, long)}
   * @param srcMem Ref: {@link UpdateSketch#heapify(Memory, long) UpdateSketch.heapify(Memory, long)}
   *   {@code srcMem}
   * @param seed Ref: {@link UpdateSketch#heapify(Memory, long) UpdateSketch.heapify(Memory, long)}
   *   {@code seed}
   * @return {@link UpdateSketch UpdateSketch}
   */
  public static UpdateSketch heapifyUpdateSketch(final Memory srcMem, final long seed) {
    return UpdateSketch.heapify(srcMem, seed);
  }

  /**
   * Ref: {@link Sketch#wrap(Memory) Sketch.wrap(Memory)}
   * @param srcMem Ref: {@link Sketch#wrap(Memory) Sketch.wrap(Memory)} {@code srcMem}
   * @return {@link Sketch Sketch}
   */
  public static Sketch wrapSketch(final Memory srcMem) {
    return Sketch.wrap(srcMem);
  }

  /**
   * Ref: {@link Sketch#wrap(Memory, long) Sketch.wrap(Memory, long)}
   * @param srcMem Ref: {@link Sketch#wrap(Memory, long) Sketch.wrap(Memory, long)} {@code srcMem}
   * @param seed Ref: {@link Sketch#wrap(Memory, long) Sketch.wrap(Memory, long)} {@code seed}
   * @return {@link Sketch Sketch}
   */
  public static Sketch wrapSketch(final Memory srcMem, final long seed) {
    return Sketch.wrap(srcMem, seed);
  }

  /**
   * Ref: {@link UpdateSketch#wrap(Memory) UpdateSketch.wrap(Memory)}
   * @param srcMem Ref: {@link UpdateSketch#wrap(Memory) UpdateSketch.wrap(Memory)} {@code srcMem}
   * @return {@link UpdateSketch UpdateSketch}
   */
  public static UpdateSketch wrapUpdateSketch(final WritableMemory srcMem) {
    return wrapUpdateSketch(srcMem, DEFAULT_UPDATE_SEED);
  }

  /**
   * Ref: {@link UpdateSketch#wrap(Memory, long) UpdateSketch.wrap(Memory, long)}
   * @param srcMem Ref: {@link UpdateSketch#wrap(Memory, long) UpdateSketch.wrap(Memory, long)} {@code srcMem}
   * @param seed Ref: {@link UpdateSketch#wrap(Memory, long) UpdateSketch.wrap(Memory, long)} {@code seed}
   * @return {@link UpdateSketch UpdateSketch}
   */
  public static UpdateSketch wrapUpdateSketch(final WritableMemory srcMem, final long seed) {
    return UpdateSketch.wrap(srcMem, seed);
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
  public static SetOperation heapifySetOperation(final Memory srcMem) {
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
  public static SetOperation heapifySetOperation(final Memory srcMem, final long seed) {
    return SetOperation.heapify(srcMem, seed);
  }

  /**
   * Ref: {@link SetOperation#wrap(Memory) SetOperation.wrap(Memory)}
   * @param srcMem Ref: {@link SetOperation#wrap(Memory) SetOperation.wrap(Memory)} {@code srcMem}
   * @return {@link SetOperation SetOperation}
   */
  public static SetOperation wrapSetOperation(final Memory srcMem) {
    return wrapSetOperation(srcMem, DEFAULT_UPDATE_SEED);
  }

  /**
   * Ref: {@link SetOperation#wrap(Memory) SetOperation.wrap(Memory)}
   * @param srcMem Ref: {@link SetOperation#wrap(Memory) SetOperation.wrap(Memory)} {@code srcMem}
   * @return {@link SetOperation SetOperation}
   */
  public static SetOperation wrapSetOperation(final WritableMemory srcMem) {
    return wrapSetOperation(srcMem, DEFAULT_UPDATE_SEED);
  }

  /**
   * Convenience method, calls {@link SetOperation#wrap(Memory)} and casts the result to a Union
   * @param srcMem Ref: {@link SetOperation#wrap(Memory)} {@code srcMem}
   * @return a Union backed by the given Memory
   */
  public static Union wrapUnion(final Memory srcMem) {
    return (Union) SetOperation.wrap(srcMem);
  }

  /**
   * Convenience method, calls {@link SetOperation#wrap(Memory)} and casts the result to a Union
   * @param srcMem Ref: {@link SetOperation#wrap(Memory)} {@code srcMem}
   * @return a Union backed by the given Memory
   */
  public static Union wrapUnion(final WritableMemory srcMem) {
    return (Union) SetOperation.wrap(srcMem);
  }

  /**
   * Convenience method, calls {@link SetOperation#wrap(Memory)} and casts the result to a Intersection
   * @param srcMem Ref: {@link SetOperation#wrap(Memory)} {@code srcMem}
   * @return a Intersection backed by the given Memory
   */
  public static Intersection wrapIntersection(final Memory srcMem) {
    return (Intersection) SetOperation.wrap(srcMem);
  }

  /**
   * Convenience method, calls {@link SetOperation#wrap(Memory)} and casts the result to a Intersection
   * @param srcMem Ref: {@link SetOperation#wrap(Memory)} {@code srcMem}
   * @return a Intersection backed by the given Memory
   */
  public static Intersection wrapIntersection(final WritableMemory srcMem) {
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
  public static SetOperation wrapSetOperation(final Memory srcMem, final long seed) {
    return SetOperation.wrap(srcMem, seed);
  }

  /**
   * Ref: {@link SetOperation#wrap(Memory, long) SetOperation.wrap(Memory, long)}
   * @param srcMem Ref: {@link SetOperation#wrap(Memory, long) SetOperation.wrap(Memory, long)}
   * {@code srcMem}
   * @param seed Ref: {@link SetOperation#wrap(Memory, long) SetOperation.wrap(Memory, long)}
   * {@code seed}
   * @return {@link SetOperation SetOperation}
   */
  public static SetOperation wrapSetOperation(final WritableMemory srcMem, final long seed) {
    return SetOperation.wrap(srcMem, seed);
  }

  //Get size methods, etc

  /**
   * Ref: {@link Sketch#getMaxCompactSketchBytes(int)}
   * @param numberOfEntries  Ref: {@link Sketch#getMaxCompactSketchBytes(int)}
   * {@code numberOfEntries}
   * @return Ref: {@link Sketch#getMaxCompactSketchBytes(int)}
   */
  public static int getMaxCompactSketchBytes(final int numberOfEntries) {
    return Sketch.getMaxCompactSketchBytes(numberOfEntries);
  }

  /**
   * Ref: {@link Sketch#getMaxUpdateSketchBytes(int)}
   * @param nomEntries Ref: {@link Sketch#getMaxUpdateSketchBytes(int)} {@code nomEntries}
   * @return Ref: {@link Sketch#getMaxUpdateSketchBytes(int)}
   */
  public static int getMaxUpdateSketchBytes(final int nomEntries) {
    return Sketch.getMaxUpdateSketchBytes(nomEntries);
  }

  /**
   * Ref: {@link Sketch#getSerializationVersion(Memory)}
   * @param srcMem Ref: {@link Sketch#getSerializationVersion(Memory)} {@code srcMem}
   * @return Ref: {@link Sketch#getSerializationVersion(Memory)}
   */
  public static int getSerializationVersion(final Memory srcMem) {
    return Sketch.getSerializationVersion(srcMem);
  }

  /**
   * Ref: {@link SetOperation#getMaxUnionBytes(int)}
   * @param nomEntries Ref: {@link SetOperation#getMaxUnionBytes(int)} {@code nomEntries}
   * @return Ref: {@link SetOperation#getMaxUnionBytes(int)}
   */
  public static int getMaxUnionBytes(final int nomEntries) {
    return SetOperation.getMaxUnionBytes(nomEntries);
  }

  /**
   * Ref: {@link SetOperation#getMaxIntersectionBytes(int)}
   * @param nomEntries Ref: {@link SetOperation#getMaxIntersectionBytes(int)} {@code nomEntries}
   * @return Ref: {@link SetOperation#getMaxIntersectionBytes(int)}
   */
  public static int getMaxIntersectionBytes(final int nomEntries) {
    return SetOperation.getMaxIntersectionBytes(nomEntries);
  }

  //Get estimates and bounds from Memory

  /**
   * Gets the unique count estimate from a valid memory image of a Sketch
   * @param srcMem <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @return the sketch's best estimate of the cardinality of the input stream.
   */
  public static double getEstimate(final Memory srcMem) {
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
  public static double getUpperBound(final int numStdDev, final Memory srcMem) {
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
  public static double getLowerBound(final int numStdDev, final Memory srcMem) {
    return Sketch.lowerBound(getRetainedEntries(srcMem), getThetaLong(srcMem), numStdDev, getEmpty(srcMem));
  }

  //Restricted static methods

  static int getPreambleLongs(final Memory srcMem) {
    return srcMem.getByte(PREAMBLE_LONGS_BYTE) & 0X3F; //for SerVer 1,2,3
  }

  static int getRetainedEntries(final Memory srcMem) {
    final int serVer = srcMem.getByte(SER_VER_BYTE);
    if (serVer == 1) {
      final int entries = srcMem.getInt(RETAINED_ENTRIES_INT);
      if ((getThetaLong(srcMem) == Long.MAX_VALUE) && (entries == 0)) {
        return 0;
      }
      return entries;
    }
    //SerVer 2 or 3
    final int preLongs = getPreambleLongs(srcMem);
    final boolean empty = (srcMem.getByte(FLAGS_BYTE) & EMPTY_FLAG_MASK) != 0; //for SerVer 2 & 3
    if (preLongs == 1) {
      return empty ? 0 : 1;
    }
    //preLongs > 1
    return srcMem.getInt(RETAINED_ENTRIES_INT); //for SerVer 1,2,3
  }

  static long getThetaLong(final Memory srcMem) {
    final int preLongs = getPreambleLongs(srcMem);
    return (preLongs < 3) ? Long.MAX_VALUE : srcMem.getLong(THETA_LONG); //for SerVer 1,2,3
  }

  static boolean getEmpty(final Memory srcMem) {
    final int serVer = srcMem.getByte(SER_VER_BYTE);
    if (serVer == 1) {
      return ((getThetaLong(srcMem) == Long.MAX_VALUE) && (getRetainedEntries(srcMem) == 0));
    }
    return (srcMem.getByte(FLAGS_BYTE) & EMPTY_FLAG_MASK) != 0; //for SerVer 2 & 3
  }

  static void checkIfValidThetaSketch(final Memory srcMem) {
    final int fam = srcMem.getByte(FAMILY_BYTE);
    if (!Sketch.isValidSketchID(fam)) {
     throw new SketchesArgumentException("Source Memory not a valid Sketch. Family: "
         + Family.idToFamily(fam).toString());
    }
  }
}
