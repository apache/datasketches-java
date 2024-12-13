/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.datasketches.theta;

import static org.apache.datasketches.theta.PreambleUtil.EMPTY_FLAG_MASK;
import static org.apache.datasketches.theta.PreambleUtil.FAMILY_BYTE;
import static org.apache.datasketches.theta.PreambleUtil.FLAGS_BYTE;
import static org.apache.datasketches.theta.PreambleUtil.PREAMBLE_LONGS_BYTE;
import static org.apache.datasketches.theta.PreambleUtil.RETAINED_ENTRIES_INT;
import static org.apache.datasketches.theta.PreambleUtil.SER_VER_BYTE;
import static org.apache.datasketches.theta.PreambleUtil.THETA_LONG;

import org.apache.datasketches.common.Family;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.datasketches.thetacommon.ThetaUtil;

/**
 * This class brings together the common sketch and set operation creation methods and
 * the public static methods into one place.
 *
 * @author Lee Rhodes
 */
public final class Sketches {

  private Sketches() {}

  /**
   * Gets the unique count estimate from a valid memory image of a Sketch
   * @param srcMem <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @return the sketch's best estimate of the cardinality of the input stream.
   */
  public static double getEstimate(final Memory srcMem) {
    checkIfValidThetaSketch(srcMem);
    return Sketch.estimate(getThetaLong(srcMem), getRetainedEntries(srcMem));
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

  /**
   * Ref: {@link SetOperation#getMaxAnotBResultBytes(int)}.
   * Returns the maximum number of bytes for the returned CompactSketch, given the maximum
   * value of nomEntries of the first sketch A of AnotB.
   * @param maxNomEntries the given value
   * @return the maximum number of bytes.
   */
  public static int getMaxAnotBResultBytes(final int maxNomEntries) {
    return SetOperation.getMaxAnotBResultBytes(maxNomEntries);
  }

  /**
   * Returns the maximum number of storage bytes required for a CompactSketch with the given
   * number of actual entries.
   * @param numberOfEntries the actual number of retained entries stored in the sketch.
   * @return the maximum number of storage bytes required for a CompactSketch with the given number
   * of retained entries.
   */
  public static int getMaxCompactSketchBytes(final int numberOfEntries) {
    return Sketch.getMaxCompactSketchBytes(numberOfEntries);
  }

  /**
   * Returns the maximum number of storage bytes required for a CompactSketch given the configured
   * log_base2 of the number of nominal entries, which is a power of 2.
   * @param lgNomEntries <a href="{@docRoot}/resources/dictionary.html#nomEntries">Nominal Entries</a>
   * @return the maximum number of storage bytes required for a CompactSketch with the given
   * lgNomEntries.
   * @see Sketch#getCompactSketchMaxBytes(int)
   */
  public static int getCompactSketchMaxBytes(final int lgNomEntries) {
    return Sketch.getCompactSketchMaxBytes(lgNomEntries);
  }

  /**
   * Ref: {@link SetOperation#getMaxIntersectionBytes(int)}
   * @param nomEntries Ref: {@link SetOperation#getMaxIntersectionBytes(int)}, {@code nomEntries}
   * @return Ref: {@link SetOperation#getMaxIntersectionBytes(int)}
   */
  public static int getMaxIntersectionBytes(final int nomEntries) {
    return SetOperation.getMaxIntersectionBytes(nomEntries);
  }

  /**
   * Ref: {@link SetOperation#getMaxUnionBytes(int)}
   * @param nomEntries Ref: {@link SetOperation#getMaxUnionBytes(int)}, {@code nomEntries}
   * @return Ref: {@link SetOperation#getMaxUnionBytes(int)}
   */
  public static int getMaxUnionBytes(final int nomEntries) {
    return SetOperation.getMaxUnionBytes(nomEntries);
  }

  /**
   * Ref: {@link Sketch#getMaxUpdateSketchBytes(int)}
   * @param nomEntries Ref: {@link Sketch#getMaxUpdateSketchBytes(int)}, {@code nomEntries}
   * @return Ref: {@link Sketch#getMaxUpdateSketchBytes(int)}
   */
  public static int getMaxUpdateSketchBytes(final int nomEntries) {
    return Sketch.getMaxUpdateSketchBytes(nomEntries);
  }

  /**
   * Ref: {@link Sketch#getSerializationVersion(Memory)}
   * @param srcMem Ref: {@link Sketch#getSerializationVersion(Memory)}, {@code srcMem}
   * @return Ref: {@link Sketch#getSerializationVersion(Memory)}
   */
  public static int getSerializationVersion(final Memory srcMem) {
    return Sketch.getSerializationVersion(srcMem);
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

  //Heapify Operations

  /**
   * Ref: {@link CompactSketch#heapify(Memory) CompactSketch.heapify(Memory)}
   * @param srcMem Ref: {@link CompactSketch#heapify(Memory) CompactSketch.heapify(Memory)}, {@code srcMem}
   * @return {@link CompactSketch CompactSketch}
   */
  public static CompactSketch heapifyCompactSketch(final Memory srcMem) {
    return CompactSketch.heapify(srcMem);
  }

  /**
   * Ref: {@link CompactSketch#heapify(Memory, long) CompactSketch.heapify(Memory, long)}
   * @param srcMem Ref: {@link CompactSketch#heapify(Memory, long) CompactSketch.heapify(Memory, long)}, {@code srcMem}
   * @param expectedSeed Ref: {@link CompactSketch#heapify(Memory, long) CompactSketch.heapify(Memory, long)},
   * {@code expectedSeed}
   * @return {@link CompactSketch CompactSketch}
   */
  public static CompactSketch heapifyCompactSketch(final Memory srcMem, final long expectedSeed) {
    return CompactSketch.heapify(srcMem, expectedSeed);
  }

  /**
   * Ref: {@link CompactSketch#wrap(Memory) CompactSketch.wrap(Memory)}
   * @param srcMem Ref: {@link CompactSketch#wrap(Memory) CompactSketch.wrap(Memory)}, {@code srcMem}
   * @return {@link CompactSketch CompactSketch}
   */
  public static CompactSketch wrapCompactSketch(final Memory srcMem) {
    return CompactSketch.wrap(srcMem);
  }

  /**
   * Ref: {@link CompactSketch#wrap(Memory, long) CompactSketch.wrap(Memory, long)}
   * @param srcMem Ref: {@link CompactSketch#wrap(Memory, long) CompactSketch.wrap(Memory, long)}, {@code srcMem}
   * @param expectedSeed Ref: {@link CompactSketch#wrap(Memory, long) CompactSketch.wrap(Memory, long)},
   * {@code expectedSeed}
   * @return {@link CompactSketch CompactSketch}
   */
  public static CompactSketch wrapCompactSketch(final Memory srcMem, final long expectedSeed) {
    return CompactSketch.wrap(srcMem, expectedSeed);
  }

  /**
   * Ref: {@link SetOperation#heapify(Memory) SetOperation.heapify(Memory)}
   * @param srcMem Ref: {@link SetOperation#heapify(Memory) SetOperation.heapify(Memory)}, {@code srcMem}
   * @return {@link SetOperation SetOperation}
   */
  public static SetOperation heapifySetOperation(final Memory srcMem) {
    return SetOperation.heapify(srcMem);
  }

  /**
   * Ref: {@link SetOperation#heapify(Memory, long) SetOperation.heapify(Memory, long)}
   * @param srcMem Ref: {@link SetOperation#heapify(Memory, long) SetOperation.heapify(Memory, long)},
   * {@code srcMem}
   * @param expectedSeed the seed used to validate the given Memory image.
   * Ref: {@link SetOperation#heapify(Memory, long) SetOperation.heapify(Memory, long)},
   * {@code expectedSeed}
   * @return {@link SetOperation SetOperation}
   */
  public static SetOperation heapifySetOperation(final Memory srcMem, final long expectedSeed) {
    return SetOperation.heapify(srcMem, expectedSeed);
  }

  /**
   * Ref: {@link Sketch#heapify(Memory) Sketch.heapify(Memory)}
   * @param srcMem Ref: {@link Sketch#heapify(Memory) Sketch.heapify(Memory)}, {@code srcMem}
   * @return {@link Sketch Sketch}
   */
  public static Sketch heapifySketch(final Memory srcMem) {
    return Sketch.heapify(srcMem);
  }

  /**
   * Ref: {@link Sketch#heapify(Memory, long) Sketch.heapify(Memory, long)}
   * @param srcMem Ref: {@link Sketch#heapify(Memory, long) Sketch.heapify(Memory, long)}, {@code srcMem}
   * @param expectedSeed the seed used to validate the given Memory image.
   * Ref: {@link Sketch#heapify(Memory, long) Sketch.heapify(Memory, long)}, {@code expectedSeed}
   * @return {@link Sketch Sketch}
   */
  public static Sketch heapifySketch(final Memory srcMem, final long expectedSeed) {
    return Sketch.heapify(srcMem, expectedSeed);
  }

  /**
   * Ref: {@link UpdateSketch#heapify(Memory) UpdateSketch.heapify(Memory)}
   * @param srcMem Ref: {@link UpdateSketch#heapify(Memory) UpdateSketch.heapify(Memory)}, {@code srcMem}
   * @return {@link UpdateSketch UpdateSketch}
   */
  public static UpdateSketch heapifyUpdateSketch(final Memory srcMem) {
    return UpdateSketch.heapify(srcMem);
  }

  /**
   * Ref: {@link UpdateSketch#heapify(Memory, long) UpdateSketch.heapify(Memory, long)}
   * @param srcMem Ref: {@link UpdateSketch#heapify(Memory, long) UpdateSketch.heapify(Memory, long)},
   *   {@code srcMem}
   * @param expectedSeed the seed used to validate the given Memory image.
   * Ref: {@link UpdateSketch#heapify(Memory, long) UpdateSketch.heapify(Memory, long)},
   *   {@code expectedSeed}
   * @return {@link UpdateSketch UpdateSketch}
   */
  public static UpdateSketch heapifyUpdateSketch(final Memory srcMem, final long expectedSeed) {
    return UpdateSketch.heapify(srcMem, expectedSeed);
  }

  //Builders

  /**
   * Ref: {@link SetOperationBuilder SetOperationBuilder}
   * @return {@link SetOperationBuilder SetOperationBuilder}
   */
  public static SetOperationBuilder setOperationBuilder() {
    return new SetOperationBuilder();
  }

  /**
   * Ref: {@link UpdateSketchBuilder UpdateSketchBuilder}
   * @return {@link UpdateSketchBuilder UpdateSketchBuilder}
   */
  public static UpdateSketchBuilder updateSketchBuilder() {
    return new UpdateSketchBuilder();
  }

  //Wrap operations

  /**
   * Convenience method, calls {@link SetOperation#wrap(Memory)} and casts the result to a Intersection
   * @param srcMem Ref: {@link SetOperation#wrap(Memory)}, {@code srcMem}
   * @return a Intersection backed by the given Memory
   */
  public static Intersection wrapIntersection(final Memory srcMem) {
    return (Intersection) SetOperation.wrap(srcMem);
  }

  /**
   * Convenience method, calls {@link SetOperation#wrap(Memory)} and casts the result to a Intersection
   * @param srcMem Ref: {@link SetOperation#wrap(Memory)}, {@code srcMem}
   * @return a Intersection backed by the given Memory
   */
  public static Intersection wrapIntersection(final WritableMemory srcMem) {
    return (Intersection) SetOperation.wrap(srcMem);
  }

  /**
   * Ref: {@link SetOperation#wrap(Memory) SetOperation.wrap(Memory)}
   * @param srcMem Ref: {@link SetOperation#wrap(Memory) SetOperation.wrap(Memory)}, {@code srcMem}
   * @return {@link SetOperation SetOperation}
   */
  public static SetOperation wrapSetOperation(final Memory srcMem) {
    return wrapSetOperation(srcMem, ThetaUtil.DEFAULT_UPDATE_SEED);
  }

  /**
   * Ref: {@link SetOperation#wrap(Memory, long) SetOperation.wrap(Memory, long)}
   * @param srcMem Ref: {@link SetOperation#wrap(Memory, long) SetOperation.wrap(Memory, long)},
   * {@code srcMem}
   * @param expectedSeed the seed used to validate the given Memory image.
   * Ref: {@link SetOperation#wrap(Memory, long) SetOperation.wrap(Memory, long)},
   * {@code expectedSeed}
   * @return {@link SetOperation SetOperation}
   */
  public static SetOperation wrapSetOperation(final Memory srcMem, final long expectedSeed) {
    return SetOperation.wrap(srcMem, expectedSeed);
  }

  /**
   * Ref: {@link SetOperation#wrap(Memory) SetOperation.wrap(Memory)}
   * @param srcMem Ref: {@link SetOperation#wrap(Memory) SetOperation.wrap(Memory)}, {@code srcMem}
   * @return {@link SetOperation SetOperation}
   */
  public static SetOperation wrapSetOperation(final WritableMemory srcMem) {
    return wrapSetOperation(srcMem, ThetaUtil.DEFAULT_UPDATE_SEED);
  }

  /**
   * Ref: {@link SetOperation#wrap(Memory, long) SetOperation.wrap(Memory, long)}
   * @param srcMem Ref: {@link SetOperation#wrap(Memory, long) SetOperation.wrap(Memory, long)},
   * {@code srcMem}
   * @param expectedSeed the seed used to validate the given Memory image.
   * Ref: {@link SetOperation#wrap(Memory, long) SetOperation.wrap(Memory, long)},
   * {@code expectedSeed}
   * @return {@link SetOperation SetOperation}
   */
  public static SetOperation wrapSetOperation(final WritableMemory srcMem, final long expectedSeed) {
    return SetOperation.wrap(srcMem, expectedSeed);
  }

  /**
   * Ref: {@link Sketch#wrap(Memory) Sketch.wrap(Memory)}
   * @param srcMem Ref: {@link Sketch#wrap(Memory) Sketch.wrap(Memory)}, {@code srcMem}
   * @return {@link Sketch Sketch}
   */
  public static Sketch wrapSketch(final Memory srcMem) {
    return Sketch.wrap(srcMem);
  }

  /**
   * Ref: {@link Sketch#wrap(Memory, long) Sketch.wrap(Memory, long)}
   * @param srcMem Ref: {@link Sketch#wrap(Memory, long) Sketch.wrap(Memory, long)}, {@code srcMem}
   * @param expectedSeed the expectedSeed used to validate the given Memory image.
   * Ref: {@link Sketch#wrap(Memory, long) Sketch.wrap(Memory, long)}, {@code expectedSeed}
   * @return {@link Sketch Sketch}
   */
  public static Sketch wrapSketch(final Memory srcMem, final long expectedSeed) {
    return Sketch.wrap(srcMem, expectedSeed);
  }

  /**
   * Convenience method, calls {@link SetOperation#wrap(Memory)} and casts the result to a Union
   * @param srcMem Ref: {@link SetOperation#wrap(Memory)}, {@code srcMem}
   * @return a Union backed by the given Memory
   */
  public static Union wrapUnion(final Memory srcMem) {
    return (Union) SetOperation.wrap(srcMem);
  }

  /**
   * Convenience method, calls {@link SetOperation#wrap(Memory)} and casts the result to a Union
   * @param srcMem Ref: {@link SetOperation#wrap(Memory)}, {@code srcMem}
   * @return a Union backed by the given Memory
   */
  public static Union wrapUnion(final WritableMemory srcMem) {
    return (Union) SetOperation.wrap(srcMem);
  }

  /**
   * Ref: {@link UpdateSketch#wrap(Memory) UpdateSketch.wrap(Memory)}
   * @param srcMem Ref: {@link UpdateSketch#wrap(Memory) UpdateSketch.wrap(Memory)}, {@code srcMem}
   * @return {@link UpdateSketch UpdateSketch}
   */
  public static UpdateSketch wrapUpdateSketch(final WritableMemory srcMem) {
    return wrapUpdateSketch(srcMem, ThetaUtil.DEFAULT_UPDATE_SEED);
  }

  /**
   * Ref: {@link UpdateSketch#wrap(Memory, long) UpdateSketch.wrap(Memory, long)}
   * @param srcMem Ref: {@link UpdateSketch#wrap(Memory, long) UpdateSketch.wrap(Memory, long)}, {@code srcMem}
   * @param expectedSeed the seed used to validate the given Memory image.
   * Ref: {@link UpdateSketch#wrap(Memory, long) UpdateSketch.wrap(Memory, long)}, {@code expectedSeed}
   * @return {@link UpdateSketch UpdateSketch}
   */
  public static UpdateSketch wrapUpdateSketch(final WritableMemory srcMem, final long expectedSeed) {
    return UpdateSketch.wrap(srcMem, expectedSeed);
  }

  //Restricted static methods

  static void checkIfValidThetaSketch(final Memory srcMem) {
    final int fam = srcMem.getByte(FAMILY_BYTE);
    if (!Sketch.isValidSketchID(fam)) {
     throw new SketchesArgumentException("Source Memory not a valid Sketch. Family: "
         + Family.idToFamily(fam).toString());
    }
  }

  static boolean getEmpty(final Memory srcMem) {
    final int serVer = srcMem.getByte(SER_VER_BYTE);
    if (serVer == 1) {
      return ((getThetaLong(srcMem) == Long.MAX_VALUE) && (getRetainedEntries(srcMem) == 0));
    }
    return (srcMem.getByte(FLAGS_BYTE) & EMPTY_FLAG_MASK) != 0; //for SerVer 2 & 3
  }

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
}
