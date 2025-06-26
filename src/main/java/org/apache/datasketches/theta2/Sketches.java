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

package org.apache.datasketches.theta2;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_INT_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED;
import static org.apache.datasketches.theta2.PreambleUtil.EMPTY_FLAG_MASK;
import static org.apache.datasketches.theta2.PreambleUtil.FAMILY_BYTE;
import static org.apache.datasketches.theta2.PreambleUtil.FLAGS_BYTE;
import static org.apache.datasketches.theta2.PreambleUtil.PREAMBLE_LONGS_BYTE;
import static org.apache.datasketches.theta2.PreambleUtil.RETAINED_ENTRIES_INT;
import static org.apache.datasketches.theta2.PreambleUtil.SER_VER_BYTE;
import static org.apache.datasketches.theta2.PreambleUtil.THETA_LONG;

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.Family;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.thetacommon2.ThetaUtil;

/**
 * This class brings together the common sketch and set operation creation methods and
 * the public static methods into one place.
 *
 * @author Lee Rhodes
 */
public final class Sketches {

  private Sketches() {}

  /**
   * Gets the unique count estimate from a valid MemorySegment image of a Sketch
   * @param srcSeg the source MemorySegment
   * @return the sketch's best estimate of the cardinality of the input stream.
   */
  public static double getEstimate(final MemorySegment srcSeg) {
    checkIfValidThetaSketch(srcSeg);
    return Sketch.estimate(getThetaLong(srcSeg), getRetainedEntries(srcSeg));
  }

  /**
   * Gets the approximate lower error bound from a valid MemorySegment image of a Sketch
   * given the specified number of Standard Deviations.
   * This will return getEstimate() if isEmpty() is true.
   *
   * @param numStdDev
   * <a href="{@docRoot}/resources/dictionary.html#numStdDev">See Number of Standard Deviations</a>
   * @param srcSeg the source MemorySegment
   * @return the lower bound.
   */
  public static double getLowerBound(final int numStdDev, final MemorySegment srcSeg) {
    return Sketch.lowerBound(getRetainedEntries(srcSeg), getThetaLong(srcSeg), numStdDev, getEmpty(srcSeg));
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
   * Ref: {@link Sketch#getSerializationVersion(MemorySegment)}
   * @param srcSeg Ref: {@link Sketch#getSerializationVersion(MemorySegment)}, {@code srcSeg}
   * @return Ref: {@link Sketch#getSerializationVersion(MemorySegment)}
   */
  public static int getSerializationVersion(final MemorySegment srcSeg) {
    return Sketch.getSerializationVersion(srcSeg);
  }

  /**
   * Gets the approximate upper error bound from a valid MemorySegment image of a Sketch
   * given the specified number of Standard Deviations.
   * This will return getEstimate() if isEmpty() is true.
   *
   * @param numStdDev
   * <a href="{@docRoot}/resources/dictionary.html#numStdDev">See Number of Standard Deviations</a>
   * @param srcSeg the source MemorySegment
   * @return the upper bound.
   */
  public static double getUpperBound(final int numStdDev, final MemorySegment srcSeg) {
    return Sketch.upperBound(getRetainedEntries(srcSeg), getThetaLong(srcSeg), numStdDev, getEmpty(srcSeg));
  }

  //Heapify Operations

  /**
   * Ref: {@link CompactSketch#heapify(MemorySegment) CompactSketch.heapify(MemorySegment)}
   * @param srcSeg Ref: {@link CompactSketch#heapify(MemorySegment) CompactSketch.heapify(MemorySegment)}, {@code srcSeg}
   * @return {@link CompactSketch CompactSketch}
   */
  public static CompactSketch heapifyCompactSketch(final MemorySegment srcSeg) {
    return CompactSketch.heapify(srcSeg);
  }

  /**
   * Ref: {@link CompactSketch#heapify(MemorySegment, long) CompactSketch.heapify(MemorySegment, long)}
   * @param srcSeg Ref: {@link CompactSketch#heapify(MemorySegment, long) CompactSketch.heapify(MemorySegment, long)}, {@code srcSeg}
   * @param expectedSeed Ref: {@link CompactSketch#heapify(MemorySegment, long) CompactSketch.heapify(MemorySegment, long)},
   * {@code expectedSeed}
   * @return {@link CompactSketch CompactSketch}
   */
  public static CompactSketch heapifyCompactSketch(final MemorySegment srcSeg, final long expectedSeed) {
    return CompactSketch.heapify(srcSeg, expectedSeed);
  }

  /**
   * Ref: {@link CompactSketch#wrap(MemorySegment) CompactSketch.wrap(MemorySegment)}
   * @param srcSeg Ref: {@link CompactSketch#wrap(MemorySegment) CompactSketch.wrap(MemorySegment)}, {@code srcSeg}
   * @return {@link CompactSketch CompactSketch}
   */
  public static CompactSketch wrapCompactSketch(final MemorySegment srcSeg) {
    return CompactSketch.wrap(srcSeg);
  }

  /**
   * Ref: {@link CompactSketch#wrap(MemorySegment, long) CompactSketch.wrap(MemorySegment, long)}
   * @param srcSeg Ref: {@link CompactSketch#wrap(MemorySegment, long) CompactSketch.wrap(MemorySegment, long)}, {@code srcSeg}
   * @param expectedSeed Ref: {@link CompactSketch#wrap(MemorySegment, long) CompactSketch.wrap(MemorySegment, long)},
   * {@code expectedSeed}
   * @return {@link CompactSketch CompactSketch}
   */
  public static CompactSketch wrapCompactSketch(final MemorySegment srcSeg, final long expectedSeed) {
    return CompactSketch.wrap(srcSeg, expectedSeed);
  }

  /**
   * Ref: {@link SetOperation#heapify(MemorySegment) SetOperation.heapify(MemorySegment)}
   * @param srcSeg Ref: {@link SetOperation#heapify(MemorySegment) SetOperation.heapify(MemorySegment)}, {@code srcSeg}
   * @return {@link SetOperation SetOperation}
   */
  public static SetOperation heapifySetOperation(final MemorySegment srcSeg) {
    return SetOperation.heapify(srcSeg);
  }

  /**
   * Ref: {@link SetOperation#heapify(MemorySegment, long) SetOperation.heapify(MemorySegment, long)}
   * @param srcSeg Ref: {@link SetOperation#heapify(MemorySegment, long) SetOperation.heapify(MemorySegment, long)},
   * {@code srcSeg}
   * @param expectedSeed the seed used to validate the given MemorySegment image.
   * Ref: {@link SetOperation#heapify(MemorySegment, long) SetOperation.heapify(MemorySegment, long)},
   * {@code expectedSeed}
   * @return {@link SetOperation SetOperation}
   */
  public static SetOperation heapifySetOperation(final MemorySegment srcSeg, final long expectedSeed) {
    return SetOperation.heapify(srcSeg, expectedSeed);
  }

  /**
   * Ref: {@link Sketch#heapify(MemorySegment) Sketch.heapify(MemorySegment)}
   * @param srcSeg Ref: {@link Sketch#heapify(MemorySegment) Sketch.heapify(MemorySegment)}, {@code srcSeg}
   * @return {@link Sketch Sketch}
   */
  public static Sketch heapifySketch(final MemorySegment srcSeg) {
    return Sketch.heapify(srcSeg);
  }

  /**
   * Ref: {@link Sketch#heapify(MemorySegment, long) Sketch.heapify(MemorySegment, long)}
   * @param srcSeg Ref: {@link Sketch#heapify(MemorySegment, long) Sketch.heapify(MemorySegment, long)}, {@code srcSeg}
   * @param expectedSeed the seed used to validate the given MemorySegment image.
   * Ref: {@link Sketch#heapify(MemorySegment, long) Sketch.heapify(MemorySegment, long)}, {@code expectedSeed}
   * @return {@link Sketch Sketch}
   */
  public static Sketch heapifySketch(final MemorySegment srcSeg, final long expectedSeed) {
    return Sketch.heapify(srcSeg, expectedSeed);
  }

  /**
   * Ref: {@link UpdateSketch#heapify(MemorySegment) UpdateSketch.heapify(MemorySegment)}
   * @param srcSeg Ref: {@link UpdateSketch#heapify(MemorySegment) UpdateSketch.heapify(MemorySegment)}, {@code srcSeg}
   * @return {@link UpdateSketch UpdateSketch}
   */
  public static UpdateSketch heapifyUpdateSketch(final MemorySegment srcSeg) {
    return UpdateSketch.heapify(srcSeg);
  }

  /**
   * Ref: {@link UpdateSketch#heapify(MemorySegment, long) UpdateSketch.heapify(MemorySegment, long)}
   * @param srcSeg Ref: {@link UpdateSketch#heapify(MemorySegment, long) UpdateSketch.heapify(MemorySegment, long)},
   *   {@code srcSeg}
   * @param expectedSeed the seed used to validate the given MemorySegment image.
   * Ref: {@link UpdateSketch#heapify(MemorySegment, long) UpdateSketch.heapify(MemorySegment, long)},
   *   {@code expectedSeed}
   * @return {@link UpdateSketch UpdateSketch}
   */
  public static UpdateSketch heapifyUpdateSketch(final MemorySegment srcSeg, final long expectedSeed) {
    return UpdateSketch.heapify(srcSeg, expectedSeed);
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
   * Convenience method, calls {@link SetOperation#wrap(MemorySegment)} and casts the result to a Intersection
   * @param srcSeg Ref: {@link SetOperation#wrap(MemorySegment)}, {@code srcSeg}
   * @return a Intersection backed by the given MemorySegment
   */
  public static Intersection wrapIntersection(final MemorySegment srcSeg) {
    return (Intersection) SetOperation.wrap(srcSeg);
  }

  /**
   * Ref: {@link SetOperation#wrap(MemorySegment) SetOperation.wrap(MemorySegment)}
   * @param srcSeg Ref: {@link SetOperation#wrap(MemorySegment) SetOperation.wrap(MemorySegment)}, {@code srcSeg}
   * @return {@link SetOperation SetOperation}
   */
  public static SetOperation wrapSetOperation(final MemorySegment srcSeg) {
    return wrapSetOperation(srcSeg, ThetaUtil.DEFAULT_UPDATE_SEED);
  }

  /**
   * Ref: {@link SetOperation#wrap(MemorySegment, long) SetOperation.wrap(MemorySegment, long)}
   * @param srcSeg Ref: {@link SetOperation#wrap(MemorySegment, long) SetOperation.wrap(MemorySegment, long)}, {@code srcSeg}
   * @param expectedSeed the seed used to validate the given MemorySegment image.
   * Ref: {@link SetOperation#wrap(MemorySegment, long) SetOperation.wrap(MemorySegment, long)}, {@code expectedSeed}
   * @return {@link SetOperation SetOperation}
   */
  public static SetOperation wrapSetOperation(final MemorySegment srcSeg, final long expectedSeed) {
    return SetOperation.wrap(srcSeg, expectedSeed);
  }

  /**
   * Ref: {@link Sketch#wrap(MemorySegment) Sketch.wrap(MemorySegment)}
   * @param srcSeg Ref: {@link Sketch#wrap(MemorySegment) Sketch.wrap(MemorySegment)}, {@code srcSeg}
   * @return {@link Sketch Sketch}
   */
  public static Sketch wrapSketch(final MemorySegment srcSeg) {
    return Sketch.wrap(srcSeg);
  }

  /**
   * Ref: {@link Sketch#wrap(MemorySegment, long) Sketch.wrap(MemorySegment, long)}
   * @param srcSeg Ref: {@link Sketch#wrap(MemorySegment, long) Sketch.wrap(MemorySegment, long)}, {@code srcSeg}
   * @param expectedSeed the expectedSeed used to validate the given MemorySegment image.
   * Ref: {@link Sketch#wrap(MemorySegment, long) Sketch.wrap(MemorySegment, long)}, {@code expectedSeed}
   * @return {@link Sketch Sketch}
   */
  public static Sketch wrapSketch(final MemorySegment srcSeg, final long expectedSeed) {
    return Sketch.wrap(srcSeg, expectedSeed);
  }

  /**
   * Convenience method, calls {@link SetOperation#wrap(MemorySegment)} and casts the result to a Union
   * @param srcSeg Ref: {@link SetOperation#wrap(MemorySegment)}, {@code srcSeg}
   * @return a Union backed by the given MemorySegment.
   */
  public static Union wrapUnion(final MemorySegment srcSeg) {
    return (Union) SetOperation.wrap(srcSeg);
  }

  /**
   * Ref: {@link UpdateSketch#wrap(MemorySegment) UpdateSketch.wrap(MemorySegment)}
   * @param srcSeg Ref: {@link UpdateSketch#wrap(MemorySegment) UpdateSketch.wrap(MemorySegment)}, {@code srcSeg}
   * @return {@link UpdateSketch UpdateSketch}
   */
  public static UpdateSketch wrapUpdateSketch(final MemorySegment srcSeg) {
    return wrapUpdateSketch(srcSeg, ThetaUtil.DEFAULT_UPDATE_SEED);
  }

  /**
   * Ref: {@link UpdateSketch#wrap(MemorySegment, long) UpdateSketch.wrap(MemorySegment, long)}
   * @param srcSeg Ref: {@link UpdateSketch#wrap(MemorySegment, long) UpdateSketch.wrap(MemorySegment, long)}, {@code srcSeg}
   * @param expectedSeed the seed used to validate the given MemorySegment image.
   * Ref: {@link UpdateSketch#wrap(MemorySegment, long) UpdateSketch.wrap(MemorySegment, long)}, {@code expectedSeed}
   * @return {@link UpdateSketch UpdateSketch}
   */
  public static UpdateSketch wrapUpdateSketch(final MemorySegment srcSeg, final long expectedSeed) {
    return UpdateSketch.wrap(srcSeg, expectedSeed);
  }

  //Restricted static methods

  static void checkIfValidThetaSketch(final MemorySegment srcSeg) {
    final int fam = srcSeg.get(JAVA_BYTE, FAMILY_BYTE);
    if (!Sketch.isValidSketchID(fam)) {
     throw new SketchesArgumentException("Source MemorySegment not a valid Sketch. Family: "
         + Family.idToFamily(fam).toString());
    }
  }

  static boolean getEmpty(final MemorySegment srcSeg) {
    final int serVer = srcSeg.get(JAVA_BYTE, SER_VER_BYTE);
    if (serVer == 1) {
      return ((getThetaLong(srcSeg) == Long.MAX_VALUE) && (getRetainedEntries(srcSeg) == 0));
    }
    return (srcSeg.get(JAVA_BYTE, FLAGS_BYTE) & EMPTY_FLAG_MASK) != 0; //for SerVer 2 & 3
  }

  static int getPreambleLongs(final MemorySegment srcSeg) {
    return srcSeg.get(JAVA_BYTE, PREAMBLE_LONGS_BYTE) & 0X3F; //for SerVer 1,2,3
  }

  static int getRetainedEntries(final MemorySegment srcSeg) {
    final int serVer = srcSeg.get(JAVA_BYTE, SER_VER_BYTE);
    if (serVer == 1) {
      final int entries = srcSeg.get(JAVA_INT_UNALIGNED, RETAINED_ENTRIES_INT);
      if ((getThetaLong(srcSeg) == Long.MAX_VALUE) && (entries == 0)) {
        return 0;
      }
      return entries;
    }
    //SerVer 2 or 3
    final int preLongs = getPreambleLongs(srcSeg);
    final boolean empty = (srcSeg.get(JAVA_BYTE, FLAGS_BYTE) & EMPTY_FLAG_MASK) != 0; //for SerVer 2 & 3
    if (preLongs == 1) {
      return empty ? 0 : 1;
    }
    //preLongs > 1
    return srcSeg.get(JAVA_INT_UNALIGNED, RETAINED_ENTRIES_INT); //for SerVer 1,2,3
  }

  static long getThetaLong(final MemorySegment srcSeg) {
    final int preLongs = getPreambleLongs(srcSeg);
    return (preLongs < 3) ? Long.MAX_VALUE : srcSeg.get(JAVA_LONG_UNALIGNED, THETA_LONG); //for SerVer 1,2,3
  }
}
