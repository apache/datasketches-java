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

package org.apache.datasketches.tuple;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.Family;
import org.apache.datasketches.common.SketchesArgumentException;

/**
 * Multipurpose serializer-deserializer for a collection of sketches defined by the enum.
 */
public final class SerializerDeserializer {

  /**
   * No argument constructor.
   */
  public SerializerDeserializer() { }

  /**
   * Defines the sketch classes that this SerializerDeserializer can handle.
   */
  public static enum SketchType {
    /** QuickSelectSketch */
    QuickSelectSketch,
    /** CompactTupleSketch */
    CompactTupleSketch,
    /** ArrayOfDoublesQuickSelectSketch */
    ArrayOfDoublesQuickSelectSketch,
    /** ArrayOfDoublesCompactSketch */
    ArrayOfDoublesCompactSketch,
    /** ArrayOfDoublesUnion */
    ArrayOfDoublesUnion }

  static final int TYPE_BYTE_OFFSET = 3;

  /**
   * Validates the preamble-Longs value given the family ID
   * @param familyId the given family ID
   * @param preambleLongs the given preambleLongs value
   */
  public static void validateFamily(final byte familyId, final byte preambleLongs) {
    final Family family = Family.idToFamily(familyId);
    if (family.equals(Family.TUPLE)) {
      if (preambleLongs < Family.TUPLE.getMinPreLongs() || preambleLongs > Family.TUPLE.getMaxPreLongs()) {
        throw new SketchesArgumentException(
            "Possible corruption: Invalid PreambleLongs value for family TUPLE: " + preambleLongs);
      }
    } else {
      throw new SketchesArgumentException(
          "Possible corruption: Invalid Family: " + family.toString());
    }
  }

  /**
   * Validates the sketch type byte versus the expected value
   * @param sketchTypeByte the given sketch type byte
   * @param expectedType the expected value
   */
  public static void validateType(final byte sketchTypeByte, final SketchType expectedType) {
    final SketchType sketchType = getSketchType(sketchTypeByte);
    if (!sketchType.equals(expectedType)) {
      throw new SketchesArgumentException("Sketch Type mismatch. Expected " + expectedType.name()
        + ", got " + sketchType.name());
    }
  }

  /**
   * Gets the sketch type byte from the given MemorySegment image
   * @param seg the given MemorySegment image
   * @return the SketchType
   */
  public static SketchType getSketchType(final MemorySegment seg) {
    final byte sketchTypeByte = seg.get(JAVA_BYTE, TYPE_BYTE_OFFSET);
    return getSketchType(sketchTypeByte);
  }

  private static SketchType getSketchType(final byte sketchTypeByte) {
    if ((sketchTypeByte < 0) || (sketchTypeByte >= SketchType.values().length)) {
      throw new SketchesArgumentException("Invalid Sketch Type " + sketchTypeByte);
    }
    return SketchType.values()[sketchTypeByte];
  }

}
