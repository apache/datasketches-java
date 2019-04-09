/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.tuple;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.SketchesArgumentException;

final class SerializerDeserializer {
  static enum SketchType { QuickSelectSketch, CompactSketch, ArrayOfDoublesQuickSelectSketch,
    ArrayOfDoublesCompactSketch, ArrayOfDoublesUnion }

  static final int TYPE_BYTE_OFFSET = 3;

  static void validateFamily(final byte familyId, final byte preambleLongs) {
    final Family family = Family.idToFamily(familyId);
    if (family.equals(Family.TUPLE)) {
      if (preambleLongs != Family.TUPLE.getMinPreLongs()) {
        throw new SketchesArgumentException(
            "Possible corruption: Invalid PreambleLongs value for family TUPLE: " + preambleLongs);
      }
    } else {
      throw new SketchesArgumentException(
          "Possible corruption: Invalid Family: " + family.toString());
    }
  }

  static void validateType(final byte sketchTypeByte, final SketchType expectedType) {
    final SketchType sketchType = getSketchType(sketchTypeByte);
    if (!sketchType.equals(expectedType)) {
      throw new SketchesArgumentException("Sketch Type mismatch. Expected " + expectedType.name()
        + ", got " + sketchType.name());
    }
  }

  static SketchType getSketchType(final Memory mem) {
    final byte sketchTypeByte = mem.getByte(TYPE_BYTE_OFFSET);
    return getSketchType(sketchTypeByte);
  }

  private static SketchType getSketchType(final byte sketchTypeByte) {
    if ((sketchTypeByte < 0) || (sketchTypeByte >= SketchType.values().length)) {
      throw new SketchesArgumentException("Invalid Sketch Type " + sketchTypeByte);
    }
    return SketchType.values()[sketchTypeByte];
  }

  // Deprecated methods below. Retained here to support reading legacy data.

  //  private static final Map<String, Method> deserializeMethodCache = new HashMap<>();

  //  static <T> DeserializeResult<T> deserializeFromMemory(final Memory mem, final int offset) {
  //    final int classNameLength = mem.getByte(offset);
  //    final byte[] classNameBuffer = new byte[classNameLength];
  //    mem.getByteArray(offset + 1, classNameBuffer, 0, classNameLength);
  //    final String className = new String(classNameBuffer, UTF_8);
  //    final DeserializeResult<T> result =
  //        deserializeFromMemory(mem, offset + classNameLength + 1, className);
  //    return new DeserializeResult<>(result.getObject(), result.getSize() + classNameLength + 1);
  //  }

  //  @SuppressWarnings("unchecked")
  //  static <T> DeserializeResult<T>
  //      deserializeFromMemory(final Memory mem, final int offset, final String className) {
  //    try {
  //      Method method = deserializeMethodCache.get(className);
  //      if (method == null) {
  //          method = Class.forName(className).getMethod("fromMemory", Memory.class);
  //          deserializeMethodCache.put(className, method);
  //      }
  //      return (DeserializeResult<T>)
  //          method.invoke(null, mem.region(offset, mem.getCapacity() - offset));
  //    } catch (final IllegalAccessException | SketchesArgumentException | InvocationTargetException
  //        | NoSuchMethodException | SecurityException | ClassNotFoundException e) {
  //      throw new SketchesArgumentException("Failed to deserialize class " + className + " " + e);
  //    }
  //  }

}
