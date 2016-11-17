/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches;

import com.yahoo.memory.Memory;
import com.yahoo.memory.NativeMemory;

/**
 * Methods of serializing and deserializing arrays of the object version of primitive types of Number.
 *
 * <p>This class serializes numbers with a leading byte (ASCII character) indicating the type. The class keeps
 * the values byte aligned, even though only 3 bits are strictly necessary to encode one of
 * the 6 different primitives with object types that extend Number.</p>
 *
 * <p>Classes handled are: <tt>Long</tt>, <tt>Integer</tt>, <tt>Short</tt>, <tt>Byte</tt>,
 * <tt>Double</tt>, and <tt>Float</tt>.</p>
 *
 * @author Jon Malkin
 */
public class ArrayOfNumbersSerDe extends ArrayOfItemsSerDe<Number> {

  private static final byte LONG_INDICATOR    = 'L' & 0x8F;
  private static final byte INTEGER_INDICATOR = 'I' & 0x8F;
  private static final byte SHORT_INDICATOR   = 'S' & 0x8F;
  private static final byte BYTE_INDICATOR    = 'B' & 0x8F;
  private static final byte DOUBLE_INDICATOR  = 'D' & 0x8F;
  private static final byte FLOAT_INDICATOR   = 'F' & 0x8F;

  @Override
  public byte[] serializeToByteArray(final Number[] items) {
    int length = 0;
    for (final Number item: items) {
      if (item == null) {
        length += Byte.BYTES;
      } else if (item instanceof Long) {
        length += Byte.BYTES + Long.BYTES;
      } else if (item instanceof Integer) {
        length += Byte.BYTES + Integer.BYTES;
      } else if (item instanceof Short) {
        length += Byte.BYTES + Short.BYTES;
      } else if (item instanceof Byte) {
        length += Byte.BYTES << 1;
      } else if (item instanceof Double) {
        length += Byte.BYTES + Double.BYTES;
      } else if (item instanceof Float) {
        length += Byte.BYTES + Float.BYTES;
      } else {
        throw new SketchesArgumentException(
            "Item must be one of: Long, Integer, Short, Byte, Double, Float, (null)");
      }
    }
    final byte[] bytes = new byte[length];
    final Memory mem = new NativeMemory(bytes);
    long offsetBytes = 0;
    for (final Number item: items) {
      if (item instanceof Long) {
        mem.putByte(offsetBytes, LONG_INDICATOR);
        mem.putLong(offsetBytes + 1, item.longValue());
        offsetBytes += Byte.BYTES + Long.BYTES;
      } else if (item instanceof Integer) {
        mem.putByte(offsetBytes, INTEGER_INDICATOR);
        mem.putInt(offsetBytes + 1, item.intValue());
        offsetBytes += Byte.BYTES + Integer.BYTES;
      } else if (item instanceof Short) {
        mem.putByte(offsetBytes, SHORT_INDICATOR);
        mem.putShort(offsetBytes + 1, item.shortValue());
        offsetBytes += Byte.BYTES + Short.BYTES;
      } else if (item instanceof Byte) {
        mem.putByte(offsetBytes, BYTE_INDICATOR);
        mem.putByte(offsetBytes + 1, item.byteValue());
        offsetBytes += Byte.BYTES << 1;
      } else if (item instanceof Double) {
        mem.putByte(offsetBytes, DOUBLE_INDICATOR);
        mem.putDouble(offsetBytes + 1, item.doubleValue());
        offsetBytes += Byte.BYTES + Double.BYTES;
      } else if (item instanceof Float) {
        mem.putByte(offsetBytes, FLOAT_INDICATOR);
        mem.putFloat(offsetBytes + 1, item.floatValue());
        offsetBytes += Byte.BYTES + Float.BYTES;
      } else {
        throw new SketchesArgumentException("Item must be one of: Long, Integer, Short, Byte, Double, Float");
      }
    }
    return bytes;
  }

  @Override
  public Number[] deserializeFromMemory(final Memory mem, final int length) {
    final Number[] array = new Number[length];
    long offsetBytes = 0;
    for (int i = 0; i < length; i++) {
      checkMemorySize(mem, offsetBytes);
      final byte numType = mem.getByte(offsetBytes);
      offsetBytes += Byte.BYTES;

      switch (numType) {
        case LONG_INDICATOR:
          checkMemorySize(mem, offsetBytes + Long.BYTES);
          array[i] = mem.getLong(offsetBytes);
          offsetBytes += Long.BYTES;
          break;
        case INTEGER_INDICATOR:
          checkMemorySize(mem, offsetBytes + Integer.BYTES);
          array[i] = mem.getInt(offsetBytes);
          offsetBytes += Integer.BYTES;
          break;
        case SHORT_INDICATOR:
          checkMemorySize(mem, offsetBytes + Short.BYTES);
          array[i] = mem.getShort(offsetBytes);
          offsetBytes += Short.BYTES;
          break;
        case BYTE_INDICATOR:
          checkMemorySize(mem, offsetBytes);
          array[i] = mem.getByte(offsetBytes);
          offsetBytes += Byte.BYTES;
          break;
        case DOUBLE_INDICATOR:
          checkMemorySize(mem, offsetBytes + Double.BYTES);
          array[i] = mem.getDouble(offsetBytes);
          offsetBytes += Double.BYTES;
          break;
        case FLOAT_INDICATOR:
          checkMemorySize(mem, offsetBytes + Float.BYTES);
          array[i] = mem.getFloat(offsetBytes);
          offsetBytes += Float.BYTES;
          break;
        default:
          throw new SketchesArgumentException("Unrecognized entry type reading Number array entry " + i + ": "
                  + numType);
      }
    }

    return array;
  }

}
