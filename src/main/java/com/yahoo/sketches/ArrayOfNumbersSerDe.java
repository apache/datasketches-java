/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches;

import com.yahoo.memory.Memory;
import com.yahoo.memory.UnsafeUtil;
import com.yahoo.memory.WritableMemory;

/**
 * Methods of serializing and deserializing arrays of the object version of primitive types of
 * Number.
 *
 * <p>This class serializes numbers with a leading byte (ASCII character) indicating the type.
 * The class keeps the values byte aligned, even though only 3 bits are strictly necessary to
 * encode one of the 6 different primitives with object types that extend Number.</p>
 *
 * <p>Classes handled are: <tt>Long</tt>, <tt>Integer</tt>, <tt>Short</tt>, <tt>Byte</tt>,
 * <tt>Double</tt>, and <tt>Float</tt>.</p>
 *
 * @author Jon Malkin
 */
public class ArrayOfNumbersSerDe extends ArrayOfItemsSerDe<Number> {

  // values selected to enable backwards compatibility
  private static final byte LONG_INDICATOR    = 12;
  private static final byte INTEGER_INDICATOR = 9;
  private static final byte SHORT_INDICATOR   = 3;
  private static final byte BYTE_INDICATOR    = 2;
  private static final byte DOUBLE_INDICATOR  = 4;
  private static final byte FLOAT_INDICATOR   = 6;

  @Override
  public byte[] serializeToByteArray(final Number[] items) {
    int length = 0;
    for (final Number item: items) {
      if (item instanceof Long) {
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
            "Item must be one of: Long, Integer, Short, Byte, Double, Float");
      }
    }
    final byte[] bytes = new byte[length];
    final WritableMemory mem = WritableMemory.wrap(bytes);
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
      } else { // (item instanceof Float) 0- already checked poosibilities above
        mem.putByte(offsetBytes, FLOAT_INDICATOR);
        mem.putFloat(offsetBytes + 1, item.floatValue());
        offsetBytes += Byte.BYTES + Float.BYTES;
      }
    }
    return bytes;
  }

  @Override
  public Number[] deserializeFromMemory(final Memory mem, final int length) {
    final Number[] array = new Number[length];
    long offsetBytes = 0;
    for (int i = 0; i < length; i++) {
      UnsafeUtil.checkBounds(offsetBytes, Byte.BYTES, mem.getCapacity());
      final byte numType = mem.getByte(offsetBytes);
      offsetBytes += Byte.BYTES;

      switch (numType) {
        case LONG_INDICATOR:
          UnsafeUtil.checkBounds(offsetBytes, Long.BYTES, mem.getCapacity());
          array[i] = mem.getLong(offsetBytes);
          offsetBytes += Long.BYTES;
          break;
        case INTEGER_INDICATOR:
          UnsafeUtil.checkBounds(offsetBytes, Integer.BYTES, mem.getCapacity());
          array[i] = mem.getInt(offsetBytes);
          offsetBytes += Integer.BYTES;
          break;
        case SHORT_INDICATOR:
          UnsafeUtil.checkBounds(offsetBytes, Short.BYTES, mem.getCapacity());
          array[i] = mem.getShort(offsetBytes);
          offsetBytes += Short.BYTES;
          break;
        case BYTE_INDICATOR:
          UnsafeUtil.checkBounds(offsetBytes, Byte.BYTES, mem.getCapacity());
          array[i] = mem.getByte(offsetBytes);
          offsetBytes += Byte.BYTES;
          break;
        case DOUBLE_INDICATOR:
          UnsafeUtil.checkBounds(offsetBytes, Double.BYTES, mem.getCapacity());
          array[i] = mem.getDouble(offsetBytes);
          offsetBytes += Double.BYTES;
          break;
        case FLOAT_INDICATOR:
          UnsafeUtil.checkBounds(offsetBytes, Float.BYTES, mem.getCapacity());
          array[i] = mem.getFloat(offsetBytes);
          offsetBytes += Float.BYTES;
          break;
        default:
          throw new SketchesArgumentException("Unrecognized entry type reading Number array entry "
              + i + ": " + numType);
      }
    }

    return array;
  }
}
