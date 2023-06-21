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

package org.apache.datasketches.common;

import static org.apache.datasketches.common.ByteArrayUtil.putDoubleLE;
import static org.apache.datasketches.common.ByteArrayUtil.putFloatLE;
import static org.apache.datasketches.common.ByteArrayUtil.putIntLE;
import static org.apache.datasketches.common.ByteArrayUtil.putLongLE;
import static org.apache.datasketches.common.ByteArrayUtil.putShortLE;

import org.apache.datasketches.memory.Memory;

/**
 * Methods of serializing and deserializing arrays of the object version of primitive types of
 * Number. The array can be a mix of primitive object types.
 *
 * <p>This class serializes numbers with a leading byte (ASCII character) indicating the type.
 * The class keeps the values byte aligned, even though only 3 bits are strictly necessary to
 * encode one of the 6 different primitives with object types that extend Number.</p>
 *
 * <p>Classes handled are: <code>Long</code>, <code>Integer</code>, <code>Short</code>,
 * <code>Byte</code>, <code>Double</code>, and <code>Float</code>.</p>
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
  public byte[] serializeToByteArray(final Number item) {
    final byte[] byteArr;
    if (item instanceof Long) {
      byteArr = new byte[Long.BYTES + 1];
      byteArr[0] = LONG_INDICATOR;
      putLongLE(byteArr, 1, (Long)item);
    } else if (item instanceof Integer) {
      byteArr = new byte[Integer.BYTES + 1];
      byteArr[0] = INTEGER_INDICATOR;
      putIntLE(byteArr, 1, (Integer)item);
    } else if (item instanceof Short) {
      byteArr = new byte[Short.BYTES + 1];
      byteArr[0] = SHORT_INDICATOR;
      putShortLE(byteArr, 1, (Short)item);
    } else if (item instanceof Byte) {
      byteArr = new byte[Byte.BYTES + 1];
      byteArr[0] = BYTE_INDICATOR;
      byteArr[1] = (byte)item;
    } else if (item instanceof Double) {
      byteArr = new byte[Double.BYTES + 1];
      byteArr[0] = DOUBLE_INDICATOR;
      putDoubleLE(byteArr, 1, (Double)item);
    } else if (item instanceof Float) {
      byteArr = new byte[Float.BYTES + 1];
      byteArr[0] = FLOAT_INDICATOR;
      putFloatLE(byteArr, 1, (Float)item);
    } else {
      throw new SketchesArgumentException(
          "Item must be one of: Long, Integer, Short, Byte, Double, Float. "
          + "item: " + item.toString());
    }
    return byteArr;
  }

  @Override
  public byte[] serializeToByteArray(final Number[] items) {
    final int bytes = sizeOf(items);

    final byte[] byteArr = new byte[bytes];
    int offsetBytes = 0;
    for (final Number item: items) {
      if (item instanceof Long) {
        byteArr[offsetBytes++] = LONG_INDICATOR;
        putLongLE(byteArr, offsetBytes, item.longValue());
        offsetBytes += Long.BYTES;
      } else if (item instanceof Integer) {
        byteArr[offsetBytes++] = INTEGER_INDICATOR;
        putIntLE(byteArr, offsetBytes, item.intValue());
        offsetBytes += Integer.BYTES;
      } else if (item instanceof Short) {
        byteArr[offsetBytes++] = SHORT_INDICATOR;
        putShortLE(byteArr, offsetBytes, item.shortValue());
        offsetBytes += Short.BYTES;
      } else if (item instanceof Byte) {
        byteArr[offsetBytes++] = BYTE_INDICATOR;
        byteArr[offsetBytes] = item.byteValue();
        offsetBytes += Byte.BYTES;
      } else if (item instanceof Double) {
        byteArr[offsetBytes++] = DOUBLE_INDICATOR;
        putDoubleLE(byteArr, offsetBytes, item.doubleValue());
        offsetBytes += Double.BYTES;
      } else if (item instanceof Float) {
        byteArr[offsetBytes++] = FLOAT_INDICATOR;
        putFloatLE(byteArr, offsetBytes, item.floatValue());
        offsetBytes += Float.BYTES;
      } else {
        throw new SketchesArgumentException(
            "Item must be one of: Long, Integer, Short, Byte, Double, Float. "
            + "item: " + item.toString());
      }
    }
    return byteArr;
  }

  @Override
  public Number deserializeOneFromMemory(final Memory mem, final long offset) {
    final Number number;
    long offsetBytes = offset;
    Util.checkBounds(offsetBytes, Byte.BYTES, mem.getCapacity());
    final byte itemId = mem.getByte(offsetBytes);
    offsetBytes += Byte.BYTES;

    switch (itemId) {
    case LONG_INDICATOR:
      Util.checkBounds(offsetBytes, Long.BYTES, mem.getCapacity());
      number = mem.getLong(offsetBytes);
      break;
    case INTEGER_INDICATOR:
      Util.checkBounds(offsetBytes, Integer.BYTES, mem.getCapacity());
      number = mem.getInt(offsetBytes);
      break;
    case SHORT_INDICATOR:
      Util.checkBounds(offsetBytes, Short.BYTES, mem.getCapacity());
      number = mem.getShort(offsetBytes);
      break;
    case BYTE_INDICATOR:
      Util.checkBounds(offsetBytes, Byte.BYTES, mem.getCapacity());
      number = mem.getByte(offsetBytes);
      break;
    case DOUBLE_INDICATOR:
      Util.checkBounds(offsetBytes, Double.BYTES, mem.getCapacity());
      number = mem.getDouble(offsetBytes);
      break;
    case FLOAT_INDICATOR:
      Util.checkBounds(offsetBytes, Float.BYTES, mem.getCapacity());
      number = mem.getFloat(offsetBytes);
      break;
    default:
      throw new SketchesArgumentException(
          "Item must be one of: Long, Integer, Short, Byte, Double, Float. "
          + "itemId: " + itemId);
  }
    return number;
  }

  @Override
  @Deprecated
  public Number[] deserializeFromMemory(final Memory mem, final int numItems) {
    return deserializeFromMemory(mem, 0, numItems);
  }

  @Override
  public Number[] deserializeFromMemory(final Memory mem, final long offset, final int numItems) {
    final Number[] array = new Number[numItems];
    long offsetBytes = offset;
    for (int i = 0; i < numItems; i++) {
      Util.checkBounds(offsetBytes, Byte.BYTES, mem.getCapacity());
      final byte typeId = mem.getByte(offsetBytes);
      offsetBytes += Byte.BYTES;

      switch (typeId) {
        case LONG_INDICATOR:
          Util.checkBounds(offsetBytes, Long.BYTES, mem.getCapacity());
          array[i] = mem.getLong(offsetBytes);
          offsetBytes += Long.BYTES;
          break;
        case INTEGER_INDICATOR:
          Util.checkBounds(offsetBytes, Integer.BYTES, mem.getCapacity());
          array[i] = mem.getInt(offsetBytes);
          offsetBytes += Integer.BYTES;
          break;
        case SHORT_INDICATOR:
          Util.checkBounds(offsetBytes, Short.BYTES, mem.getCapacity());
          array[i] = mem.getShort(offsetBytes);
          offsetBytes += Short.BYTES;
          break;
        case BYTE_INDICATOR:
          Util.checkBounds(offsetBytes, Byte.BYTES, mem.getCapacity());
          array[i] = mem.getByte(offsetBytes);
          offsetBytes += Byte.BYTES;
          break;
        case DOUBLE_INDICATOR:
          Util.checkBounds(offsetBytes, Double.BYTES, mem.getCapacity());
          array[i] = mem.getDouble(offsetBytes);
          offsetBytes += Double.BYTES;
          break;
        case FLOAT_INDICATOR:
          Util.checkBounds(offsetBytes, Float.BYTES, mem.getCapacity());
          array[i] = mem.getFloat(offsetBytes);
          offsetBytes += Float.BYTES;
          break;
        default:
          throw new SketchesArgumentException(
              "Item must be one of: Long, Integer, Short, Byte, Double, Float. "
              + "index: " + i + ", typeId: " + typeId);
      }
    }
    return array;
  }

  @Override
  public int sizeOf(final Number item) {
    if ( item instanceof Long)         { return Byte.BYTES + Long.BYTES; }
    else if ( item instanceof Integer) { return Byte.BYTES + Integer.BYTES; }
    else if ( item instanceof Short)   { return Byte.BYTES + Short.BYTES; }
    else if ( item instanceof Byte)    { return Byte.BYTES + Byte.BYTES; }
    else if ( item instanceof Double)  { return Byte.BYTES + Double.BYTES; }
    else if ( item instanceof Float)   { return Byte.BYTES + Float.BYTES; }
    else { throw new SketchesArgumentException(
        "Item must be one of: Long, Integer, Short, Byte, Double, Float. "
        + "item: " + item.toString()); }
  }

  @Override
  public int sizeOf(final Number[] items) {
    int bytes = 0;
    for (final Number item: items) {
      if (item instanceof Long)         { bytes += Byte.BYTES + Long.BYTES; }
      else if (item instanceof Integer) { bytes += Byte.BYTES + Integer.BYTES; }
      else if (item instanceof Short)   { bytes += Byte.BYTES + Short.BYTES; }
      else if (item instanceof Byte)    { bytes += Byte.BYTES + Byte.BYTES; }
      else if (item instanceof Double)  { bytes += Byte.BYTES + Double.BYTES; }
      else if (item instanceof Float)   { bytes += Byte.BYTES + Float.BYTES; }
      else { throw new SketchesArgumentException(
          "Item must be one of: Long, Integer, Short, Byte, Double, Float. "
          + "item: " + item.toString()); }
    }
    return bytes;
  }

  @Override
  public int sizeOf(final Memory mem, final long offset, final int numItems) {
    long offsetBytes = offset;
    for (int i = 0; i < numItems; i++) {
      Util.checkBounds(offsetBytes, Byte.BYTES, mem.getCapacity());
      final byte typeId = mem.getByte(offsetBytes);
      offsetBytes += Byte.BYTES;

      switch (typeId) {
        case LONG_INDICATOR:
          Util.checkBounds(offsetBytes, Long.BYTES, mem.getCapacity());
          offsetBytes += Long.BYTES;
          break;
        case INTEGER_INDICATOR:
          Util.checkBounds(offsetBytes, Integer.BYTES, mem.getCapacity());
          offsetBytes += Integer.BYTES;
          break;
        case SHORT_INDICATOR:
          Util.checkBounds(offsetBytes, Short.BYTES, mem.getCapacity());
          offsetBytes += Short.BYTES;
          break;
        case BYTE_INDICATOR:
          Util.checkBounds(offsetBytes, Byte.BYTES, mem.getCapacity());
          offsetBytes += Byte.BYTES;
          break;
        case DOUBLE_INDICATOR:
          Util.checkBounds(offsetBytes, Double.BYTES, mem.getCapacity());
          offsetBytes += Double.BYTES;
          break;
        case FLOAT_INDICATOR:
          Util.checkBounds(offsetBytes, Float.BYTES, mem.getCapacity());
          offsetBytes += Float.BYTES;
          break;
        default:
          throw new SketchesArgumentException(
              "Item must be one of: Long, Integer, Short, Byte, Double, Float. "
              + "index: " + i + ", typeId: " + typeId);
      }
    }
    return (int)offsetBytes;
  }

}
