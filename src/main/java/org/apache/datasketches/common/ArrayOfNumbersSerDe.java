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

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_DOUBLE_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_FLOAT_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_INT_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_SHORT_UNALIGNED;
import static org.apache.datasketches.common.ByteArrayUtil.copyBytes;
import static org.apache.datasketches.common.ByteArrayUtil.putDoubleLE;
import static org.apache.datasketches.common.ByteArrayUtil.putFloatLE;
import static org.apache.datasketches.common.ByteArrayUtil.putIntLE;
import static org.apache.datasketches.common.ByteArrayUtil.putLongLE;
import static org.apache.datasketches.common.ByteArrayUtil.putShortLE;

import java.lang.foreign.MemorySegment;
import java.util.Objects;

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
    Objects.requireNonNull(item, "Item must not be null");
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
    Objects.requireNonNull(items, "Items must not be null");
    final int numItems = items.length;
    int totalBytes = 0;
    final byte[][] serialized2DArray = new byte[numItems][];
    for (int i = 0; i < numItems; i++) {
      serialized2DArray[i] = serializeToByteArray(items[i]);
      totalBytes += serialized2DArray[i].length;
    }
    final byte[] out = new byte[totalBytes];
    int offset = 0;
    for (int i = 0; i < numItems; i++) {
      final int itemLen = serialized2DArray[i].length;
      copyBytes(serialized2DArray[i], 0, out, offset, itemLen);
      offset += itemLen;
    }
    return out;
  }

  @Override
  public Number[] deserializeFromMemorySegment(final MemorySegment seg, final int numItems) {
    return deserializeFromMemorySegment(seg, 0, numItems);
  }

  @Override
  public Number[] deserializeFromMemorySegment(final MemorySegment seg, final long offsetBytes, final int numItems) {
    Objects.requireNonNull(seg, "MemorySegment must not be null");
    if (numItems <= 0) { return new Number[0]; }
    final Number[] array = new Number[numItems];
    long offset = offsetBytes;
    for (int i = 0; i < numItems; i++) {
      Util.checkBounds(offset, Byte.BYTES, seg.byteSize());
      final byte typeId = seg.get(JAVA_BYTE, offset);
      offset += Byte.BYTES;

      switch (typeId) {
        case LONG_INDICATOR:
          Util.checkBounds(offset, Long.BYTES, seg.byteSize());
          array[i] = seg.get(JAVA_LONG_UNALIGNED, offset);
          offset += Long.BYTES;
          break;
        case INTEGER_INDICATOR:
          Util.checkBounds(offset, Integer.BYTES, seg.byteSize());
          array[i] = seg.get(JAVA_INT_UNALIGNED, offset);
          offset += Integer.BYTES;
          break;
        case SHORT_INDICATOR:
          Util.checkBounds(offset, Short.BYTES, seg.byteSize());
          array[i] = seg.get(JAVA_SHORT_UNALIGNED, offset);
          offset += Short.BYTES;
          break;
        case BYTE_INDICATOR:
          Util.checkBounds(offset, Byte.BYTES, seg.byteSize());
          array[i] = seg.get(JAVA_BYTE, offset);
          offset += Byte.BYTES;
          break;
        case DOUBLE_INDICATOR:
          Util.checkBounds(offset, Double.BYTES, seg.byteSize());
          array[i] = seg.get(JAVA_DOUBLE_UNALIGNED, offset);
          offset += Double.BYTES;
          break;
        case FLOAT_INDICATOR:
          Util.checkBounds(offset, Float.BYTES, seg.byteSize());
          array[i] = seg.get(JAVA_FLOAT_UNALIGNED, offset);
          offset += Float.BYTES;
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
    Objects.requireNonNull(item, "Item must not be null");
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
    Objects.requireNonNull(items, "Items must not be null");
    int totalBytes = 0;
    for (final Number item : items) {
      totalBytes += sizeOf(item);
    }
    return totalBytes;
  }

  @Override
  public int sizeOf(final MemorySegment seg, final long offsetBytes, final int numItems) {
    Objects.requireNonNull(seg, "MemorySegment must not be null");
    long offset = offsetBytes;
    for (int i = 0; i < numItems; i++) {
      Util.checkBounds(offset, Byte.BYTES, seg.byteSize());
      final byte typeId = seg.get(JAVA_BYTE, offset);
      offset += Byte.BYTES;

      switch (typeId) {
        case LONG_INDICATOR:
          Util.checkBounds(offset, Long.BYTES, seg.byteSize());
          offset += Long.BYTES;
          break;
        case INTEGER_INDICATOR:
          Util.checkBounds(offset, Integer.BYTES, seg.byteSize());
          offset += Integer.BYTES;
          break;
        case SHORT_INDICATOR:
          Util.checkBounds(offset, Short.BYTES, seg.byteSize());
          offset += Short.BYTES;
          break;
        case BYTE_INDICATOR:
          Util.checkBounds(offset, Byte.BYTES, seg.byteSize());
          offset += Byte.BYTES;
          break;
        case DOUBLE_INDICATOR:
          Util.checkBounds(offset, Double.BYTES, seg.byteSize());
          offset += Double.BYTES;
          break;
        case FLOAT_INDICATOR:
          Util.checkBounds(offset, Float.BYTES, seg.byteSize());
          offset += Float.BYTES;
          break;
        default:
          throw new SketchesArgumentException(
              "Item must be one of: Long, Integer, Short, Byte, Double, Float. "
              + "index: " + i + ", typeId: " + typeId);
      }
    }
    return (int)(offset - offsetBytes);
  }

  @Override
  public String toString(final Number item) {
    if (item == null) { return "null"; }
    return item.toString();
  }

  @Override
  public Class<Number> getClassOfT() { return Number.class; }
}
