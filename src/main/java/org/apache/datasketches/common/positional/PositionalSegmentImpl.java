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

package org.apache.datasketches.common.positional;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_CHAR_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_DOUBLE_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_FLOAT_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_INT_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_SHORT_UNALIGNED;

import java.lang.foreign.MemorySegment;

/**
 * Implementation of PositionalSegment
 */
final class PositionalSegmentImpl extends PositionalImpl implements PositionalSegment {
  private static final byte SHORT_SHIFT     = 1;
  private static final byte CHAR_SHIFT      = 1;
  private static final byte INT_SHIFT       = 2;
  private static final byte LONG_SHIFT      = 3;
  private static final byte FLOAT_SHIFT     = 2;
  private static final byte DOUBLE_SHIFT    = 3;

  private final MemorySegment seg;

  /**
   * Constructs with the given MemorySegment
   * @param seg the given MemorySegment
   */
  PositionalSegmentImpl(final MemorySegment seg) {
    super(seg.byteSize());
    this.seg = seg;
  }

  @Override
  public PositionalSegment asSlice() {
    final MemorySegment slice = seg.asSlice(getPosition());
    return new PositionalSegmentImpl(slice);
  }

  @Override
  public MemorySegment getMemorySegment() {
    return seg;
  }

  //PRIMITIVE getX() and getXArray()

  @Override
  public boolean getBoolean() {
    return getByte() != 0;
  }

  @Override
  public boolean getBoolean(final long offsetBytes) {
    return getByte(offsetBytes) != 0;
  }

  @Override
  public byte getByte() {
    final byte aByte = seg.get(JAVA_BYTE, getPosition());
    incrementPosition(Byte.BYTES);
    return aByte;
  }

  @Override
  public byte getByte(final long offsetBytes) {
    return seg.get(JAVA_BYTE, offsetBytes);
  }

  @Override
  public void getByteArray(final byte[] dstArray, final int dstOffsetBytes, final int lengthBytes) {
    MemorySegment.copy(seg, JAVA_BYTE, getPosition(), dstArray, dstOffsetBytes, lengthBytes);
    incrementPosition(lengthBytes);
  }

  @Override
  public char getChar() {
    final char achar = seg.get(JAVA_CHAR_UNALIGNED, getPosition());
    incrementPosition(Character.BYTES);
    return achar;
  }

  @Override
  public char getChar(final long offsetBytes) {
    return seg.get(JAVA_CHAR_UNALIGNED, offsetBytes);
  }

  @Override
  public void getCharArray(final char[] dstArray, final int dstOffsetChars, final int lengthChars) {
    MemorySegment.copy(seg, JAVA_CHAR_UNALIGNED, getPosition(), dstArray, dstOffsetChars, lengthChars);
    incrementPosition(lengthChars << CHAR_SHIFT);
  }

  @Override
  public double getDouble() {
    final double dbl = seg.get(JAVA_DOUBLE_UNALIGNED, getPosition());
    incrementPosition(Double.BYTES);
    return dbl;
  }

  @Override
  public double getDouble(final long offsetBytes) {
    return seg.get(JAVA_DOUBLE_UNALIGNED, offsetBytes);
  }

  @Override
  public void getDoubleArray(final double[] dstArray, final int dstOffsetDoubles, final int lengthDoubles) {
    MemorySegment.copy(seg, JAVA_DOUBLE_UNALIGNED, getPosition(), dstArray, dstOffsetDoubles, lengthDoubles);
    incrementPosition(lengthDoubles << DOUBLE_SHIFT);
  }

  @Override
  public float getFloat() {
    final float flt = seg.get(JAVA_FLOAT_UNALIGNED, getPosition());
    incrementPosition(Float.BYTES);
    return flt;
  }

  @Override
  public float getFloat(final long offsetBytes) {
    return seg.get(JAVA_FLOAT_UNALIGNED, offsetBytes);
  }

  @Override
  public void getFloatArray(final float[] dstArray, final int dstOffsetFloats, final int lengthFloats) {
    MemorySegment.copy(seg, JAVA_FLOAT_UNALIGNED, getPosition(), dstArray, dstOffsetFloats, lengthFloats);
    incrementPosition(lengthFloats << FLOAT_SHIFT);
  }

  @Override
  public int getInt() {
    final int i = seg.get(JAVA_INT_UNALIGNED, getPosition());
    incrementPosition(Integer.BYTES);
    return i;
  }

  @Override
  public int getInt(final long offsetBytes) {
    return seg.get(JAVA_INT_UNALIGNED, offsetBytes);
  }

  @Override
  public void getIntArray(final int[] dstArray, final int dstOffsetInts, final int lengthInts) {
    MemorySegment.copy(seg, JAVA_INT_UNALIGNED, getPosition(), dstArray, dstOffsetInts, lengthInts);
    incrementPosition(lengthInts << INT_SHIFT);
  }

  @Override
  public long getLong() {
    final long along = seg.get(JAVA_LONG_UNALIGNED, getPosition());
    incrementPosition(Long.BYTES);
    return along;
  }

  @Override
  public long getLong(final long offsetBytes) {
    return seg.get(JAVA_LONG_UNALIGNED, offsetBytes);
  }

  @Override
  public void getLongArray(final long[] dstArray, final int dstOffsetLongs, final int lengthLongs) {
    MemorySegment.copy(seg, JAVA_LONG_UNALIGNED, getPosition(), dstArray, dstOffsetLongs, lengthLongs);
    incrementPosition(lengthLongs << LONG_SHIFT);
  }

  @Override
  public short getShort() {
    final short ashort = seg.get(JAVA_SHORT_UNALIGNED, getPosition());
    incrementPosition(Short.BYTES);
    return ashort;
  }

  @Override
  public short getShort(final long offsetBytes) {
    return seg.get(JAVA_SHORT_UNALIGNED, offsetBytes);
  }

  @Override
  public void getShortArray(final short[] dstArray, final int dstOffsetShorts, final int lengthShorts) {
    MemorySegment.copy(seg, JAVA_SHORT_UNALIGNED, getPosition(), dstArray, dstOffsetShorts, lengthShorts);
    incrementPosition(lengthShorts << SHORT_SHIFT);
  }

  //PRIMITIVE putX() and putXArray() implementations

  @Override
  public void setBoolean(final boolean value) {
    setByte(value ? (byte)1 : 0);
  }

  @Override
  public void setBoolean(final long offsetBytes, final boolean value) {
    setByte(offsetBytes, value ? (byte)1 : 0);
  }

  @Override
  public void setByte(final byte value) {
    seg.set(JAVA_BYTE, getPosition(), value);
    incrementPosition(Byte.BYTES);
  }

  @Override
  public void setByte(final long offsetBytes, final byte value) {
    seg.set(JAVA_BYTE, offsetBytes, value);
  }

  @Override
  public void setByteArray(final byte[] srcArray, final int srcOffsetBytes, final int lengthBytes) {
    MemorySegment.copy(srcArray, srcOffsetBytes, seg, JAVA_BYTE, getPosition(), lengthBytes);
    incrementPosition(lengthBytes);
  }

  @Override
  public void setChar(final char value) {
    seg.set(JAVA_CHAR_UNALIGNED, getPosition(), value);
    incrementPosition(Character.BYTES);
  }

  @Override
  public void setChar(final long offsetBytes, final char value) {
    seg.set(JAVA_CHAR_UNALIGNED, offsetBytes, value);
  }

  @Override
  public void setCharArray(final char[] srcArray, final int srcOffsetChars, final int lengthChars) {
    MemorySegment.copy(srcArray, srcOffsetChars, seg, JAVA_CHAR_UNALIGNED, getPosition(), lengthChars);
    incrementPosition(lengthChars << CHAR_SHIFT);
  }

  @Override
  public void setDouble(final double value) {
    seg.set(JAVA_DOUBLE_UNALIGNED, getPosition(), value);
    incrementPosition(Double.BYTES);
  }

  @Override
  public void setDouble(final long offsetBytes, final double value) {
    seg.set(JAVA_DOUBLE_UNALIGNED, offsetBytes, value);
  }

  @Override
  public void setDoubleArray(final double[] srcArray, final int srcOffsetDoubles, final int lengthDoubles) {
    MemorySegment.copy(srcArray, srcOffsetDoubles, seg, JAVA_DOUBLE_UNALIGNED, getPosition(), lengthDoubles);
    incrementPosition(lengthDoubles << DOUBLE_SHIFT);
  }

  @Override
  public void setFloat(final float value) {
    seg.set(JAVA_FLOAT_UNALIGNED, getPosition(), value);
    incrementPosition(Float.BYTES);
  }

  @Override
  public void setFloat(final long offsetBytes, final float value) {
    seg.set(JAVA_FLOAT_UNALIGNED, offsetBytes, value);
  }

  @Override
  public void setFloatArray(final float[] srcArray, final int srcOffsetFloats, final int lengthFloats) {
    MemorySegment.copy(srcArray, srcOffsetFloats, seg, JAVA_FLOAT_UNALIGNED, getPosition(), lengthFloats);
    incrementPosition(lengthFloats << FLOAT_SHIFT);
  }

  @Override
  public void setInt(final int value) {
    seg.set(JAVA_INT_UNALIGNED, getPosition(), value);
    incrementPosition(Integer.BYTES);
  }

  @Override
  public void setInt(final long offsetBytes, final int value) {
    seg.set(JAVA_INT_UNALIGNED, offsetBytes, value);
  }

  @Override
  public void setIntArray(final int[] srcArray, final int srcOffsetInts, final int lengthInts) {
    MemorySegment.copy(srcArray, srcOffsetInts, seg, JAVA_INT_UNALIGNED, getPosition(), lengthInts);
    incrementPosition(lengthInts << INT_SHIFT);
  }

  @Override
  public void setLong(final long value) {
    seg.set(JAVA_LONG_UNALIGNED, getPosition(), value);
    incrementPosition(Long.BYTES);
  }

  @Override
  public void setLong(final long offsetBytes, final long value) {
    seg.set(JAVA_LONG_UNALIGNED, offsetBytes, value);
  }

  @Override
  public void setLongArray(final long[] srcArray, final int srcOffsetLongs, final int lengthLongs) {
    MemorySegment.copy(srcArray, srcOffsetLongs, seg, JAVA_LONG_UNALIGNED, getPosition(), lengthLongs);
    incrementPosition(lengthLongs << LONG_SHIFT);
  }

  @Override
  public void setShort(final short value) {
    seg.set(JAVA_SHORT_UNALIGNED, getPosition(), value);
    incrementPosition(Short.BYTES);
  }

  @Override
  public void setShort(final long offsetBytes, final short value) {
    seg.set(JAVA_SHORT_UNALIGNED, offsetBytes, value);
  }

  @Override
  public void setShortArray(final short[] srcArray, final int srcOffsetShorts, final int lengthShorts) {
    MemorySegment.copy(srcArray, srcOffsetShorts, seg, JAVA_SHORT_UNALIGNED, getPosition(), lengthShorts);
    incrementPosition(lengthShorts << SHORT_SHIFT);
  }

}
