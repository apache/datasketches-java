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

import java.lang.foreign.MemorySegment;

/**
 * Defines the API for relative positional access to a MemorySegment.
 *
 * @author Lee Rhodes
 */
public interface PositionalSegment extends Positional {

  /**
   * Gets an instance of this PositionalSegment.
   * @param seg the given MemorySegment to create the PositionalSegment from.
   * @return a new PositionalSegment.
   */
  static PositionalSegment wrap(final MemorySegment seg) {
    return new PositionalSegmentImpl(seg);
  }

  /**
   * Returns a slice of this PositionalSegment at the current <i>position</i>.
   * The end of the slice is the end of the underlying segment.
   * @return a slice of this PositionalSegment at the current <i>position</i>.
   */
  PositionalSegment asSlice();

  /**
   * Returns the underlying MemorySegment.
   * The current <i>start</i>, <i>position</i> and <i>end</i> are ignored.
   * @return the underlying MemorySegment
   */
  MemorySegment getMemorySegment();

  //PRIMITIVE getX() and getXArray()

  /**
   * Gets the boolean value at the current position.
   * Increments the position by <i>Byte.BYTES</i>.
   * @return the boolean at the current position
   */
  boolean getBoolean();

  /**
   * Gets the boolean value at the given offset.
   * This does not change the position.
   * @param offsetBytes offset bytes relative to this MemorySegment start
   * @return the boolean at the given offset
   */
  boolean getBoolean(long offsetBytes);

  //intentionally removed getBooleanArray(...)

  /**
   * Gets the byte value at the current position.
   * Increments the position by <i>Byte.BYTES</i>.
   * @return the byte at the current position
   */
  byte getByte();

  /**
   * Gets the byte value at the given offset.
   * This does not change the position.
   * @param offsetBytes offset bytes relative to this MemorySegment start
   * @return the byte at the given offset
   */
  byte getByte(long offsetBytes);

  /**
   * Gets the byte array at the current position.
   * Increments the position by <i>Byte.BYTES * (lengthBytes - dstOffsetBytes)</i>.
   * @param dstArray The preallocated destination array.
   * @param dstOffsetBytes offset in array units
   * @param lengthBytes number of array units to transfer
   */
  void getByteArray(
      byte[] dstArray,
      int dstOffsetBytes,
      int lengthBytes);

  /**
   * Gets the char value at the current position.
   * Increments the position by <i>Character.BYTES</i>.
   * @return the char at the current position
   */
  char getChar();

  /**
   * Gets the char value at the given offset.
   * This does not change the position.
   * @param offsetBytes offset bytes relative to this MemorySegment start
   * @return the char at the given offset
   */
  char getChar(long offsetBytes);

  /**
   * Gets the char array at the current position.
   * Increments the position by <i>Character.BYTES * (lengthChars - dstOffsetChars)</i>.
   * @param dstArray The preallocated destination array.
   * @param dstOffsetChars offset in array units
   * @param lengthChars number of array units to transfer
   */
  void getCharArray(
      char[] dstArray,
      int dstOffsetChars,
      int lengthChars);

  /**
   * Gets the double value at the current position.
   * Increments the position by <i>Double.BYTES</i>.
   * @return the double at the current position
   */
  double getDouble();

  /**
   * Gets the double value at the given offset.
   * This does not change the position.
   * @param offsetBytes offset bytes relative to this MemorySegment start
   * @return the double at the given offset
   */
  double getDouble(long offsetBytes);

  /**
   * Gets the double array at the current position.
   * Increments the position by <i>Double.BYTES * (lengthDoubles - dstOffsetDoubles)</i>.
   * @param dstArray The preallocated destination array.
   * @param dstOffsetDoubles offset in array units
   * @param lengthDoubles number of array units to transfer
   */
  void getDoubleArray(
      double[] dstArray,
      int dstOffsetDoubles,
      int lengthDoubles);

  /**
   * Gets the float value at the current position.
   * Increments the position by <i>Float.BYTES</i>.
   * @return the float at the current position
   */
  float getFloat();

  /**
   * Gets the float value at the given offset.
   * This does not change the position.
   * @param offsetBytes offset bytes relative to this MemorySegment start
   * @return the float at the given offset
   */
  float getFloat(long offsetBytes);

  /**
   * Gets the float array at the current position.
   * Increments the position by <i>Float.BYTES * (lengthFloats - dstOffsetFloats)</i>.
   * @param dstArray The preallocated destination array.
   * @param dstOffsetFloats offset in array units
   * @param lengthFloats number of array units to transfer
   */
  void getFloatArray(
      float[] dstArray,
      int dstOffsetFloats,
      int lengthFloats);

  /**
   * Gets the int value at the current position.
   * Increments the position by <i>Integer.BYTES</i>.
   * @return the int at the current position
   */
  int getInt();

  /**
   * Gets the int value at the given offset.
   * This does not change the position.
   * @param offsetBytes offset bytes relative to this MemorySegment start
   * @return the int at the given offset
   */
  int getInt(long offsetBytes);

  /**
   * Gets the int array at the current position.
   * Increments the position by <i>Integer.BYTES * (lengthInts - dstOffsetInts)</i>.
   * @param dstArray The preallocated destination array.
   * @param dstOffsetInts offset in array units
   * @param lengthInts number of array units to transfer
   */
  void getIntArray(
      int[] dstArray,
      int dstOffsetInts,
      int lengthInts);

  /**
   * Gets the long value at the current position.
   * Increments the position by <i>Long.BYTES</i>.
   * @return the long at the current position
   */
  long getLong();

  /**
   * Gets the long value at the given offset.
   * This does not change the position.
   * @param offsetBytes offset bytes relative to this MemorySegment start
   * @return the long at the given offset
   */
  long getLong(long offsetBytes);

  /**
   * Gets the long array at the current position.
   * Increments the position by <i>Long.BYTES * (lengthLongs - dstOffsetLongs)</i>.
   * @param dstArray The preallocated destination array.
   * @param dstOffsetLongs offset in array units
   * @param lengthLongs number of array units to transfer
   */
  void getLongArray(
      long[] dstArray,
      int dstOffsetLongs,
      int lengthLongs);

  /**
   * Gets the short value at the current position.
   * Increments the position by <i>Short.BYTES</i>.
   * @return the short at the current position
   */
  short getShort();

  /**
   * Gets the short value at the given offset.
   * This does not change the position.
   * @param offsetBytes offset bytes relative to this MemorySegment start
   * @return the short at the given offset
   */
  short getShort(long offsetBytes);

  /**
   * Gets the short array at the current position.
   * Increments the position by <i>Short.BYTES * (lengthShorts - dstOffsetShorts)</i>.
   * @param dstArray The preallocated destination array.
   * @param dstOffsetShorts offset in array units
   * @param lengthShorts number of array units to transfer
   */
  void getShortArray(
      short[] dstArray,
      int dstOffsetShorts,
      int lengthShorts);

  //PRIMITIVE setX() and setXArray()

  /**
   * Sets the boolean value at the current position.
   * Increments the position by <i>Byte.BYTES</i>.
   * @param value the value to put
   */
  void setBoolean(boolean value);

  /**
   * Sets the boolean value at the given offset.
   * This does not change the position.
   * @param offsetBytes offset bytes relative to this MemorySegment start.
   * @param value the value to put
   */
  void setBoolean(
      long offsetBytes,
      boolean value);

  //intentionally removed putBooleanArray(...)

  /**
   * Sets the byte value at the current position.
   * Increments the position by <i>Byte.BYTES</i>.
   * @param value the value to put
   */
  void setByte(byte value);

  /**
   * Sets the byte value at the given offset.
   * This does not change the position.
   * @param offsetBytes offset bytes relative to this MemorySegment start
   * @param value the value to put
   */
  void setByte(
      long offsetBytes,
      byte value);

  /**
   * Sets the byte array at the current position.
   * Increments the position by <i>Byte.BYTES * (lengthBytes - srcOffsetBytes)</i>.
   * @param srcArray The source array.
   * @param srcOffsetBytes offset in array units
   * @param lengthBytes number of array units to transfer
   */
  void setByteArray(
      byte[] srcArray,
      int srcOffsetBytes,
      int lengthBytes);

  /**
   * Sets the char value at the current position.
   * Increments the position by <i>Character.BYTES</i>.
   * @param value the value to put
   */
  void setChar(char value);

  /**
   * Sets the char value at the given offset.
   * This does not change the position.
   * @param offsetBytes offset bytes relative to this MemorySegment start
   * @param value the value to put
   */
  void setChar(
      long offsetBytes,
      char value);

  /**
   * Sets the char array at the current position.
   * Increments the position by <i>Character.BYTES * (lengthChars - srcOffsetChars)</i>.
   * @param srcArray The source array.
   * @param srcOffsetChars offset in array units
   * @param lengthChars number of array units to transfer
   */
  void setCharArray(
      char[] srcArray,
      int srcOffsetChars,
      int lengthChars);

  /**
   * Sets the double value at the current position.
   * Increments the position by <i>Double.BYTES</i>.
   * @param value the value to put
   */
  void setDouble(double value);

  /**
   * Sets the double value at the given offset.
   * This does not change the position.
   * @param offsetBytes offset bytes relative to this MemorySegment start
   * @param value the value to put
   */
  void setDouble(
      long offsetBytes,
      double value);

  /**
   * Sets the double array at the current position.
   * Increments the position by <i>Double.BYTES * (lengthDoubles - srcOffsetDoubles)</i>.
   * @param srcArray The source array.
   * @param srcOffsetDoubles offset in array units
   * @param lengthDoubles number of array units to transfer
   */
  void setDoubleArray(
      double[] srcArray,
      int srcOffsetDoubles,
      int lengthDoubles);

  /**
   * Sets the float value at the current position.
   * Increments the position by <i>Float.BYTES</i>.
   * @param value the value to put
   */
  void setFloat(float value);

  /**
   * Sets the float value at the given offset.
   * This does not change the position.
   * @param offsetBytes offset bytes relative to this MemorySegment start
   * @param value the value to put
   */
  void setFloat(
      long offsetBytes,
      float value);

  /**
   * Sets the float array at the current position.
   * Increments the position by <i>Float.BYTES * (lengthFloats - srcOffsetFloats)</i>.
   * @param srcArray The source array.
   * @param srcOffsetFloats offset in array units
   * @param lengthFloats number of array units to transfer
   */
  void setFloatArray(
      float[] srcArray,
      int srcOffsetFloats,
      int lengthFloats);

  /**
   * Sets the int value at the current position.
   * Increments the position by <i>Integer.BYTES</i>.
   * @param value the value to put
   */
  void setInt(int value);

  /**
   * Sets the int value at the given offset.
   * This does not change the position.
   * @param offsetBytes offset bytes relative to this MemorySegment start
   * @param value the value to put
   */
  void setInt(
      long offsetBytes,
      int value);

  /**
   * Sets the int array at the current position.
   * Increments the position by <i>Integer.BYTES * (lengthInts - srcOffsetInts)</i>.
   * @param srcArray The source array.
   * @param srcOffsetInts offset in array units
   * @param lengthInts number of array units to transfer
   */
  void setIntArray(
      int[] srcArray,
      int srcOffsetInts,
      int lengthInts);

  /**
   * Sets the long value at the current position.
   * Increments the position by <i>Long.BYTES</i>.
   * @param value the value to put
   */
  void setLong(long value);

  /**
   * Sets the long value at the given offset.
   * This does not change the position.
   * @param offsetBytes offset bytes relative to this MemorySegment start
   * @param value the value to put
   */
  void setLong(
      long offsetBytes,
      long value);

  /**
   * Sets the long array at the current position.
   * Increments the position by <i>Long.BYTES * (lengthLongs - srcOffsetLongs)</i>.
   * @param srcArray The source array.
   * @param srcOffsetLongs offset in array units
   * @param lengthLongs number of array units to transfer
   */
  void setLongArray(
      long[] srcArray,
      int srcOffsetLongs,
      int lengthLongs);

  /**
   * Sets the short value at the current position.
   * Increments the position by <i>Short.BYTES</i>.
   * @param value the value to put
   */
  void setShort(short value);

  /**
   * Sets the short value at the given offset.
   * This does not change the position.
   * @param offsetBytes offset bytes relative to this MemorySegment start
   * @param value the value to put
   */
  void setShort(
      long offsetBytes,
      short value);

  /**
   * Sets the short array at the current position.
   * Increments the position by <i>Short.BYTES * (lengthShorts - srcOffsetShorts)</i>.
   * @param srcArray The source array.
   * @param srcOffsetShorts offset in array units
   * @param lengthShorts number of array units to transfer
   */
  void setShortArray(
      short[] srcArray,
      int srcOffsetShorts,
      int lengthShorts);

}
