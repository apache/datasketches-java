/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.memory;

import java.nio.ByteBuffer;

/**
 * The Memory interface defines <i>get</i> and <i>put</i> methods for all Java primitive and
 * primitive array types to/from a byte offset that is relative to the base address of some
 * object or region of native memory defined by the implementing class.
 * The methods of this interface leverage the capabilities of the sun.misc.Unsafe class.
 *
 * <p>In contrast to the <i>java.nio.ByteBuffer</i> classes, which were designed for native
 * streaming I/O and include concepts such as <i>position, limit, mark, flip</i> and <i>rewind</i>,
 * this interface specifically bypasses these concepts and instead provides a rich collection of
 * primitive, bit, array and copy methods that access the data directly from a single byte offset.
 *
 * @author Lee Rhodes
 */
public interface Memory {

  /**
   * Gets the boolean value at the given offset
   * @param offsetBytes offset bytes relative to this Memory start
   * @return the boolean at the given offset
   */
  boolean getBoolean(long offsetBytes);

  /**
   * Gets the boolean array at the given offset
   * @param offsetBytes offset bytes relative to this Memory start
   * @param dstArray The preallocated destination array.
   * @param dstOffset offset in array units
   * @param length number of array units to transfer
   */
  void getBooleanArray(long offsetBytes, boolean[] dstArray, int dstOffset, int length);

  /**
   * Gets the byte at the given offset
   * @param offsetBytes offset bytes relative to this Memory start
   * @return the byte at the given offset
   */
  byte getByte(long offsetBytes);

  /**
   * Gets the byte array at the given offset
   * @param offsetBytes offset bytes relative to this Memory start
   * @param dstArray The preallocated destination array.
   * @param dstOffset offset in array units
   * @param length number of array units to transfer
   */
  void getByteArray(long offsetBytes, byte[] dstArray, int dstOffset, int length);

  /**
   * Gets the char at the given offset
   * @param offsetBytes offset bytes relative to this Memory start
   * @return the char at the given offset
   */
  char getChar(long offsetBytes);

  /**
   * Gets the char array at the given offset
   * @param offsetBytes offset bytes relative to this Memory start
   * @param dstArray The preallocated destination array.
   * @param dstOffset offset in array units
   * @param length number of array units to transfer
   */
  void getCharArray(long offsetBytes, char[] dstArray, int dstOffset, int length);

  /**
   * Gets the double at the given offset
   * @param offsetBytes offset bytes relative to this Memory start
   * @return the double at the given offset
   */
  double getDouble(long offsetBytes);

  /**
   * Gets the double array at the given offset
   * @param offsetBytes offset bytes relative to this Memory start
   * @param dstArray The preallocated destination array.
   * @param dstOffset offset in array units
   * @param length number of array units to transfer
   */
  void getDoubleArray(long offsetBytes, double[] dstArray, int dstOffset, int length);

  /**
   * Gets the float at the given offset
   * @param offsetBytes offset bytes relative to this Memory start
   * @return the float at the given offset
   */
  float getFloat(long offsetBytes);

  /**
   * Gets the float array at the given offset
   * @param offsetBytes offset bytes relative to this Memory start
   * @param dstArray The preallocated destination array.
   * @param dstOffset offset in array units
   * @param length number of array units to transfer
   */
  void getFloatArray(long offsetBytes, float[] dstArray, int dstOffset, int length);

  /**
   * Gets the int at the given offset
   * @param offsetBytes offset bytes relative to this Memory start
   * @return the int at the given offset
   */
  int getInt(long offsetBytes);

  /**
   * Gets the int array at the given offset
   * @param offsetBytes offset bytes relative to this Memory start
   * @param dstArray The preallocated destination array.
   * @param dstOffset offset in array units
   * @param length number of array units to transfer
   */
  void getIntArray(long offsetBytes, int[] dstArray, int dstOffset, int length);

  /**
   * Gets the long at the given offset
   * @param offsetBytes offset bytes relative to this Memory start
   * @return the long at the given offset
   */
  long getLong(long offsetBytes);

  /**
   * Gets the long array at the given offset
   * @param offsetBytes offset bytes relative to this Memory start
   * @param dstArray The preallocated destination array.
   * @param dstOffset offset in array units
   * @param length number of array units to transfer
   */
  void getLongArray(long offsetBytes, long[] dstArray, int dstOffset, int length);

  /**
   * Gets the short at the given offset
   * @param offsetBytes offset bytes relative to this Memory start
   * @return the short at the given offset
   */
  short getShort(long offsetBytes);

  /**
   * Gets the short array at the given offset
   * @param offsetBytes offset bytes relative to this Memory start
   * @param dstArray The preallocated destination array.
   * @param dstOffset offset in array units
   * @param length number of array units to transfer
   */
  void getShortArray(long offsetBytes, short[] dstArray, int dstOffset, int length);

  /**
   * Returns true if all bits defined by the bitMask are clear
   * @param offsetBytes offset bytes relative to this Memory start
   * @param bitMask bits set to one will be checked
   * @return true if all bits defined by the bitMask are clear
   */
  boolean isAllBitsClear(long offsetBytes, byte bitMask);

  /**
   * Returns true if all bits defined by the bitMask are set
   * @param offsetBytes offset bytes relative to this Memory start
   * @param bitMask bits set to one will be checked
   * @return true if all bits defined by the bitMask are set
   */
  boolean isAllBitsSet(long offsetBytes, byte bitMask);

  /**
   * Returns true if any bits defined by the bitMask are clear
   * @param offsetBytes offset bytes relative to this Memory start
   * @param bitMask bits set to one will be checked
   * @return true if any bits defined by the bitMask are clear
   */
  boolean isAnyBitsClear(long offsetBytes, byte bitMask);

  /**
   * Returns true if any bits defined by the bitMask are set
   * @param offsetBytes offset bytes relative to this Memory start
   * @param bitMask bits set to one will be checked
   * @return true if any bits defined by the bitMask are set
   */
  boolean isAnyBitsSet(long offsetBytes, byte bitMask);

  /**
   * Puts the boolean value at the given offset
   * @param offsetBytes offset bytes relative to this Memory start
   * @param value the value to put
   */
  void putBoolean(long offsetBytes, boolean value);

  /**
   * Puts the boolean array at the given offset
   * @param offsetBytes offset bytes relative to this Memory start
   * @param srcArray The source array.
   * @param srcOffset offset in array units
   * @param length number of array units to transfer
   */
  void putBooleanArray(long offsetBytes, boolean[] srcArray, int srcOffset, int length);

  /**
   * Puts the byte value at the given offset
   * @param offsetBytes offset bytes relative to this Memory start
   * @param value the value to put
   */
  void putByte(long offsetBytes, byte value);

  /**
   * Puts the byte array at the given offset
   * @param offsetBytes offset bytes relative to this Memory start
   * @param srcArray The source array.
   * @param srcOffset offset in array units
   * @param length number of array units to transfer
   */
  void putByteArray(long offsetBytes, byte[] srcArray, int srcOffset, int length);

  /**
   * Puts the char value at the given offset
   * @param offsetBytes offset bytes relative to this Memory start
   * @param value the value to put
   */
  void putChar(long offsetBytes, char value);

  /**
   * Puts the char array at the given offset
   * @param offsetBytes offset bytes relative to this Memory start
   * @param srcArray The source array.
   * @param srcOffset offset in array units
   * @param length number of array units to transfer
   */
  void putCharArray(long offsetBytes, char[] srcArray, int srcOffset, int length);

  /**
   * Puts the double value at the given offset
   * @param offsetBytes offset bytes relative to this Memory start
   * @param value the value to put
   */
  void putDouble(long offsetBytes, double value);

  /**
   * Puts the double array at the given offset
   * @param offsetBytes offset bytes relative to this Memory start
   * @param srcArray The source array.
   * @param srcOffset offset in array units
   * @param length number of array units to transfer
   */
  void putDoubleArray(long offsetBytes, double[] srcArray, int srcOffset, int length);

  /**
   * Puts the float value at the given offset
   * @param offsetBytes offset bytes relative to this Memory start
   * @param value the value to put
   */
  void putFloat(long offsetBytes, float value);

  /**
   * Puts the float array at the given offset
   * @param offsetBytes offset bytes relative to this Memory start
   * @param srcArray The source array.
   * @param srcOffset offset in array units
   * @param length number of array units to transfer
   */
  void putFloatArray(long offsetBytes, float[] srcArray, int srcOffset, int length);

  /**
   * Puts the int value at the given offset
   * @param offsetBytes offset bytes relative to this Memory start
   * @param value the value to put
   */
  void putInt(long offsetBytes, int value);

  /**
   * Puts the int array at the given offset
   * @param offsetBytes offset bytes relative to this Memory start
   * @param srcArray The source array.
   * @param srcOffset offset in array units
   * @param length number of array units to transfer
   */
  void putIntArray(long offsetBytes, int[] srcArray, int srcOffset, int length);

  /**
   * Puts the long value at the given offset
   * @param offsetBytes offset bytes relative to this Memory start
   * @param value the value to put
   */
  void putLong(long offsetBytes, long value);

  /**
   * Puts the long array at the given offset
   * @param offsetBytes offset bytes relative to this Memory start
   * @param srcArray The source array.
   * @param srcOffset offset in array units
   * @param length number of array units to transfer
   */
  void putLongArray(long offsetBytes, long[] srcArray, int srcOffset, int length);

  /**
   * Puts the short value at the given offset
   * @param offsetBytes offset bytes relative to this Memory start
   * @param value the value to put
   */
  void putShort(long offsetBytes, short value);

  /**
   * Puts the short array at the given offset
   * @param offsetBytes offset bytes relative to this Memory start
   * @param srcArray The source array.
   * @param srcOffset offset in array units
   * @param length number of array units to transfer
   */
  void putShortArray(long offsetBytes, short[] srcArray, int srcOffset, int length);

  /**
   * Sets the bits defined by the bitMask
   * @param offsetBytes offset bytes relative to this Memory start
   * @param bitMask the bits set to one will be set
   */
  void setBits(long offsetBytes, byte bitMask);

  //Atomic methods

  /**
   * Atomically adds the given value to the integer located at offsetBytes.
   * @param offsetBytes offset bytes relative to this Memory start
   * @param delta the amount to add
   * @return the modified value
   */
  int addAndGetInt(long offsetBytes, int delta);

  /**
   * Atomically adds the given value to the long located at offsetBytes.
   * @param offsetBytes offset bytes relative to this Memory start
   * @param delta the amount to add
   * @return the modified value
   */
  long addAndGetLong(long offsetBytes, long delta);

  /**
   * Atomically sets the current value at the memory location to the given updated value
   * if and only if the current value {@code ==} the expected value.
   * @param offsetBytes offset bytes relative to this Memory start
   * @param expect the expected value
   * @param update the new value
   * @return {@code true} if successful. False return indicates that
   * the current value at the memory location was not equal to the expected value.
   */
  boolean compareAndSwapInt(long offsetBytes, int expect, int update);

  /**
   * Atomically sets the current value at the memory location to the given updated value
   * if and only if the current value {@code ==} the expected value.
   * @param offsetBytes offset bytes relative to this Memory start
   * @param expect the expected value
   * @param update the new value
   * @return {@code true} if successful. False return indicates that
   * the current value at the memory location was not equal to the expected value.
   */
  boolean compareAndSwapLong(long offsetBytes, long expect, long update);

  /**
   * Atomically exchanges the given value with the current value located at offsetBytes.
   * @param offsetBytes offset bytes relative to this Memory start
   * @param newValue new value
   * @return the previous value
   */
  int getAndSetInt(long offsetBytes, int newValue);

  /**
   * Atomically exchanges the given value with the current value located at offsetBytes.
   * @param offsetBytes offset bytes relative to this Memory start
   * @param newValue new value
   * @return the previous value
   */
  long getAndSetLong(long offsetBytes, long newValue);

  //Non-primitive Memory interface methods

  /**
   * Returns the backing on-heap primitive array if there is one, otherwise returns null
   * @return the backing on-heap primitive array if there is one, otherwise returns null
   */
  Object array();

  /**
   * Returns a read-only version of this memory
   * @return a read-only version of this memory
   */
  Memory asReadOnlyMemory();

  /**
   * Returns the backing ByteBuffer if there is one, otherwise returns null
   * @return the backing ByteBuffer if there is one, otherwise returns null
   */
  ByteBuffer byteBuffer();

  /**
   * Clears all bytes of this Memory to zero
   */
  void clear();

  /**
   * Clears a portion of this Memory to zero.
   * @param offsetBytes offset bytes relative to this Memory start
   * @param lengthBytes the length in bytes
   */
  void clear(long offsetBytes, long lengthBytes);

  /**
   * Clears the bits defined by the bitMask
   * @param offsetBytes offset bytes relative to this Memory start.
   * @param bitMask the bits set to one will be cleared
   */
  void clearBits(long offsetBytes, byte bitMask);

  /**
   * Copies bytes from a source range of this Memory to a destination range of the given Memory
   * using the same low-level system copy function as found in
   * {@link java.lang.System#arraycopy(Object, int, Object, int, int)}.
   * These regions may not overlap.  This will be checked if asserts are enabled in the JVM.
   * @param srcOffsetBytes the source offset
   * @param dstOffsetBytes the destintaion offset
   * @param lengthBytes the number of bytes to copy
   * @deprecated Use {@link Memory#copy(long, Memory, long, long)} instead.
   */
  @Deprecated
  void copy(long srcOffsetBytes, long dstOffsetBytes, long lengthBytes);

  /**
   * Copies bytes from a source range of this Memory to a destination range of the given Memory
   * using the same low-level system copy function as found in
   * {@link java.lang.System#arraycopy(Object, int, Object, int, int)}.
   * @param srcOffsetBytes the source offset for this Memory
   * @param destination the destination Memory, which may not be Read-Only.
   * @param dstOffsetBytes the destintaion offset
   * @param lengthBytes the number of bytes to copy
   */
  void copy(long srcOffsetBytes, Memory destination, long dstOffsetBytes, long lengthBytes);

  /**
   * Fills all bytes of this Memory region to the given byte value.
   * @param value the given byte value
   */
  void fill(byte value);

  /**
   * Fills a portion of this Memory region to the given byte value.
   * @param offsetBytes offset bytes relative to this Memory start
   * @param lengthBytes the length in bytes
   * @param value the given byte value
   */
  void fill(long offsetBytes, long lengthBytes, byte value);


  /**
   * Returns the start address of this <i>Memory</i> relative to its parent plus the offset in bytes.
   * Note that the parent could be native memory or some backing object.
   *
   * @param offsetBytes the given offset in bytes from the start address of this Memory
   * relative to its parent.
   * @return the start address of this <i>Memory</i> relative to its parent plus the offset in bytes.
   */
  long getAddress(final long offsetBytes);

  /**
   * Gets the capacity of this Memory in bytes
   * @return the capacity of this Memory in bytes
   */
  long getCapacity();

  /**
   * Returns the cumulative offset in bytes of this Memory from the root of the Memory hierarchy
   * including the given offsetBytes.
   *
   * @param offsetBytes the given offset in bytes
   * @return the cumulative offset in bytes of this Memory from the root of the Memory hierarchy
   * including the given offsetBytes.
   */
  long getCumulativeOffset(final long offsetBytes);

  /**
   * Returns a MemoryRequest or null
   * @return a MemoryRequest or null
   */
  MemoryRequest getMemoryRequest();

  /**
   * Gets the parent Memory or backing array. Used internally.
   * @return the parent Memory.
   */
  Object getParent();

  /**
   * Returns true if this Memory is backed by an on-heap primitive array
   * @return true if this Memory is backed by an on-heap primitive array
   */
  boolean hasArray();

  /**
   * Returns true if this Memory is backed by a ByteBuffer
   * @return true if this Memory is backed by a ByteBuffer
   */
  boolean hasByteBuffer();

  /**
   * Returns true if this Memory has a capacity greater than zero
   * @return true if this Memory has a capacity greater than zero
   */
  boolean isAllocated();

  /**
   * Returns true if the backing memory is direct (off-heap) memory.
   * @return true if the backing memory is direct (off-heap) memory.
   */
  boolean isDirect();

  /**
   * Returns true if this Memory is read only
   * @return true if this Memory is read only
   */
  boolean isReadOnly();

  /**
   * Returns true if the backing resource of this Memory is identical with the backing resource
   * of the given Memory
   * @param mem the given Memory
   * @return true if the backing resource of this Memory is identical with the backing resource
   * of the given Memory
   */
  boolean isSameResource(Memory mem);

  /**
   * Sets a MemoryRequest
   * @param memReq the MemoryRequest
   */
  void setMemoryRequest(MemoryRequest memReq);

  /**
   * Returns a formatted hex string of an area of this Memory.
   * Used primarily for testing.
   * @param header descriptive header
   * @param offsetBytes offset bytes relative to this Memory start
   * @param lengthBytes number of bytes to convert to a hex string
   * @return a formatted hex string in a human readable array
   */
  String toHexString(String header, long offsetBytes, int lengthBytes);

  /**
   * Because the <i>Memory</i> classes now use the JVM <i>Cleaner</i>, calling <i>freeMemory()</i>,
   * which also calls <i>Cleaner</i>, is optional.
   * In any case, calling <i>freeMemory()</i> is only relevant for <i>Memory</i> objects that have
   * actually allocated native memory, which are those that have been allocated using
   * {@link AllocMemory} or {@link MemoryMappedFile}.
   *
   * <p>Preemptively calling <i>freeMemory()</i> may reduce the load on the JVM GarbageCollector,
   * but the significance of this will have to be verified in the target system environment.
   * Calling <i>freeMemory()</i> always disables the current instance by setting the capacity to
   * zero.</p>
   */
  void freeMemory();
}
