/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.memory;


/**
 * This API defines primitive memory access methods that leverage much of the capabilities of
 * the sun.misc.Unsafe class.
 * 
 * <p> This API is primarily designed to provide flexible and direct manipulation of in-memory 
 * primitive and primitive array data structures.
 * 
 * In contrast to the <i>java.nio.ByteBuffer</i> classes, which were designed for native I/O and 
 * includes concepts such as <i>position, limit, mark, flip,</i> and <i>rewind</i>, this interface
 * specifically bypasses these concepts and instead provides a much richer collection of primitive,
 * bit, array and copy methods that access the data directly from a single byte offset.  
 * In addition, the MemoryRegion class provides a means of hierarchically partitioning a 
 * large block of native memory into smaller regions of memory, each with their own "capacity" 
 * and offsets.  This provides much more flexibility in accessing and managing complex data 
 * structures off-heap.</p>
 * 
 * <p> This API provides a rich selection of access methods including partial array copies to/from
 * the heap, bit manipulation and incrementing methods. The MemoryUtil class provides 
 * a powerful native memory to native memory copy method and other useful functions. </p>
 * 
 * @author Lee Rhodes
 */
public interface Memory {

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
   * Copies bytes from a source region of this Memory to a destination region of this Memory.
   * These regions may not overlap.  This will be checked if asserts are enabled in the JVM.
   * @param srcOffsetBytes the source offset
   * @param dstOffsetBytes the destintaion offset
   * @param lengthBytes the number of bytes to copy
   */
  void copy(long srcOffsetBytes, long dstOffsetBytes, long lengthBytes);
  
  /**
   * Atomically adds the given value by the integer located at offsetBytes.
   * @param offsetBytes offset bytes relative to this Memory start
   * @param delta the amount to add
   * @return the modified value
   */
  int getAndAddInt(long offsetBytes, int delta);
  
  /**
   * Atomically adds the given value to the long located at offsetBytes.
   * @param offsetBytes offset bytes relative to this Memory start
   * @param delta the amount to add
   * @return the modified value
   */
  long getAndAddLong(long offsetBytes, long delta);
  
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
  
  
  /**
   * Gets the boolean value at the given offset
   * @param offsetBytes offset bytes relative to this Memory start
   * @return the boolean at the given offset
   */
  boolean getBoolean(long offsetBytes);
  
  /**
   * Gets the boolean array at the given offset
   * @param offsetBytes offset bytes relative to this Memory start
   * @param dstArray The destination array.
   * @param dstOffset offset in array units
   * @param length number of array units to transfer
   * The size of this array determines the bytes transferred.
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
   * @param dstArray The destination array.
   * @param dstOffset offset in array units
   * @param length number of array units to transfer
   * The size of this array determines the bytes transferred.
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
   * @param dstArray The destination array.
   * @param dstOffset offset in array units
   * @param length number of array units to transfer
   * The size of this array determines the bytes transferred.
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
   * @param dstArray The destination array.
   * @param dstOffset offset in array units
   * @param length number of array units to transfer
   * The size of this array determines the bytes transferred.
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
   * @param dstArray The destination array.
   * @param dstOffset offset in array units
   * @param length number of array units to transfer
   * The size of this array determines the bytes transferred.
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
   * @param dstArray The destination array.
   * @param dstOffset offset in array units
   * @param length number of array units to transfer
   * The size of this array determines the bytes transferred.
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
   * @param dstArray The destination array.
   * @param dstOffset offset in array units
   * @param length number of array units to transfer
   * The size of this array determines the bytes transferred.
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
   * @param dstArray The destination array.
   * @param dstOffset offset in array units
   * @param length number of array units to transfer
   * The size of this array determines the bytes transferred.
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
   * The size of this array determines the bytes transferred.
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
   * The size of this array determines the bytes transferred.
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
   * The size of this array determines the bytes transferred.
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
   * The size of this array determines the bytes transferred.
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
   * The size of this array determines the bytes transferred.
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
   * The size of this array determines the bytes transferred.
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
   * The size of this array determines the bytes transferred.
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
   * The size of this array determines the bytes transferred.
   */
  void putShortArray(long offsetBytes, short[] srcArray, int srcOffset, int length);
  
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
   * Sets the bits defined by the bitMask
   * @param offsetBytes offset bytes relative to this Memory start
   * @param bitMask the bits set to one will be set
   */
  void setBits(long offsetBytes, byte bitMask);
  
  //Non-primitive methods

  /**
   * Returns the start address of this Memory plus the offset.
   * @param offset the given offset from the start address of this Memory
   * @return the start address of this Memory plus the offset.
   */
  long getAddress(final long offset);
  
  /**
   * Gets the capacity of this Memory in bytes
   * @return the capacity of this Memory in bytes
   */
  long getCapacity();
  
  /**
   * Gets the parent Memory or backing array. Used internally.
   * @return the parent Memory.
   */
  Object getParent();
  
  /**
   * Returns a formatted hex string of an area of this Memory. 
   * Used primarily for testing.
   * @param header decriptive header
   * @param offsetBytes offset bytes relative to this Memory start 
   * @param lengthBytes number of bytes to convert to a hex string
   * @return a formatted hex string in a human readable array
   */
  String toHexString(String header, long offsetBytes, int lengthBytes);
  
}