/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.memory;

import static com.yahoo.sketches.memory.UnsafeUtil.BOOLEAN_SHIFT;
import static com.yahoo.sketches.memory.UnsafeUtil.ARRAY_BOOLEAN_INDEX_SCALE;
import static com.yahoo.sketches.memory.UnsafeUtil.BYTE_SHIFT;
import static com.yahoo.sketches.memory.UnsafeUtil.ARRAY_BYTE_INDEX_SCALE;
import static com.yahoo.sketches.memory.UnsafeUtil.CHAR_SHIFT;
import static com.yahoo.sketches.memory.UnsafeUtil.ARRAY_CHAR_INDEX_SCALE;
import static com.yahoo.sketches.memory.UnsafeUtil.DOUBLE_SHIFT;
import static com.yahoo.sketches.memory.UnsafeUtil.ARRAY_DOUBLE_INDEX_SCALE;
import static com.yahoo.sketches.memory.UnsafeUtil.FLOAT_SHIFT;
import static com.yahoo.sketches.memory.UnsafeUtil.ARRAY_FLOAT_INDEX_SCALE;
import static com.yahoo.sketches.memory.UnsafeUtil.INT_SHIFT;
import static com.yahoo.sketches.memory.UnsafeUtil.ARRAY_INT_INDEX_SCALE;
import static com.yahoo.sketches.memory.UnsafeUtil.LONG_SHIFT;
import static com.yahoo.sketches.memory.UnsafeUtil.ARRAY_LONG_INDEX_SCALE;
import static com.yahoo.sketches.memory.UnsafeUtil.SHORT_SHIFT;
import static com.yahoo.sketches.memory.UnsafeUtil.ARRAY_SHORT_INDEX_SCALE;
import static com.yahoo.sketches.memory.UnsafeUtil.assertBounds;

/**
 * The MemoryRegion class implements the Memory interface and provides a means of 
 * hierarchically partitioning a large block of native memory into 
 * smaller regions of memory, each with their own capacity and offsets. 
 * 
 * <p>If asserts are enabled in the JVM, the methods in this class perform bounds checking against 
 * the region's defined boundaries and then redirect the call to the parent Memory class. If the
 * parent class is also a MemoryRegion it does a similar check and then calls its parent. 
 * The root of this hierarchy will be a NativeMemory class that ultimately performs the desired
 * task. If asserts are not enabled the JIT compiler will eliminate all the asserts and the 
 * hierarchical calls should collapse to a call to the NativeMemory method.</p> 
 * 
 * <p>Because asserts must be specifically enabled in the JVM, it is incumbent on the user of this 
 * class to make sure that their code is thoroughly tested.  
 * Violating memory bounds can cause memory segment faults, which takes
 * down the JVM and can be very difficult to debug.</p>
 * 
 * @see NativeMemory
 * @author Lee Rhodes
 */
public class MemoryRegion implements Memory {
  /**
   * The parent Memory object from which an offset and capacity is defined by this class.
   * This field is used to keep a reference to the parent Memory to 
   * ensure that its memory isn't freed before we are done with it.
   */
  private final Memory mem_;
  private volatile long memOffsetBytes_;
  private volatile long capacityBytes_;
  private volatile MemoryRequest memReq_ = null;
  
  /**
   * Defines a region of the given parent Memory by defining an offset and capacity that are 
   * within the boundaries of the parent.
   * @param memory the parent Memory
   * @param memOffsetBytes the starting offset in bytes of this region with respect to the 
   * start of the parent memory.
   * @param capacityBytes the capacity in bytes of this region.
   */
  public MemoryRegion(Memory memory, long memOffsetBytes, long capacityBytes) {
    assertBounds(memOffsetBytes, capacityBytes, memory.getCapacity());
    mem_ = memory;
    memOffsetBytes_ = memOffsetBytes;
    capacityBytes_ = capacityBytes;
  }
  
  /**
   * Defines a region of the given parent Memory by defining an offset and capacity that are 
   * within the boundaries of the parent.
   * @param memory the parent Memory
   * @param memOffsetBytes the starting offset in bytes of this region with respect to the 
   * start of the parent memory.
   * @param capacityBytes the capacity in bytes of this region.
   * @param memReq a MemoryRequest object
   */
  public MemoryRegion(Memory memory, long memOffsetBytes, long capacityBytes, MemoryRequest memReq) {
    assertBounds(memOffsetBytes, capacityBytes, memory.getCapacity());
    mem_ = memory;
    memOffsetBytes_ = memOffsetBytes;
    capacityBytes_ = capacityBytes;
    memReq_ = memReq;
  }
  
  public void reassign(long memOffsetBytes, long capacityBytes) {
    assertBounds(memOffsetBytes, capacityBytes, mem_.getCapacity());
    memOffsetBytes_ = memOffsetBytes;
    capacityBytes_ = capacityBytes;
  }
  
  @Override
  public void clear() {
    fill(0, capacityBytes_, (byte) 0);
  }

  @Override
  public void clear(long offsetBytes, long lengthBytes) {
    fill(offsetBytes, lengthBytes, (byte) 0);
  }

  @Override
  public void clearBits(long offsetBytes, byte bitMask) {
    assertBounds(offsetBytes, ARRAY_BYTE_INDEX_SCALE, capacityBytes_);
    mem_.clearBits(getAddress(offsetBytes), bitMask);
  }

  @Override
  public void copy(long srcOffsetBytes, long dstOffsetBytes, long lengthBytes) {
    assertBounds(srcOffsetBytes, lengthBytes, capacityBytes_);
    assertBounds(srcOffsetBytes, lengthBytes, capacityBytes_);
    long min = Math.min(srcOffsetBytes, dstOffsetBytes);
    long max = Math.max(srcOffsetBytes, dstOffsetBytes);
    assertBounds(min, lengthBytes, max); //regions must not overlap
    long srcAdd = getAddress(srcOffsetBytes);
    long dstAdd = getAddress(dstOffsetBytes); 
    mem_.copy(srcAdd, dstAdd, lengthBytes);
  }

  @Override
  public int getAndAddInt(long offsetBytes, int delta) {
    assertBounds(offsetBytes, ARRAY_INT_INDEX_SCALE, capacityBytes_);
    long unsafeRawAddress = getAddress(offsetBytes);
    return mem_.getAndAddInt(unsafeRawAddress, delta);
  }

  @Override
  public long getAndAddLong(long offsetBytes, long delta) {
    assertBounds(offsetBytes, ARRAY_LONG_INDEX_SCALE, capacityBytes_);
    long unsafeRawAddress = getAddress(offsetBytes);
    return mem_.getAndAddLong(unsafeRawAddress, delta);
  }

  @Override
  public int getAndSetInt(long offsetBytes, int newValue) {
    assertBounds(offsetBytes, ARRAY_INT_INDEX_SCALE, capacityBytes_);
    long unsafeRawAddress = getAddress(offsetBytes);
    return mem_.getAndSetInt(unsafeRawAddress, newValue);
  }

  @Override
  public long getAndSetLong(long offsetBytes, long newValue) {
    assertBounds(offsetBytes, ARRAY_LONG_INDEX_SCALE, capacityBytes_);
    long unsafeRawAddress = getAddress(offsetBytes);
    return mem_.getAndSetLong(unsafeRawAddress, newValue);
  }

  @Override
  public boolean getBoolean(long offsetBytes) {
    assertBounds(offsetBytes, ARRAY_BOOLEAN_INDEX_SCALE, capacityBytes_);
    return mem_.getBoolean(getAddress(offsetBytes));
  }

  @Override
  public void getBooleanArray(long offsetBytes, boolean[] dstArray, int dstOffset, int length) {
    long copyBytes = length << BOOLEAN_SHIFT;
    assertBounds(offsetBytes, copyBytes, capacityBytes_);
    assertBounds(dstOffset, length, dstArray.length);
    mem_.getBooleanArray(getAddress(offsetBytes), dstArray, dstOffset, length);
  }

  @Override
  public byte getByte(long offsetBytes) {
    assertBounds(offsetBytes, ARRAY_BYTE_INDEX_SCALE, capacityBytes_);
    return mem_.getByte(getAddress(offsetBytes));
  }

  @Override
  public void getByteArray(long offsetBytes, byte[] dstArray, int dstOffset, int length) {
    long copyBytes = length << BYTE_SHIFT;
    assertBounds(offsetBytes, copyBytes, capacityBytes_);
    assertBounds(dstOffset, length, dstArray.length);
    mem_.getByteArray(getAddress(offsetBytes), dstArray, dstOffset, length);
  }

  @Override
  public char getChar(long offsetBytes) {
    assertBounds(offsetBytes, ARRAY_CHAR_INDEX_SCALE, capacityBytes_);
    return mem_.getChar(getAddress(offsetBytes));
  }

  @Override
  public void getCharArray(long offsetBytes, char[] dstArray, int dstOffset, int length) {
    long copyBytes = length << CHAR_SHIFT;
    assertBounds(offsetBytes, copyBytes, capacityBytes_);
    assertBounds(dstOffset, length, dstArray.length);
    mem_.getCharArray(getAddress(offsetBytes), dstArray, dstOffset, length);
  }

  @Override
  public double getDouble(long offsetBytes) {
    assertBounds(offsetBytes, ARRAY_DOUBLE_INDEX_SCALE, capacityBytes_);
    return mem_.getDouble(getAddress(offsetBytes));
  }

  @Override
  public void getDoubleArray(long offsetBytes, double[] dstArray, int dstOffset, int length) {
    long copyBytes = length << DOUBLE_SHIFT;
    assertBounds(offsetBytes, copyBytes, capacityBytes_);
    assertBounds(dstOffset, length, dstArray.length);
    mem_.getDoubleArray(getAddress(offsetBytes), dstArray, dstOffset, length);
  }

  @Override
  public float getFloat(long offsetBytes) {
    assertBounds(offsetBytes, ARRAY_FLOAT_INDEX_SCALE, capacityBytes_);
    return mem_.getFloat(getAddress(offsetBytes));
  }

  @Override
  public void getFloatArray(long offsetBytes, float[] dstArray, int dstOffset, int length) {
    long copyBytes = length << FLOAT_SHIFT;
    assertBounds(offsetBytes, copyBytes, capacityBytes_);
    assertBounds(dstOffset, length, dstArray.length);
    mem_.getFloatArray(getAddress(offsetBytes), dstArray, dstOffset, length);
  }

  @Override
  public int getInt(long offsetBytes) {
    assertBounds(offsetBytes, ARRAY_INT_INDEX_SCALE, capacityBytes_);
    return mem_.getInt(getAddress(offsetBytes));
  }

  @Override
  public void getIntArray(long offsetBytes, int[] dstArray, int dstOffset, int length) {
    long copyBytes = length << INT_SHIFT;
    assertBounds(offsetBytes, copyBytes, capacityBytes_);
    assertBounds(dstOffset, length, dstArray.length);
    mem_.getIntArray(getAddress(offsetBytes), dstArray, dstOffset, length);
  }

  @Override
  public long getLong(long offsetBytes) {
    assertBounds(offsetBytes, ARRAY_LONG_INDEX_SCALE, capacityBytes_);
    return mem_.getLong(getAddress(offsetBytes));
  }

  @Override
  public void getLongArray(long offsetBytes, long[] dstArray, int dstOffset, int length) {
    long copyBytes = length << LONG_SHIFT;
    assertBounds(offsetBytes, copyBytes, capacityBytes_);
    assertBounds(dstOffset, length, dstArray.length);
    mem_.getLongArray(getAddress(offsetBytes), dstArray, dstOffset, length);
  }

  @Override
  public short getShort(long offsetBytes) {
    assertBounds(offsetBytes, ARRAY_SHORT_INDEX_SCALE, capacityBytes_);
    return mem_.getShort(getAddress(offsetBytes));
  }

  @Override
  public void getShortArray(long offsetBytes, short[] dstArray, int dstOffset, int length) {
    long copyBytes = length << SHORT_SHIFT;
    assertBounds(offsetBytes, copyBytes, capacityBytes_);
    assertBounds(dstOffset, length, dstArray.length);
    mem_.getShortArray(getAddress(offsetBytes), dstArray, dstOffset, length);
  }

  @Override
  public boolean isAllBitsClear(long offsetBytes, byte bitMask) {
    assertBounds(offsetBytes, ARRAY_BYTE_INDEX_SCALE, capacityBytes_);
    long unsafeRawAddress = getAddress(offsetBytes);
    int value = ~mem_.getByte(unsafeRawAddress) & bitMask & 0XFF; 
    return value == bitMask;
  }

  @Override
  public boolean isAllBitsSet(long offsetBytes, byte bitMask) {
    assertBounds(offsetBytes, ARRAY_BYTE_INDEX_SCALE, capacityBytes_);
    long unsafeRawAddress = getAddress(offsetBytes);
    int value = mem_.getByte(unsafeRawAddress) & bitMask & 0XFF;
    return value == bitMask;
  }

  @Override
  public boolean isAnyBitsClear(long offsetBytes, byte bitMask) {
    assertBounds(offsetBytes, ARRAY_BYTE_INDEX_SCALE, capacityBytes_);
    long unsafeRawAddress = getAddress(offsetBytes);
    int value = ~mem_.getByte(unsafeRawAddress) & bitMask & 0XFF; 
    return value != 0;
  }

  @Override
  public boolean isAnyBitsSet(long offsetBytes, byte bitMask) {
    assertBounds(offsetBytes, ARRAY_BYTE_INDEX_SCALE, capacityBytes_);
    long unsafeRawAddress = getAddress(offsetBytes);
    int value = mem_.getByte(unsafeRawAddress) & bitMask & 0XFF;
    return value != 0;
  }

  @Override
  public void putBoolean(long offsetBytes, boolean srcValue) {
    assertBounds(offsetBytes, ARRAY_BOOLEAN_INDEX_SCALE, capacityBytes_);
    mem_.putBoolean(getAddress(offsetBytes), srcValue);
  }

  @Override
  public void putBooleanArray(long offsetBytes, boolean[] srcArray, int srcOffset, int length) {
    long copyBytes = length << BOOLEAN_SHIFT;
    assertBounds(srcOffset, length, srcArray.length);
    assertBounds(offsetBytes, copyBytes, capacityBytes_);
    mem_.putBooleanArray(getAddress(offsetBytes), srcArray, srcOffset, length);
  }

  @Override
  public void putByte(long offsetBytes, byte srcValue) {
    assertBounds(offsetBytes, ARRAY_BYTE_INDEX_SCALE, capacityBytes_);
    mem_.putByte(getAddress(offsetBytes), srcValue);
  }

  @Override
  public void putByteArray(long offsetBytes, byte[] srcArray, int srcOffset, int length) {
    long copyBytes = length << BYTE_SHIFT;
    assertBounds(srcOffset, length, srcArray.length);
    assertBounds(offsetBytes, copyBytes, capacityBytes_);
    mem_.putByteArray(getAddress(offsetBytes), srcArray, srcOffset, length);
  }

  @Override
  public void putChar(long offsetBytes, char srcValue) {
    assertBounds(offsetBytes, ARRAY_CHAR_INDEX_SCALE, capacityBytes_);
    mem_.putChar(getAddress(offsetBytes), srcValue);
  }

  @Override
  public void putCharArray(long offsetBytes, char[] srcArray, int srcOffset, int length) {
    long copyBytes = length << CHAR_SHIFT;
    assertBounds(srcOffset, length, srcArray.length);
    assertBounds(offsetBytes, copyBytes, capacityBytes_);
    mem_.putCharArray(getAddress(offsetBytes), srcArray, srcOffset, length);
  }

  @Override
  public void putDouble(long offsetBytes, double srcValue) {
    assertBounds(offsetBytes, ARRAY_DOUBLE_INDEX_SCALE, capacityBytes_);
    mem_.putDouble(getAddress(offsetBytes), srcValue);
  }

  @Override
  public void putDoubleArray(long offsetBytes, double[] srcArray, int srcOffset, int length) {
    long copyBytes = length << DOUBLE_SHIFT;
    assertBounds(srcOffset, length, srcArray.length);
    assertBounds(offsetBytes, copyBytes, capacityBytes_);
    mem_.putDoubleArray(getAddress(offsetBytes), srcArray, srcOffset, length);
  }

  @Override
  public void putFloat(long offsetBytes, float srcValue) {
    assertBounds(offsetBytes, ARRAY_FLOAT_INDEX_SCALE, capacityBytes_);
    mem_.putFloat(getAddress(offsetBytes), srcValue);
  }

  @Override
  public void putFloatArray(long offsetBytes, float[] srcArray, int srcOffset, int length) {
    long copyBytes = length << FLOAT_SHIFT;
    assertBounds(srcOffset, length, srcArray.length);
    assertBounds(offsetBytes, copyBytes, capacityBytes_);
    mem_.putFloatArray(getAddress(offsetBytes), srcArray, srcOffset, length);
  }

  @Override
  public void putInt(long offsetBytes, int srcValue) {
    assertBounds(offsetBytes, ARRAY_INT_INDEX_SCALE, capacityBytes_);
    mem_.putInt(getAddress(offsetBytes), srcValue);
  }

  @Override
  public void putIntArray(long offsetBytes, int[] srcArray, int srcOffset, int length) {
    long copyBytes = length << INT_SHIFT;
    assertBounds(srcOffset, length, srcArray.length);
    assertBounds(offsetBytes, copyBytes, capacityBytes_);
    mem_.putIntArray(getAddress(offsetBytes), srcArray, srcOffset, length);
  }

  @Override
  public void putLong(long offsetBytes, long srcValue) {
    assertBounds(offsetBytes, ARRAY_LONG_INDEX_SCALE, capacityBytes_);
    mem_.putLong(getAddress(offsetBytes), srcValue);
  }

  @Override
  public void putLongArray(long offsetBytes, long[] srcArray, int srcOffset, int length) {
    long copyBytes = length << LONG_SHIFT;
    assertBounds(srcOffset, length, srcArray.length);
    assertBounds(offsetBytes, copyBytes, capacityBytes_);
    mem_.putLongArray(getAddress(offsetBytes), srcArray, srcOffset, length);
  }

  @Override
  public void putShort(long offsetBytes, short srcValue) {
    assertBounds(offsetBytes, ARRAY_SHORT_INDEX_SCALE, capacityBytes_);
    mem_.putShort(getAddress(offsetBytes), srcValue);
  }

  @Override
  public void putShortArray(long offsetBytes, short[] srcArray, int srcOffset, int length) {
    long copyBytes = length << SHORT_SHIFT;
    assertBounds(srcOffset, length, srcArray.length);
    assertBounds(offsetBytes, copyBytes, capacityBytes_);
    mem_.putShortArray(getAddress(offsetBytes), srcArray, srcOffset, length);
  }

  @Override
  public void fill(byte value) {
    fill(0, capacityBytes_, value);
  }

  @Override
  public void fill(long offsetBytes, long lengthBytes, byte value) {
    assertBounds(offsetBytes, lengthBytes, capacityBytes_);
    mem_.fill(getAddress(offsetBytes), lengthBytes, value);
  }

  @Override
  public void setBits(long offsetBytes, byte bitMask) {
    assertBounds(offsetBytes, ARRAY_BYTE_INDEX_SCALE, capacityBytes_);
    long unsafeRawAddress = getAddress(offsetBytes);
    byte value = mem_.getByte(unsafeRawAddress);
    mem_.putByte(unsafeRawAddress, (byte)(value | bitMask));
  }

  //Non-data Memory interface methods

  @Override
  public final long getAddress(final long offsetBytes) {
    return memOffsetBytes_ + offsetBytes;
  }

  @Override
  public long getCapacity() {
    return capacityBytes_;
  }

  @Override
  public MemoryRequest getMemoryRequest() {
    return memReq_;
  }

  @Override
  public Object getParent() {
    return mem_;
  }

  @Override
  public void setMemoryRequest(MemoryRequest memReq) {
    memReq_ = memReq;
  }

  @Override
  public String toHexString(String header, long offsetBytes, int lengthBytes) {
    assertBounds(offsetBytes, lengthBytes, capacityBytes_);
    StringBuilder sb = new StringBuilder();
    sb.append(header).append("\n");
    String s1 = String.format("(..., %d, %d)", offsetBytes, lengthBytes);
    sb.append(this.getClass().getSimpleName()).append(".toHexString").
       append(s1).append(", hash: ").append(this.hashCode()).append("\n");
    sb.append("  MemoryRequest: ");
    if (memReq_ != null) {
      sb.append(memReq_.getClass().getSimpleName()).append(", hash: ").append(memReq_.hashCode());
    } else sb.append("null");
    return mem_.toHexString(sb.toString(), getAddress(offsetBytes), lengthBytes);
  }

}
