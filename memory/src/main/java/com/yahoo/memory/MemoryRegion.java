/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.memory;

import static com.yahoo.memory.UnsafeUtil.ARRAY_BOOLEAN_INDEX_SCALE;
import static com.yahoo.memory.UnsafeUtil.ARRAY_BYTE_INDEX_SCALE;
import static com.yahoo.memory.UnsafeUtil.ARRAY_CHAR_INDEX_SCALE;
import static com.yahoo.memory.UnsafeUtil.ARRAY_DOUBLE_INDEX_SCALE;
import static com.yahoo.memory.UnsafeUtil.ARRAY_FLOAT_INDEX_SCALE;
import static com.yahoo.memory.UnsafeUtil.ARRAY_INT_INDEX_SCALE;
import static com.yahoo.memory.UnsafeUtil.ARRAY_LONG_INDEX_SCALE;
import static com.yahoo.memory.UnsafeUtil.ARRAY_SHORT_INDEX_SCALE;
import static com.yahoo.memory.UnsafeUtil.BOOLEAN_SHIFT;
import static com.yahoo.memory.UnsafeUtil.BYTE_SHIFT;
import static com.yahoo.memory.UnsafeUtil.CHAR_SHIFT;
import static com.yahoo.memory.UnsafeUtil.DOUBLE_SHIFT;
import static com.yahoo.memory.UnsafeUtil.FLOAT_SHIFT;
import static com.yahoo.memory.UnsafeUtil.INT_SHIFT;
import static com.yahoo.memory.UnsafeUtil.LONG_SHIFT;
import static com.yahoo.memory.UnsafeUtil.LS;
import static com.yahoo.memory.UnsafeUtil.SHORT_SHIFT;
import static com.yahoo.memory.UnsafeUtil.assertBounds;

import java.nio.ByteBuffer;

/**
 * The MemoryRegion class implements the Memory interface and provides a means of
 * hierarchically partitioning a large block of native memory into
 * smaller regions of memory, each with their own capacity and offsets. MemoryRegion is immutable.
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
  private final long memOffsetBytes_;
  private final long capacityBytes_;
  private MemoryRequest memReq_ = null;

  /**
   * Defines a region of the given parent Memory by defining an offset and capacity that are
   * within the boundaries of the parent.
   * @param memory the parent Memory
   * @param memOffsetBytes the starting offset in bytes of this region with respect to the
   * start of the parent memory.
   * @param capacityBytes the capacity in bytes of this region.
   */
  public MemoryRegion(final Memory memory, final long memOffsetBytes, final long capacityBytes) {
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
  public MemoryRegion(final Memory memory, final long memOffsetBytes, final long capacityBytes,
      final MemoryRequest memReq) {
    assertBounds(memOffsetBytes, capacityBytes, memory.getCapacity());
    mem_ = memory;
    memOffsetBytes_ = memOffsetBytes;
    capacityBytes_ = capacityBytes;
    memReq_ = memReq;
  }

  /**
   * Reassign the offset and capacity of this MemoryRegion.
   * This is now deprecated and will throw an exception.  MemoryRegion is now immutable.
   * @param memOffsetBytes the given offset from the parent's start
   * @param capacityBytes the given capacity of this region
   * @deprecated This could have created difficult to diagnose bugs in MemoryRegion hierarchies.
   * This is now deprecated and will throw an exception.  MemoryRegion is now immutable.
   */
  public void reassign(final long memOffsetBytes, final long capacityBytes) {
    throw new UnsupportedOperationException("MemoryRegion is immutable.");
  }

  @Override
  public void clear() {
    fill(0, capacityBytes_, (byte) 0);
  }

  @Override
  public void clear(final long offsetBytes, final long lengthBytes) {
    fill(offsetBytes, lengthBytes, (byte) 0);
  }

  @Override
  public void clearBits(final long offsetBytes, final byte bitMask) {
    assertBounds(offsetBytes, ARRAY_BYTE_INDEX_SCALE, capacityBytes_);
    mem_.clearBits(getAddress(offsetBytes), bitMask);
  }

  @Override
  public void copy(final long srcOffsetBytes, final long dstOffsetBytes, final long lengthBytes) {
    assertBounds(srcOffsetBytes, lengthBytes, capacityBytes_);
    assertBounds(srcOffsetBytes, lengthBytes, capacityBytes_);
    final long min = Math.min(srcOffsetBytes, dstOffsetBytes);
    final long max = Math.max(srcOffsetBytes, dstOffsetBytes);
    assertBounds(min, lengthBytes, max); //regions must not overlap
    final long srcAdd = getAddress(srcOffsetBytes);
    final long dstAdd = getAddress(dstOffsetBytes);
    mem_.copy(srcAdd, dstAdd, lengthBytes);
  }

  @Override
  public void fill(final byte value) {
    fill(0, capacityBytes_, value);
  }

  @Override
  public void fill(final long offsetBytes, final long lengthBytes, final byte value) {
    assertBounds(offsetBytes, lengthBytes, capacityBytes_);
    mem_.fill(getAddress(offsetBytes), lengthBytes, value);
  }

  @Override
  public boolean getBoolean(final long offsetBytes) {
    assertBounds(offsetBytes, ARRAY_BOOLEAN_INDEX_SCALE, capacityBytes_);
    return mem_.getBoolean(getAddress(offsetBytes));
  }

  @Override
  public void getBooleanArray(final long offsetBytes, final boolean[] dstArray, final int dstOffset,
      final int length) {
    final long copyBytes = length << BOOLEAN_SHIFT;
    assertBounds(offsetBytes, copyBytes, capacityBytes_);
    assertBounds(dstOffset, length, dstArray.length);
    mem_.getBooleanArray(getAddress(offsetBytes), dstArray, dstOffset, length);
  }

  @Override
  public byte getByte(final long offsetBytes) {
    assertBounds(offsetBytes, ARRAY_BYTE_INDEX_SCALE, capacityBytes_);
    return mem_.getByte(getAddress(offsetBytes));
  }

  @Override
  public void getByteArray(final long offsetBytes, final byte[] dstArray, final int dstOffset,
      final int length) {
    final long copyBytes = length << BYTE_SHIFT;
    assertBounds(offsetBytes, copyBytes, capacityBytes_);
    assertBounds(dstOffset, length, dstArray.length);
    mem_.getByteArray(getAddress(offsetBytes), dstArray, dstOffset, length);
  }

  @Override
  public char getChar(final long offsetBytes) {
    assertBounds(offsetBytes, ARRAY_CHAR_INDEX_SCALE, capacityBytes_);
    return mem_.getChar(getAddress(offsetBytes));
  }

  @Override
  public void getCharArray(final long offsetBytes, final char[] dstArray, final int dstOffset,
      final int length) {
    final long copyBytes = length << CHAR_SHIFT;
    assertBounds(offsetBytes, copyBytes, capacityBytes_);
    assertBounds(dstOffset, length, dstArray.length);
    mem_.getCharArray(getAddress(offsetBytes), dstArray, dstOffset, length);
  }

  @Override
  public double getDouble(final long offsetBytes) {
    assertBounds(offsetBytes, ARRAY_DOUBLE_INDEX_SCALE, capacityBytes_);
    return mem_.getDouble(getAddress(offsetBytes));
  }

  @Override
  public void getDoubleArray(final long offsetBytes, final double[] dstArray, final int dstOffset,
      final int length) {
    final long copyBytes = length << DOUBLE_SHIFT;
    assertBounds(offsetBytes, copyBytes, capacityBytes_);
    assertBounds(dstOffset, length, dstArray.length);
    mem_.getDoubleArray(getAddress(offsetBytes), dstArray, dstOffset, length);
  }

  @Override
  public float getFloat(final long offsetBytes) {
    assertBounds(offsetBytes, ARRAY_FLOAT_INDEX_SCALE, capacityBytes_);
    return mem_.getFloat(getAddress(offsetBytes));
  }

  @Override
  public void getFloatArray(final long offsetBytes, final float[] dstArray, final int dstOffset,
      final int length) {
    final long copyBytes = length << FLOAT_SHIFT;
    assertBounds(offsetBytes, copyBytes, capacityBytes_);
    assertBounds(dstOffset, length, dstArray.length);
    mem_.getFloatArray(getAddress(offsetBytes), dstArray, dstOffset, length);
  }

  @Override
  public int getInt(final long offsetBytes) {
    assertBounds(offsetBytes, ARRAY_INT_INDEX_SCALE, capacityBytes_);
    return mem_.getInt(getAddress(offsetBytes));
  }

  @Override
  public void getIntArray(final long offsetBytes, final int[] dstArray, final int dstOffset,
      final int length) {
    final long copyBytes = length << INT_SHIFT;
    assertBounds(offsetBytes, copyBytes, capacityBytes_);
    assertBounds(dstOffset, length, dstArray.length);
    mem_.getIntArray(getAddress(offsetBytes), dstArray, dstOffset, length);
  }

  @Override
  public long getLong(final long offsetBytes) {
    assertBounds(offsetBytes, ARRAY_LONG_INDEX_SCALE, capacityBytes_);
    return mem_.getLong(getAddress(offsetBytes));
  }

  @Override
  public void getLongArray(final long offsetBytes, final long[] dstArray, final int dstOffset,
      final int length) {
    final long copyBytes = length << LONG_SHIFT;
    assertBounds(offsetBytes, copyBytes, capacityBytes_);
    assertBounds(dstOffset, length, dstArray.length);
    mem_.getLongArray(getAddress(offsetBytes), dstArray, dstOffset, length);
  }

  @Override
  public short getShort(final long offsetBytes) {
    assertBounds(offsetBytes, ARRAY_SHORT_INDEX_SCALE, capacityBytes_);
    return mem_.getShort(getAddress(offsetBytes));
  }

  @Override
  public void getShortArray(final long offsetBytes, final short[] dstArray, final int dstOffset,
      final int length) {
    final long copyBytes = length << SHORT_SHIFT;
    assertBounds(offsetBytes, copyBytes, capacityBytes_);
    assertBounds(dstOffset, length, dstArray.length);
    mem_.getShortArray(getAddress(offsetBytes), dstArray, dstOffset, length);
  }

  @Override
  public boolean isAllBitsClear(final long offsetBytes, final byte bitMask) {
    assertBounds(offsetBytes, ARRAY_BYTE_INDEX_SCALE, capacityBytes_);
    final int value = ~mem_.getByte(getAddress(offsetBytes)) & bitMask & 0XFF;
    return value == bitMask;
  }

  @Override
  public boolean isAllBitsSet(final long offsetBytes, final byte bitMask) {
    assertBounds(offsetBytes, ARRAY_BYTE_INDEX_SCALE, capacityBytes_);
    final int value = mem_.getByte(getAddress(offsetBytes)) & bitMask & 0XFF;
    return value == bitMask;
  }

  @Override
  public boolean isAnyBitsClear(final long offsetBytes, final byte bitMask) {
    assertBounds(offsetBytes, ARRAY_BYTE_INDEX_SCALE, capacityBytes_);
    final int value = ~mem_.getByte(getAddress(offsetBytes)) & bitMask & 0XFF;
    return value != 0;
  }

  @Override
  public boolean isAnyBitsSet(final long offsetBytes, final byte bitMask) {
    assertBounds(offsetBytes, ARRAY_BYTE_INDEX_SCALE, capacityBytes_);
    final int value = mem_.getByte(getAddress(offsetBytes)) & bitMask & 0XFF;
    return value != 0;
  }

  @Override
  public void putBoolean(final long offsetBytes, final boolean srcValue) {
    assertBounds(offsetBytes, ARRAY_BOOLEAN_INDEX_SCALE, capacityBytes_);
    mem_.putBoolean(getAddress(offsetBytes), srcValue);
  }

  @Override
  public void putBooleanArray(final long offsetBytes, final boolean[] srcArray, final int srcOffset,
      final int length) {
    final long copyBytes = length << BOOLEAN_SHIFT;
    assertBounds(srcOffset, length, srcArray.length);
    assertBounds(offsetBytes, copyBytes, capacityBytes_);
    mem_.putBooleanArray(getAddress(offsetBytes), srcArray, srcOffset, length);
  }

  @Override
  public void putByte(final long offsetBytes, final byte srcValue) {
    assertBounds(offsetBytes, ARRAY_BYTE_INDEX_SCALE, capacityBytes_);
    mem_.putByte(getAddress(offsetBytes), srcValue);
  }

  @Override
  public void putByteArray(final long offsetBytes, final byte[] srcArray, final int srcOffset,
      final int length) {
    final long copyBytes = length << BYTE_SHIFT;
    assertBounds(srcOffset, length, srcArray.length);
    assertBounds(offsetBytes, copyBytes, capacityBytes_);
    mem_.putByteArray(getAddress(offsetBytes), srcArray, srcOffset, length);
  }

  @Override
  public void putChar(final long offsetBytes, final char srcValue) {
    assertBounds(offsetBytes, ARRAY_CHAR_INDEX_SCALE, capacityBytes_);
    mem_.putChar(getAddress(offsetBytes), srcValue);
  }

  @Override
  public void putCharArray(final long offsetBytes, final char[] srcArray, final int srcOffset,
      final int length) {
    final long copyBytes = length << CHAR_SHIFT;
    assertBounds(srcOffset, length, srcArray.length);
    assertBounds(offsetBytes, copyBytes, capacityBytes_);
    mem_.putCharArray(getAddress(offsetBytes), srcArray, srcOffset, length);
  }

  @Override
  public void putDouble(final long offsetBytes, final double srcValue) {
    assertBounds(offsetBytes, ARRAY_DOUBLE_INDEX_SCALE, capacityBytes_);
    mem_.putDouble(getAddress(offsetBytes), srcValue);
  }

  @Override
  public void putDoubleArray(final long offsetBytes, final double[] srcArray, final int srcOffset,
      final int length) {
    final long copyBytes = length << DOUBLE_SHIFT;
    assertBounds(srcOffset, length, srcArray.length);
    assertBounds(offsetBytes, copyBytes, capacityBytes_);
    mem_.putDoubleArray(getAddress(offsetBytes), srcArray, srcOffset, length);
  }

  @Override
  public void putFloat(final long offsetBytes, final float srcValue) {
    assertBounds(offsetBytes, ARRAY_FLOAT_INDEX_SCALE, capacityBytes_);
    mem_.putFloat(getAddress(offsetBytes), srcValue);
  }

  @Override
  public void putFloatArray(final long offsetBytes, final float[] srcArray, final int srcOffset,
      final int length) {
    final long copyBytes = length << FLOAT_SHIFT;
    assertBounds(srcOffset, length, srcArray.length);
    assertBounds(offsetBytes, copyBytes, capacityBytes_);
    mem_.putFloatArray(getAddress(offsetBytes), srcArray, srcOffset, length);
  }

  @Override
  public void putInt(final long offsetBytes, final int srcValue) {
    assertBounds(offsetBytes, ARRAY_INT_INDEX_SCALE, capacityBytes_);
    mem_.putInt(getAddress(offsetBytes), srcValue);
  }

  @Override
  public void putIntArray(final long offsetBytes, final int[] srcArray, final int srcOffset,
      final int length) {
    final long copyBytes = length << INT_SHIFT;
    assertBounds(srcOffset, length, srcArray.length);
    assertBounds(offsetBytes, copyBytes, capacityBytes_);
    mem_.putIntArray(getAddress(offsetBytes), srcArray, srcOffset, length);
  }

  @Override
  public void putLong(final long offsetBytes, final long srcValue) {
    assertBounds(offsetBytes, ARRAY_LONG_INDEX_SCALE, capacityBytes_);
    mem_.putLong(getAddress(offsetBytes), srcValue);
  }

  @Override
  public void putLongArray(final long offsetBytes, final long[] srcArray, final int srcOffset,
      final int length) {
    final long copyBytes = length << LONG_SHIFT;
    assertBounds(srcOffset, length, srcArray.length);
    assertBounds(offsetBytes, copyBytes, capacityBytes_);
    mem_.putLongArray(getAddress(offsetBytes), srcArray, srcOffset, length);
  }

  @Override
  public void putShort(final long offsetBytes, final short srcValue) {
    assertBounds(offsetBytes, ARRAY_SHORT_INDEX_SCALE, capacityBytes_);
    mem_.putShort(getAddress(offsetBytes), srcValue);
  }

  @Override
  public void putShortArray(final long offsetBytes, final short[] srcArray, final int srcOffset,
      final int length) {
    final long copyBytes = length << SHORT_SHIFT;
    assertBounds(srcOffset, length, srcArray.length);
    assertBounds(offsetBytes, copyBytes, capacityBytes_);
    mem_.putShortArray(getAddress(offsetBytes), srcArray, srcOffset, length);
  }

  @Override
  public void setBits(final long offsetBytes, final byte bitMask) {
    assertBounds(offsetBytes, ARRAY_BYTE_INDEX_SCALE, capacityBytes_);
    final long relativeOffset = getAddress(offsetBytes);
    final byte value = mem_.getByte(relativeOffset);
    mem_.putByte(relativeOffset, (byte)(value | bitMask));
  }

  //Atomic methods

  @Override
  public int addAndGetInt(final long offsetBytes, final int delta) {
    assertBounds(offsetBytes, ARRAY_INT_INDEX_SCALE, capacityBytes_);
    return mem_.addAndGetInt(getAddress(offsetBytes), delta);
  }

  @Override
  public long addAndGetLong(final long offsetBytes, final long delta) {
    assertBounds(offsetBytes, ARRAY_LONG_INDEX_SCALE, capacityBytes_);
    return mem_.addAndGetLong(getAddress(offsetBytes), delta);
  }

  @Override
  public boolean compareAndSwapInt(final long offsetBytes, final int expect, final int update) {
    assertBounds(offsetBytes, ARRAY_INT_INDEX_SCALE, capacityBytes_);
    return mem_.compareAndSwapInt(getAddress(offsetBytes), expect, update);
  }

  @Override
  public boolean compareAndSwapLong(final long offsetBytes, final long expect, final long update) {
    assertBounds(offsetBytes, ARRAY_INT_INDEX_SCALE, capacityBytes_);
    return mem_.compareAndSwapLong(getAddress(offsetBytes), expect, update);
  }

  @Override
  public int getAndSetInt(final long offsetBytes, final int newValue) {
    assertBounds(offsetBytes, ARRAY_INT_INDEX_SCALE, capacityBytes_);
    return mem_.getAndSetInt(getAddress(offsetBytes), newValue);
  }

  @Override
  public long getAndSetLong(final long offsetBytes, final long newValue) {
    assertBounds(offsetBytes, ARRAY_LONG_INDEX_SCALE, capacityBytes_);
    return mem_.getAndSetLong(getAddress(offsetBytes), newValue);
  }

  //Non-primitive Memory interface methods

  @Override
  public Object array() {
    return mem_.array();
  }

  @Override
  public Memory asReadOnlyMemory() {
    final Memory readOnlyMem = mem_.asReadOnlyMemory();
    return new MemoryRegionR(readOnlyMem, memOffsetBytes_, capacityBytes_, memReq_);
  }

  @Override
  public ByteBuffer byteBuffer() {
    return mem_.byteBuffer();
  }

  /**
   * Returns the start address of this Memory relative to its parent plus the given offsetBytes.
   * @param offsetBytes the given offset in bytes from the start address of this Memory
   * relative to its parent.
   * @return the start address of this Memory relative to its parent plus the offset in bytes.
   */
  @Override
  public final long getAddress(final long offsetBytes) {
    return memOffsetBytes_ + offsetBytes;
  }

  @Override
  public long getCapacity() {
    return capacityBytes_;
  }

  @Override
  public long getCumulativeOffset(final long offsetBytes) {
    return mem_.getCumulativeOffset(0L) + getAddress(offsetBytes);
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
  public boolean hasArray() {
    return mem_.hasArray();
  }

  @Override
  public boolean hasByteBuffer() {
    return mem_.hasByteBuffer();
  }

  @Override
  public boolean isAllocated() {
    return (capacityBytes_ > 0L);
  }

  @Override
  public boolean isDirect() {
    return mem_.isDirect();
  }

  @Override
  public boolean isReadOnly() {
    return false;
  }

  @Override
  public void setMemoryRequest(final MemoryRequest memReq) {
    memReq_ = memReq;
  }

  @Override
  public String toHexString(final String header, final long offsetBytes, final int lengthBytes) {
    assertBounds(offsetBytes, lengthBytes, capacityBytes_);
    final StringBuilder sb = new StringBuilder();
    sb.append(header).append(LS);
    final String s1 = String.format("(..., %d, %d)", offsetBytes, lengthBytes);
    sb.append(this.getClass().getSimpleName()).append(".toHexString")
      .append(s1).append(", hash: ").append(this.hashCode()).append(LS);
    sb.append("  MemoryRequest: ");
    if (memReq_ != null) {
      sb.append(memReq_.getClass().getSimpleName()).append(", hash: ").append(memReq_.hashCode());
    } else { sb.append("null"); }
    return mem_.toHexString(sb.toString(), getAddress(offsetBytes), lengthBytes);
  }

}
