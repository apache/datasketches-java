/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.memory;

import java.nio.ByteBuffer;

/**
 * Read-only version of MemoryRegion
 *
 * @author Praveenkumar Venkatesan
 */
public class MemoryRegionR extends MemoryRegion {

  public MemoryRegionR(final Memory memory, final long memOffsetBytes, final long capacityBytes,
      final MemoryRequest memReq) {
    super(memory, memOffsetBytes, capacityBytes, memReq);
  }

  @Override
  public void clear() {
    throw new ReadOnlyMemoryException();
  }

  @Override
  public void clear(final long offsetBytes, final long lengthBytes) {
    throw new ReadOnlyMemoryException();
  }

  @Override
  public void clearBits(final long offsetBytes, final byte bitMask) {
    throw new ReadOnlyMemoryException();
  }

  @Override
  public void copy(final long srcOffsetBytes, final long dstOffsetBytes, final long lengthBytes) {
    throw new ReadOnlyMemoryException();
  }

  @Override
  public void fill(final long offsetBytes, final long lengthBytes, final byte value) {
    throw new ReadOnlyMemoryException();
  }

  @Override
  public void fill(final byte value) {
    throw new ReadOnlyMemoryException();
  }

  @Override
  public int getAndAddInt(final long offsetBytes, final int delta) {
    throw new ReadOnlyMemoryException();
  }

  @Override
  public long getAndAddLong(final long offsetBytes, final long delta) {
    throw new ReadOnlyMemoryException();
  }

  @Override
  public int getAndSetInt(final long offsetBytes, final int newValue) {
    throw new ReadOnlyMemoryException();
  }

  @Override
  public long getAndSetLong(final long offsetBytes, final long newValue) {
    throw new ReadOnlyMemoryException();
  }

  @Override
  public void putBoolean(final long offsetBytes, final boolean srcValue) {
    throw new ReadOnlyMemoryException();
  }

  @Override
  public void putBooleanArray(final long offsetBytes, final boolean[] srcArray, final int srcOffset,
      final int length) {
    throw new ReadOnlyMemoryException();
  }

  @Override
  public void putByte(final long offsetBytes, final byte srcValue) {
    throw new ReadOnlyMemoryException();
  }

  @Override
  public void putByteArray(final long offsetBytes, final byte[] srcArray, final int srcOffset,
      final int length) {
    throw new ReadOnlyMemoryException();
  }

  @Override
  public void putChar(final long offsetBytes, final char srcValue) {
    throw new ReadOnlyMemoryException();
  }

  @Override
  public void putCharArray(final long offsetBytes, final char[] srcArray, final int srcOffset,
      final int length) {
    throw new ReadOnlyMemoryException();
  }

  @Override
  public void putDouble(final long offsetBytes, final double srcValue) {
    throw new ReadOnlyMemoryException();
  }

  @Override
  public void putDoubleArray(final long offsetBytes, final double[] srcArray, final int srcOffset,
      final int length) {
    throw new ReadOnlyMemoryException();
  }

  @Override
  public void putFloat(final long offsetBytes, final float srcValue) {
    throw new ReadOnlyMemoryException();
  }

  @Override
  public void putFloatArray(final long offsetBytes, final float[] srcArray, final int srcOffset,
      final int length) {
    throw new ReadOnlyMemoryException();
  }

  @Override
  public void putInt(final long offsetBytes, final int srcValue) {
    throw new ReadOnlyMemoryException();
  }

  @Override
  public void putIntArray(final long offsetBytes, final int[] srcArray, final int srcOffset,
      final int length) {
    throw new ReadOnlyMemoryException();
  }

  @Override
  public void putLong(final long offsetBytes, final long srcValue) {
    throw new ReadOnlyMemoryException();
  }

  @Override
  public void putLongArray(final long offsetBytes, final long[] srcArray, final int srcOffset,
      final int length) {
    throw new ReadOnlyMemoryException();
  }

  @Override
  public void putShort(final long offsetBytes, final short srcValue) {
    throw new ReadOnlyMemoryException();
  }

  @Override
  public void putShortArray(final long offsetBytes, final short[] srcArray, final int srcOffset,
      final int length) {
    throw new ReadOnlyMemoryException();
  }

  @Override
  public void setBits(final long offsetBytes, final byte bitMask) {
    throw new ReadOnlyMemoryException();
  }

  // Non-data Memory interface methods

  @Override
  public Object array() {
    throw new ReadOnlyMemoryException();
  }

  // asReadOnlyMemory() //OK

  @Override
  public ByteBuffer byteBuffer() {
    throw new ReadOnlyMemoryException();
  }

  // getAddress() cannot be overridden, but harmless
  // getCapacity() OK
  // getCumulativeOffset() //OK
  // getMemoryRequest() //OK

  @Override
  public NativeMemory getNativeMemory() {
    throw new ReadOnlyMemoryException();
  }

  @Override
  public Object getParent() {
    throw new ReadOnlyMemoryException();
  }

  // hasArray() OK
  // hasByteBuffer() OK
  // isAllocated() OK
  // isDirect() OK

  @Override
  public boolean isReadOnly() {
    return true;
  }

  @Override
  public void setMemoryRequest(final MemoryRequest memReq) {
    throw new ReadOnlyMemoryException();
  }

  // toHexString OK

  // copy Memory to Memory OK, Checks if destination is writable.

}

