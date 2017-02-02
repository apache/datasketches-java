/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.memory;

import static com.yahoo.memory.UnsafeUtil.ARRAY_BOOLEAN_BASE_OFFSET;
import static com.yahoo.memory.UnsafeUtil.ARRAY_BOOLEAN_INDEX_SCALE;
import static com.yahoo.memory.UnsafeUtil.ARRAY_BYTE_BASE_OFFSET;
import static com.yahoo.memory.UnsafeUtil.ARRAY_BYTE_INDEX_SCALE;
import static com.yahoo.memory.UnsafeUtil.ARRAY_CHAR_BASE_OFFSET;
import static com.yahoo.memory.UnsafeUtil.ARRAY_CHAR_INDEX_SCALE;
import static com.yahoo.memory.UnsafeUtil.ARRAY_DOUBLE_BASE_OFFSET;
import static com.yahoo.memory.UnsafeUtil.ARRAY_DOUBLE_INDEX_SCALE;
import static com.yahoo.memory.UnsafeUtil.ARRAY_FLOAT_BASE_OFFSET;
import static com.yahoo.memory.UnsafeUtil.ARRAY_FLOAT_INDEX_SCALE;
import static com.yahoo.memory.UnsafeUtil.ARRAY_INT_BASE_OFFSET;
import static com.yahoo.memory.UnsafeUtil.ARRAY_INT_INDEX_SCALE;
import static com.yahoo.memory.UnsafeUtil.ARRAY_LONG_BASE_OFFSET;
import static com.yahoo.memory.UnsafeUtil.ARRAY_LONG_INDEX_SCALE;
import static com.yahoo.memory.UnsafeUtil.ARRAY_SHORT_BASE_OFFSET;
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
import static com.yahoo.memory.UnsafeUtil.UNSAFE_COPY_THRESHOLD;
import static com.yahoo.memory.UnsafeUtil.assertBounds;
import static com.yahoo.memory.UnsafeUtil.checkOverlap;
import static com.yahoo.memory.UnsafeUtil.unsafe;

import java.nio.ByteBuffer;

/**
 * The NativeMemory class implements the Memory interface and is used to access Java byte arrays,
 * int arrays, long arrays and ByteBuffers by presenting them as arguments to the constructors
 * of this class.
 *
 * <p>The sub-class AllocMemory is used to allocate direct, off-heap native memory, which is then
 * accessed by the NativeMemory methods.
 *
 * <p>These methods extend many of the sun.misc.Unsafe class methods. Unsafe is an internal,
 * low-level class used by many Java library classes for performance reasons. To achieve high
 * performance Unsafe DOES NOT do any bounds checking. And for the same performance reasons, the
 * methods in this class DO NOT do any bounds checking when running in a default-configured JVM.
 * However, the methods in this class will perform bounds checking if the JVM is configured to
 * enable asserts (-ea). Test enviornments such as JUnit and TestNG automatically configure the
 * JVM to enable asserts. Thus, it is incumbent on the user of this class to make sure that their
 * code is thoroughly tested.  Violating memory bounds can cause memory segment faults, which takes
 * down the JVM and can be very difficult to debug. </p>
 *
 * @author Lee Rhodes
 */
//@SuppressWarnings("restriction")
public class NativeMemory implements Memory {
  /* Truth table that distinguishes between Requires Free and actual off-heap Direct mode.
  Class        Case                 ObjBaseOff MemArr byteBuf rawAdd CapacityBytes  ReqFree Direct
  NativeMemory byte[], int[], long[]        >0  valid    null      0            >0    FALSE  FALSE
  NativeMemory ByteBuffer Direct             0   null   valid     >0            >0    FALSE   TRUE
  NativeMemory ByteBuffer not Direct        >0  valid   valid      0            >0    FALSE  FALSE
  AllocMemory                                0   null    null     >0            >0     TRUE   TRUE
  MemoryMappedFile                           0   null    null     >0            >0     TRUE   TRUE
  */
  protected final long objectBaseOffset_;
  protected final Object memArray_;
  //holding on to this to make sure that it is not garbage collected before we are done with it.
  protected final ByteBuffer byteBuf_;
  protected long nativeRawStartAddress_;
  protected long capacityBytes_;
  protected MemoryRequest memReq_ = null; //set via AllocMemory or NativeMemory

  //only sets the finals
  protected NativeMemory(final long objectBaseOffset, final Object memArray,
      final ByteBuffer byteBuf) {
    objectBaseOffset_ = objectBaseOffset;
    memArray_ = memArray;
    byteBuf_ = byteBuf;
  }

  /**
   * Provides access to the given byteArray using Memory interface
   * @param byteArray an on-heap byte array
   */
  public NativeMemory(final byte[] byteArray) {
    this(ARRAY_BYTE_BASE_OFFSET, byteArray, null);
    if ((byteArray == null) || (byteArray.length == 0)) {
      throw new IllegalArgumentException(
          "Array must must not be null and have a length greater than zero.");
    }
    nativeRawStartAddress_ = 0L;
    capacityBytes_ = byteArray.length;
  }

  /**
   * Provides access to the given intArray using Memory interface
   * @param intArray an on-heap int array
   */
  public NativeMemory(final int[] intArray) {
    this(ARRAY_INT_BASE_OFFSET, intArray, null);
    if ((intArray == null) || (intArray.length == 0)) {
      throw new IllegalArgumentException(
          "Array must must not be null and have a length greater than zero.");
    }
    nativeRawStartAddress_ = 0L;
    capacityBytes_ = intArray.length << INT_SHIFT;
  }

  /**
   * Provides access to the given longArray using Memory interface
   * @param longArray an on-heap long array
   */
  public NativeMemory(final long[] longArray) {
    this(ARRAY_LONG_BASE_OFFSET, longArray, null);
    if ((longArray == null) || (longArray.length == 0)) {
      throw new IllegalArgumentException(
          "Array must must not be null and have a length greater than zero.");
    }
    nativeRawStartAddress_ = 0L;
    capacityBytes_ = longArray.length << LONG_SHIFT;
  }

  /**
   * Provides access to the backing store of the given ByteBuffer using Memory interface
   * @param byteBuf the given ByteBuffer
   */
  public NativeMemory(final ByteBuffer byteBuf) {
    if (byteBuf.isDirect()) {
      objectBaseOffset_ = 0L;
      memArray_ = null;
      nativeRawStartAddress_ = ((sun.nio.ch.DirectBuffer)byteBuf).address();
    }
    else { //must have array
      objectBaseOffset_ = ARRAY_BYTE_BASE_OFFSET + byteBuf.arrayOffset() * ARRAY_BYTE_INDEX_SCALE;
      memArray_ = byteBuf.array();
      nativeRawStartAddress_ = 0L;
    }
    byteBuf_ = byteBuf;
    capacityBytes_ = byteBuf.capacity();
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
    final long unsafeRawAddress = getAddress(offsetBytes);
    int value = unsafe.getByte(memArray_, unsafeRawAddress) & 0XFF;
    value &= ~bitMask;
    unsafe.putByte(memArray_, unsafeRawAddress, (byte)value);
  }

  @Override
  public void copy(final long srcOffsetBytes, final long dstOffsetBytes, final long lengthBytes) {
    assertBounds(srcOffsetBytes, lengthBytes, capacityBytes_);
    assertBounds(dstOffsetBytes, lengthBytes, capacityBytes_);
    assert checkOverlap(srcOffsetBytes, dstOffsetBytes, lengthBytes) : "regions must not overlap";

    long srcAdd = getAddress(srcOffsetBytes);
    long dstAdd = getAddress(dstOffsetBytes);
    long lenBytes = lengthBytes;

    while (lenBytes > 0) {
      final long size = (lenBytes > UNSAFE_COPY_THRESHOLD) ? UNSAFE_COPY_THRESHOLD : lenBytes;
      unsafe.copyMemory(memArray_, srcAdd, memArray_, dstAdd, lenBytes);
      lenBytes -= size;
      srcAdd += size;
      dstAdd += size;
    }
  }

  @Override
  public void fill(final byte value) {
    fill(0, capacityBytes_, value);
  }

  @Override
  public void fill(final long offsetBytes, final long lengthBytes, final byte value) {
    assertBounds(offsetBytes, lengthBytes, capacityBytes_);
    unsafe.setMemory(memArray_, getAddress(offsetBytes), lengthBytes, value);
  }

  @Override
  public boolean getBoolean(final long offsetBytes) {
    assertBounds(offsetBytes, ARRAY_BOOLEAN_INDEX_SCALE, capacityBytes_);
    return unsafe.getBoolean(memArray_, getAddress(offsetBytes));
  }

  @Override
  public void getBooleanArray(final long offsetBytes, final boolean[] dstArray, final int dstOffset,
      final int length) {
    final long copyBytes = length << BOOLEAN_SHIFT;
    assertBounds(offsetBytes, copyBytes, capacityBytes_);
    assertBounds(dstOffset, length, dstArray.length);
    unsafe.copyMemory(
      memArray_,
      getAddress(offsetBytes),
      dstArray,
      ARRAY_BOOLEAN_BASE_OFFSET + (dstOffset << BOOLEAN_SHIFT),
      copyBytes);
  }

  @Override
  public byte getByte(final long offsetBytes) {
    assertBounds(offsetBytes, ARRAY_BYTE_INDEX_SCALE, capacityBytes_);
    return unsafe.getByte(memArray_, getAddress(offsetBytes));
  }

  @Override
  public void getByteArray(final long offsetBytes, final byte[] dstArray, final int dstOffset,
      final int length) {
    final long copyBytes = length << BYTE_SHIFT;
    assertBounds(offsetBytes, copyBytes, capacityBytes_);
    assertBounds(dstOffset, length, dstArray.length);
    unsafe.copyMemory(
      memArray_,
      getAddress(offsetBytes),
      dstArray,
      ARRAY_BYTE_BASE_OFFSET + (dstOffset << BYTE_SHIFT),
      copyBytes);
  }

  @Override
  public char getChar(final long offsetBytes) {
    assertBounds(offsetBytes, ARRAY_CHAR_INDEX_SCALE, capacityBytes_);
    return unsafe.getChar(memArray_, getAddress(offsetBytes));
  }

  @Override
  public void getCharArray(final long offsetBytes, final char[] dstArray, final int dstOffset,
      final int length) {
    final long copyBytes = length << CHAR_SHIFT;
    assertBounds(offsetBytes, copyBytes, capacityBytes_);
    assertBounds(dstOffset, length, dstArray.length);
    unsafe.copyMemory(
      memArray_,
      getAddress(offsetBytes),
      dstArray,
      ARRAY_CHAR_BASE_OFFSET + (dstOffset << CHAR_SHIFT),
      copyBytes);
  }

  @Override
  public double getDouble(final long offsetBytes) {
    assertBounds(offsetBytes, ARRAY_DOUBLE_INDEX_SCALE, capacityBytes_);
    return unsafe.getDouble(memArray_, getAddress(offsetBytes));
  }

  @Override
  public void getDoubleArray(final long offsetBytes, final double[] dstArray, final int dstOffset,
      final int length) {
    final long copyBytes = length << DOUBLE_SHIFT;
    assertBounds(offsetBytes, copyBytes, capacityBytes_);
    assertBounds(dstOffset, length, dstArray.length);
    unsafe.copyMemory(
      memArray_,
      getAddress(offsetBytes),
      dstArray,
      ARRAY_DOUBLE_BASE_OFFSET + (dstOffset << DOUBLE_SHIFT),
      copyBytes);
  }

  @Override
  public float getFloat(final long offsetBytes) {
    assertBounds(offsetBytes, ARRAY_FLOAT_INDEX_SCALE, capacityBytes_);
    return unsafe.getFloat(memArray_, getAddress(offsetBytes));
  }

  @Override
  public void getFloatArray(final long offsetBytes, final float[] dstArray, final int dstOffset,
      final int length) {
    final long copyBytes = length << FLOAT_SHIFT;
    assertBounds(offsetBytes, copyBytes, capacityBytes_);
    assertBounds(dstOffset, length, dstArray.length);
    unsafe.copyMemory(
      memArray_,
      getAddress(offsetBytes),
      dstArray,
      ARRAY_FLOAT_BASE_OFFSET + (dstOffset << FLOAT_SHIFT),
      copyBytes);
  }

  @Override
  public int getInt(final long offsetBytes) {
    assertBounds(offsetBytes, ARRAY_INT_INDEX_SCALE, capacityBytes_);
    return unsafe.getInt(memArray_, getAddress(offsetBytes));
  }

  @Override
  public void getIntArray(final long offsetBytes, final int[] dstArray, final int dstOffset,
      final int length) {
    final long copyBytes = length << INT_SHIFT;
    assertBounds(offsetBytes, copyBytes, capacityBytes_);
    assertBounds(dstOffset, length, dstArray.length);
    unsafe.copyMemory(
      memArray_,
      getAddress(offsetBytes),
      dstArray,
      ARRAY_INT_BASE_OFFSET  + (dstOffset << INT_SHIFT),
      copyBytes);
  }

  @Override
  public long getLong(final long offsetBytes) {
    assertBounds(offsetBytes, ARRAY_LONG_INDEX_SCALE, capacityBytes_);
    return unsafe.getLong(memArray_, getAddress(offsetBytes));
  }

  @Override
  public void getLongArray(final long offsetBytes, final long[] dstArray, final int dstOffset,
      final int length) {
    final long copyBytes = length << LONG_SHIFT;
    assertBounds(offsetBytes, copyBytes, capacityBytes_);
    assertBounds(dstOffset, length, dstArray.length);
    unsafe.copyMemory(
      memArray_,
      getAddress(offsetBytes),
      dstArray,
      ARRAY_LONG_BASE_OFFSET + (dstOffset << LONG_SHIFT),
      copyBytes);
  }

  @Override
  public short getShort(final long offsetBytes) {
    assertBounds(offsetBytes, ARRAY_SHORT_INDEX_SCALE, capacityBytes_);
    return unsafe.getShort(memArray_, getAddress(offsetBytes));
  }

  @Override
  public void getShortArray(final long offsetBytes, final short[] dstArray, final int dstOffset,
      final int length) {
    final long copyBytes = length << SHORT_SHIFT;
    assertBounds(offsetBytes, copyBytes, capacityBytes_);
    assertBounds(dstOffset, length, dstArray.length);
    unsafe.copyMemory(
      memArray_,
      getAddress(offsetBytes),
      dstArray,
      ARRAY_SHORT_BASE_OFFSET + (dstOffset << SHORT_SHIFT),
      copyBytes);
  }

  @Override
  public boolean isAllBitsClear(final long offsetBytes, final byte bitMask) {
    assertBounds(offsetBytes, ARRAY_BYTE_INDEX_SCALE, capacityBytes_);
    final int value = ~unsafe.getByte(memArray_, getAddress(offsetBytes)) & bitMask & 0XFF;
    return value == bitMask;
  }

  @Override
  public boolean isAllBitsSet(final long offsetBytes, final byte bitMask) {
    assertBounds(offsetBytes, ARRAY_BYTE_INDEX_SCALE, capacityBytes_);
    final int value = unsafe.getByte(memArray_, getAddress(offsetBytes)) & bitMask & 0XFF;
    return value == bitMask;
  }

  @Override
  public boolean isAnyBitsClear(final long offsetBytes, final byte bitMask) {
    assertBounds(offsetBytes, ARRAY_BYTE_INDEX_SCALE, capacityBytes_);
    final int value = ~unsafe.getByte(memArray_, getAddress(offsetBytes)) & bitMask & 0XFF;
    return value != 0;
  }

  @Override
  public boolean isAnyBitsSet(final long offsetBytes, final byte bitMask) {
    assertBounds(offsetBytes, ARRAY_BYTE_INDEX_SCALE, capacityBytes_);
    final int value = unsafe.getByte(memArray_, getAddress(offsetBytes)) & bitMask & 0XFF;
    return value != 0;
  }

  @Override
  public void putBoolean(final long offsetBytes, final boolean srcValue) {
    assertBounds(offsetBytes, ARRAY_BOOLEAN_INDEX_SCALE, capacityBytes_);
    unsafe.putBoolean(memArray_, getAddress(offsetBytes), srcValue);
  }

  @Override
  public void putBooleanArray(final long offsetBytes, final boolean[] srcArray, final int srcOffset,
      final int length) {
    final long copyBytes = length << BOOLEAN_SHIFT;
    assertBounds(srcOffset, length, srcArray.length);
    assertBounds(offsetBytes, copyBytes, capacityBytes_);
    unsafe.copyMemory(
      srcArray,
      ARRAY_BOOLEAN_BASE_OFFSET + (srcOffset << BOOLEAN_SHIFT),
      memArray_,
      getAddress(offsetBytes),
      copyBytes
      );
  }

  @Override
  public void putByte(final long offsetBytes, final byte srcValue) {
    assertBounds(offsetBytes, ARRAY_BYTE_INDEX_SCALE, capacityBytes_);
    unsafe.putByte(memArray_, getAddress(offsetBytes), srcValue);
  }

  @Override
  public void putByteArray(final long offsetBytes, final byte[] srcArray, final int srcOffset,
      final int length) {
    final long copyBytes = length << BYTE_SHIFT;
    assertBounds(srcOffset, length, srcArray.length);
    assertBounds(offsetBytes, copyBytes, capacityBytes_);
    unsafe.copyMemory(
      srcArray,
      ARRAY_BYTE_BASE_OFFSET + (srcOffset << BYTE_SHIFT),
      memArray_,
      getAddress(offsetBytes),
      copyBytes
      );
  }

  @Override
  public void putChar(final long offsetBytes, final char srcValue) {
    assertBounds(offsetBytes, ARRAY_CHAR_INDEX_SCALE, capacityBytes_);
    unsafe.putChar(memArray_, getAddress(offsetBytes), srcValue);
  }

  @Override
  public void putCharArray(final long offsetBytes, final char[] srcArray, final int srcOffset,
      final int length) {
    final long copyBytes = length << CHAR_SHIFT;
    assertBounds(srcOffset, length, srcArray.length);
    assertBounds(offsetBytes, copyBytes, capacityBytes_);
    unsafe.copyMemory(
      srcArray,
      ARRAY_CHAR_BASE_OFFSET + (srcOffset << CHAR_SHIFT),
      memArray_,
      getAddress(offsetBytes),
      copyBytes);
  }

  @Override
  public void putDouble(final long offsetBytes, final double srcValue) {
    assertBounds(offsetBytes, ARRAY_DOUBLE_INDEX_SCALE, capacityBytes_);
    unsafe.putDouble(memArray_, getAddress(offsetBytes), srcValue);
  }

  @Override
  public void putDoubleArray(final long offsetBytes, final double[] srcArray, final int srcOffset,
      final int length) {
    final long copyBytes = length << DOUBLE_SHIFT;
    assertBounds(srcOffset, length, srcArray.length);
    assertBounds(offsetBytes, copyBytes, capacityBytes_);
    unsafe.copyMemory(
      srcArray,
      ARRAY_DOUBLE_BASE_OFFSET + (srcOffset << DOUBLE_SHIFT),
      memArray_,
      getAddress(offsetBytes),
      copyBytes);
  }

  @Override
  public void putFloat(final long offsetBytes, final float srcValue) {
    assertBounds(offsetBytes, ARRAY_FLOAT_INDEX_SCALE, capacityBytes_);
    unsafe.putFloat(memArray_, getAddress(offsetBytes), srcValue);
  }

  @Override
  public void putFloatArray(final long offsetBytes, final float[] srcArray, final int srcOffset,
      final int length) {
    final long copyBytes = length << FLOAT_SHIFT;
    assertBounds(srcOffset, length, srcArray.length);
    assertBounds(offsetBytes, copyBytes, capacityBytes_);
    unsafe.copyMemory(
      srcArray,
      ARRAY_FLOAT_BASE_OFFSET + (srcOffset << FLOAT_SHIFT),
      memArray_,
      getAddress(offsetBytes),
      copyBytes);
  }

  @Override
  public void putInt(final long offsetBytes, final int srcValue) {
    assertBounds(offsetBytes, ARRAY_INT_INDEX_SCALE, capacityBytes_);
    unsafe.putInt(memArray_, getAddress(offsetBytes), srcValue);
  }

  @Override
  public void putIntArray(final long offsetBytes, final int[] srcArray, final int srcOffset,
      final int length) {
    final long copyBytes = length << INT_SHIFT;
    assertBounds(srcOffset, length, srcArray.length);
    assertBounds(offsetBytes, copyBytes, capacityBytes_);
    unsafe.copyMemory(
      srcArray,
      ARRAY_INT_BASE_OFFSET + (srcOffset << INT_SHIFT),
      memArray_,
      getAddress(offsetBytes),
      copyBytes);
  }

  @Override
  public void putLong(final long offsetBytes, final long srcValue) {
    assertBounds(offsetBytes, ARRAY_LONG_INDEX_SCALE, capacityBytes_);
    unsafe.putLong(memArray_, getAddress(offsetBytes), srcValue);
  }

  @Override
  public void putLongArray(final long offsetBytes, final long[] srcArray, final int srcOffset,
      final int length) {
    final long copyBytes = length << LONG_SHIFT;
    assertBounds(srcOffset, length, srcArray.length);
    assertBounds(offsetBytes, copyBytes, capacityBytes_);
    unsafe.copyMemory(
      srcArray,
      ARRAY_LONG_BASE_OFFSET + (srcOffset << LONG_SHIFT),
      memArray_,
      getAddress(offsetBytes),
      copyBytes);
  }

  @Override
  public void putShort(final long offsetBytes, final short srcValue) {
    assertBounds(offsetBytes, ARRAY_SHORT_INDEX_SCALE, capacityBytes_);
    unsafe.putShort(memArray_, getAddress(offsetBytes), srcValue);
  }

  @Override
  public void putShortArray(final long offsetBytes, final short[] srcArray, final int srcOffset,
      final int length) {
    final long copyBytes = length << SHORT_SHIFT;
    assertBounds(srcOffset, length, srcArray.length);
    assertBounds(offsetBytes, copyBytes, capacityBytes_);
    unsafe.copyMemory(
      srcArray,
      ARRAY_SHORT_BASE_OFFSET + (srcOffset << SHORT_SHIFT),
      memArray_,
      getAddress(offsetBytes),
      copyBytes);
  }

  @Override
  public void setBits(final long offsetBytes, final byte bitMask) {
    assertBounds(offsetBytes, ARRAY_BYTE_INDEX_SCALE, capacityBytes_);
    final long unsafeRawAddress = getAddress(offsetBytes);
    final byte value = unsafe.getByte(memArray_, unsafeRawAddress);
    unsafe.putByte(memArray_, unsafeRawAddress, (byte)(value | bitMask));
  }

  //Atomic methods

  @Override
  public int addAndGetInt(final long offsetBytes, final int delta) {
    assertBounds(offsetBytes, ARRAY_INT_INDEX_SCALE, capacityBytes_);
    return unsafe.getAndAddInt(memArray_, getAddress(offsetBytes), delta) + delta;
  }

  @Override
  public long addAndGetLong(final long offsetBytes, final long delta) {
    assertBounds(offsetBytes, ARRAY_LONG_INDEX_SCALE, capacityBytes_);
    return unsafe.getAndAddLong(memArray_, getAddress(offsetBytes), delta) + delta;
  }

  @Override
  public boolean compareAndSwapInt(final long offsetBytes, final int expect, final int update) {
    assertBounds(offsetBytes, ARRAY_INT_INDEX_SCALE, capacityBytes_);
    return unsafe.compareAndSwapInt(memArray_, getAddress(offsetBytes), expect, update);
  }

  @Override
  public boolean compareAndSwapLong(final long offsetBytes, final long expect, final long update) {
    assertBounds(offsetBytes, ARRAY_INT_INDEX_SCALE, capacityBytes_);
    return unsafe.compareAndSwapLong(memArray_, getAddress(offsetBytes), expect, update);
  }

  @Override
  public int getAndSetInt(final long offsetBytes, final int newValue) {
    assertBounds(offsetBytes, ARRAY_INT_INDEX_SCALE, capacityBytes_);
    return unsafe.getAndSetInt(memArray_, getAddress(offsetBytes), newValue);
  }

  @Override
  public long getAndSetLong(final long offsetBytes, final long newValue) {
    assertBounds(offsetBytes, ARRAY_LONG_INDEX_SCALE, capacityBytes_);
    return unsafe.getAndSetLong(memArray_, getAddress(offsetBytes), newValue);
  }

  //Non-primitive Memory interface methods

  @Override
  public Object array() {
    return memArray_;
  }

  @Override
  public Memory asReadOnlyMemory() {
    final NativeMemoryR nmr = new NativeMemoryR(objectBaseOffset_, memArray_, byteBuf_);
    nmr.nativeRawStartAddress_ = nativeRawStartAddress_;
    nmr.capacityBytes_ = capacityBytes_;
    nmr.memReq_ = memReq_;
    return nmr;
  }

  @Override
  public ByteBuffer byteBuffer() {
    return byteBuf_;
  }

  /**
   * Returns the Unsafe address plus the given offsetBytes. The Unsafe address may be either the
   * raw native memory address when in direct mode, or the objectBaseOffset if the memArray object
   * is not null.
   * <p>
   * Note that the precise definition of the returned address is slightly different for
   * {@link Memory#getAddress(long)} as it may have a parent in the Memory hierarchy. Also note
   * that for this class only, the {@link #getCumulativeOffset(long)} will return the same
   * result as this method.</p>
   * @param offsetBytes the given offset in bytes from the start address of this Memory.
   * @return the Unsafe address plus the given offsetBytes.
   * @see Memory#getAddress(long)
   */
  @Override
  public final long getAddress(final long offsetBytes) {
    assertBounds(offsetBytes, 0, capacityBytes_);
    assert (nativeRawStartAddress_ > 0) ^ (objectBaseOffset_ > 0); //only one must be zero
    return nativeRawStartAddress_ + objectBaseOffset_ + offsetBytes;
  }

  @Override
  public long getCapacity() {
    return capacityBytes_;
  }

  @Override
  public long getCumulativeOffset(final long offsetBytes) {
    return getAddress(offsetBytes);
  }

  @Override
  public MemoryRequest getMemoryRequest() {
    return memReq_;
  }

  @Override
  public Object getParent() {
    return memArray_;
  }

  @Override
  public boolean hasArray() {
    return (memArray_ != null);
  }

  @Override
  public boolean hasByteBuffer() {
    return (byteBuf_ != null);
  }

  @Override
  public boolean isAllocated() {
    return (capacityBytes_ > 0L);
  }

  @Override
  public boolean isDirect() {
    return nativeRawStartAddress_ > 0;
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
    final StringBuilder sb = new StringBuilder();
    sb.append(header).append(LS);
    final String s1 = String.format("(..., %d, %d)", offsetBytes, lengthBytes);
    sb.append(this.getClass().getSimpleName()).append(".toHexString")
      .append(s1).append(", hash: ").append(this.hashCode()).append(LS);
    sb.append("  MemoryRequest: ");
    if (memReq_ != null) {
      sb.append(memReq_.getClass().getSimpleName()).append(", hash: ").append(memReq_.hashCode());
    } else { sb.append("null"); }
    return toHex(sb.toString(), offsetBytes, lengthBytes);
  }

  //NativeMemory only methods

  /**
   * Copies bytes from a source Memory to the destination Memory.  If the source and destination
   * are the same Memory use the single Memory copy method.  Nonetheless, if the source and
   * destination Memories are derived from the same underlying base Memory, the source and the
   * destination regions should not overlap within the base Memory region.
   * This is difficult to check at run time, so be warned that this overlap could cause
   * unpredictable results.
   * @param source the source Memory
   * @param srcOffsetBytes the source offset
   * @param destination the destination Memory
   * @param dstOffsetBytes the destination offset
   * @param lengthBytes the number of bytes to copy
   */
  public static final void copy(final Memory source, final long srcOffsetBytes,
      final Memory destination, final long dstOffsetBytes, final long lengthBytes) {

    assertBounds(srcOffsetBytes, lengthBytes, source.getCapacity());
    assertBounds(dstOffsetBytes, lengthBytes, destination.getCapacity());

    if (destination.isReadOnly()) {
      throw new ReadOnlyMemoryException();
    }

    long srcAdd = source.getCumulativeOffset(srcOffsetBytes);
    long dstAdd = destination.getCumulativeOffset(dstOffsetBytes);
    final Object srcParent = (source.isDirect()) ? null : source.array();
    final Object dstParent = (destination.isDirect()) ? null : destination.array();
    long lenBytes = lengthBytes;

    while (lenBytes > 0) {
      final long chunkBytes = (lenBytes > UNSAFE_COPY_THRESHOLD) ? UNSAFE_COPY_THRESHOLD : lenBytes;
      unsafe.copyMemory(srcParent, srcAdd, dstParent, dstAdd, lenBytes);
      lenBytes -= chunkBytes;
      srcAdd += chunkBytes;
      dstAdd += chunkBytes;
    }
  }

  /**
   * This frees this Memory only if it is required. This always sets the capacity to zero
   * and the reference to MemoryRequest to null, which effectively disables this instance.
   *
   * <p>It is always safe to call this method when you are done with this class.
   */
  public void freeMemory() {
    if (requiresFree()) {
        unsafe.freeMemory(nativeRawStartAddress_);
        nativeRawStartAddress_ = 0L;
    }
    capacityBytes_ = 0L;
    memReq_ = null;
  }

  //Restricted methods

  /**
   * Returns a formatted hex string of an area of this Memory.
   * Used primarily for testing.
   * @param header a descriptive header
   * @param offsetBytes offset bytes relative to the Memory start
   * @param lengthBytes number of bytes to convert to a hex string
   * @return a formatted hex string in a human readable array
   */
  private String toHex(final String header, final long offsetBytes, final int lengthBytes) {
    assertBounds(offsetBytes, lengthBytes, capacityBytes_);
    final long unsafeRawAddress = getAddress(offsetBytes);
    final StringBuilder sb = new StringBuilder();
    sb.append(header).append(LS);
    sb.append("Raw Address         : ").append(nativeRawStartAddress_).append(LS);
    sb.append("Object Offset       : ").append(objectBaseOffset_).append(": ");
    sb.append( (memArray_ == null) ? "null" : memArray_.getClass().getSimpleName()).append(LS);
    sb.append("Relative Offset     : ").append(offsetBytes).append(LS);
    sb.append("Total Offset        : ").append(unsafeRawAddress).append(LS);
    sb.append("Native Region       :  0  1  2  3  4  5  6  7");
    long j = offsetBytes;
    final StringBuilder sb2 = new StringBuilder();
    for (long i = 0; i < lengthBytes; i++) {
      final int b = unsafe.getByte(memArray_, unsafeRawAddress + i) & 0XFF;
      if ((i != 0) && ((i % 8) == 0)) {
        sb.append(String.format("%n%20s: ", j)).append(sb2);
        j += 8;
        sb2.setLength(0);
      }
      sb2.append(String.format("%02x ", b));
    }
    sb.append(String.format("%n%20s: ", j)).append(sb2).append(LS);
    return sb.toString();
  }

  /**
   * Returns true if the object requires being freed.
   * This method exists to standardize the check between freeMemory() and finalize()
   *
   * @return true if the object should be freed when it is no longer needed
   */
  protected boolean requiresFree() {
    return (nativeRawStartAddress_ != 0L) && (byteBuf_ == null);
  }

}
