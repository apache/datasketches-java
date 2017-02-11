/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.memory;

import static com.yahoo.memory.MemoryUtil.checkBooleanArr;
import static com.yahoo.memory.MemoryUtil.checkByteArr;
import static com.yahoo.memory.MemoryUtil.checkByteBufRO;
import static com.yahoo.memory.MemoryUtil.checkCharArr;
import static com.yahoo.memory.MemoryUtil.checkDoubleArr;
import static com.yahoo.memory.MemoryUtil.checkFloatArr;
import static com.yahoo.memory.MemoryUtil.checkIntArr;
import static com.yahoo.memory.MemoryUtil.checkLongArr;
import static com.yahoo.memory.MemoryUtil.checkShortArr;
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

import java.lang.reflect.Field;
import java.nio.ByteBuffer;

/**
 * The NativeMemory class implements the Memory interface and is used to access Java primitive
 * arrays, and ByteBuffers by presenting them as arguments to the constructors of this class.
 *
 * <p>The sub-class AllocMemory is used to allocate direct, off-heap native memory, which is then
 * accessed by the NativeMemory methods.
 *
 * <p>These methods extend many of the sun.misc.Unsafe class methods. Unsafe is an internal,
 * low-level class used by many Java library classes for performance reasons. To achieve high
 * performance Unsafe DOES NOT do any bounds checking. And for the same performance reasons, the
 * methods in this class DO NOT do any bounds checking when running in a default-configured JVM.</p>
 *
 * <p>However, the methods in this class will perform bounds checking if the JVM is configured to
 * enable asserts (-ea). Test enviornments such as JUnit and TestNG automatically configure the
 * JVM to enable asserts. Thus, it is incumbent on the user of this class to make sure that their
 * code is thoroughly tested.  Violating memory bounds can cause memory segment faults, which takes
 * down the JVM and can be very difficult to debug. </p>
 *
 * @author Lee Rhodes
 */
//@SuppressWarnings("restriction")
public class NativeMemory implements Memory {
  // Truth table that shows relationship between objectBaseOffset, memArray, byteBuf,
  // nativeBaseAddress, capacityBytes and Direct.
  //Class        Case                 ObjBaseOff MemArr ByteBuf RawAdd CapBytes   Direct
  //NativeMemory primitive arrays             >0  valid    null      0       >0    FALSE
  //NativeMemory ByteBuffer Direct             0   null   valid     >0       >0     TRUE
  //NativeMemory ByteBuffer Heap              >0  valid   valid      0       >0    FALSE
  //AllocMemory                                0   null    null     >0       >0     TRUE
  //MemoryMappedFile                           0   null    null     >0       >0     TRUE

  final long objectBaseOffset_; //includes the slice() offset for heap.
  final Object memArray_;
  final ByteBuffer byteBuf_; //holding on to this so that it is not GCed before we are done with it.
  final boolean direct_;

  long nativeBaseAddress_; //includes the slice() offset for direct.
  long capacityBytes_;
  private long cumBaseOffset_;
  MemoryRequest memReq_ = null; //also optionally set via AllocMemory

  protected NativeMemory(
      final long nativeBaseAddress,
      final long capacityBytes,
      final long objectBaseOffset,
      final Object memArray,
      final ByteBuffer byteBuf) {
    nativeBaseAddress_ = nativeBaseAddress;
    capacityBytes_ = capacityBytes;
    objectBaseOffset_ = objectBaseOffset;
    memArray_ = memArray;
    byteBuf_ = byteBuf;

    cumBaseOffset_ = nativeBaseAddress_ + objectBaseOffset_;
    direct_ = nativeBaseAddress_ > 0L;
    if (((cumBaseOffset_ - 1) | nativeBaseAddress | objectBaseOffset | (capacityBytes_ - 1)) < 0) {
      throw new IllegalArgumentException("Illegal constructor arguments.");
    }
  }

  /**
   * Provides access to the given boolean array using Memory interface
   * @param booleanArray an on-heap boolean array
   */
  public NativeMemory(final boolean[] booleanArray) {
    this(0L, checkBooleanArr(booleanArray), ARRAY_BOOLEAN_BASE_OFFSET, booleanArray, null);
  }

  /**
   * Provides access to the given byte array using Memory interface
   * @param byteArray an on-heap byte array
   */
  public NativeMemory(final byte[] byteArray) {
    this(0L, checkByteArr(byteArray), ARRAY_BYTE_BASE_OFFSET, byteArray, null);
  }

  /**
   * Provides access to the given char array using Memory interface
   * @param charArray an on-heap char array
   */
  public NativeMemory(final char[] charArray) {
    this(0L, checkCharArr(charArray), ARRAY_CHAR_BASE_OFFSET, charArray, null);
  }

  /**
   * Provides access to the given short array using Memory interface
   * @param shortArray an on-heap short array
   */
  public NativeMemory(final short[] shortArray) {
    this(0L, checkShortArr(shortArray), ARRAY_SHORT_BASE_OFFSET, shortArray, null);
  }

  /**
   * Provides access to the given int array using Memory interface
   * @param intArray an on-heap int array
   */
  public NativeMemory(final int[] intArray) {
    this(0L, checkIntArr(intArray), ARRAY_INT_BASE_OFFSET, intArray, null);
  }

  /**
   * Provides access to the given long array using Memory interface
   * @param longArray an on-heap long array
   */
  public NativeMemory(final long[] longArray) {
    this(0L, checkLongArr(longArray), ARRAY_LONG_BASE_OFFSET, longArray, null);
  }

  /**
   * Provides access to the given float array using Memory interface
   * @param floatArray an on-heap float array
   */
  public NativeMemory(final float[] floatArray) {
    this(0L, checkFloatArr(floatArray), ARRAY_FLOAT_BASE_OFFSET, floatArray, null);
  }

  /**
   * Provides access to the given double array using Memory interface
   * @param doubleArray an on-heap double array
   */
  public NativeMemory(final double[] doubleArray) {
    this(0L, checkDoubleArr(doubleArray), ARRAY_DOUBLE_BASE_OFFSET, doubleArray, null);
  }

  /**
   * Provides access to the backing store of the given ByteBuffer using Memory interface.
   * If the given <i>ByteBuffer</i> is read-only, the returned <i>Memory</i> will also be a
   * read-only instance of <i>NativeMemory</i>.
   * @param byteBuffer the given <i>ByteBuffer</i>
   * @return a <i>Memory</i> object
   */
  public static Memory wrap(final ByteBuffer byteBuffer) {
    final long nativeBaseAddress;  //includes the slice() offset for direct.
    final long capacityBytes = byteBuffer.capacity();
    final long objectBaseOffset;  //includes the slice() offset for heap.
    final Object memArray;
    final ByteBuffer byteBuf = byteBuffer;

    final boolean readOnly = byteBuffer.isReadOnly();
    final boolean direct = byteBuffer.isDirect();

    if (readOnly) {

      //READ-ONLY DIRECT
      if (direct) {
        //address() is already adjusted for direct slices, so arrayOffset = 0
        nativeBaseAddress = ((sun.nio.ch.DirectBuffer) byteBuf).address();
        objectBaseOffset = 0L;
        memArray = null;

        final NativeMemoryR nmr = new NativeMemoryR(
            nativeBaseAddress, capacityBytes, objectBaseOffset, memArray, byteBuf);
        nmr.memReq_ = null;
        return nmr;
      }

      //READ-ONLY HEAP
      //The messy acquisition of arrayOffset and array
      final long arrayOffset; //created from a slice();
      try {
        Field field = ByteBuffer.class.getDeclaredField("offset");
        field.setAccessible(true);
        arrayOffset = (int) field.get(byteBuffer);

        field = ByteBuffer.class.getDeclaredField("hb"); //the backing byte[]
        field.setAccessible(true);
        memArray = field.get(byteBuffer);
      }
      catch (final IllegalAccessException | NoSuchFieldException e) {
        throw new RuntimeException(
            "Could not get offset/byteArray from OnHeap ByteBuffer instance: " + e.getClass());
      }

      nativeBaseAddress = 0L;
      //adjust for slice() arrayOffset()
      objectBaseOffset = ARRAY_BYTE_BASE_OFFSET + arrayOffset * ARRAY_BYTE_INDEX_SCALE;

      final NativeMemoryR nmr = new NativeMemoryR(
          nativeBaseAddress, capacityBytes, objectBaseOffset, memArray, byteBuf);
      nmr.memReq_ = null;
      return nmr;
    }

    //WRITABLE
    else {

      //WRITABLE-DIRECT
      if (direct) {
        nativeBaseAddress = ((sun.nio.ch.DirectBuffer) byteBuf).address();
        objectBaseOffset = 0L;
        memArray = null;

        final NativeMemory nm = new NativeMemory(
            nativeBaseAddress, capacityBytes, objectBaseOffset, memArray, byteBuf);
        nm.memReq_ = null;
        return nm;
      }

      //WRITABLE-HEAP
      nativeBaseAddress = 0L;
      //adjust for slice() arrayOffset()
      objectBaseOffset = ARRAY_BYTE_BASE_OFFSET + byteBuf.arrayOffset() * ARRAY_BYTE_INDEX_SCALE;
      memArray = byteBuf.array();

      final NativeMemory nm = new NativeMemory(
          nativeBaseAddress, capacityBytes, objectBaseOffset, memArray, byteBuf);
      nm.memReq_ = null;
      return nm;
    }
  }

  /**
   * Provides access to the backing store of the given writable ByteBuffer using Memory interface.
   * For read-only ByteBuffers you must use {@link #wrap(ByteBuffer)}.
   * @param byteBuf the given writable ByteBuffer
   *
   * @deprecated Replaced by {@link #wrap(ByteBuffer)}, which supports both writable and
   * read-only ByteBuffers
   */
  @Deprecated
  public NativeMemory(final ByteBuffer byteBuf) { //will eventually be removed
    checkByteBufRO(byteBuf);

    if (byteBuf.isDirect()) {
      objectBaseOffset_ = 0L;
      memArray_ = null;
      nativeBaseAddress_ = ((sun.nio.ch.DirectBuffer) byteBuf).address();
      cumBaseOffset_ = nativeBaseAddress_ + objectBaseOffset_;
    }
    else { //must have array, but arrayOffset is not accessible when Read-only
      objectBaseOffset_ = ARRAY_BYTE_BASE_OFFSET + byteBuf.arrayOffset() * ARRAY_BYTE_INDEX_SCALE;
      memArray_ = byteBuf.array();
      nativeBaseAddress_ = 0L;
      cumBaseOffset_ = nativeBaseAddress_ + objectBaseOffset_;
    }
    byteBuf_ = byteBuf;
    capacityBytes_ = byteBuf.capacity();
    direct_ = nativeBaseAddress_ > 0L;
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
    final NativeMemoryR nmr = new NativeMemoryR(this);
    nmr.memReq_ = memReq_;
    return nmr;
  }

  @Override
  public ByteBuffer byteBuffer() {
    return byteBuf_;
  }

  @Override
  public long getAddress(final long offsetBytes) {
    assertBounds(offsetBytes, 0, capacityBytes_);
    return cumBaseOffset_ + offsetBytes;
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
    return direct_;
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

  @Override
  public void freeMemory() {
    nativeBaseAddress_ = 0L;
    capacityBytes_ = 0L;
    cumBaseOffset_ = 0L;
    memReq_ = null;
  }

  //NativeMemory only methods

  /**
   * Copies bytes from the source Memory to the destination Memory using the same low-level system
   * copy function as found in {@link java.lang.System#arraycopy(Object, int, Object, int, int)}.
   *
   * <p>If the source and destination are derived from the same underlying base Memory use
   * {@link Memory#copy(long, long, long)} instead. Nonetheless, the source and destination regions
   * should not overlap as it could cause unpredictable results.</p>
   *
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
    sb.append("Native Base Address : ").append(nativeBaseAddress_).append(LS);
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

}
