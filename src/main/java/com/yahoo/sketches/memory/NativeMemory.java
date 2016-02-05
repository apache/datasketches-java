/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.memory;

import java.nio.ByteBuffer;

import static com.yahoo.sketches.memory.UnsafeUtil.BOOLEAN_ARRAY_BASE_OFFSET;
import static com.yahoo.sketches.memory.UnsafeUtil.BOOLEAN_SHIFT;
import static com.yahoo.sketches.memory.UnsafeUtil.BOOLEAN_SIZE;
import static com.yahoo.sketches.memory.UnsafeUtil.BYTE_ARRAY_BASE_OFFSET;
import static com.yahoo.sketches.memory.UnsafeUtil.BYTE_SHIFT;
import static com.yahoo.sketches.memory.UnsafeUtil.BYTE_SIZE;
import static com.yahoo.sketches.memory.UnsafeUtil.CHAR_ARRAY_BASE_OFFSET;
import static com.yahoo.sketches.memory.UnsafeUtil.CHAR_SHIFT;
import static com.yahoo.sketches.memory.UnsafeUtil.CHAR_SIZE;
import static com.yahoo.sketches.memory.UnsafeUtil.DOUBLE_ARRAY_BASE_OFFSET;
import static com.yahoo.sketches.memory.UnsafeUtil.DOUBLE_SHIFT;
import static com.yahoo.sketches.memory.UnsafeUtil.DOUBLE_SIZE;
import static com.yahoo.sketches.memory.UnsafeUtil.FLOAT_ARRAY_BASE_OFFSET;
import static com.yahoo.sketches.memory.UnsafeUtil.FLOAT_SHIFT;
import static com.yahoo.sketches.memory.UnsafeUtil.FLOAT_SIZE;
import static com.yahoo.sketches.memory.UnsafeUtil.INT_ARRAY_BASE_OFFSET;
import static com.yahoo.sketches.memory.UnsafeUtil.INT_SHIFT;
import static com.yahoo.sketches.memory.UnsafeUtil.INT_SIZE;
import static com.yahoo.sketches.memory.UnsafeUtil.LONG_ARRAY_BASE_OFFSET;
import static com.yahoo.sketches.memory.UnsafeUtil.LONG_SHIFT;
import static com.yahoo.sketches.memory.UnsafeUtil.LONG_SIZE;
import static com.yahoo.sketches.memory.UnsafeUtil.SHORT_ARRAY_BASE_OFFSET;
import static com.yahoo.sketches.memory.UnsafeUtil.SHORT_SHIFT;
import static com.yahoo.sketches.memory.UnsafeUtil.SHORT_SIZE;
import static com.yahoo.sketches.memory.UnsafeUtil.UNSAFE_COPY_THRESHOLD;
import static com.yahoo.sketches.memory.UnsafeUtil.assertBounds;
import static com.yahoo.sketches.memory.UnsafeUtil.checkOverlap;
import static com.yahoo.sketches.memory.UnsafeUtil.compatibilityMethods;
import static com.yahoo.sketches.memory.UnsafeUtil.unsafe;

/**
 * The NativeMemory class implements the Memory interface and is used to access Java byte arrays, 
 * long arrays and ByteBuffers by presenting them as arguments to the constructors of this class.
 * <p>The sub-class AllocMemory is used to allocate direct, off-heap native memory, which is then 
 * accessed by the NativeMemory methods. 
 * 
 * @author Lee Rhodes
 */
@SuppressWarnings("restriction")
public class NativeMemory implements Memory {
  /* 
  Class        Case                 ObjBaseOff MemArr byteBuf rawAdd CapacityBytes  ReqFree Direct
  NativeMemory byteArr                      >0  valid    null      0            >0    FALSE  FALSE
  NativeMemory longArr                      >0  valid    null      0            >0    FALSE  FALSE
  NativeMemory ByteBuffer Direct             0   null   valid     >0            >0    FALSE   TRUE
  NativeMemory ByteBuffer not Direct        >0  valid   valid      0            >0    FALSE  FALSE
  AllocMemory  All cases                     0   null    null     >0            >0     TRUE   TRUE
  */
  protected final long objectBaseOffset_;
  protected final Object memArray_;
  //holding on to this to make sure that it is not garbage collected before we are done with it.
  protected final ByteBuffer byteBuf_;
  protected volatile long nativeRawStartAddress_;
  protected volatile long capacityBytes_;
  protected volatile MemoryRequest memReq_ = null; //set via AllocMemory
  
  //only sets the finals
  protected NativeMemory(long objectBaseOffset, Object memArray, ByteBuffer byteBuf) {
    objectBaseOffset_ = objectBaseOffset;
    memArray_ = memArray;
    byteBuf_ = byteBuf;
  }
  
  /**
   * Provides access to the given byteArray using Memory interface
   * @param byteArray an on-heap byte array
   */
  public NativeMemory(byte[] byteArray) {
    this(BYTE_ARRAY_BASE_OFFSET, byteArray, null);
    if ((byteArray == null) || (byteArray.length == 0)) {
      throw new IllegalArgumentException(
          "Array must must not be null and have a length greater than zero.");
    }
    nativeRawStartAddress_ = 0L;
    capacityBytes_ = byteArray.length;
    
  }
  
  /**
   * Provides access to the given longArray using Memory interface
   * @param longArray an on-heap long array
   */
  public NativeMemory(long[] longArray) {
    this(LONG_ARRAY_BASE_OFFSET, longArray, null); 
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
  public NativeMemory(ByteBuffer byteBuf) {
    if (byteBuf.isDirect()) {
      objectBaseOffset_ = 0L;
      memArray_ = null;
      byteBuf_ = byteBuf;
      nativeRawStartAddress_ = ((sun.nio.ch.DirectBuffer)byteBuf).address();
    } 
    else { //must have array
      objectBaseOffset_ = BYTE_ARRAY_BASE_OFFSET;
      memArray_ = byteBuf.array();
      byteBuf_ = byteBuf;
      nativeRawStartAddress_ = 0L;
    }
    capacityBytes_ = byteBuf.capacity();
    
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
    assertBounds(offsetBytes, BYTE_SIZE, capacityBytes_);
    int value = unsafe.getByte(memArray_, getAddress(offsetBytes)) & 0XFF;
    value &= ~bitMask;   
    unsafe.putByte(memArray_, getAddress(offsetBytes), (byte)value);
  }
  
  @Override
  public void copy(long srcOffsetBytes, long dstOffsetBytes, long lengthBytes) {
    assertBounds(srcOffsetBytes, lengthBytes, capacityBytes_);
    assertBounds(dstOffsetBytes, lengthBytes, capacityBytes_);
    assert checkOverlap(srcOffsetBytes, dstOffsetBytes, lengthBytes): "regions must not overlap";
    long srcAdd = getAddress(srcOffsetBytes);
    long dstAdd = getAddress(dstOffsetBytes);
    
    while (lengthBytes > 0) {
      long size = (lengthBytes > UNSAFE_COPY_THRESHOLD)? UNSAFE_COPY_THRESHOLD : lengthBytes;
      unsafe.copyMemory(memArray_, srcAdd, memArray_, dstAdd, lengthBytes);
      lengthBytes -= size;
      srcAdd += size;
      dstAdd += size;
    }
  }
  
  @Override
  public int getAndAddInt(long offsetBytes, int delta) {
    assertBounds(offsetBytes, INT_SIZE, capacityBytes_);
    long unsafeRawAddress = getAddress(offsetBytes);
    return compatibilityMethods.getAndAddInt(memArray_, unsafeRawAddress, delta);
  }
  
  @Override
  public long getAndAddLong(long offsetBytes, long increment) {
    assertBounds(offsetBytes, LONG_SIZE, capacityBytes_);
    long unsafeRawAddress = getAddress(offsetBytes);
    return compatibilityMethods.getAndAddLong(memArray_, unsafeRawAddress, increment);
  }
  
  @Override
  public int getAndSetInt(long offsetBytes, int newValue) {
    assertBounds(offsetBytes, INT_SIZE, capacityBytes_);
    long unsafeRawAddress = getAddress(offsetBytes);
    return compatibilityMethods.getAndSetInt(memArray_, unsafeRawAddress, newValue);
  }
  
  @Override
  public long getAndSetLong(long offsetBytes, long newValue) {
    assertBounds(offsetBytes, LONG_SIZE, capacityBytes_);
    long unsafeRawAddress = getAddress(offsetBytes);
    return compatibilityMethods.getAndSetLong(memArray_, unsafeRawAddress, newValue);
  }
  
  @Override
  public boolean getBoolean(long offsetBytes) {
    assertBounds(offsetBytes, BOOLEAN_SIZE, capacityBytes_);
    return unsafe.getBoolean(memArray_, getAddress(offsetBytes));
  }
  
  @Override
  public void getBooleanArray(long offsetBytes, boolean[] dstArray, int dstOffset, int length) {
    long copyBytes = length << BOOLEAN_SHIFT;
    assertBounds(offsetBytes, copyBytes, capacityBytes_);
    assertBounds(dstOffset, length, dstArray.length);
    unsafe.copyMemory(
      memArray_, 
      getAddress(offsetBytes),
      dstArray, 
      BOOLEAN_ARRAY_BASE_OFFSET + (dstOffset << BOOLEAN_SHIFT),
      copyBytes);
  }
  
  @Override
  public byte getByte(long offsetBytes) {
    assertBounds(offsetBytes, BYTE_SIZE, capacityBytes_);
    return unsafe.getByte(memArray_, getAddress(offsetBytes));
  }
  
  @Override
  public void getByteArray(long offsetBytes, byte[] dstArray, int dstOffset, int length) {
    long copyBytes = length << BYTE_SHIFT;
    assertBounds(offsetBytes, copyBytes, capacityBytes_);
    assertBounds(dstOffset, length, dstArray.length);
    unsafe.copyMemory(
      memArray_,
      getAddress(offsetBytes), 
      dstArray, 
      BYTE_ARRAY_BASE_OFFSET + (dstOffset << BYTE_SHIFT),
      copyBytes);
  }
  
  @Override
  public char getChar(long offsetBytes) {
    assertBounds(offsetBytes, CHAR_SIZE, capacityBytes_);
    return unsafe.getChar(memArray_, getAddress(offsetBytes));
  }
  
  @Override
  public void getCharArray(long offsetBytes, char[] dstArray, int dstOffset, int length) {
    long copyBytes = length << CHAR_SHIFT;
    assertBounds(offsetBytes, copyBytes, capacityBytes_);
    assertBounds(dstOffset, length, dstArray.length);
    unsafe.copyMemory(
      memArray_, 
      getAddress(offsetBytes), 
      dstArray, 
      CHAR_ARRAY_BASE_OFFSET + (dstOffset << CHAR_SHIFT),
      copyBytes);
  }
  
  @Override
  public double getDouble(long offsetBytes) {
    assertBounds(offsetBytes, DOUBLE_SIZE, capacityBytes_);
    return unsafe.getDouble(memArray_, getAddress(offsetBytes));
  }
  
  @Override
  public void getDoubleArray(long offsetBytes, double[] dstArray, int dstOffset, int length) {
    long copyBytes = length << DOUBLE_SHIFT;
    assertBounds(offsetBytes, copyBytes, capacityBytes_);
    assertBounds(dstOffset, length, dstArray.length);
    unsafe.copyMemory(
      memArray_,  
      getAddress(offsetBytes), 
      dstArray, 
      DOUBLE_ARRAY_BASE_OFFSET + (dstOffset << DOUBLE_SHIFT),
      copyBytes);
  }
  
  @Override
  public float getFloat(long offsetBytes) {
    assertBounds(offsetBytes, FLOAT_SIZE, capacityBytes_);
    return unsafe.getFloat(memArray_, getAddress(offsetBytes));
  }
  
  @Override
  public void getFloatArray(long offsetBytes, float[] dstArray, int dstOffset, int length) {
    long copyBytes = length << FLOAT_SHIFT;
    assertBounds(offsetBytes, copyBytes, capacityBytes_);
    assertBounds(dstOffset, length, dstArray.length);
    unsafe.copyMemory(
      memArray_,  
      getAddress(offsetBytes),
      dstArray, 
      FLOAT_ARRAY_BASE_OFFSET + (dstOffset << FLOAT_SHIFT),
      copyBytes);
  }
  
  @Override
  public int getInt(long offsetBytes) {
    assertBounds(offsetBytes, INT_SIZE, capacityBytes_);
    return unsafe.getInt(memArray_, getAddress(offsetBytes));
  }
  
  @Override
  public void getIntArray(long offsetBytes, int[] dstArray, int dstOffset, int length) {
    long copyBytes = length << INT_SHIFT;
    assertBounds(offsetBytes, copyBytes, capacityBytes_);
    assertBounds(dstOffset, length, dstArray.length);
    unsafe.copyMemory(
      memArray_, 
      getAddress(offsetBytes), 
      dstArray, 
      INT_ARRAY_BASE_OFFSET  + (dstOffset << INT_SHIFT),
      copyBytes);
  }
  
  @Override
  public long getLong(long offsetBytes) {
    assertBounds(offsetBytes, LONG_SIZE, capacityBytes_);
    return unsafe.getLong(memArray_, getAddress(offsetBytes));
  }
  
  @Override
  public void getLongArray(long offsetBytes, long[] dstArray, int dstOffset, int length) {
    long copyBytes = length << LONG_SHIFT;
    assertBounds(offsetBytes, copyBytes, capacityBytes_);
    assertBounds(dstOffset, length, dstArray.length);
    unsafe.copyMemory(
      memArray_,  
      getAddress(offsetBytes),
      dstArray, 
      LONG_ARRAY_BASE_OFFSET + (dstOffset << LONG_SHIFT),
      copyBytes);
  }
  
  @Override
  public short getShort(long offsetBytes) {
    assertBounds(offsetBytes, SHORT_SIZE, capacityBytes_);
    return unsafe.getShort(memArray_, getAddress(offsetBytes));
  }
  
  @Override
  public void getShortArray(long offsetBytes, short[] dstArray, int dstOffset, int length) {
    long copyBytes = length << SHORT_SHIFT;
    assertBounds(offsetBytes, copyBytes, capacityBytes_);
    assertBounds(dstOffset, length, dstArray.length);
    unsafe.copyMemory(
      memArray_,  
      getAddress(offsetBytes),
      dstArray, 
      SHORT_ARRAY_BASE_OFFSET + (dstOffset << SHORT_SHIFT),
      copyBytes);
  }
  
  @Override
  public boolean isAllBitsClear(long offsetBytes, byte bitMask) {
    assertBounds(offsetBytes, BYTE_SIZE, capacityBytes_);
    long unsafeRawAddress = getAddress(offsetBytes);
    int value = ~unsafe.getByte(memArray_, unsafeRawAddress) & bitMask & 0XFF; 
    return value == bitMask;
  }
  
  @Override
  public boolean isAllBitsSet(long offsetBytes, byte bitMask) {
    assertBounds(offsetBytes, BYTE_SIZE, capacityBytes_);
    long unsafeRawAddress = getAddress(offsetBytes);
    int value = unsafe.getByte(memArray_, unsafeRawAddress) & bitMask & 0XFF;
    return value == bitMask;
  }
  
  @Override
  public boolean isAnyBitsClear(long offsetBytes, byte bitMask) {
    assertBounds(offsetBytes, BYTE_SIZE, capacityBytes_);
    long unsafeRawAddress = getAddress(offsetBytes);
    int value = ~unsafe.getByte(memArray_, unsafeRawAddress) & bitMask & 0XFF; 
    return value != 0;
  }
  
  @Override
  public boolean isAnyBitsSet(long offsetBytes, byte bitMask) {
    assertBounds(offsetBytes, BYTE_SIZE, capacityBytes_);
    long unsafeRawAddress = getAddress(offsetBytes);
    int value = unsafe.getByte(memArray_, unsafeRawAddress) & bitMask & 0XFF;
    return value != 0;
  }
  
  @Override
  public void putBoolean(long offsetBytes, boolean srcValue) {
    assertBounds(offsetBytes, BOOLEAN_SIZE, capacityBytes_);
    unsafe.putBoolean(memArray_, getAddress(offsetBytes), srcValue);
  }
  
  @Override
  public void putBooleanArray(long offsetBytes, boolean[] srcArray, int srcOffset, int length) {
    long copyBytes = length << BOOLEAN_SHIFT;
    assertBounds(srcOffset, length, srcArray.length);
    assertBounds(offsetBytes, copyBytes, capacityBytes_);
    unsafe.copyMemory(
      srcArray,
      BOOLEAN_ARRAY_BASE_OFFSET + (srcOffset << BOOLEAN_SHIFT),
      memArray_, 
      getAddress(offsetBytes),
      copyBytes
      );
  }
  
  @Override
  public void putByte(long offsetBytes, byte srcValue) {
    assertBounds(offsetBytes, BYTE_SIZE, capacityBytes_);
    unsafe.putByte(memArray_, getAddress(offsetBytes), srcValue);
  }
  
  @Override
  public void putByteArray(long offsetBytes, byte[] srcArray, int srcOffset, int length) {
    long copyBytes = length << BYTE_SHIFT;
    assertBounds(srcOffset, length, srcArray.length);
    assertBounds(offsetBytes, copyBytes, capacityBytes_);
    unsafe.copyMemory(
      srcArray,
      BYTE_ARRAY_BASE_OFFSET + (srcOffset << BYTE_SHIFT),
      memArray_, 
      getAddress(offsetBytes),
      copyBytes
      );
  }
  
  @Override
  public void putChar(long offsetBytes, char srcValue) {
    assertBounds(offsetBytes, CHAR_SIZE, capacityBytes_);
    unsafe.putChar(memArray_, getAddress(offsetBytes), srcValue);
  }
  
  @Override
  public void putCharArray(long offsetBytes, char[] srcArray, int srcOffset, int length) {
    long copyBytes = length << CHAR_SHIFT;
    assertBounds(srcOffset, length, srcArray.length);
    assertBounds(offsetBytes, copyBytes, capacityBytes_);
    unsafe.copyMemory(
      srcArray,
      CHAR_ARRAY_BASE_OFFSET + (srcOffset << CHAR_SHIFT),
      memArray_, 
      getAddress(offsetBytes),
      copyBytes);
  }
  
  @Override
  public void putDouble(long offsetBytes, double srcValue) {
    assertBounds(offsetBytes, DOUBLE_SIZE, capacityBytes_);
    unsafe.putDouble(memArray_, getAddress(offsetBytes), srcValue);
  }
  
  @Override
  public void putDoubleArray(long offsetBytes, double[] srcArray, int srcOffset, int length) {
    long copyBytes = length << DOUBLE_SHIFT;
    assertBounds(srcOffset, length, srcArray.length);
    assertBounds(offsetBytes, copyBytes, capacityBytes_);
    unsafe.copyMemory(
      srcArray,
      DOUBLE_ARRAY_BASE_OFFSET + (srcOffset << DOUBLE_SHIFT),
      memArray_, 
      getAddress(offsetBytes),
      copyBytes);
  }
  
  @Override
  public void putFloat(long offsetBytes, float srcValue) {
    assertBounds(offsetBytes, FLOAT_SIZE, capacityBytes_);
    unsafe.putFloat(memArray_, getAddress(offsetBytes), srcValue);
  }
  
  @Override
  public void putFloatArray(long offsetBytes, float[] srcArray, int srcOffset, int length) {
    long copyBytes = length << FLOAT_SHIFT;
    assertBounds(srcOffset, length, srcArray.length);
    assertBounds(offsetBytes, copyBytes, capacityBytes_);
    unsafe.copyMemory(
      srcArray,
      FLOAT_ARRAY_BASE_OFFSET+ (srcOffset << FLOAT_SHIFT),
      memArray_, 
      getAddress(offsetBytes),
      copyBytes);
  }
  
  @Override
  public void putInt(long offsetBytes, int srcValue) {
    assertBounds(offsetBytes, INT_SIZE, capacityBytes_);
    unsafe.putInt(memArray_, getAddress(offsetBytes), srcValue);
  }
  
  @Override
  public void putIntArray(long offsetBytes, int[] srcArray, int srcOffset, int length) {
    long copyBytes = length << INT_SHIFT;
    assertBounds(srcOffset, length, srcArray.length);
    assertBounds(offsetBytes, copyBytes, capacityBytes_);
    unsafe.copyMemory(
      srcArray,
      INT_ARRAY_BASE_OFFSET + (srcOffset << INT_SHIFT),
      memArray_, 
      getAddress(offsetBytes),
      copyBytes);
  }
  
  @Override
  public void putLong(long offsetBytes, long srcValue) {
    assertBounds(offsetBytes, LONG_SIZE, capacityBytes_);
    unsafe.putLong(memArray_, getAddress(offsetBytes), srcValue);
  }
  
  @Override
  public void putLongArray(long offsetBytes, long[] srcArray, int srcOffset, int length) {
    long copyBytes = length << LONG_SHIFT;
    assertBounds(srcOffset, length, srcArray.length);
    assertBounds(offsetBytes, copyBytes, capacityBytes_);
    unsafe.copyMemory(
      srcArray,
      LONG_ARRAY_BASE_OFFSET + (srcOffset << LONG_SHIFT),
      memArray_, 
      getAddress(offsetBytes),
      copyBytes);
  }
  
  @Override
  public void putShort(long offsetBytes, short srcValue) {
    assertBounds(offsetBytes, SHORT_SIZE, capacityBytes_);
    unsafe.putShort(memArray_, getAddress(offsetBytes), srcValue);
  }
  
  @Override
  public void putShortArray(long offsetBytes, short[] srcArray, int srcOffset, int length) {
    long copyBytes = length << SHORT_SHIFT;
    assertBounds(srcOffset, length, srcArray.length); 
    assertBounds(offsetBytes, copyBytes, capacityBytes_);
    unsafe.copyMemory(
      srcArray,
      SHORT_ARRAY_BASE_OFFSET + (srcOffset << SHORT_SHIFT),
      memArray_,
      getAddress(offsetBytes),
      copyBytes);
  }
  
  @Override
  public void fill(byte value) {
    fill(0, capacityBytes_, value);
  }
  
  @Override
  public void fill(long offsetBytes, long lengthBytes, byte value) {
    assertBounds(offsetBytes, lengthBytes, capacityBytes_);
    unsafe.setMemory(memArray_, getAddress(offsetBytes), lengthBytes, value);
  }
  
  @Override
  public void setBits(long offsetBytes, byte bitMask) {
    assertBounds(offsetBytes, BYTE_SIZE, capacityBytes_);
    long unsafeRawAddress = getAddress(offsetBytes);
    byte value = unsafe.getByte(memArray_, unsafeRawAddress);
    unsafe.putByte(memArray_, unsafeRawAddress, (byte)(value | bitMask));
  }
  
  //Non-data Memory interface methods
  
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
  public MemoryRequest getMemoryRequest() {
    return memReq_;
  }
  
  @Override
  public Object getParent() {
    return memArray_;
  }
  
  @Override
  public void setMemoryRequest(MemoryRequest memReq) {
    memReq_ = memReq;
  }
  
  @Override
  public String toHexString(String header, long offsetBytes, int lengthBytes) {
    StringBuilder sb = new StringBuilder();
    sb.append(header).append("\n");
    String s1 = String.format("(..., %d, %d)", offsetBytes, lengthBytes);
    sb.append(this.getClass().getSimpleName()).append(".toHexString").
       append(s1).append(", hash: ").append(this.hashCode()).append("\n");
    sb.append("  MemoryRequest: ");
    if (memReq_ != null) {
      sb.append(memReq_.getClass().getSimpleName()).append(", hash: ").append(memReq_.hashCode());
    } else sb.append("null");
    return toHex(sb.toString(), offsetBytes, lengthBytes);
  }
  
  //NativeMemory only methods
  
  /**
   * Returns the backing on-heap primitive array if there is one, otherwise returns null
   * @return the backing on-heap primitive array if there is one, otherwise returns null
   */
  public Object array() {
    return memArray_;
  }
  
  /**
   * Returns the backing ByteBuffer if there is one, otherwise returns null
   * @return the backing ByteBuffer if there is one, otherwise returns null
   */
  public ByteBuffer byteBuffer() {
    return byteBuf_;
  }
  
  /**
   * Returns true if this NativeMemory is accessing native (off-heap) memory directly. 
   * This includes the case of a Direct ByteBuffer.
   * @return true if this NativeMemory is accessing native (off-heap) memory directly.
   */
  public boolean isDirect() {
    return nativeRawStartAddress_ > 0;
  }
  
  /**
   * This frees this Memory only if it is required. This always sets the capacity to zero
   * and the reference to MemoryRequest to null, which effectively disables this class. 
   * However, 
   * 
   * It is always safe to call this method when you are done with this class.
   */
  public void freeMemory() {
    if (requiresFree()) {
        unsafe.freeMemory(nativeRawStartAddress_); 
        nativeRawStartAddress_ = 0L;
    }
    capacityBytes_ = 0L;
    memReq_ = null;
  }
  
  /**
   * Returns true if this Memory is backed by an on-heap primitive array
   * @return true if this Memory is backed by an on-heap primitive array
   */
  public boolean hasArray() {
    return (memArray_ != null);
  }
  
  /**
   * Returns true if this Memory is backed by a ByteBuffer
   * @return true if this Memory is backed by a ByteBuffer
   */
  public boolean hasByteBuffer() {
    return (byteBuf_ != null);
  }
  
  /**
   * Returns true if the underlying memory of this Memory has a capacity greater than zero
   * @return true if the underlying memory of this Memory has a capacity greater than zero
   */
  public boolean isAllocated() {
    return (capacityBytes_ > 0L);
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
  private String toHex(String header, long offsetBytes, int lengthBytes) {
    assertBounds(offsetBytes, lengthBytes, capacityBytes_);
    long address = getAddress(offsetBytes);
    StringBuilder sb = new StringBuilder();
    sb.append(header).append("\n");
    sb.append("Raw Address         : ").append(nativeRawStartAddress_).append("\n");
    sb.append("Object Offset       : ").append(objectBaseOffset_).append(": ");
    sb.append( (memArray_ == null)? "null" : memArray_.getClass().getSimpleName()).append("\n");
    sb.append("Relative Offset     : ").append(offsetBytes).append("\n");
    sb.append("Total Offset        : ").append(address).append("\n");
    sb.append("Native Region       :  0  1  2  3  4  5  6  7");
    long j = offsetBytes;
    StringBuilder sb2 = new StringBuilder();
    for (long i=0; i<lengthBytes; i++) {
      int b = unsafe.getByte(memArray_, address + i) & 0XFF;
      if ((i != 0) && ((i % 8) == 0)) {
        sb.append(String.format("\n%20s: ", j)).append(sb2);
        j += 8;
        sb2.setLength(0);
      }
      sb2.append(String.format("%02x ", b));
    }
    sb.append(String.format("\n%20s: ", j)).append(sb2).append("\n");
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
