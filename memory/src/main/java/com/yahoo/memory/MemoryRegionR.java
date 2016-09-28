package com.yahoo.memory;

import java.nio.ByteBuffer;

/**
 * Read-only version of MemoryRegion
 * 
 * @author Praveenkumar Venkatesan
 */
public class MemoryRegionR extends MemoryRegion {

    public MemoryRegionR(Memory memory, long memOffsetBytes, long capacityBytes, MemoryRequest memReq) {
        super(memory, memOffsetBytes, capacityBytes, memReq);
    }

    @Override
    public void clear() {
        throw new ReadOnlyMemoryException();
    }

    @Override
    public void clear(long offsetBytes, long lengthBytes) {
        throw new ReadOnlyMemoryException();
    }

    @Override
    public void clearBits(long offsetBytes, byte bitMask) {
        throw new ReadOnlyMemoryException();
    }

    @Override
    public void copy(long srcOffsetBytes, long dstOffsetBytes, long lengthBytes) {
        throw new ReadOnlyMemoryException();
    }

    @Override
    public void fill(long offsetBytes, long lengthBytes, byte value) {
        throw new ReadOnlyMemoryException();
    }

    @Override
    public void fill(byte value) {
        throw new ReadOnlyMemoryException();
    }
    
    @Override
    public int getAndAddInt(long offsetBytes, int delta) {
        throw new ReadOnlyMemoryException();
    }
    
    @Override
    public long getAndAddLong(long offsetBytes, long delta) {
        throw new ReadOnlyMemoryException();
    }
    
    @Override
    public int getAndSetInt(long offsetBytes, int newValue) {
        throw new ReadOnlyMemoryException();
    }

    @Override
    public long getAndSetLong(long offsetBytes, long newValue) {
        throw new ReadOnlyMemoryException();
    }
    
    @Override
    public void putBoolean(long offsetBytes, boolean srcValue) {
        throw new ReadOnlyMemoryException();
    }

    @Override
    public void putBooleanArray(long offsetBytes, boolean[] srcArray, int srcOffset, int length) {
        throw new ReadOnlyMemoryException();
    }

    @Override
    public void putByte(long offsetBytes, byte srcValue) {
        throw new ReadOnlyMemoryException();
    }

    @Override
    public void putByteArray(long offsetBytes, byte[] srcArray, int srcOffset, int length) {
        throw new ReadOnlyMemoryException();
    }

    @Override
    public void putChar(long offsetBytes, char srcValue) {
        throw new ReadOnlyMemoryException();
    }

    @Override
    public void putCharArray(long offsetBytes, char[] srcArray, int srcOffset, int length) {
        throw new ReadOnlyMemoryException();
    }

    @Override
    public void putDouble(long offsetBytes, double srcValue) {
        throw new ReadOnlyMemoryException();
    }

    @Override
    public void putDoubleArray(long offsetBytes, double[] srcArray, int srcOffset, int length) {
        throw new ReadOnlyMemoryException();
    }

    @Override
    public void putFloat(long offsetBytes, float srcValue) {
        throw new ReadOnlyMemoryException();
    }

    @Override
    public void putFloatArray(long offsetBytes, float[] srcArray, int srcOffset, int length) {
        throw new ReadOnlyMemoryException();
    }

    @Override
    public void putInt(long offsetBytes, int srcValue) {
        throw new ReadOnlyMemoryException();
    }

    @Override
    public void putIntArray(long offsetBytes, int[] srcArray, int srcOffset, int length) {
        throw new ReadOnlyMemoryException();
    }

    @Override
    public void putLong(long offsetBytes, long srcValue) {
        throw new ReadOnlyMemoryException();
    }

    @Override
    public void putLongArray(long offsetBytes, long[] srcArray, int srcOffset, int length) {
        throw new ReadOnlyMemoryException();
    }

    @Override
    public void putShort(long offsetBytes, short srcValue) {
        throw new ReadOnlyMemoryException();
    }

    @Override
    public void putShortArray(long offsetBytes, short[] srcArray, int srcOffset, int length) {
        throw new ReadOnlyMemoryException();
    }

    @Override
    public void setBits(long offsetBytes, byte bitMask) {
        throw new ReadOnlyMemoryException();
    }
    
    //Non-data Memory interface methods
    
    @Override
    public Object array() {
      throw new ReadOnlyMemoryException();
    }
    
    @Override
    public Memory asReadOnlyMemory() { //TODO ?? essentially a copy method
      throw new ReadOnlyMemoryException();
    }
    
    @Override
    public ByteBuffer byteBuffer() {
      throw new ReadOnlyMemoryException();
    }
    
    //getAddress() cannot be overridden, but harmless
    //getCapacity() OK
    
    @Override
    public long getCumulativeOffset(final long offsetBytes) { //why would this be needed
      throw new ReadOnlyMemoryException();
    }
    
    @Override
    public MemoryRequest getMemoryRequest() { //why would this be needed
      throw new ReadOnlyMemoryException();
    }

    @Override
    public NativeMemory getNativeMemory() {
      throw new ReadOnlyMemoryException();
    }
    
    @Override
    public Object getParent() {
      throw new ReadOnlyMemoryException();
    }
    
    //hasArray() OK
    //hasByteBuffer() OK
    //isAllocated() OK
    //isDirect() OK
    
    @Override
    public boolean isReadOnly() {
        return true;
    }

    @Override
    public void setMemoryRequest(MemoryRequest memReq) {
        throw new ReadOnlyMemoryException();
    }
    
    //toHexString OK
    
    //copy Memory to Memory OK, Checks if destination is writable.
    
}

