/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.memory;

import static com.yahoo.sketches.memory.UnsafeUtil.unsafe;

/**
 * The AllocMemory class is a subclass of NativeMemory and is used to allocate direct, off-heap 
 * native memory, which is then accessed by the NativeMemory methods. 
 * It is the responsibility of the calling class to free this memory using freeMemory() when done. 
 * 
 * <p>The task of direct allocation was moved to this sub-class for performance reasons. 
 *
 * @author Lee Rhodes
 */
//@SuppressWarnings("restriction")
public class AllocMemory extends MemoryMappedFile {
  
  /**
   * Constructor for allocate native memory. 
   * 
   * <p>Allocates and provides access to capacityBytes directly in native (off-heap) memory 
   * leveraging the Memory interface.  The MemoryRequest callback is set to null.
   * @param capacityBytes the size in bytes of the native memory
   */
  public AllocMemory(long capacityBytes) {
    super(0L, null, null);
    super.nativeRawStartAddress_ = unsafe.allocateMemory(capacityBytes);
    super.capacityBytes_ = capacityBytes;
    super.memReq_ = null;
  }
  
  /**
   * Constructor for allocate native memory with MemoryRequest.
   * 
   * <p>Allocates and provides access to capacityBytes directly in native (off-heap) memory leveraging
   * the Memory interface. 
   * @param capacityBytes the size in bytes of the native memory
   * @param memReq The MemoryRequest callback
   */
  public AllocMemory(long capacityBytes, MemoryRequest memReq) {
    super(0L, null, null);
    super.nativeRawStartAddress_ = unsafe.allocateMemory(capacityBytes);
    super.capacityBytes_ = capacityBytes;
    super.memReq_ = memReq;
  }
  
  /**
   * Constructor for reallocate native memory.
   * 
   * <p>Reallocates the given off-heap NativeMemory to a new a new native (off-heap) memory 
   * location and copies the contents of the original given NativeMemory to the new location. 
   * Any memory beyond the capacity of the original given NativeMemory will be uninitialized. 
   * Dispose of this new memory by calling {@link NativeMemory#freeMemory()}. 
   * @param origMem The original NativeMemory that needs to be reallocated and must not be null. 
   * The OS is free to just expand the capacity of the current allocation at the same native 
   * address, or reassign a completely different native address in which case the origMem will be 
   * freed by the OS. 
   * The origMem capacity will be set to zero and must not be used again.
   * 
   * @param newCapacityBytes the desired new capacity of the newly allocated memory in bytes
   * @param memReq The MemoryRequest callback, which may be null.
   */
  public AllocMemory(NativeMemory origMem, long newCapacityBytes, MemoryRequest memReq) {
    super(0L, null, null);
    super.nativeRawStartAddress_ = unsafe.reallocateMemory(origMem.nativeRawStartAddress_, 
        newCapacityBytes);
    super.capacityBytes_ = newCapacityBytes;
    this.memReq_ = memReq;
    origMem.nativeRawStartAddress_ = 0; //does not require freeMem
    origMem.capacityBytes_ = 0; //Cannot be used again
  }
  
  /**
   * Constructor for allocate native memory, copy and clear.
   * 
   * <p>Allocate a new native (off-heap) memory with capacityBytes; copy the contents of origMem 
   * from zero to copyToBytes; clear the new memory from copyToBytes to capacityBytes.
   * @param origMem The original NativeMemory, a portion of which will be copied to the 
   * newly allocated Memory. 
   * The reference must not be null.
   * This origMem is not modified in any way, may be reused and must be freed appropriately.
   * @param copyToBytes the upper limit of the region to be copied from origMem to the newly 
   * allocated memory. 
   * @param capacityBytes the desired new capacity of the newly allocated memory in bytes and the 
   * upper limit of the region to be cleared. 
   * @param memReq The MemoryRequest callback, which may be null.
   */
  public AllocMemory(NativeMemory origMem, long copyToBytes, long capacityBytes, 
      MemoryRequest memReq) {
    super(0L, null, null);
    super.nativeRawStartAddress_ = unsafe.allocateMemory(capacityBytes);
    super.capacityBytes_ = capacityBytes;
    this.memReq_ = memReq;
    NativeMemory.copy(origMem, 0, this, 0, copyToBytes);
    this.clear(copyToBytes, capacityBytes - copyToBytes);
  }
  
  @Override
  public void freeMemory() {
    super.freeMemory();
  }
  
}
