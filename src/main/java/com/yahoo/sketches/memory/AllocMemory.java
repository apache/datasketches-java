/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.memory;

import static com.yahoo.sketches.memory.UnsafeUtil.unsafe;

import java.util.Arrays;

/**
 * The AllocMemory class is a subclass of NativeMemory and is used to allocate direct, off-heap 
 * native memory, which is then accessed by the NativeMemory methods. 
 * It is the responsibility of the calling class to free this memory using freeMemory() when done. 
 * <p>The task of direct allocation was moved to this sub-class for performance reasons. 
 *
 * @author Lee Rhodes
 */
@SuppressWarnings("restriction")
public class AllocMemory extends NativeMemory {
  
  /**
   * Allocates and provides access to capacityBytes directly in native (off-heap) memory leveraging
   * the Memory interface.  The MemoryRequest callback is set to null.
   * @param capacityBytes the size in bytes of the native memory
   */
  public AllocMemory(final long capacityBytes) {
    super(0L, null, null, unsafe.allocateMemory(capacityBytes), capacityBytes);
    super.memReq_ = null;
  }
  
  /**
   * Allocates and provides access to capacityBytes directly in native (off-heap) memory leveraging
   * the Memory interface.  
   * @param capacityBytes the size in bytes of the native memory
   * @param memReq The MemoryRequest callback
   */
  public AllocMemory(final long capacityBytes, MemoryRequest memReq) {
    super(0L, null, null, unsafe.allocateMemory(capacityBytes), capacityBytes);
    super.memReq_ = memReq;
  }
  
  @Override
  public void freeMemory() {
    super.freeMemory();
  }
  
  /**
   * If the JVM calls this method and a "freeMemory() has not been called" a <i>System.err</i>
   * message will be logged.
   */
  @Override
  protected void finalize() {
    if (requiresFree()) {
      System.err.println(
          "ERROR: freeMemory() has not been called: Address: "+ nativeRawStartAddress_ +
          ", capacity: " + capacityBytes_);
      System.err.println(Arrays.toString(Thread.currentThread().getStackTrace()));
    }
  }
  
}