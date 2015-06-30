/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.memory;

import static com.yahoo.sketches.memory.UnsafeUtil.unsafe;

import java.util.Arrays;

/**
 * This class is used to allocate memory directly off-heap. It is the responsibility of the 
 * calling class to free this memory using NativeMemory.freeMemory() when done.  
 * <p>The task of direct allocation was moved to this sub-class for performance reasons. 
 * (Thanks to Himanshu Gupta for pointing this out.).
 *
 * @author Lee Rhodes
 */
@SuppressWarnings("restriction")
public class AllocMemory extends NativeMemory {
  
  /**
   * Allocates and provides access to capacityBytes directly in native (off-heap) memory leveraging
   * the Memory interface
   * @param capacityBytes the size in bytes of the native memory
   */
  public AllocMemory(final long capacityBytes) {
    super(0L, null, null, unsafe.allocateMemory(capacityBytes), capacityBytes);
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