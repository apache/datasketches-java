/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.memory;

import static com.yahoo.memory.UnsafeUtil.unsafe;

import sun.misc.Cleaner;

/**
 * The AllocMemory class is a subclass of NativeMemory<sup>1</sup>.  AllocMemory is used to
 * allocate direct, off-heap memory, which can then be accessed by the NativeMemory methods.
 *
 * <p>This class leverages the JVM Cleaner class that replaces {@link java.lang.Object#finalize()}
 * and serves as a back-up if the calling class does not call {@link #freeMemory()}.</p>
 *
 * @author Lee Rhodes
 */
//@SuppressWarnings("restriction")
public class AllocMemory extends NativeMemory {
  private final Cleaner cleaner;

  /**
   * Constructor to allocate native memory.
   *
   * <p>Allocates and provides access to capacityBytes directly in native (off-heap) memory
   * leveraging the Memory interface.  The MemoryRequest callback is set to null.
   * The allocated memory will be 8-byte aligned, but may not be page aligned.
   * @param capacityBytes the size in bytes of the native memory
   */
  public AllocMemory(final long capacityBytes) {
    this(capacityBytes, null);
  }

  /**
   * Constructor for allocate native memory with MemoryRequest.
   *
   * <p>Allocates and provides access to capacityBytes directly in native (off-heap) memory
   * leveraging the Memory interface.
   * The allocated memory will be 8-byte aligned, but may not be page aligned.
   * @param capacityBytes the size in bytes of the native memory
   * @param memReq The MemoryRequest callback
   */
  public AllocMemory(final long capacityBytes, final MemoryRequest memReq) {
    super(
        unsafe.allocateMemory(capacityBytes),
        capacityBytes,
        0L,
        null,
        null);
    super.memReq_ = memReq;

    cleaner = Cleaner.create(this, new Deallocator(nativeBaseAddress_));
  }

  /**
   * Constructor to allocate new off-heap native memory, copy the contents from the given origMem.
   *
   * <p>Reallocates the given off-heap NativeMemory to a new a new native (off-heap) memory
   * location and copies the contents of the original given NativeMemory to the new location.
   * Any memory beyond the capacity of the original given NativeMemory will be uninitialized.
   * The new allocated memory will be 8-byte aligned, but may not be page aligned.
   * @param origMem The original NativeMemory that needs its contents to be reallocated.
   * It must not be null.
   *
   * @param newCapacityBytes the desired new capacity of the newly allocated memory in bytes
   * @param memReq The MemoryRequest callback, which may be null.
   */
  public AllocMemory(final NativeMemory origMem, final long newCapacityBytes,
      final MemoryRequest memReq) {
    this(origMem, origMem.getCapacity(), newCapacityBytes, false, memReq);
  }

  /**
   * Constructor to allocate new off-heap NativeMemory, copy from the given origMem,
   * and optionally clear the remainder.
   *
   * <p>Allocate a new native (off-heap) memory with capacityBytes; copy the contents of origMem
   * from zero to copyToBytes; clear the new memory from copyToBytes to capacityBytes.
   * The new allocated memory will be 8-byte aligned, but may not be page aligned.
   *
   * @param origMem The original Memory, a portion of which will be copied to the newly allocated
   * NativeMemory. The reference must not be null.
   * This origMem is not modified in any way, and may be reused .
   *
   * @param copyToBytes the upper limit of the region to be copied from origMem to the newly
   * allocated memory, and the lower limit of the region to be cleared, if requested.
   *
   * @param capacityBytes the desired new capacity of the newly allocated memory in bytes and the
   * upper limit of the region to be cleared.
   *
   * @param clear if true the remaining region from copyToBytes to capacityBytes will be cleared.
   *
   * @param memReq The MemoryRequest callback, which may be null.
   */
  public AllocMemory(final NativeMemory origMem, final long copyToBytes, final long capacityBytes,
      final boolean clear, final MemoryRequest memReq) {
    this(capacityBytes, memReq);
    NativeMemory.copy(origMem, 0, this, 0, copyToBytes);
    if (clear) { this.clear(copyToBytes, capacityBytes - copyToBytes); }
  }

  @Override
  public void freeMemory() {
    super.capacityBytes_ = 0L;
    super.memReq_ = null;
    cleaner.clean();
    nativeBaseAddress_ = 0L;
  }

  private static final class Deallocator
      implements Runnable
  {
    private long nativeBaseAddress_;

    private Deallocator(final long nativeBaseAddress) {
      assert (nativeBaseAddress != 0);
      this.nativeBaseAddress_ = nativeBaseAddress;
    }

    @Override
    public void run() {
      if (nativeBaseAddress_ == 0) {
        // Paranoia
        return;
      }

      unsafe.freeMemory(nativeBaseAddress_);
    }
  }
}
