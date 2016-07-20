/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.memory;

/**
 * The MemoryRequest is a callback interface that is accessible from the Memory interface and 
 * provides a means for a Memory object to request more memory from the calling class and to 
 * free Memory that is no longer needed.
 * 
 * @author Lee Rhodes
 */
public interface MemoryRequest {
  
  /**
   * Request new Memory with the given capacity.
   * @param capacityBytes The capacity being requested
   * @return new Memory with the given capacity. It may be null.
   */
  Memory request(long capacityBytes);
  
  /**
   * Request for allocate, copy and clear.
   * 
   * <p>Request to allocate new Memory with the capacityBytes; copy the contents of origMem from
   * zero to copyToBytes; clear the new memory from copyToBytes to capacityBytes.
   * @param origMem The original Memory, a portion of which will be copied to the
   * newly allocated Memory.
   * The reference must not be null.
   * This origMem is not modified in any way, may be reused and must be freed appropriately.
   * @param copyToBytes the upper limit of the region to be copied from origMem to the newly 
   * allocated memory.
   * @param capacityBytes the desired new capacity of the newly allocated memory in bytes and the 
   * upper limit of the region to be cleared.
   * @return The new Memory with the given capacity. It may be null.
   */
  Memory request(Memory origMem, long copyToBytes, long capacityBytes);
  
  /**
   * The given Memory with its capacity is to be freed
   * @param mem The Memory to be freed
   */
  void free(Memory mem);
  
  /**
   * The given memToFree with its capacity is to be freed.  Providing a reference to the 
   * newly granted Memory enables the receiver of the callback to link the Memory to be
   * freed with this new Memory.
   * @param memToFree the Memory to be freed.
   * @param newMem this reference enables the receiver of the callback to link the Memory to be
   * freed with this new Memory.
   */
  void free(Memory memToFree, Memory newMem);
}
