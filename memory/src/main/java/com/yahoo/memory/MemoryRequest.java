/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.memory;

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
   * @return new Memory with the given capacity. If this request is refused it will be null.
   */
  Memory request(long capacityBytes);

  /**
   * Request for allocate and copy.
   *
   * <p>Request to allocate new Memory with the capacityBytes; copy the contents of origMem from
   * zero to copyToBytes.</p>
   *
   * @param origMem The original Memory, a portion, starting at zero, which will be copied to the
   * newly allocated Memory. This reference must not be null.
   * This origMem must not modified in any way, and may be reused or freed by the implementation.
   * The requesting application may NOT assume anything about the origMem.
   *
   * @param copyToBytes the upper limit of the region to be copied from origMem to the newly
   * allocated memory. The upper region of the new Memory may or may not be cleared depending
   * on the implementation.
   *
   * @param capacityBytes the desired new capacity of the newly allocated memory in bytes.
   * @return The new Memory with the given capacity. If this request is refused it will be null.
   */
  Memory request(Memory origMem, long copyToBytes, long capacityBytes);

  /**
   * The given Memory with its capacity is to be freed. It is assumed that the implementation of
   * this interface knows the type of Memory that was created and how to free it.
   * @param mem The Memory to be freed
   */
  void free(Memory mem);

  /**
   * The given memToFree with its capacity may be freed by the implementation.
   * Providing a reference to newMem enables the implementation to link the memToFree to the
   * newMem, if desired.
   *
   * @param memToFree the Memory to be freed. It is assumed that the implementation of
   * this interface knows the type of Memory that was created and how to free it, if desired.
   *
   * @param newMem
   * Providing a reference to newMem enables the implementation to link the memToFree to the
   * newMem, if desired.
   */
  void free(Memory memToFree, Memory newMem);
}
