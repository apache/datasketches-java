/*
 * Copyright 2015, Yahoo! Inc.
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
