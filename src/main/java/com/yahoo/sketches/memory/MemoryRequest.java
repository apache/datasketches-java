/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.memory;

/**
 * This is used as a callback interface to request additional memory and to free memory that is
 * no longer needed.
 * 
 * @author Lee Rhodes
 */
public interface MemoryRequest {
  
  /**
   * Request new Memory with the given capacity
   * @param capacityBytes The capacity being requested
   * @return new Memory with the given capacity
   */
  Memory request(long capacityBytes);
  
  /**
   * This memory with its capacity is no longer needed
   * @param mem The memory to be freed
   */
  void free(Memory mem);
  
  /**
   * This enables linking of the Memory to be freed to the new Memory that was allocated to
   * replace it. 
   * @param freeAndLink instance of the MemoryFreeAndLink class.
   */
  void free(MemoryFreeAndLink freeAndLink);
}
