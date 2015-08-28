package com.yahoo.sketches.memory;

/**
 * This is used as a callback interface to request additional memory and to free memory that is
 * no longer needed.
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
}
