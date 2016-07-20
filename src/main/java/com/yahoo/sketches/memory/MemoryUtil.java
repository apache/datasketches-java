/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.memory;

import static com.yahoo.sketches.memory.UnsafeUtil.assertBounds;

/**
 * Useful utilities that work with Memory.
 * 
 * @author Lee Rhodes
 */
public final class MemoryUtil {
  
  private MemoryUtil() {}
  
  /**
   * 
   * @deprecated this method was moved to 
   * {@link NativeMemory#copy(Memory, long, Memory, long, long)}
   * @param source the source Memory
   * @param srcOffsetBytes the source offset
   * @param destination the destination Memory
   * @param dstOffsetBytes the destination offset
   * @param lengthBytes the number of bytes to copy
   */
  @Deprecated
  public static void copy(Memory source, long srcOffsetBytes, Memory destination, 
      long dstOffsetBytes, long lengthBytes) {
    NativeMemory.copy(source, srcOffsetBytes, destination, dstOffsetBytes, lengthBytes);
  }
  
  /**
   * Searches a range of the specified array of longs for the specified value using the binary 
   * search algorithm. The range must be sorted (as by the sort(long[], int, int) method) prior 
   * to making this call. If it is not sorted, the results are undefined. If the range contains 
   * multiple elements with the specified value, there is no guarantee which one will be found.
   * @param mem the Memory to be searched
   * @param fromLongIndex the index of the first element (inclusive) to be searched
   * @param toLongIndex the index of the last element (exclusive) to be searched
   * @param key the value to be searched for
   * @return index of the search key, if it is contained in the array within the specified range; 
   * otherwise, (-(insertion point) - 1). The insertion point is defined as the point at which 
   * the key would be inserted into the array: the index of the first element in the range greater 
   * than the key, or toIndex if all elements in the range are less than the specified key. 
   * Note that this guarantees that the return value will be &ge; 0 if and only if the key is found.
   */
  public static int binarySearchLongs(Memory mem, int fromLongIndex, int toLongIndex, long key) {
    assertBounds(fromLongIndex << 3, (toLongIndex - fromLongIndex) << 3, mem.getCapacity());
    int low = fromLongIndex;
    int high = toLongIndex - 1;
    
    while (low <= high) {
      int mid = (low + high) >>> 1;
      long midVal = mem.getLong(mid << 3);
      
      if (midVal < key)      { low = mid + 1;  }
      else if (midVal > key) { high = mid - 1; }
      else                   { return mid;     } // key found
    }
    return -(low + 1); // key not found.
  }

}
