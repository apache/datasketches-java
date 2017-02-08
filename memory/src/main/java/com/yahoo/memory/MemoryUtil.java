/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.memory;

import static com.yahoo.memory.UnsafeUtil.assertBounds;

/**
 * Useful utilities that work with Memory.
 *
 * @author Lee Rhodes
 */
public final class MemoryUtil {

  private MemoryUtil() {}

  /**
   * General purpose copy function between two Memories.
   * @param source the source Memory
   * @param srcOffsetBytes the source offset
   * @param destination the destination Memory
   * @param dstOffsetBytes the destination offset
   * @param lengthBytes the number of bytes to copy
   * @deprecated this method was moved to {@link NativeMemory#copy(Memory, long, Memory, long, long)}
   */
  @Deprecated
  public static void copy(final Memory source, final long srcOffsetBytes, final Memory destination,
      final long dstOffsetBytes, final long lengthBytes) {
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
  public static int binarySearchLongs(final Memory mem, final int fromLongIndex,
      final int toLongIndex, final long key) {
    assertBounds(fromLongIndex << 3, (toLongIndex - fromLongIndex) << 3, mem.getCapacity());
    int low = fromLongIndex;
    int high = toLongIndex - 1;

    while (low <= high) {
      final int mid = (low + high) >>> 1;
      final long midVal = mem.getLong(mid << 3);

      if (midVal < key)      { low = mid + 1;  }
      else if (midVal > key) { high = mid - 1; }
      else                   { return mid;     } // key found
    }
    return -(low + 1); // key not found.
  }

  /**
   * Exception handler for requesting a new Memory allocation of the given newCapacityBytes,
   * using the MemoryRequest callback interface.
   * If <i>copy</i> is true, the <i>origMem</i> will be copied into the new Memory.
   *
   * @param origMem The original Memory that needs to be replaced by a newly allocated Memory.
   * @param newCapacityBytes The required capacity of the new Memory.
   * @param copy if true, data from the origMem will be copied to the new Memory as space allows
   * and the origMemory will be requested to be freed.
   * If false, no copy will occur and the request to free the origMem will not occur.
   * @return the newly requested Memory
   */
  public static Memory memoryRequestHandler(final Memory origMem, final long newCapacityBytes,
      final boolean copy) {
    final MemoryRequest memReq = origMem.getMemoryRequest();
    if (memReq == null) {
      throw new IllegalArgumentException(
          "Insufficient space. MemoryRequest callback cannot be null.");
    }
    final Memory newDstMem = (copy)
        ? memReq.request(origMem, origMem.getCapacity(), newCapacityBytes)
        : memReq.request(newCapacityBytes);

    if (newDstMem == null) {
      throw new IllegalArgumentException(
          "Insufficient space and Memory returned by MemoryRequest cannot be null.");
    }
    final long newCap = newDstMem.getCapacity(); //may be more than requested, but not less.
    if (newCap < newCapacityBytes) {
      memReq.free(newDstMem);
      throw new IllegalArgumentException(
          "Insufficient space and Memory returned by MemoryRequest is not the requested capacity: "
          + "Returned: " + newCap + " < Requested: " + newCapacityBytes);
    }

    if (copy) { //copy and request free.
      NativeMemory.copy(origMem, 0, newDstMem, 0, Math.min(origMem.getCapacity(), newCap));
      memReq.free(origMem, newDstMem);
    }
    return newDstMem;
  }
}
