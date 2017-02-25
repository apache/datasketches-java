/*
 * Copyright 2016-17, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.sampling;

import static com.yahoo.sketches.sampling.PreambleUtil.EMPTY_FLAG_MASK;
import static com.yahoo.sketches.sampling.PreambleUtil.SER_VER;
import static com.yahoo.sketches.sampling.PreambleUtil.extractEncodedReservoirSize;
import static com.yahoo.sketches.sampling.PreambleUtil.extractFlags;
import static com.yahoo.sketches.sampling.PreambleUtil.extractPreLongs;
import static com.yahoo.sketches.sampling.PreambleUtil.insertReservoirSize;
import static com.yahoo.sketches.sampling.PreambleUtil.insertSerVer;

import com.yahoo.memory.Memory;
import com.yahoo.memory.MemoryRegion;
import com.yahoo.memory.NativeMemory;

/**
 * Used to convert reservoir sample sketches serialized as version 1 to version 2. Note that this
 * refers to the serialization version in the binary header of the sketch, not the version
 * number of the DataSketches Library.
 *
 * @author Jon Malkin
 */
final class VersionConverter {
  /**
   * Because this is a package-private method, assumes srcMem points to a proper version 1
   * reservoir sample. This method does no validation. The method modifies the Memory in place if
   * possible, copying only if <tt>srcMem</tt> is read-only.
   * @param srcMem Memory object holding a serialized v1 reservoir sample
   * @return Memory object containing the same reservoir sample serialized as v2.
   */
  static Memory convertSketch1to2(final Memory srcMem) {
    return perform1to2Changes(srcMem);
  }

  /**
   * Because this is a package-private method, assumes srcMem points to a proper version 1
   * reservoir union. This method does no validation. The method modifies the Memory in place if
   * possible, copying only if <tt>srcMem</tt> is read-only.
   * @param srcMem Memory object holding a serialized v1 reservoir union
   * @return Memory object containing the same reservoir union serialized as v2.
   */
  static Memory convertUnion1to2(final Memory srcMem) {
    // convert the union preamble
    final Memory converted = perform1to2Changes(srcMem);

    final Object memObj = converted.array(); // may be null
    final long memAddr = converted.getCumulativeOffset(0L);

    // if sketch gadget exists, convert that, too
    final int preLongs = extractPreLongs(memObj, memAddr);
    final int flags = extractFlags(memObj, memAddr);
    final boolean isEmpty = (flags & EMPTY_FLAG_MASK) > 0;

    if (!isEmpty) {
      final int memCap = (int) converted.getCapacity();
      final int preLongBytes = preLongs << 3;
      final MemoryRegion sketchMem = new MemoryRegion(converted, preLongBytes, memCap - preLongBytes);
      convertSketch1to2(sketchMem);
    }

    return converted;
  }

  /* This method avoids a potential issue with read-only on-heap memory where srcMem.array()
   * would fail. By copying in the event of read-only memory, subsequent calls will work properly.
   * Perhaps not the most elegant solution in that we may not always want to copy a read-only
   * memory, but the result will be correct.
   */
  private static Memory perform1to2Changes(final Memory srcMem) {
    final int memCap = (int) srcMem.getCapacity();

    Memory converted = srcMem;
    if (srcMem.isReadOnly()) {
      final byte[] data = new byte[memCap];
      srcMem.getByteArray(0, data, 0, memCap);
      converted = new NativeMemory(data);
    }

    final Object memObj = converted.array(); // may be null
    final long memAddr = converted.getCumulativeOffset(0L);

    // get encoded k, decode, write new value
    final short encodedK = extractEncodedReservoirSize(memObj, memAddr);
    final int k = ReservoirSize.decodeValue(encodedK);
    insertReservoirSize(memObj, memAddr, k);

    // update serialization version
    insertSerVer(memObj, memAddr, SER_VER);

    return converted;
  }
}
