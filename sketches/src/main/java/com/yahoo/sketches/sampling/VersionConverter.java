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
  static Memory convertSketch1to2(Memory srcMem) {
    return perform1to2Changes(srcMem);
  }

  /**
   * Because this is a package-private method, assumes srcMem points to a proper version 1
   * reservoir union. This method does no validation. The method modifies the Memory in place if
   * possible, copying only if <tt>srcMem</tt> is read-only.
   * @param srcMem Memory object holding a serialized v1 reservoir union
   * @return Memory object containing the same reservoir union serialized as v2.
   */
  static Memory convertUnion1to2(Memory srcMem) {
    // convert the union preamble
    Memory converted = perform1to2Changes(srcMem);

    // if sketch gadget exists, convert that, too
    long pre0 = converted.getLong(0);
    int preLongs = extractPreLongs(pre0);
    int flags = extractFlags(pre0);
    boolean isEmpty = (flags & EMPTY_FLAG_MASK) > 0;

    if (!isEmpty) {
      int memCap = (int) converted.getCapacity();
      int preLongBytes = preLongs << 3;
      MemoryRegion sketchMem = new MemoryRegion(converted, preLongBytes, memCap - preLongBytes);
      convertSketch1to2(sketchMem);
    }

    return converted;
  }

  private static Memory perform1to2Changes(Memory srcMem) {
    int memCap = (int) srcMem.getCapacity();

    Memory converted = srcMem;
    if (srcMem.isReadOnly()) {
      byte[] data = new byte[memCap];
      srcMem.getByteArray(0, data, 0, memCap);
      converted = new NativeMemory(data);
    }

    // get encoded k, decode, write new value
    long pre0 = converted.getLong(0);
    short encodedK = extractEncodedReservoirSize(pre0);
    int k = ReservoirSize.decodeValue(encodedK);
    pre0 = insertReservoirSize(k, pre0);

    // update serialization version
    pre0 = insertSerVer(SER_VER, pre0);
    converted.putLong(0, pre0);

    return converted;
  }
}
