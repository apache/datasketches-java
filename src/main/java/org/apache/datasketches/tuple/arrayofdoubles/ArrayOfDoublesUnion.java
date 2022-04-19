/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.datasketches.tuple.arrayofdoubles;

import static java.lang.Math.min;
import static org.apache.datasketches.Util.DEFAULT_UPDATE_SEED;

import org.apache.datasketches.Family;
import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.datasketches.tuple.SerializerDeserializer;
import org.apache.datasketches.tuple.Util;

/**
 * The base class for unions of tuple sketches of type ArrayOfDoubles.
 */
public abstract class ArrayOfDoublesUnion {

  static final byte serialVersionUID = 1;
  //For layout see toByteArray()
  static final int PREAMBLE_SIZE_BYTES = 16;
  static final int PREAMBLE_LONGS_BYTE = 0; // not used, always 1
  static final int SERIAL_VERSION_BYTE = 1;
  static final int FAMILY_ID_BYTE = 2;
  static final int SKETCH_TYPE_BYTE = 3;
  static final int FLAGS_BYTE = 4;
  static final int NUM_VALUES_BYTE = 5;
  static final int SEED_HASH_SHORT = 6;
  static final int THETA_LONG = 8;

  ArrayOfDoublesQuickSelectSketch gadget_;
  long unionThetaLong_;

  /**
   * Constructs this Union initializing it with the given sketch, which can be on-heap or off-heap.
   * @param sketch the given sketch.
   */
  ArrayOfDoublesUnion(final ArrayOfDoublesQuickSelectSketch sketch) {
    gadget_ = sketch;
    unionThetaLong_ = sketch.getThetaLong();
  }

  /**
   * Heapify the given Memory as an ArrayOfDoublesUnion
   * @param srcMem the given source Memory
   * @return an ArrayOfDoublesUnion
   */
  public static ArrayOfDoublesUnion heapify(final Memory srcMem) {
    return heapify(srcMem, DEFAULT_UPDATE_SEED);
  }

  /**
   * Heapify the given Memory and seed as an ArrayOfDoublesUnion
   * @param srcMem the given source Memory
   * @param seed the given seed
   * @return an ArrayOfDoublesUnion
   */
  public static ArrayOfDoublesUnion heapify(final Memory srcMem, final long seed) {
    return HeapArrayOfDoublesUnion.heapifyUnion(srcMem, seed);
  }

  /**
   * Wrap the given Memory as an ArrayOfDoublesUnion
   * @param srcMem the given source Memory
   * @return an ArrayOfDoublesUnion
   */
  public static ArrayOfDoublesUnion wrap(final Memory srcMem) {
    return wrap(srcMem, DEFAULT_UPDATE_SEED);
  }

  /**
   * Wrap the given Memory and seed as an ArrayOfDoublesUnion
   * @param srcMem the given source Memory
   * @param seed the given seed
   * @return an ArrayOfDoublesUnion
   */
  public static ArrayOfDoublesUnion wrap(final Memory srcMem, final long seed) {
    return DirectArrayOfDoublesUnion.wrapUnion((WritableMemory) srcMem, seed, false);
  }

  /**
   * Wrap the given WritableMemory as an ArrayOfDoublesUnion
   * @param srcMem the given source Memory
   * @return an ArrayOfDoublesUnion
   */
  public static ArrayOfDoublesUnion wrap(final WritableMemory srcMem) {
    return wrap(srcMem, DEFAULT_UPDATE_SEED);
  }

  /**
   * Wrap the given WritableMemory and seed as an ArrayOfDoublesUnion
   * @param srcMem the given source Memory
   * @param seed the given seed
   * @return an ArrayOfDoublesUnion
   */
  public static ArrayOfDoublesUnion wrap(final WritableMemory srcMem, final long seed) {
    return DirectArrayOfDoublesUnion.wrapUnion(srcMem, seed, true);
  }

  /**
   * Updates the union by adding a set of entries from a given sketch, which can be on-heap or off-heap.
   * Both the given tupleSketch and the internal state of the Union must have the same <i>numValues</i>.
   *
   * <p>Nulls and empty sketches are ignored.</p>
   *
   * @param tupleSketch sketch to add to the union
   */
  public void union(final ArrayOfDoublesSketch tupleSketch) {
    if (tupleSketch == null) { return; }
    Util.checkSeedHashes(gadget_.getSeedHash(), tupleSketch.getSeedHash());
    if (gadget_.getNumValues() != tupleSketch.getNumValues()) {
      throw new SketchesArgumentException("Incompatible sketches: number of values mismatch "
          + gadget_.getNumValues() + " and " + tupleSketch.getNumValues());
    }

    if (tupleSketch.isEmpty()) { return; }
    else { gadget_.setNotEmpty(); }

    setUnionThetaLong(min(min(unionThetaLong_, tupleSketch.getThetaLong()), gadget_.getThetaLong()));

    if (tupleSketch.getRetainedEntries() == 0) { return; }
    final ArrayOfDoublesSketchIterator it = tupleSketch.iterator();
    while (it.next()) {
      if (it.getKey() < unionThetaLong_) {
        gadget_.merge(it.getKey(), it.getValues());
      }
    }
    // keep the union theta as low as possible for performance
    if (gadget_.getThetaLong() < unionThetaLong_) {
      setUnionThetaLong(gadget_.getThetaLong());
    }
  }

  /**
   * Returns the resulting union in the form of a compact sketch
   * @param dstMem memory for the result (can be null)
   * @return compact sketch representing the union (off-heap if memory is provided)
   */
  public ArrayOfDoublesCompactSketch getResult(final WritableMemory dstMem) {
    long unionThetaLong = unionThetaLong_;
    if (gadget_.getRetainedEntries() > gadget_.getNominalEntries()) {
      unionThetaLong = Math.min(unionThetaLong, gadget_.getNewThetaLong());
    }
    if (dstMem == null) {
      return new HeapArrayOfDoublesCompactSketch(gadget_, unionThetaLong);
    }
    return new DirectArrayOfDoublesCompactSketch(gadget_, unionThetaLong, dstMem);
  }

  /**
   * Returns the resulting union in the form of a compact sketch
   * @return on-heap compact sketch representing the union
   */
  public ArrayOfDoublesCompactSketch getResult() {
    return getResult(null);
  }

  /**
   * Resets the union to an empty state
   */
  public void reset() {
    gadget_.reset();
    setUnionThetaLong(gadget_.getThetaLong());
  }

  // Layout of first 16 bytes:
  // Long || Start Byte Adr:
  // Adr:
  //      ||    7   |    6   |    5   |    4   |    3    |    2   |    1   |     0              |
  //  0   ||  Seed Hash=0    | #Dbls=0|Flags=0 | SkType  | FamID  | SerVer |  Preamble_Longs    |
  //      ||   15   |   14   |   13   |   12   |   11    |   10   |    9   |     8              |
  //  1   ||---------------------------Union Theta Long-----------------------------------------|
  /**
   * @return a byte array representation of this object
   */
  public byte[] toByteArray() {
    final int sizeBytes = PREAMBLE_SIZE_BYTES + gadget_.getSerializedSizeBytes();
    final byte[] byteArray = new byte[sizeBytes];
    final WritableMemory mem = WritableMemory.writableWrap(byteArray);
    mem.putByte(PREAMBLE_LONGS_BYTE, (byte) 1); // unused, always 1
    mem.putByte(SERIAL_VERSION_BYTE, serialVersionUID);
    mem.putByte(FAMILY_ID_BYTE, (byte) Family.TUPLE.getID());
    mem.putByte(SKETCH_TYPE_BYTE, (byte) SerializerDeserializer.SketchType.ArrayOfDoublesUnion.ordinal());
    //byte 4-7 automatically zero
    mem.putLong(THETA_LONG, unionThetaLong_);
    gadget_.serializeInto(mem.writableRegion(PREAMBLE_SIZE_BYTES, mem.getCapacity() - PREAMBLE_SIZE_BYTES));
    return byteArray;
  }

  /**
   * @param nomEntries Nominal number of entries. Forced to the nearest power of 2 greater than or equal to
   * given value.
   * @param numValues Number of double values to keep for each key
   * @return maximum required storage bytes given nomEntries and numValues
   */
  public static int getMaxBytes(final int nomEntries, final int numValues) {
    return ArrayOfDoublesQuickSelectSketch.getMaxBytes(nomEntries, numValues) + PREAMBLE_SIZE_BYTES;
  }

  void setUnionThetaLong(final long thetaLong) {
    unionThetaLong_ = thetaLong;
  }

}
