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

package org.apache.datasketches.tuple2.arrayofdoubles;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED;
import static org.apache.datasketches.thetacommon2.ThetaUtil.checkSeedHashes;
import static java.lang.Math.min;

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.Family;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.thetacommon2.ThetaUtil;
import org.apache.datasketches.tuple2.SerializerDeserializer;

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
   * Heapify the given MemorySegment as an ArrayOfDoublesUnion
   * @param srcSeg the given source MemorySegment
   * @return an ArrayOfDoublesUnion
   */
  public static ArrayOfDoublesUnion heapify(final MemorySegment srcSeg) {
    return heapify(srcSeg, ThetaUtil.DEFAULT_UPDATE_SEED);
  }

  /**
   * Heapify the given MemorySegment and seed as an ArrayOfDoublesUnion
   * @param srcSeg the given source MemorySegment
   * @param seed the given seed
   * @return an ArrayOfDoublesUnion
   */
  public static ArrayOfDoublesUnion heapify(final MemorySegment srcSeg, final long seed) {
    return HeapArrayOfDoublesUnion.heapifyUnion(srcSeg, seed);
  }

  /**
   * Wrap the given MemorySegment as an ArrayOfDoublesUnion
   * @param srcSeg the given source MemorySegment
   * @return an ArrayOfDoublesUnion
   */
  public static ArrayOfDoublesUnion wrap(final MemorySegment srcSeg) {
    return wrap(srcSeg, ThetaUtil.DEFAULT_UPDATE_SEED);
  }

  /**
   * Wrap the given MemorySegment and seed as an ArrayOfDoublesUnion
   * @param srcSeg the given source MemorySegment
   * @param seed the given seed
   * @return an ArrayOfDoublesUnion
   */
  public static ArrayOfDoublesUnion wrap(final MemorySegment srcSeg, final long seed) {
    return DirectArrayOfDoublesUnion.wrapUnion(srcSeg, seed, !srcSeg.isReadOnly());
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
    checkSeedHashes(gadget_.getSeedHash(), tupleSketch.getSeedHash());
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
   * @param dstSeg MemorySegment for the result (can be null)
   * @return compact sketch representing the union (off-heap if MemorySegment is provided)
   */
  public ArrayOfDoublesCompactSketch getResult(final MemorySegment dstSeg) {
    long unionThetaLong = unionThetaLong_;
    if (gadget_.getRetainedEntries() > gadget_.getNominalEntries()) {
      unionThetaLong = Math.min(unionThetaLong, gadget_.getNewThetaLong());
    }
    if (dstSeg == null) {
      return new HeapArrayOfDoublesCompactSketch(gadget_, unionThetaLong);
    }
    return new DirectArrayOfDoublesCompactSketch(gadget_, unionThetaLong, dstSeg);
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
   * Returns a byte array representation of this object
   * @return a byte array representation of this object
   */
  public byte[] toByteArray() {
    final int sizeBytes = PREAMBLE_SIZE_BYTES + gadget_.getSerializedSizeBytes();
    final byte[] byteArray = new byte[sizeBytes];
    final MemorySegment seg = MemorySegment.ofArray(byteArray);
    seg.set(JAVA_BYTE, PREAMBLE_LONGS_BYTE, (byte) 1); // unused, always 1
    seg.set(JAVA_BYTE, SERIAL_VERSION_BYTE, serialVersionUID);
    seg.set(JAVA_BYTE, FAMILY_ID_BYTE, (byte) Family.TUPLE.getID());
    seg.set(JAVA_BYTE, SKETCH_TYPE_BYTE, (byte) SerializerDeserializer.SketchType.ArrayOfDoublesUnion.ordinal());
    //byte 4-7 automatically zero
    seg.set(JAVA_LONG_UNALIGNED, THETA_LONG, unionThetaLong_);
    gadget_.serializeInto(seg.asSlice(PREAMBLE_SIZE_BYTES, seg.byteSize() - PREAMBLE_SIZE_BYTES));
    return byteArray;
  }

  /**
   * Returns maximum required storage bytes given nomEntries and numValues
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
