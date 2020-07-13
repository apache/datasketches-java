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

  static final int PREAMBLE_SIZE_BYTES = 16;
  // Layout of first 16 bytes:
  // Long || Start Byte Adr:
  // Adr:
  //      ||    7   |    6   |    5   |    4   |    3   |    2   |    1   |     0              |
  //  0   ||    Seed Hash    | #Dbls  |  Flags | SkType | FamID  | SerVer |  Preamble_Longs    |
  //      ||   15   |   14   |   13   |   12   |   11   |   10   |    9   |     8              |
  //  1   ||------------------------------Theta Long-------------------------------------------|

  static final int PREAMBLE_LONGS_BYTE = 0; // not used, always 1
  static final int SERIAL_VERSION_BYTE = 1;
  static final int FAMILY_ID_BYTE = 2;
  static final int SKETCH_TYPE_BYTE = 3;
  static final int FLAGS_BYTE = 4;
  static final int NUM_VALUES_BYTE = 5;
  static final int SEED_HASH_SHORT = 6;
  static final int THETA_LONG = 8;

  ArrayOfDoublesQuickSelectSketch sketch_;
  long theta_;

  ArrayOfDoublesUnion(final ArrayOfDoublesQuickSelectSketch sketch) {
    sketch_ = sketch;
    theta_ = sketch.getThetaLong();
  }

  /**
   * Heapify the given Memory as an ArrayOfDoublesUnion
   * @param mem the given Memory
   * @return an ArrayOfDoublesUnion
   */
  public static ArrayOfDoublesUnion heapify(final Memory mem) {
    return heapify(mem, DEFAULT_UPDATE_SEED);
  }

  /**
   * Heapify the given Memory and seed as an ArrayOfDoublesUnion
   * @param mem the given Memory
   * @param seed the given seed
   * @return an ArrayOfDoublesUnion
   */
  public static ArrayOfDoublesUnion heapify(final Memory mem, final long seed) {
    return HeapArrayOfDoublesUnion.heapifyUnion(mem, seed);
  }

  /**
   * Wrap the given Memory as an ArrayOfDoublesUnion
   * @param mem the given Memory
   * @return an ArrayOfDoublesUnion
   */
  public static ArrayOfDoublesUnion wrap(final Memory mem) {
    return wrap(mem, DEFAULT_UPDATE_SEED);
  }

  /**
   * Wrap the given Memory and seed as an ArrayOfDoublesUnion
   * @param mem the given Memory
   * @param seed the given seed
   * @return an ArrayOfDoublesUnion
   */
  public static ArrayOfDoublesUnion wrap(final Memory mem, final long seed) {
    return DirectArrayOfDoublesUnion.wrapUnion((WritableMemory) mem, seed, false);
  }

  /**
   * Wrap the given WritableMemory as an ArrayOfDoublesUnion
   * @param mem the given Memory
   * @return an ArrayOfDoublesUnion
   */
  public static ArrayOfDoublesUnion wrap(final WritableMemory mem) {
    return wrap(mem, DEFAULT_UPDATE_SEED);
  }

  /**
   * Wrap the given WritableMemory and seed as an ArrayOfDoublesUnion
   * @param mem the given Memory
   * @param seed the given seed
   * @return an ArrayOfDoublesUnion
   */
  public static ArrayOfDoublesUnion wrap(final WritableMemory mem, final long seed) {
    return DirectArrayOfDoublesUnion.wrapUnion(mem, seed, true);
  }

  /**
   * Updates the union by adding a set of entries from a given sketch
   * @param sketchIn sketch to add to the union
   */
  public void update(final ArrayOfDoublesSketch sketchIn) {
    if (sketchIn == null) { return; }
    Util.checkSeedHashes(sketch_.getSeedHash(), sketchIn.getSeedHash());
    if (sketch_.getNumValues() != sketchIn.getNumValues()) {
      throw new SketchesArgumentException("Incompatible sketches: number of values mismatch "
          + sketch_.getNumValues() + " and " + sketchIn.getNumValues());
    }
    if (sketchIn.isEmpty()) { return; }
    if (sketchIn.getThetaLong() < theta_) {
      setThetaLong(sketchIn.getThetaLong());
    }
    final ArrayOfDoublesSketchIterator it = sketchIn.iterator();
    while (it.next()) {
      if (it.getKey() < theta_) {
        sketch_.merge(it.getKey(), it.getValues());
      }
    }
    // keep the union theta as low as low as possible for performance
    if (sketch_.getThetaLong() < theta_) {
      setThetaLong(sketch_.getThetaLong());
    }
  }

  /**
   * Returns the resulting union in the form of a compact sketch
   * @param dstMem memory for the result (can be null)
   * @return compact sketch representing the union (off-heap if memory is provided)
   */
  public ArrayOfDoublesCompactSketch getResult(final WritableMemory dstMem) {
    long theta = theta_;
    if (sketch_.getRetainedEntries() > sketch_.getNominalEntries()) {
      theta = Math.min(theta, sketch_.getNewTheta());
    }
    if (dstMem == null) {
      return new HeapArrayOfDoublesCompactSketch(sketch_, theta);
    }
    return new DirectArrayOfDoublesCompactSketch(sketch_, theta, dstMem);
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
    sketch_.reset();
    setThetaLong(sketch_.getThetaLong());
  }

  /**
   * @return a byte array representation of this object
   */
  public byte[] toByteArray() {
    final int sizeBytes = PREAMBLE_SIZE_BYTES + sketch_.getSerializedSizeBytes();
    final byte[] byteArray = new byte[sizeBytes];
    final WritableMemory mem = WritableMemory.wrap(byteArray);
    mem.putByte(PREAMBLE_LONGS_BYTE, (byte) 1); // unused, always 1
    mem.putByte(SERIAL_VERSION_BYTE, serialVersionUID);
    mem.putByte(FAMILY_ID_BYTE, (byte) Family.TUPLE.getID());
    mem.putByte(SKETCH_TYPE_BYTE, (byte) SerializerDeserializer.SketchType.ArrayOfDoublesUnion.ordinal());
    //byte 4-7 automatically zero
    mem.putLong(THETA_LONG, theta_);
    sketch_.serializeInto(mem.writableRegion(PREAMBLE_SIZE_BYTES, mem.getCapacity() - PREAMBLE_SIZE_BYTES));
    return byteArray;
  }

  /**
   * @param nomEntries Nominal number of entries. Forced to the nearest power of 2 greater than
   * given value.
   * @param numValues Number of double values to keep for each key
   * @return maximum required storage bytes given nomEntries and numValues
   */
  public static int getMaxBytes(final int nomEntries, final int numValues) {
    return ArrayOfDoublesQuickSelectSketch.getMaxBytes(nomEntries, numValues) + PREAMBLE_SIZE_BYTES;
  }

  void setThetaLong(final long theta) {
    theta_ = theta;
  }

}
