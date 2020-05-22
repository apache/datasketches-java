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

import org.apache.datasketches.Family;
import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.datasketches.tuple.SerializerDeserializer;

/**
 * Direct Union operation for tuple sketches of type ArrayOfDoubles.
 * <p>This implementation uses data in a given Memory that is owned and managed by the caller.
 * This Memory can be off-heap, which if managed properly will greatly reduce the need for
 * the JVM to perform garbage collection.</p>
 */
class DirectArrayOfDoublesUnion extends ArrayOfDoublesUnion {

  final WritableMemory mem_;

  /**
   * Creates an instance of DirectArrayOfDoublesUnion
   * @param nomEntries Nominal number of entries. Forced to the nearest power of 2 greater than
   * given value.
   * @param numValues Number of double values to keep for each key.
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   * @param dstMem <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   */
  DirectArrayOfDoublesUnion(final int nomEntries, final int numValues, final long seed,
      final WritableMemory dstMem) {
    super(new DirectArrayOfDoublesQuickSelectSketch(nomEntries, 3, 1f, numValues, seed,
        dstMem.writableRegion(PREAMBLE_SIZE_BYTES, dstMem.getCapacity() - PREAMBLE_SIZE_BYTES)));
    mem_ = dstMem;
    mem_.putByte(PREAMBLE_LONGS_BYTE, (byte) 1); // unused, always 1
    mem_.putByte(SERIAL_VERSION_BYTE, serialVersionUID);
    mem_.putByte(FAMILY_ID_BYTE, (byte) Family.TUPLE.getID());
    mem_.putByte(SKETCH_TYPE_BYTE, (byte) SerializerDeserializer.SketchType.ArrayOfDoublesUnion.ordinal());
    mem_.putLong(THETA_LONG, sketch_.getThetaLong());
  }

  DirectArrayOfDoublesUnion(final ArrayOfDoublesQuickSelectSketch sketch, final WritableMemory mem) {
    super(sketch);
    mem_ = mem;
    theta_ = mem.getLong(THETA_LONG);
  }

  @Override
  void setThetaLong(final long theta) {
    super.setThetaLong(theta);
    mem_.putLong(THETA_LONG, theta);
  }

  static ArrayOfDoublesUnion wrapUnion(final WritableMemory mem, final long seed, final boolean isWritable) {
    final byte version = mem.getByte(ArrayOfDoublesUnion.SERIAL_VERSION_BYTE);
    if (version != ArrayOfDoublesUnion.serialVersionUID) {
      throw new SketchesArgumentException("Serial version mismatch. Expected: "
        + serialVersionUID + ", actual: " + version);
    }
    SerializerDeserializer.validateFamily(mem.getByte(FAMILY_ID_BYTE), mem.getByte(PREAMBLE_LONGS_BYTE));
    SerializerDeserializer.validateType(mem.getByte(SKETCH_TYPE_BYTE),
        SerializerDeserializer.SketchType.ArrayOfDoublesUnion);

    if (isWritable) {
      final WritableMemory sketchMem = mem.writableRegion(PREAMBLE_SIZE_BYTES,
          mem.getCapacity() - PREAMBLE_SIZE_BYTES);
      return new DirectArrayOfDoublesUnion(new DirectArrayOfDoublesQuickSelectSketch(sketchMem, seed), mem);
    }
    final Memory sketchMem = mem.region(PREAMBLE_SIZE_BYTES, mem.getCapacity() - PREAMBLE_SIZE_BYTES);
    return new DirectArrayOfDoublesUnionR(new DirectArrayOfDoublesQuickSelectSketchR(sketchMem, seed), mem);
  }

}
