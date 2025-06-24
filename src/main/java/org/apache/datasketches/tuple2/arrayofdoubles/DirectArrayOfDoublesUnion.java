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

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.Family;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.tuple2.SerializerDeserializer;

/**
 * Direct Union operation for tuple sketches of type ArrayOfDoubles.
 *
 * <p>This implementation uses data in a given Memory that is owned and managed by the caller.
 * This Memory can be off-heap, which if managed properly will greatly reduce the need for
 * the JVM to perform garbage collection.</p>
 */
class DirectArrayOfDoublesUnion extends ArrayOfDoublesUnion {

  final MemorySegment seg_;

  /**
   * Creates an instance of DirectArrayOfDoublesUnion
   * @param nomEntries Nominal number of entries. Forced to the nearest power of 2 greater than given value.
   * @param numValues Number of double values to keep for each key.
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   * @param dstSeg the destination MemorySegment
   */
  DirectArrayOfDoublesUnion(final int nomEntries, final int numValues, final long seed,
      final MemorySegment dstSeg) {
    super(new DirectArrayOfDoublesQuickSelectSketch(nomEntries, 3, 1f, numValues, seed,
        dstSeg.asSlice(PREAMBLE_SIZE_BYTES, dstSeg.byteSize() - PREAMBLE_SIZE_BYTES)));
    seg_ = dstSeg;
    seg_.set(JAVA_BYTE, PREAMBLE_LONGS_BYTE, (byte) 1); // unused, always 1
    seg_.set(JAVA_BYTE, SERIAL_VERSION_BYTE, serialVersionUID);
    seg_.set(JAVA_BYTE, FAMILY_ID_BYTE, (byte) Family.TUPLE.getID());
    seg_.set(JAVA_BYTE, SKETCH_TYPE_BYTE, (byte) SerializerDeserializer.SketchType.ArrayOfDoublesUnion.ordinal());
    seg_.set(JAVA_LONG_UNALIGNED, THETA_LONG, gadget_.getThetaLong());
  }

  //Called from wrapUnion below and extended by DirectArrayOfDoublesUnionR
  DirectArrayOfDoublesUnion(final ArrayOfDoublesQuickSelectSketch gadget, final MemorySegment seg) {
    super(gadget);
    seg_ = seg;
    unionThetaLong_ = seg.get(JAVA_LONG_UNALIGNED, THETA_LONG);
  }

  @Override
  void setUnionThetaLong(final long thetaLong) {
    super.setUnionThetaLong(thetaLong);
    seg_.set(JAVA_LONG_UNALIGNED, THETA_LONG, thetaLong);
  }

  static ArrayOfDoublesUnion wrapUnion(final MemorySegment seg, final long seed, final boolean isWritable) {
    final byte version = seg.get(JAVA_BYTE, ArrayOfDoublesUnion.SERIAL_VERSION_BYTE);
    if (version != ArrayOfDoublesUnion.serialVersionUID) {
      throw new SketchesArgumentException("Serial version mismatch. Expected: "
        + serialVersionUID + ", actual: " + version);
    }
    SerializerDeserializer.validateFamily(seg.get(JAVA_BYTE, FAMILY_ID_BYTE), seg.get(JAVA_BYTE, PREAMBLE_LONGS_BYTE));
    SerializerDeserializer.validateType(seg.get(JAVA_BYTE, SKETCH_TYPE_BYTE),
        SerializerDeserializer.SketchType.ArrayOfDoublesUnion);

    if (isWritable) {
      final MemorySegment sketchSeg = seg.asSlice(PREAMBLE_SIZE_BYTES, seg.byteSize() - PREAMBLE_SIZE_BYTES);
      return new DirectArrayOfDoublesUnion(new DirectArrayOfDoublesQuickSelectSketch(sketchSeg, seed), seg);
    }
    final MemorySegment sketchSeg = seg.asSlice(PREAMBLE_SIZE_BYTES, seg.byteSize() - PREAMBLE_SIZE_BYTES);
    return new DirectArrayOfDoublesUnionR(new DirectArrayOfDoublesQuickSelectSketchR(sketchSeg, seed), seg);
  }

}
