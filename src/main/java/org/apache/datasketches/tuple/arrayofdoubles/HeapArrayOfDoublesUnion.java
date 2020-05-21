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

import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.tuple.SerializerDeserializer;

/**
 * The on-heap implementation of the Union set operation for tuple sketches of type
 * ArrayOfDoubles.
 */
final class HeapArrayOfDoublesUnion extends ArrayOfDoublesUnion {

  /**
   * Creates an instance of HeapArrayOfDoublesUnion with a custom seed
   * @param nomEntries Nominal number of entries. Forced to the nearest power of 2 greater than
   * given value.
   * @param numValues Number of double values to keep for each key.
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   */
  HeapArrayOfDoublesUnion(final int nomEntries, final int numValues, final long seed) {
    super(new HeapArrayOfDoublesQuickSelectSketch(nomEntries, 3, 1f, numValues, seed));
  }

  HeapArrayOfDoublesUnion(final ArrayOfDoublesQuickSelectSketch sketch, final long theta) {
    super(sketch);
    theta_ = theta;
  }

  /**
   * This is to create an instance given a serialized form and a custom seed
   * @param mem <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   * @return a ArrayOfDoublesUnion on the Java heap
   */
  static ArrayOfDoublesUnion heapifyUnion(final Memory mem, final long seed) {
    final byte version = mem.getByte(SERIAL_VERSION_BYTE);
    if (version != serialVersionUID) {
      throw new SketchesArgumentException("Serial version mismatch. Expected: "
        + serialVersionUID + ", actual: " + version);
    }
    SerializerDeserializer.validateFamily(mem.getByte(FAMILY_ID_BYTE), mem.getByte(PREAMBLE_LONGS_BYTE));
    SerializerDeserializer.validateType(mem.getByte(SKETCH_TYPE_BYTE),
        SerializerDeserializer.SketchType.ArrayOfDoublesUnion);

    final Memory sketchMem = mem.region(PREAMBLE_SIZE_BYTES, mem.getCapacity() - PREAMBLE_SIZE_BYTES);
    final ArrayOfDoublesQuickSelectSketch sketch = new HeapArrayOfDoublesQuickSelectSketch(sketchMem, seed);
    return new HeapArrayOfDoublesUnion(sketch, mem.getLong(THETA_LONG));
  }

}
