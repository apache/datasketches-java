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

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_INT_UNALIGNED;
import static org.apache.datasketches.common.Util.checkBounds;

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.SketchesArgumentException;

/**
 * Top level compact tuple sketch of type ArrayOfDoubles. Compact sketches are never created
 * directly.  They are created as a result of the compact() method on a QuickSelectSketch
 * or the getResult() method of a set operation like TupleUnion, TupleIntersection or TupleAnotB.
 * A CompactTupleSketch consists of a compact list (i.e. no intervening spaces) of hash values,
 * a corresponding list of double values, and a value for theta. The lists may or may
 * not be ordered. It is read-only.
 */
public abstract class ArrayOfDoublesCompactSketch extends ArrayOfDoublesSketch {

  static final byte serialVersionUID = 1;

  // Layout of retained entries:
  // Long || Start Byte Adr:
  // Adr:
  //      ||   23   |   22   |   21   |   20   |   19   |   18   |   17   |    16     |
  //  3   ||-----------------------------------|----------Retained Entries------------|

  static final int EMPTY_SIZE = 16;
  static final int RETAINED_ENTRIES_INT = 16;
  // 4 bytes of padding for 8 byte alignment
  static final int ENTRIES_START = 24;

  ArrayOfDoublesCompactSketch(final int numValues) {
    super(numValues);
  }

  /**
   * Checks that the given image is large enough for the number of entries and values it declares,
   * before anything is allocated or read from it.
   * @param seg the serialized compact sketch
   * @throws SketchesArgumentException if the image is too small or its counts are invalid
   */
  static void checkCompactImage(final MemorySegment seg) {
    checkBounds(0, EMPTY_SIZE, seg.byteSize());
    final int numValues = seg.get(JAVA_BYTE, NUM_VALUES_BYTE);
    if (numValues < 1) {
      throw new SketchesArgumentException("Possible corruption: numValues must be positive: " + numValues);
    }
    final boolean hasEntries = (seg.get(JAVA_BYTE, FLAGS_BYTE) & (1 << Flags.HAS_ENTRIES.ordinal())) != 0;
    if (!hasEntries) { return; }
    checkBounds(0, ENTRIES_START, seg.byteSize());
    final int count = seg.get(JAVA_INT_UNALIGNED, RETAINED_ENTRIES_INT);
    if (count < 0) {
      throw new SketchesArgumentException("Possible corruption: negative retained entries: " + count);
    }
    checkBounds(ENTRIES_START, (long) count * (SIZE_OF_KEY_BYTES + ((long) SIZE_OF_VALUE_BYTES * numValues)),
        seg.byteSize());
  }

  @Override
  public int getCurrentBytes() {
    final int count = getRetainedEntries();
    int sizeBytes = EMPTY_SIZE;
    if (count > 0) {
      sizeBytes = ENTRIES_START + (SIZE_OF_KEY_BYTES * count)
          + (SIZE_OF_VALUE_BYTES * numValues_ * count);
    }
    return sizeBytes;
  }

  @Override
  public int getMaxBytes() {
    return getCurrentBytes();
  }
}
