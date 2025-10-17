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

package org.apache.datasketches.kll;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_DOUBLE_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_FLOAT_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_INT_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_SHORT_UNALIGNED;
import static org.apache.datasketches.common.Family.idToFamily;
import static org.apache.datasketches.common.Util.LS;
import static org.apache.datasketches.common.Util.zeroPad;
import static org.apache.datasketches.kll.KllSketch.SketchStructure.COMPACT_FULL;
import static org.apache.datasketches.kll.KllSketch.SketchStructure.COMPACT_SINGLE;
import static org.apache.datasketches.kll.KllSketch.SketchStructure.UPDATABLE;
import static org.apache.datasketches.kll.KllSketch.SketchType.KLL_DOUBLES_SKETCH;
import static org.apache.datasketches.kll.KllSketch.SketchType.KLL_FLOATS_SKETCH;
import static org.apache.datasketches.kll.KllSketch.SketchType.KLL_ITEMS_SKETCH;
import static org.apache.datasketches.kll.KllSketch.SketchType.KLL_LONGS_SKETCH;

import java.lang.foreign.MemorySegment;
import java.util.Objects;

import org.apache.datasketches.common.ArrayOfItemsSerDe;
import org.apache.datasketches.common.Util;
import org.apache.datasketches.kll.KllSketch.SketchStructure;
import org.apache.datasketches.kll.KllSketch.SketchType;

/**
 * This class defines the serialized data structure and provides access methods for the preamble fields.
 *
 * <p>The intent of the design of this class was to isolate the detailed knowledge of the bit and
 * byte layout of the serialized form of the sketches derived from the base sketch classes into one place.
 * This allows the possibility of the introduction of different serialization
 * schemes with minimal impact on the rest of the library.</p>
 *
 * <h3>Visual Layout</h3>
 * The low significance bytes of the visual data structure below are on the left.
 * The multi-byte primitives are stored in native byte order.
 * The numeric <i>byte</i> and <i>short</i> fields are treated as unsigned.
 * The numeric <i>int</i> and <i>long</i> fields are treated as signed.
 *
 * <h3>Preamble Formats</h3>
 * The preamble has four formats:
 *
 * <ul>
 * <li>The serialized empty compact structure requires 8 bytes of preamble. It is not updatable.
 * It is identified by the <i>enum SketchStructure.COMPACT_EMPTY.</i></li>
 *
 * <li>The serialized, single-item compact structure requires 8 bytes of preamble, followed by the one item.
 * The size of this structure is 8 + itemSize bytes. It is not updatable.
 * It is identified by the <i>enum SketchStructure.COMPACT_SINGLE.</i></li>
 *
 * <li>A serialized, <i>n &gt; 1</i> compact structure requires 20 bytes of preamble (5 ints) followed by
 * four variable-sized fields. The details of these fields can be found in the code and are illustrated
 * in the table below.
 * The 5 int preamble is followed by the <i>levelsArr int[numLevels]</i> as bytes,
 * followed by the min and max values as bytes,
 * followed by a packed items data array as bytes. There is no free space in this structure.
 * It is not updatable.
 * It is identified by the <i>enum SketchStructure.COMPACT_FULL</i>.</li>
 *
 * <li>A serialized, <i>n &gt; 1</i> non-compact, updatable structure requires 20 bytes of preamble (5 ints).
 * This is followed by the LevelsArr int[NumLevels + 1], followed by the min and max values, and then
 * followed by an items data array that may include free space. It is updatable.
 * The details of these fields can be found in the code..
 * It is identified by the <i>enum SketchStructure.UPDATABLE</i>. This structure may not be implemented by
 * some sketches.</li>
 * </ul>
 *
 * <h3>Visual Layout</h3>
 * The fields in braces are those that can be variable in size.
 *
 * <pre>{@code
 * Serialized COMPACT_EMPTY sketch structure, Empty (8 bytes)
 * and COMPACT_SINGLE sketch structure, (single item) (8 + itemSize):
 * Int Adr:   Byte Adr ->
 *    0     ||       0      |   1    |   2   |   3    |
 *          | PreambleInts  | SerVer | FamID | Flags  |
 *
 *    1     ||       4      |   5    |   6   |   7    |
 *          ||-----------K-----------|   M   | unused |
 *
 *    2     ||       8      |
 *          ||{Single Item} ->
 *
 * Serialized COMPACT_FULL sketch structure, more than one item:
 * Int Adr:   Byte Adr ->
 *    0     ||       0      |   1    |   2   |   3    |
 *          || PreambleInts | SerVer | FamID | Flags  |
 *
 *    1     ||       4      |   5    |   6   |   7    |
 *          ||-----------K-----------|   M   | unused |
 *
 *   2,3    || 8  | 9  | 10 | 11 | 12 | 13 | 14 | 15  |
 *          ||-----------------N_LONG-----------------|
 *
 *    4     ||      16      |   17   |  18   |  19    |
 *          ||------Min K------------|NumLvls| unused |
 *
 *    5     ||     20       |
 *            { Levels Array }
 *            {   Min Item   }
 *            {   Max Item   }
 *            { Items Array  }
 *
 * Serialization Combinations for SerVer and PreambleInts
 * | Sketch Structure | SerVer         | PreInts          |
 * |------------------|----------------|------------------|
 * | Compact Empty    | Empty/Full (1) | Empty/Single (2) | ReadOnly, 8 byte Preamble, nothing else
 * | Compact Single   | Single (2)     | Empty/Single (2) | ReadOnly, 8 byte Preamble + Single Item
 * | Compact Full     | Empty/Full (1) | Full (5)         | ReadOnly, 20 Byte Preamble, Short LevelsArr, Retained Items
 * | Updatable        | Updatable (3)  | Full (5)         | Updatable, 20 Byte Preamble, Full LevelsArr, All Items
 * | ERROR            | Single (2)     | Full (5)         |
 * | ERROR            | Updatable (3)  | Empty/Single (2) |
 * }</pre>
 *
 *  @author Lee Rhodes
 */
final class KllPreambleUtil {

  private KllPreambleUtil() {}

  // Preamble byte addresses
  static final int PREAMBLE_INTS_BYTE_ADR     = 0;
  static final int SER_VER_BYTE_ADR           = 1;
  static final int FAMILY_BYTE_ADR            = 2;
  static final int FLAGS_BYTE_ADR             = 3;
  static final int K_SHORT_ADR                = 4;  // to 5
  static final int M_BYTE_ADR                 = 6;
  //                                            7 is reserved for future use
  // SINGLE ITEM ONLY
  static final int DATA_START_ADR_SINGLE_ITEM = 8; //also ok for empty

  // MULTI-ITEM
  static final int N_LONG_ADR                 = 8;  // to 15
  static final int MIN_K_SHORT_ADR            = 16; // to 17
  static final int NUM_LEVELS_BYTE_ADR        = 18;

  //                                            19 is reserved for future use
  static final int DATA_START_ADR             = 20; // Full Sketch, not single item

  // Other static members
  static final byte SERIAL_VERSION_EMPTY_FULL  = 1; // Empty or full preamble, NOT single item format, NOT updatable
  static final byte SERIAL_VERSION_SINGLE      = 2; // only single-item format, NOT updatable
  static final byte SERIAL_VERSION_UPDATABLE   = 3; // PreInts=5, Full preamble + LevelsArr + min, max + empty space
  static final byte PREAMBLE_INTS_EMPTY_SINGLE = 2; // for empty or single item
  static final byte PREAMBLE_INTS_FULL         = 5; // Full preamble, not empty nor single item.
  static final byte KLL_FAMILY                 = 15;

  // Flag bit masks
  static final int EMPTY_BIT_MASK             = 1;
  static final int LEVEL_ZERO_SORTED_BIT_MASK = 2;
  static final int SINGLE_ITEM_BIT_MASK       = 4;

  /**
   * Returns a human readable string summary of the internal state of the given sketch byte array.
   * Used primarily in testing.
   *
   * @param byteArr the given sketch byte array.
   * @param includeData if true, includes detail of retained data.
   * @return the summary string.
   */
  static String toString(final byte[] byteArr, final SketchType sketchType, final boolean includeData) {
    final MemorySegment seg = MemorySegment.ofArray(byteArr);
    return toString(seg, sketchType, includeData, null);
  }

  /**
   * Returns a human readable string summary of the internal state of the given sketch byte array.
   * Used primarily in testing.
   *
   * @param byteArr the given sketch byte array.
   * @param includeData if true, includes detail of retained data.
   * @param serDe the serialization/deserialization class, required for KllItemsSketch.
   * @return the summary string.
   */
  static String toString(final byte[] byteArr, final SketchType sketchType, final boolean includeData,
      final ArrayOfItemsSerDe<?> serDe) {
    final MemorySegment seg = MemorySegment.ofArray(byteArr);
    return toString(seg, sketchType, includeData, serDe);
  }

  /**
   * Returns a human readable string summary of the internal state of the given MemorySegment.
   * Used primarily in testing.
   *
   * @param seg the given MemorySegment
   * @param includeData if true, includes detail of retained data.
   * @return the summary string.
   */
  static String toString(final MemorySegment seg, final SketchType sketchType, final boolean includeData) {
    return toString(seg, sketchType, includeData, null);
  }

  /**
   * Returns a human readable string summary of the internal state of the given MemorySegment.
   * Used primarily in testing.
   *
   * @param seg the given MemorySegment
   * @param sketchType the sketch type: FLOATS_SKETCH, DOUBLES_SKETCH, LONGS_SKETCH, or ITEMS_SKETCH.
   * @param includeData if true, includes detail of retained data.
   * @param serDe must be supplied for KllItemsSketch, otherwise can be null.
   * @return the summary string.
   */
  static <T> String toString(final MemorySegment seg, final SketchType sketchType, final boolean includeData,
      final ArrayOfItemsSerDe<T> serDe) {
    if (sketchType == KLL_ITEMS_SKETCH) {
      Objects.requireNonNull(serDe, "SerDe parameter must not be null for ITEMS_SKETCH.");
    }
    final KllMemorySegmentValidate segVal = new KllMemorySegmentValidate(seg, sketchType, serDe);
    final SketchStructure myStructure = segVal.sketchStructure;
    final int flags = segVal.flags & 0XFF;
    final String flagsStr = (flags) + ", 0x" + (Integer.toHexString(flags)) + ", "
        + zeroPad(Integer.toBinaryString(flags), 8);
    final int preInts = segVal.preInts; //??
    final boolean emptyFlag = segVal.emptyFlag;
    final int sketchBytes = segVal.sketchBytes;
    final int typeBytes = sketchType.getBytes();
    final int familyID = getMemorySegmentFamilyID(seg);
    final String famName = idToFamily(familyID).toString();

    final StringBuilder sb = new StringBuilder();
    sb.append(Util.LS).append("### KLL SKETCH MEMORY SUMMARY:").append(LS);
    sb.append("Sketch Type                          : ").append(sketchType.toString()).append(LS);
    sb.append("SketchStructure                      : ").append(myStructure.toString()).append(LS);
    sb.append("Byte   0       : Preamble Ints       : ").append(preInts).append(LS);
    sb.append("Byte   1       : SerVer              : ").append(segVal.serVer).append(LS);
    sb.append("Byte   2       : FamilyID            : ").append(segVal.familyID).append(LS);
    sb.append("               : FamilyName          : ").append(famName).append(LS);
    sb.append("Byte   3       : Flags Field         : ").append(flagsStr).append(LS);
    sb.append("            Bit: Flag Name           : ").append(LS);
    sb.append("              0: EMPTY               : ").append(emptyFlag).append(LS);
    sb.append("              1: LEVEL_ZERO_SORTED   : ").append(segVal.level0SortedFlag).append(LS);
    sb.append("Bytes  4-5     : K                   : ").append(segVal.k).append(LS);
    sb.append("Byte   6       : Min Level Cap, M    : ").append(segVal.m).append(LS);
    sb.append("Byte   7       : (Reserved)          : ").append(LS);

    final long n = segVal.n;
    final int minK = segVal.minK;
    final int numLevels = segVal.numLevels;
    final int[] levelsArr = segVal.levelsArr; //the full levels array
    final int retainedItems = levelsArr[numLevels] - levelsArr[0];

    if ((myStructure == COMPACT_FULL) || (myStructure == UPDATABLE)) {
      sb.append("Bytes  8-15    : N                   : ").append(n).append(LS);
      sb.append("Bytes 16-17    : MinK                : ").append(minK).append(LS);
      sb.append("Byte  18       : NumLevels           : ").append(numLevels).append(LS);
    }
    else { //COMPACT_EMPTY OR COMPACT_SINGLE
      sb.append("Assumed        : N                   : ").append(n).append(LS);
      sb.append("Assumed        : MinK                : ").append(minK).append(LS);
      sb.append("Assumed        : NumLevels           : ").append(numLevels).append(LS);
    }
    sb.append("PreambleBytes                        : ").append(preInts * Integer.BYTES).append(LS);
    sb.append("Sketch Bytes                         : ").append(sketchBytes).append(LS);
    sb.append("MemorySegment Capacity Bytes                : ").append(seg.byteSize()).append(LS);
    sb.append("### END KLL Sketch MemorySegment Summary").append(LS);

    if (includeData) {
      sb.append(LS);
      sb.append("### START KLL DATA:").append(LS);
      int offsetBytes = 0;

      if (myStructure == UPDATABLE) {

        sb.append("LEVELS ARR:").append(LS);
        offsetBytes = DATA_START_ADR;
        for (int i = 0; i < (numLevels + 1); i++) {
          sb.append(i + ", " + seg.get(JAVA_INT_UNALIGNED, offsetBytes)).append(LS);
          offsetBytes += Integer.BYTES;
        }

        sb.append("MIN/MAX:").append(LS);
        if (sketchType == KLL_DOUBLES_SKETCH) {
          sb.append(seg.get(JAVA_DOUBLE_UNALIGNED, offsetBytes)).append(LS);
          offsetBytes += typeBytes;
          sb.append(seg.get(JAVA_DOUBLE_UNALIGNED, offsetBytes)).append(LS);
          offsetBytes += typeBytes;
        } else if (sketchType == KLL_FLOATS_SKETCH) {
          sb.append(seg.get(JAVA_FLOAT_UNALIGNED, offsetBytes)).append(LS);
          offsetBytes += typeBytes;
          sb.append(seg.get(JAVA_FLOAT_UNALIGNED, offsetBytes)).append(LS);
          offsetBytes += typeBytes;
        } else if (sketchType == KLL_LONGS_SKETCH) {
          sb.append(seg.get(JAVA_LONG_UNALIGNED, offsetBytes)).append(LS);
          offsetBytes += typeBytes;
          sb.append(seg.get(JAVA_LONG_UNALIGNED, offsetBytes)).append(LS);
          offsetBytes += typeBytes;
        } else { //ITEMS_SKETCH
          sb.append("<<<Updatable Structure is not suppported by KllItemsSketch>>>").append(LS);
        }

        sb.append("ALL DATA (including free space)").append(LS);
        final int itemsSpace = (sketchBytes - offsetBytes) / typeBytes;
        if (sketchType == KLL_DOUBLES_SKETCH) {
          for (int i = 0; i < itemsSpace; i++) {
            sb.append(i + ", " + seg.get(JAVA_DOUBLE_UNALIGNED, offsetBytes)).append(LS);
            offsetBytes += typeBytes;
          }
        } else if (sketchType == KLL_FLOATS_SKETCH) {
          for (int i = 0; i < itemsSpace; i++) {
            sb.append(seg.get(JAVA_FLOAT_UNALIGNED, offsetBytes)).append(LS);
            offsetBytes += typeBytes;
          }
        } else if (sketchType == KLL_LONGS_SKETCH) {
          for (int i = 0; i < itemsSpace; i++) {
            sb.append(seg.get(JAVA_LONG_UNALIGNED, offsetBytes)).append(LS);
            offsetBytes += typeBytes;
          }
        } else { //ITEMS_SKETCH
          sb.append("<<<Updatable Structure is not suppported by KllItemsSketch>>>").append(LS);
        }

      } else if (myStructure == COMPACT_FULL) {

        sb.append("LEVELS ARR:").append(LS);
        offsetBytes = DATA_START_ADR;
        int j;
        for (j = 0; j < numLevels; j++) {
          sb.append(j + ", " + seg.get(JAVA_INT_UNALIGNED, offsetBytes)).append(LS);
          offsetBytes += Integer.BYTES;
        }
        sb.append(j + ", " + levelsArr[numLevels]);
        sb.append(" (Top level of Levels Array is absent in MemorySegment)").append(LS);

        sb.append("MIN/MAX:").append(LS);
        if (sketchType == KLL_DOUBLES_SKETCH) {
          sb.append(seg.get(JAVA_DOUBLE_UNALIGNED, offsetBytes)).append(LS);
          offsetBytes += typeBytes;
          sb.append(seg.get(JAVA_DOUBLE_UNALIGNED, offsetBytes)).append(LS);
          offsetBytes += typeBytes;
        } else if (sketchType == KLL_FLOATS_SKETCH) {
          sb.append(seg.get(JAVA_FLOAT_UNALIGNED, offsetBytes)).append(LS);
          offsetBytes += typeBytes;
          sb.append(seg.get(JAVA_FLOAT_UNALIGNED, offsetBytes)).append(LS);
          offsetBytes += typeBytes;
        } else if (sketchType == KLL_LONGS_SKETCH) {
          sb.append(seg.get(JAVA_LONG_UNALIGNED, offsetBytes)).append(LS);
          offsetBytes += typeBytes;
          sb.append(seg.get(JAVA_LONG_UNALIGNED, offsetBytes)).append(LS);
          offsetBytes += typeBytes;
        } else {  //ITEMS_SKETCH
          sb.append(serDe.deserializeFromMemorySegment(seg, offsetBytes, 1)[0]).append(LS);
          offsetBytes += serDe.sizeOf(seg, offsetBytes, 1);
          sb.append(serDe.deserializeFromMemorySegment(seg, offsetBytes, 1)[0]).append(LS);
          offsetBytes += serDe.sizeOf(seg, offsetBytes, 1);
        }

        sb.append("RETAINED DATA").append(LS);
        final int itemSpace = (sketchBytes - offsetBytes) / (typeBytes == 0 ? 1 : typeBytes);
        if (sketchType == KLL_DOUBLES_SKETCH) {
          for (int i = 0; i < itemSpace; i++) {
            sb.append(i + ", " + seg.get(JAVA_DOUBLE_UNALIGNED, offsetBytes)).append(LS);
            offsetBytes += typeBytes;
          }
        } else if (sketchType == KLL_FLOATS_SKETCH) {
          for (int i = 0; i < itemSpace; i++) {
            sb.append(i + ", " + seg.get(JAVA_FLOAT_UNALIGNED, offsetBytes)).append(LS);
            offsetBytes += typeBytes;
          }
        } else if (sketchType == KLL_LONGS_SKETCH) {
          for (int i = 0; i < itemSpace; i++) {
            sb.append(i + ", " + seg.get(JAVA_LONG_UNALIGNED, offsetBytes)).append(LS);
            offsetBytes += typeBytes;
          }
        } else { //ITEMS_SKETCH
          final T[] itemsArr = serDe.deserializeFromMemorySegment(seg, offsetBytes, retainedItems);
          for (int i = 0; i < itemsArr.length; i++) {
            sb.append(i + ", " + serDe.toString(itemsArr[i])).append(LS);
          }
        }

      } else if (myStructure == COMPACT_SINGLE) {

          sb.append("SINGLE ITEM DATUM: "); //no LS
          if (sketchType == KLL_DOUBLES_SKETCH) {
            sb.append(seg.get(JAVA_DOUBLE_UNALIGNED, DATA_START_ADR_SINGLE_ITEM)).append(LS);
          } else if (sketchType == KLL_FLOATS_SKETCH) {
            sb.append(seg.get(JAVA_FLOAT_UNALIGNED, DATA_START_ADR_SINGLE_ITEM)).append(LS);
          } else if (sketchType == KLL_LONGS_SKETCH) {
            sb.append(seg.get(JAVA_LONG_UNALIGNED, DATA_START_ADR_SINGLE_ITEM)).append(LS);
          } else { //ITEMS_SKETCH
            sb.append(serDe.deserializeFromMemorySegment(seg, DATA_START_ADR_SINGLE_ITEM, 1)[0]).append(LS);
          }

      } else { //COMPACT_EMPTY
        sb.append("EMPTY, NO DATA").append(LS);
      }
      sb.append("### END KLL DATA:").append(LS);
    }
    return sb.toString();
  }

  static int getMemorySegmentPreInts(final MemorySegment seg) {
    return seg.get(JAVA_BYTE, PREAMBLE_INTS_BYTE_ADR) & 0XFF;
  }

  static int getMemorySegmentSerVer(final MemorySegment seg) {
    return seg.get(JAVA_BYTE, SER_VER_BYTE_ADR) & 0XFF;
  }

  static SketchStructure getMemorySegmentSketchStructure(final MemorySegment seg) {
    final int preInts = getMemorySegmentPreInts(seg);
    final int serVer = getMemorySegmentSerVer(seg);
    return KllSketch.SketchStructure.getSketchStructure(preInts, serVer);
  }

  static int getMemorySegmentFamilyID(final MemorySegment seg) {
    return seg.get(JAVA_BYTE, FAMILY_BYTE_ADR) & 0XFF;
  }

  static int getMemorySegmentFlags(final MemorySegment seg) {
    return seg.get(JAVA_BYTE, FLAGS_BYTE_ADR) & 0XFF;
  }

  static boolean getMemorySegmentEmptyFlag(final MemorySegment seg) {
    return (getMemorySegmentFlags(seg) & EMPTY_BIT_MASK) != 0;
  }

  static boolean getMemorySegmentLevelZeroSortedFlag(final MemorySegment seg) {
    return (getMemorySegmentFlags(seg) & LEVEL_ZERO_SORTED_BIT_MASK) != 0;
  }

  static int getMemorySegmentK(final MemorySegment seg) {
    return seg.get(JAVA_SHORT_UNALIGNED, K_SHORT_ADR) & 0XFFFF;
  }

  static int getMemorySegmentM(final MemorySegment seg) {
    return seg.get(JAVA_BYTE, M_BYTE_ADR) & 0XFF;
  }

  static long getMemorySegmentN(final MemorySegment seg) {
    return seg.get(JAVA_LONG_UNALIGNED, N_LONG_ADR);
  }

  static int getMemorySegmentMinK(final MemorySegment seg) {
    return seg.get(JAVA_SHORT_UNALIGNED, MIN_K_SHORT_ADR) & 0XFFFF;
  }

  static int getMemorySegmentNumLevels(final MemorySegment seg) {
    return seg.get(JAVA_BYTE, NUM_LEVELS_BYTE_ADR) & 0XFF;
  }

  static void setMemorySegmentPreInts(final MemorySegment wseg, final int numPreInts) {
    wseg.set(JAVA_BYTE, PREAMBLE_INTS_BYTE_ADR, (byte) numPreInts);
  }

  static void setMemorySegmentSerVer(final MemorySegment wseg, final int serVer) {
    wseg.set(JAVA_BYTE, SER_VER_BYTE_ADR, (byte) serVer);
  }

  static void setMemorySegmentFamilyID(final MemorySegment wseg, final int famId) {
    wseg.set(JAVA_BYTE, FAMILY_BYTE_ADR, (byte) famId);
  }

  static void setMemorySegmentFlags(final MemorySegment wseg, final int flags) {
    wseg.set(JAVA_BYTE, FLAGS_BYTE_ADR, (byte) flags);
  }

  static void setMemorySegmentEmptyFlag(final MemorySegment wseg,  final boolean empty) {
    final int flags = getMemorySegmentFlags(wseg);
    setMemorySegmentFlags(wseg, empty ? flags | EMPTY_BIT_MASK : flags & ~EMPTY_BIT_MASK);
  }

  static void setMemorySegmentLevelZeroSortedFlag(final MemorySegment wseg,  final boolean levelZeroSorted) {
    final int flags = getMemorySegmentFlags(wseg);
    setMemorySegmentFlags(wseg, levelZeroSorted ? flags | LEVEL_ZERO_SORTED_BIT_MASK : flags & ~LEVEL_ZERO_SORTED_BIT_MASK);
  }

  static void setMemorySegmentK(final MemorySegment wseg, final int segK) {
    wseg.set(JAVA_SHORT_UNALIGNED, K_SHORT_ADR, (short) segK);
  }

  static void setMemorySegmentM(final MemorySegment wseg, final int segM) {
    wseg.set(JAVA_BYTE, M_BYTE_ADR, (byte) segM);
  }

  static void setMemorySegmentN(final MemorySegment wseg, final long segN) {
    wseg.set(JAVA_LONG_UNALIGNED, N_LONG_ADR, segN);
  }

  static void setMemorySegmentMinK(final MemorySegment wseg, final int segMinK) {
    wseg.set(JAVA_SHORT_UNALIGNED, MIN_K_SHORT_ADR, (short) segMinK);
  }

  static void setMemorySegmentNumLevels(final MemorySegment wseg, final int segNumLevels) {
    wseg.set(JAVA_BYTE, NUM_LEVELS_BYTE_ADR, (byte) segNumLevels);
  }

}
