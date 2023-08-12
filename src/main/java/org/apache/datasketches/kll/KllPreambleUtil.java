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

import static org.apache.datasketches.common.Family.idToFamily;
import static org.apache.datasketches.common.Util.zeroPad;
import static org.apache.datasketches.kll.KllSketch.SketchStructure.COMPACT_FULL;
import static org.apache.datasketches.kll.KllSketch.SketchStructure.COMPACT_SINGLE;
import static org.apache.datasketches.kll.KllSketch.SketchStructure.UPDATABLE;
import static org.apache.datasketches.kll.KllSketch.SketchType.DOUBLES_SKETCH;
import static org.apache.datasketches.kll.KllSketch.SketchType.FLOATS_SKETCH;
import static org.apache.datasketches.kll.KllSketch.SketchType.ITEMS_SKETCH;

import java.util.Objects;

import org.apache.datasketches.common.ArrayOfItemsSerDe;
import org.apache.datasketches.common.Util;
import org.apache.datasketches.kll.KllSketch.SketchStructure;
import org.apache.datasketches.kll.KllSketch.SketchType;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;

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
 * followed by a packed items data array as bytes. There are no empty or garbage slots in this structure.
 * It is not updatable.
 * It is identified by the <i>enum SketchStructure.COMPACT_FULL</i>.</li>
 *
 * <li>A serialized, <i>n &gt; 1</i> non-compact, updatable structure requires 20 bytes of preamble (5 ints).
 * This is followed by the LevelsArr int[NumLevels + 1], followed by the min and max values, and then
 * followed by an items data array that may include empty or garbage slots. It is updatable.
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
final class KllPreambleUtil<T> {

  private KllPreambleUtil() {}

  static final String LS = System.getProperty("line.separator");

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

  /**
   * Returns a human readable string summary of the internal state of the given sketch byte array.
   * Used primarily in testing.
   *
   * @param byteArr the given sketch byte array.
   * @param includeData if true, includes detail of retained data.
   * @return the summary string.
   */
  static String toString(final byte[] byteArr, final SketchType sketchType, final boolean includeData) {
    final Memory mem = Memory.wrap(byteArr);
    return toString(mem, sketchType, includeData, null);
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
    final Memory mem = Memory.wrap(byteArr);
    return toString(mem, sketchType, includeData, serDe);
  }

  /**
   * Returns a human readable string summary of the internal state of the given Memory.
   * Used primarily in testing.
   *
   * @param mem the given Memory
   * @param includeData if true, includes detail of retained data.
   * @return the summary string.
   */
  static String toString(final Memory mem, final SketchType sketchType, final boolean includeData) {
    return toString(mem, sketchType, includeData, null);
  }

  /**
   * Returns a human readable string summary of the internal state of the given Memory.
   * Used primarily in testing.
   *
   * @param mem the given Memory
   * @param sketchType the sketch type: FLOATS_SKETCH, DOUBLES_SKETCH, or ITEMS_SKETCH.
   * @param includeData if true, includes detail of retained data.
   * @param serDe must be supplied for KllItemsSketch, otherwise can be null.
   * @return the summary string.
   */
  static <T> String toString(final Memory mem, final SketchType sketchType, final boolean includeData,
      final ArrayOfItemsSerDe<T> serDe) {
    if (sketchType == ITEMS_SKETCH) {
      Objects.requireNonNull(serDe, "SerDe parameter must not be null for ITEMS_SKETCH.");
    }
    final KllMemoryValidate memVal = new KllMemoryValidate(mem, sketchType, serDe);
    final SketchStructure myStructure = memVal.sketchStructure;
    final int flags = memVal.flags & 0XFF;
    final String flagsStr = (flags) + ", 0x" + (Integer.toHexString(flags)) + ", "
        + zeroPad(Integer.toBinaryString(flags), 8);
    final int preInts = memVal.preInts; //??
    //final boolean updatable = mySketchStructure == UPDATABLE;
    final boolean emptyFlag = memVal.emptyFlag;
    final int sketchBytes = memVal.sketchBytes;
    final int typeBytes = sketchType == DOUBLES_SKETCH ? Double.BYTES : Float.BYTES;
    final int familyID = getMemoryFamilyID(mem);
    final String famName = idToFamily(familyID).toString();

    final StringBuilder sb = new StringBuilder();
    sb.append(Util.LS).append("### KLL SKETCH MEMORY SUMMARY:").append(LS);
    sb.append("Sketch Type                          : ").append(sketchType.toString()).append(LS);
    sb.append("SketchStructure                      : ").append(myStructure.toString()).append(LS);
    sb.append("Byte   0       : Preamble Ints       : ").append(preInts).append(LS);
    sb.append("Byte   1       : SerVer              : ").append(memVal.serVer).append(LS);
    sb.append("Byte   2       : FamilyID            : ").append(memVal.familyID).append(LS);
    sb.append("               : FamilyName          : ").append(famName).append(LS);
    sb.append("Byte   3       : Flags Field         : ").append(flagsStr).append(LS);
    sb.append("            Bit: Flag Name           : ").append(LS);
    sb.append("              0: EMPTY               : ").append(emptyFlag).append(LS);
    sb.append("              1: LEVEL_ZERO_SORTED   : ").append(memVal.level0SortedFlag).append(LS);
    sb.append("Bytes  4-5     : K                   : ").append(memVal.k).append(LS);
    sb.append("Byte   6       : Min Level Cap, M    : ").append(memVal.m).append(LS);
    sb.append("Byte   7       : (Reserved)          : ").append(LS);

    final long n = memVal.n;
    final int minK = memVal.minK;
    final int numLevels = memVal.numLevels;
    final int[] levelsArr = memVal.levelsArr; //the full levels array
    final int retainedItems = levelsArr[numLevels] - levelsArr[0];

    if (myStructure == COMPACT_FULL || myStructure == UPDATABLE) {
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
    sb.append("Memory Capacity Bytes                : ").append(mem.getCapacity()).append(LS);
    sb.append("### END KLL Sketch Memory Summary").append(LS);

    if (includeData) {
      sb.append(LS);
      sb.append("### START KLL DATA:").append(LS);
      int offsetBytes = 0;

      if (myStructure == UPDATABLE) {

        sb.append("LEVELS ARR:").append(LS);
        offsetBytes = DATA_START_ADR;
        for (int i = 0; i < numLevels + 1; i++) {
          sb.append(i + ", " + mem.getInt(offsetBytes)).append(LS);
          offsetBytes += Integer.BYTES;
        }

        sb.append("MIN/MAX:").append(LS);
        if (sketchType == DOUBLES_SKETCH) {
          sb.append(mem.getDouble(offsetBytes)).append(LS);
          offsetBytes += typeBytes;
          sb.append(mem.getDouble(offsetBytes)).append(LS);
          offsetBytes += typeBytes;
        } else if (sketchType == FLOATS_SKETCH) {
          sb.append(mem.getFloat(offsetBytes)).append(LS);
          offsetBytes += typeBytes;
          sb.append(mem.getFloat(offsetBytes)).append(LS);
          offsetBytes += typeBytes;
        } else { //ITEMS_SKETCH
          sb.append("<<<Updatable Structure is not suppported by ItemsSketch>>>").append(LS);
        }

        sb.append("ALL DATA (including empty & garbage data)").append(LS);
        final int itemsSpace = (sketchBytes - offsetBytes) / typeBytes;
        if (sketchType == DOUBLES_SKETCH) {
          for (int i = 0; i < itemsSpace; i++) {
            sb.append(i + ", " + mem.getDouble(offsetBytes)).append(LS);
            offsetBytes += typeBytes;
          }
        } else if (sketchType == FLOATS_SKETCH) {
          for (int i = 0; i < itemsSpace; i++) {
            sb.append(mem.getFloat(offsetBytes)).append(LS);
            offsetBytes += typeBytes;
          }
        } else { //ITEMS_SKETCH
          sb.append("<<<Updatable Structure is not suppported by ItemsSketch>>>").append(LS);
        }

      } else if (myStructure == COMPACT_FULL) {

        sb.append("LEVELS ARR:").append(LS);
        offsetBytes = DATA_START_ADR;
        int j;
        for (j = 0; j < numLevels; j++) {
          sb.append(j + ", " + mem.getInt(offsetBytes)).append(LS);
          offsetBytes += Integer.BYTES;
        }
        sb.append(j + ", " + levelsArr[numLevels]);
        sb.append(" (Top level of Levels Array is absent in Memory)").append(LS);

        sb.append("MIN/MAX:").append(LS);
        if (sketchType == DOUBLES_SKETCH) {
          sb.append(mem.getDouble(offsetBytes)).append(LS);
          offsetBytes += typeBytes;
          sb.append(mem.getDouble(offsetBytes)).append(LS);
          offsetBytes += typeBytes;
        } else if (sketchType == FLOATS_SKETCH) {
          sb.append(mem.getFloat(offsetBytes)).append(LS);
          offsetBytes += typeBytes;
          sb.append(mem.getFloat(offsetBytes)).append(LS);
          offsetBytes += typeBytes;
        } else {  //ITEMS_SKETCH
          sb.append(serDe.deserializeFromMemory(mem, offsetBytes, 1)[0]).append(LS);
          offsetBytes += serDe.sizeOf(mem, offsetBytes, 1);
          sb.append(serDe.deserializeFromMemory(mem, offsetBytes, 1)[0]).append(LS);
          offsetBytes += serDe.sizeOf(mem, offsetBytes, 1);
        }

        sb.append("RETAINED DATA").append(LS);
        final int itemSpace = (sketchBytes - offsetBytes) / typeBytes;
        if (sketchType == DOUBLES_SKETCH) {
          for (int i = 0; i < itemSpace; i++) {
            sb.append(i + ", " + mem.getDouble(offsetBytes)).append(LS);
            offsetBytes += typeBytes;
          }
        } else if (sketchType == FLOATS_SKETCH) {
          for (int i = 0; i < itemSpace; i++) {
            sb.append(i + ", " + mem.getFloat(offsetBytes)).append(LS);
            offsetBytes += typeBytes;
          }
        } else { //ITEMS_SKETCH
          final T[] itemsArr = serDe.deserializeFromMemory(mem, offsetBytes, retainedItems);
          for (int i = 0; i < itemsArr.length; i++) {
            sb.append(i + ", " + serDe.toString(itemsArr[i])).append(LS);
          }
          offsetBytes += serDe.sizeOf(mem, offsetBytes, retainedItems);
        }

      } else if (myStructure == COMPACT_SINGLE) {

          sb.append("SINGLE ITEM DATUM: "); //no LS
          if (sketchType == DOUBLES_SKETCH) {
            sb.append(mem.getDouble(DATA_START_ADR_SINGLE_ITEM)).append(LS);
          } else if (sketchType == FLOATS_SKETCH) {
            sb.append(mem.getFloat(DATA_START_ADR_SINGLE_ITEM)).append(LS);
          } else { //ITEMS_SKETCH
            sb.append(serDe.deserializeFromMemory(mem, DATA_START_ADR_SINGLE_ITEM, 1)[0]).append(LS);
          }

      } else { //COMPACT_EMPTY
        sb.append("EMPTY, NO DATA").append(LS);
      }
      sb.append("### END KLL DATA:").append(LS);
    }
    return sb.toString();
  }

  static int getMemoryPreInts(final Memory mem) {
    return mem.getByte(PREAMBLE_INTS_BYTE_ADR) & 0XFF;
  }

  static int getMemorySerVer(final Memory mem) {
    return mem.getByte(SER_VER_BYTE_ADR) & 0XFF;
  }

  static SketchStructure getMemorySketchStructure(final Memory mem) {
    final int preInts = getMemoryPreInts(mem);
    final int serVer = getMemorySerVer(mem);
    final SketchStructure structure = KllSketch.SketchStructure.getSketchStructure(preInts, serVer);
    return structure;
  }

  static int getMemoryFamilyID(final Memory mem) {
    return mem.getByte(FAMILY_BYTE_ADR) & 0XFF;
  }

  static int getMemoryFlags(final Memory mem) {
    return mem.getByte(FLAGS_BYTE_ADR) & 0XFF;
  }

  static boolean getMemoryEmptyFlag(final Memory mem) {
    return (getMemoryFlags(mem) & EMPTY_BIT_MASK) != 0;
  }

  static boolean getMemoryLevelZeroSortedFlag(final Memory mem) {
    return (getMemoryFlags(mem) & LEVEL_ZERO_SORTED_BIT_MASK) != 0;
  }

  static int getMemoryK(final Memory mem) {
    return mem.getShort(K_SHORT_ADR) & 0XFFFF;
  }

  static int getMemoryM(final Memory mem) {
    return mem.getByte(M_BYTE_ADR) & 0XFF;
  }

  static long getMemoryN(final Memory mem) {
    return mem.getLong(N_LONG_ADR);
  }

  static int getMemoryMinK(final Memory mem) {
    return mem.getShort(MIN_K_SHORT_ADR) & 0XFFFF;
  }

  static int getMemoryNumLevels(final Memory mem) {
    return mem.getByte(NUM_LEVELS_BYTE_ADR) & 0XFF;
  }

  static void setMemoryPreInts(final WritableMemory wmem, final int numPreInts) {
    wmem.putByte(PREAMBLE_INTS_BYTE_ADR, (byte) numPreInts);
  }

  static void setMemorySerVer(final WritableMemory wmem, final int serVer) {
    wmem.putByte(SER_VER_BYTE_ADR, (byte) serVer);
  }

  static void setMemoryFamilyID(final WritableMemory wmem, final int famId) {
    wmem.putByte(FAMILY_BYTE_ADR, (byte) famId);
  }

  static void setMemoryFlags(final WritableMemory wmem, final int flags) {
    wmem.putByte(FLAGS_BYTE_ADR, (byte) flags);
  }

  static void setMemoryEmptyFlag(final WritableMemory wmem,  final boolean empty) {
    final int flags = getMemoryFlags(wmem);
    setMemoryFlags(wmem, empty ? flags | EMPTY_BIT_MASK : flags & ~EMPTY_BIT_MASK);
  }

  static void setMemoryLevelZeroSortedFlag(final WritableMemory wmem,  final boolean levelZeroSorted) {
    final int flags = getMemoryFlags(wmem);
    setMemoryFlags(wmem, levelZeroSorted ? flags | LEVEL_ZERO_SORTED_BIT_MASK : flags & ~LEVEL_ZERO_SORTED_BIT_MASK);
  }

  static void setMemoryK(final WritableMemory wmem, final int memK) {
    wmem.putShort(K_SHORT_ADR, (short) memK);
  }

  static void setMemoryM(final WritableMemory wmem, final int memM) {
    wmem.putByte(M_BYTE_ADR, (byte) memM);
  }

  static void setMemoryN(final WritableMemory wmem, final long memN) {
    wmem.putLong(N_LONG_ADR, memN);
  }

  static void setMemoryMinK(final WritableMemory wmem, final int memMinK) {
    wmem.putShort(MIN_K_SHORT_ADR, (short) memMinK);
  }

  static void setMemoryNumLevels(final WritableMemory wmem, final int memNumLevels) {
    wmem.putByte(NUM_LEVELS_BYTE_ADR, (byte) memNumLevels);
  }

}
