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

import static org.apache.datasketches.Util.zeroPad;

import org.apache.datasketches.Util;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;

//@formatter:off

/**
 * This class defines the serialized data structure and provides access methods for the key fields.
 *
 * <p>The intent of the design of this class was to isolate the detailed knowledge of the bit and
 * byte layout of the serialized form of the sketches derived from the base sketch classes into one place.
 * This allows the possibility of the introduction of different serialization
 * schemes with minimal impact on the rest of the library.</p>
 *
 * <p>
 * LAYOUT: The low significance bytes of this <i>long</i> based data structure are on the right.
 * The multi-byte primitives are stored in native byte order.
 * The single byte fields are treated as unsigned.</p>
 *
 * <p>An empty sketch requires only 8 bytes, which is only preamble.
 * A serialized, non-empty KllDoublesSketch requires at least 16 bytes of preamble.
 * A serialized, non-empty KllFloatsSketch requires at least 12 bytes of preamble.</p>
 *
 * <pre>{@code
 * Serialized float sketch layout, more than one item:
 *  Adr:
 *      ||    7    |   6   |    5   |    4   |    3   |    2    |    1   |      0       |
 *  0   || unused  |   M   |--------K--------|  Flags |  FamID  | SerVer | PreambleInts |
 *      ||   15    |   14  |   13   |   12   |   11   |   10    |    9   |      8       |
 *  1   ||---------------------------------N_LONG---------------------------------------|
 *      ||         |       |        |   20   |   19   |    18   |   17   |      16      |
 *  2   ||<-------Levels Arr Start----------]| unused |NumLevels|------Min K------------|
 *      ||         |       |        |        |        |         |        |              |
 *  ?   ||<-------Min/Max Arr Start---------]|[<----------Levels Arr End----------------|
 *      ||         |       |        |        |        |         |        |              |
 *  ?   ||<-----Float Items Arr Start-------]|[<---------Min/Max Arr End----------------|
 *      ||         |       |        |        |        |         |        |              |
 *  ?   ||         |       |        |        |[<-------Float Items Arr End--------------|
 *
 * Serialized float sketch layout, Empty (8 bytes) and Single Item (12 bytes):
 *  Adr:
 *      ||    7    |   6   |    5   |    4   |    3   |    2    |    1   |      0       |
 *  0   || unused  |   M   |--------K--------|  Flags |  FamID  | SerVer | PreambleInts |
 *      ||   15    |   14  |   13   |   12   |   11   |   10    |    9   |      8       |
 *  1   ||                                   |-------------Single Item------------------|
 *
 *
 *
 * Serialized double sketch layout, more than one item:
 *  Adr:
 *      ||    7    |   6   |    5   |    4   |    3   |    2    |    1   |      0       |
 *  0   || unused  |   M   |--------K--------|  Flags |  FamID  | SerVer | PreambleInts |
 *      ||   15    |   14  |   13   |   12   |   11   |   10    |    9   |      8       |
 *  1   ||---------------------------------N_LONG---------------------------------------|
 *      ||   23    |   22  |   21   |   20   |   19   |    18   |   17   |      16      |
 *  2   ||<-------Levels Arr Start----------]| unused |NumLevels|------Min K------------|
 *      ||         |       |        |        |        |         |        |              |
 *  ?   ||<-------Min/Max Arr Start---------]|[<----------Levels Arr End----------------|
 *      ||         |       |        |        |        |         |        |              |
 *  ?   ||<----Double Items Arr Start-------]|[<---------Min/Max Arr End----------------|
 *      ||         |       |        |        |        |         |        |              |
 *  ?   ||         |       |        |        |[<------Double Items Arr End--------------|
 *
 * Serialized double sketch layout, Empty (8 bytes) and Single Item (16 bytes):
 *  Adr:
 *      ||    7    |   6   |    5   |    4   |    3   |    2    |    1   |      0       |
 *  0   || unused  |   M   |--------K--------|  Flags |  FamID  | SerVer | PreambleInts |
 *      ||                                                               |      8       |
 *  1   ||------------------------------Single Item-------------------------------------|
 *
 * The structure of the data block depends on Layout:
 *
 *   For FLOAT_SINGLE_COMPACT or DOUBLE_SINGLE_COMPACT:
 *     The single data item is at offset DATA_START_ADR_SINGLE_ITEM = 8
 *
 *   For FLOAT_FULL_COMPACT:
 *     The int[] levels array starts at offset DATA_START_ADR_FLOAT = 20 with a length of numLevels integers;
 *     Followed by Float Min_Value, then Float Max_Value
 *     Followed by an array of Floats of length retainedItems()
 *
 *   For DOUBLE_FULL_COMPACT
 *     The int[] levels array starts at offset DATA_START_ADR_DOUBLE = 20 with a length of numLevels integers;
 *     Followed by Double Min_Value, then Double Max_Value
 *     Followed by an array of Doubles of length retainedItems()
 *
 *   For FLOAT_UPDATABLE
 *     The int[] levels array starts at offset DATA_START_ADR_FLOAT = 20 with a length of (numLevels + 1) integers;
 *     Followed by Float Min_Value, then Float Max_Value
 *     Followed by an array of Floats of length KllHelper.computeTotalItemCapacity(...).
 *
 *   For DOUBLE_UPDATABLE
 *     The int[] levels array starts at offset DATA_START_ADR_DOUBLE = 20 with a length of (numLevels + 1) integers;
 *     Followed by Double Min_Value, then Double Max_Value
 *     Followed by an array of Doubles of length KllHelper.computeTotalItemCapacity(...).
 *
 * }</pre>
 *
 *  @author Lee Rhodes
 */
final class KllPreambleUtil {

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
  static final int DATA_START_ADR_SINGLE_ITEM = 8;

  // MULTI-ITEM
  static final int N_LONG_ADR                 = 8;  // to 15
  static final int MIN_K_SHORT_ADR            = 16; // to 17
  static final int NUM_LEVELS_BYTE_ADR        = 18;

  //                                                19 is reserved for future use
  static final int DATA_START_ADR             = 20; // Full Sketch, not single item

  // Other static values
  static final byte SERIAL_VERSION_EMPTY_FULL  = 1; // Empty or full preamble, NOT single item format
  static final byte SERIAL_VERSION_SINGLE      = 2; // only single-item format
  static final byte SERIAL_VERSION_UPDATABLE   = 3; //
  static final byte PREAMBLE_INTS_EMPTY_SINGLE = 2; // for empty or single item
  static final byte PREAMBLE_INTS_FULL         = 5; // Full preamble, not empty nor single item
  static final byte KLL_FAMILY                 = 15;

  // Flag bit masks
  static final int EMPTY_BIT_MASK             = 1;
  static final int LEVEL_ZERO_SORTED_BIT_MASK = 2;
  static final int SINGLE_ITEM_BIT_MASK       = 4;
  static final int DOUBLES_SKETCH_BIT_MASK    = 8;
  static final int UPDATABLE_BIT_MASK         = 16;

  /**
   * Returns a human readable string summary of the internal state of the given sketch byte array.
   * Used primarily in testing.
   *
   * @param byteArr the given sketch byte array.
   * @param includeData if true, includes detail of retained data.
   * @return the summary string.
   */
  static String toString(final byte[] byteArr, final boolean includeData) {
    final Memory mem = Memory.wrap(byteArr);
    return toString(mem, includeData);
  }

  /**
   * Returns a human readable string summary of the internal state of the given Memory.
   * Used primarily in testing.
   *
   * @param mem the given Memory
   * @param includeData if true, includes detail of retained data.
   * @return the summary string.
   */
  static String toString(final Memory mem, final boolean includeData) {
    final KllMemoryValidate memVal = new KllMemoryValidate(mem);
    final int flags = memVal.flags & 0XFF;
    final String flagsStr = (flags) + ", 0x" + (Integer.toHexString(flags)) + ", "
        + zeroPad(Integer.toBinaryString(flags), 8);
    final int preInts = memVal.preInts;
    final boolean doublesSketch = memVal.doublesSketch;
    final boolean updatableMemFormat = memVal.updatableMemFormat;
    final boolean empty = memVal.empty;
    final boolean singleItem = memVal.singleItem;
    final int sketchBytes = memVal.sketchBytes;
    final int typeBytes = memVal.typeBytes;

    final StringBuilder sb = new StringBuilder();
    sb.append(Util.LS).append("### KLL SKETCH MEMORY SUMMARY:").append(LS);
    sb.append("Byte   0   : Preamble Ints      : ").append(preInts).append(LS);
    sb.append("Byte   1   : SerVer             : ").append(memVal.serVer).append(LS);
    sb.append("Byte   2   : FamilyID           : ").append(memVal.familyID).append(LS);
    sb.append("             FamilyName         : ").append(memVal.famName).append(LS);
    sb.append("Byte   3   : Flags Field        : ").append(flagsStr).append(LS);
    sb.append("         Bit Flag Name").append(LS);
    sb.append("           0 EMPTY COMPACT      : ").append(empty).append(LS);
    sb.append("           1 LEVEL_ZERO_SORTED  : ").append(memVal.level0Sorted).append(LS);
    sb.append("           2 SINGLE_ITEM COMPACT: ").append(singleItem).append(LS);
    sb.append("           3 DOUBLES_SKETCH     : ").append(doublesSketch).append(LS);
    sb.append("           4 UPDATABLE          : ").append(updatableMemFormat).append(LS);
    sb.append("Bytes  4-5 : K                  : ").append(memVal.k).append(LS);
    sb.append("Byte   6   : Min Level Cap, M   : ").append(memVal.m).append(LS);
    sb.append("Byte   7   : (Reserved)         : ").append(LS);

    final long n = memVal.n;
    final int minK = memVal.minK;
    final int numLevels = memVal.numLevels;
    if (updatableMemFormat || (!updatableMemFormat && !empty && !singleItem)) {
        sb.append("Bytes  8-15: N                  : ").append(n).append(LS);
        sb.append("Bytes 16-17: MinK               : ").append(minK).append(LS);
        sb.append("Byte  18   : NumLevels          : ").append(numLevels).append(LS);
    }
    else {
        sb.append("Assumed    : N                  : ").append(n).append(LS);
        sb.append("Assumed    : MinK               : ").append(minK).append(LS);
        sb.append("Assumed    : NumLevels          : ").append(numLevels).append(LS);
    }
    sb.append("PreambleBytes                   : ").append(preInts * 4).append(LS);
    sb.append("Sketch Bytes                    : ").append(sketchBytes).append(LS);
    sb.append("Memory Capacity Bytes           : ").append(mem.getCapacity()).append(LS);
    sb.append("### END KLL Sketch Memory Summary").append(LS);

    if (includeData) {
      sb.append(LS);
      sb.append("### START KLL DATA:").append(LS);
      int offsetBytes = 0;

      if (updatableMemFormat) {
        sb.append("LEVELS ARR:").append(LS);
        offsetBytes = DATA_START_ADR;
        for (int i = 0; i < numLevels + 1; i++) {
          sb.append(i + ", " + mem.getInt(offsetBytes)).append(LS);
          offsetBytes += Integer.BYTES;
        }
        sb.append("MIN/MAX:").append(LS);
        if (doublesSketch) {
          sb.append(mem.getDouble(offsetBytes)).append(LS);
          offsetBytes += typeBytes;
          sb.append(mem.getDouble(offsetBytes)).append(LS);
          offsetBytes += typeBytes;
        } else { //floats
          sb.append(mem.getFloat(offsetBytes)).append(LS);
          offsetBytes += typeBytes;
          sb.append(mem.getFloat(offsetBytes)).append(LS);
          offsetBytes += typeBytes;
        }
        sb.append("ITEMS DATA").append(LS);
        final int itemSpace = (sketchBytes - offsetBytes) / typeBytes;
        if (doublesSketch) {
          for (int i = 0; i < itemSpace; i++) {
            sb.append(i + ", " + mem.getDouble(offsetBytes)).append(LS);
            offsetBytes += typeBytes;
          }
        } else { //floats
          for (int i = 0; i < itemSpace; i++) {
            sb.append(mem.getFloat(offsetBytes)).append(LS);
            offsetBytes += typeBytes;
          }
        }

      } else if (!empty && !singleItem) { //compact full
        sb.append("LEVELS ARR:").append(LS);
        offsetBytes = DATA_START_ADR;
        for (int i = 0; i < numLevels; i++) {
          sb.append(i + ", " + mem.getInt(offsetBytes)).append(LS);
          offsetBytes += Integer.BYTES;
        }
        sb.append("(top level of Levels arr is absent)").append(LS);
        sb.append("MIN/MAX:").append(LS);
        if (doublesSketch) {
          sb.append(mem.getDouble(offsetBytes)).append(LS);
          offsetBytes += typeBytes;
          sb.append(mem.getDouble(offsetBytes)).append(LS);
          offsetBytes += typeBytes;
        } else { //floats
          sb.append(mem.getFloat(offsetBytes)).append(LS);
          offsetBytes += typeBytes;
          sb.append(mem.getFloat(offsetBytes)).append(LS);
          offsetBytes += typeBytes;
        }
        sb.append("ITEMS DATA").append(LS);
        final int itemSpace = (sketchBytes - offsetBytes) / typeBytes;
        if (doublesSketch) {
          for (int i = 0; i < itemSpace; i++) {
            sb.append(i + ", " + mem.getDouble(offsetBytes)).append(LS);
            offsetBytes += typeBytes;
          }
        } else { //floats
          for (int i = 0; i < itemSpace; i++) {
            sb.append(i + ", " + mem.getFloat(offsetBytes)).append(LS);
            offsetBytes += typeBytes;
          }
        }

      } else { //single item
        if (singleItem) {
          sb.append("SINGLE ITEM DATA").append(LS);
          sb.append(doublesSketch
              ? mem.getDouble(DATA_START_ADR_SINGLE_ITEM)
              : mem.getFloat(DATA_START_ADR_SINGLE_ITEM)).append(LS);
        }
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

  static boolean getMemorySingleItemFlag(final Memory mem) {
    return (getMemoryFlags(mem) & SINGLE_ITEM_BIT_MASK) != 0;
  }

  static boolean getMemoryDoubleSketchFlag(final Memory mem) {
    return (getMemoryFlags(mem) & DOUBLES_SKETCH_BIT_MASK) != 0;
  }

  static boolean getMemoryUpdatableFormatFlag(final Memory mem) {
    return (getMemoryFlags(mem) & UPDATABLE_BIT_MASK) != 0;
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

  static void setMemoryPreInts(final WritableMemory wmem, final int value) {
    wmem.putByte(PREAMBLE_INTS_BYTE_ADR, (byte) value);
  }

  static void setMemorySerVer(final WritableMemory wmem, final int value) {
    wmem.putByte(SER_VER_BYTE_ADR, (byte) value);
  }

  static void setMemoryFamilyID(final WritableMemory wmem, final int value) {
    wmem.putByte(FAMILY_BYTE_ADR, (byte) value);
  }

  static void setMemoryFlags(final WritableMemory wmem, final int value) {
    wmem.putByte(FLAGS_BYTE_ADR, (byte) value);
  }

  static void setMemoryEmptyFlag(final WritableMemory wmem,  final boolean empty) {
    final int flags = getMemoryFlags(wmem);
    setMemoryFlags(wmem, empty ? flags | EMPTY_BIT_MASK : flags & ~EMPTY_BIT_MASK);
  }

  static void setMemoryLevelZeroSortedFlag(final WritableMemory wmem,  final boolean levelZeroSorted) {
    final int flags = getMemoryFlags(wmem);
    setMemoryFlags(wmem, levelZeroSorted ? flags | LEVEL_ZERO_SORTED_BIT_MASK : flags & ~LEVEL_ZERO_SORTED_BIT_MASK);
  }

  static void setMemorySingleItemFlag(final WritableMemory wmem,  final boolean singleItem) {
    final int flags = getMemoryFlags(wmem);
    setMemoryFlags(wmem, singleItem ? flags | SINGLE_ITEM_BIT_MASK : flags & ~SINGLE_ITEM_BIT_MASK);
  }

  static void setMemoryDoubleSketchFlag(final WritableMemory wmem,  final boolean doubleSketch) {
    final int flags = getMemoryFlags(wmem);
    setMemoryFlags(wmem, doubleSketch ? flags | DOUBLES_SKETCH_BIT_MASK : flags & ~DOUBLES_SKETCH_BIT_MASK);
  }

  static void setMemoryUpdatableFlag(final WritableMemory wmem,  final boolean updatable) {
    final int flags = getMemoryFlags(wmem);
    setMemoryFlags(wmem, updatable ? flags | UPDATABLE_BIT_MASK : flags & ~UPDATABLE_BIT_MASK);
  }

  static void setMemoryK(final WritableMemory wmem, final int value) {
    wmem.putShort(K_SHORT_ADR, (short) value);
  }

  static void setMemoryM(final WritableMemory wmem, final int value) {
    wmem.putByte(M_BYTE_ADR, (byte) value);
  }

  static void setMemoryN(final WritableMemory wmem, final long value) {
    wmem.putLong(N_LONG_ADR, value);
  }

  static void setMemoryMinK(final WritableMemory wmem, final int value) {
    wmem.putShort(MIN_K_SHORT_ADR, (short) value);
  }

  static void setMemoryNumLevels(final WritableMemory wmem, final int value) {
    wmem.putByte(NUM_LEVELS_BYTE_ADR, (byte) value);
  }

}
