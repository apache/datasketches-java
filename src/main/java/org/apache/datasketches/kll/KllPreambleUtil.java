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
 *  2   ||<-------Levels Arr Start----------]| unused |NumLevels|--Dynamic-Min K--------|
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
 *  2   ||<-------Levels Arr Start----------]| unused |NumLevels|--Dynamic-Min K--------|
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

  /**
   * The default value of K
   */
  public static final int DEFAULT_K = 200;
  public static final int DEFAULT_M = 8;
  public static final int MAX_K = (1 << 16) - 1; // serialized as an unsigned short
  public static final int MAX_M = 8;
  public static final int MIN_M = 2;

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
  static final int DY_MIN_K_SHORT_ADR         = 16; // to 17
  static final int NUM_LEVELS_BYTE_ADR        = 18;

  // FLOAT SKETCH                               19 is reserved for future use in float sketch
  static final int DATA_START_ADR_FLOAT       = 20; // float sketch, not single item

  // DOUBLE SKETCH                              19 to 23 is reserved for future use in double sketch
  static final int DATA_START_ADR_DOUBLE      = 20; // double sketch, not single item

  // Other static values
  static final byte SERIAL_VERSION_EMPTY_FULL = 1; // Empty or full preamble, NOT single item format
  static final byte SERIAL_VERSION_SINGLE     = 2; // only single-item format
  static final byte SERIAL_VERSION_UPDATABLE  = 3; //
  static final int PREAMBLE_INTS_EMPTY_SINGLE = 2; // for empty or single item
  static final int PREAMBLE_INTS_FLOAT        = 5; // not empty nor single item, full preamble float
  static final int PREAMBLE_INTS_DOUBLE       = 5; // not empty nor single item, full preamble double

  // Flag bit masks
  static final int EMPTY_BIT_MASK             = 1;
  static final int LEVEL_ZERO_SORTED_BIT_MASK = 2;
  static final int SINGLE_ITEM_BIT_MASK       = 4;
  static final int DOUBLES_SKETCH_BIT_MASK    = 8;
  static final int UPDATABLE_BIT_MASK         = 16;

  enum Layout {
    FLOAT_FULL_COMPACT,       FLOAT_EMPTY_COMPACT,      FLOAT_SINGLE_COMPACT,
    DOUBLE_FULL_COMPACT,      DOUBLE_EMPTY_COMPACT,     DOUBLE_SINGLE_COMPACT,
    FLOAT_UPDATABLE,  DOUBLE_UPDATABLE }

  /**
   * Returns a human readable string summary of the internal state of the given byte array.
   * Used primarily in testing.
   *
   * @param byteArr the given byte array.
   * @return the summary string.
   */
  static String toString(final byte[] byteArr) {
    final Memory mem = Memory.wrap(byteArr);
    return toString(mem);
  }

  /**
   * Returns a human readable string summary of the internal state of the given Memory.
   * Used primarily in testing.
   *
   * @param mem the given Memory
   * @return the summary string.
   */
  static String toString(final Memory mem) {
    return memoryToString(mem);
  }

  static String memoryToString(final Memory mem) {
    final KllMemoryValidate memChk = new KllMemoryValidate(mem);
    final int flags = memChk.flags & 0XFF;
    final String flagsStr = (flags) + ", 0x" + (Integer.toHexString(flags)) + ", "
        + zeroPad(Integer.toBinaryString(flags), 8);
    final int preInts = memChk.preInts;
    final StringBuilder sb = new StringBuilder();
    sb.append(Util.LS).append("### KLL SKETCH MEMORY SUMMARY:").append(LS);
    sb.append("Byte   0   : Preamble Ints      : ").append(preInts).append(LS);
    sb.append("Byte   1   : SerVer             : ").append(memChk.serVer).append(LS);
    sb.append("Byte   2   : FamilyID           : ").append(memChk.familyID).append(LS);
    sb.append("             FamilyName         : ").append(memChk.famName).append(LS);
    sb.append("Byte   3   : Flags Field        : ").append(flagsStr).append(LS);
    sb.append("         Bit Flag Name").append(LS);
    sb.append("           0 EMPTY COMPACT      : ").append(memChk.empty).append(LS);
    sb.append("           1 LEVEL_ZERO_SORTED  : ").append(memChk.level0Sorted).append(LS);
    sb.append("           2 SINGLE_ITEM COMPACT: ").append(memChk.singleItem).append(LS);
    sb.append("           3 DOUBLES_SKETCH     : ").append(memChk.doublesSketch).append(LS);
    sb.append("           4 UPDATABLE          : ").append(memChk.updatable).append(LS);
    sb.append("Bytes  4-5 : K                  : ").append(memChk.k).append(LS);
    sb.append("Byte   6   : Min Level Cap, M   : ").append(memChk.m).append(LS);
    sb.append("Byte   7   : (Reserved)         : ").append(LS);

    switch (memChk.layout) {
      case DOUBLE_FULL_COMPACT:
      case FLOAT_FULL_COMPACT:
      case FLOAT_UPDATABLE:
      case DOUBLE_UPDATABLE:
      {
        sb.append("Bytes  8-15: N                  : ").append(memChk.n).append(LS);
        sb.append("Bytes 16-17: DyMinK             : ").append(memChk.dyMinK).append(LS);
        sb.append("Byte  18   : NumLevels          : ").append(memChk.numLevels).append(LS);
        break;
      }
      case FLOAT_EMPTY_COMPACT:
      case FLOAT_SINGLE_COMPACT:
      case DOUBLE_EMPTY_COMPACT:
      case DOUBLE_SINGLE_COMPACT:
      {
        sb.append("Assumed    : N                  : ").append(memChk.n).append(LS);
        sb.append("Assumed    : DyMinK             : ").append(memChk.dyMinK).append(LS);
        sb.append("Assumed    : NumLevels          : ").append(memChk.numLevels).append(LS);
        break;
      }
      default: break; //can never happen
    }
    sb.append("PreambleBytes                   : ").append(preInts * 4).append(LS);
    sb.append("Sketch Bytes                    : ").append(memChk.sketchBytes).append(LS);
    sb.append("Memory Capacity Bytes           : ").append(mem.getCapacity()).append(LS);
    sb.append("### END KLL Sketch Memory Summary").append(LS);
    return sb.toString();
  }

  static int extractPreInts(final Memory mem) {
    return mem.getByte(PREAMBLE_INTS_BYTE_ADR) & 0XFF;
  }

  static int extractSerVer(final Memory mem) {
    return mem.getByte(SER_VER_BYTE_ADR) & 0XFF;
  }

  static int extractFamilyID(final Memory mem) {
    return mem.getByte(FAMILY_BYTE_ADR) & 0XFF;
  }

  static int extractFlags(final Memory mem) {
    return mem.getByte(FLAGS_BYTE_ADR) & 0XFF;
  }

  static boolean extractEmptyFlag(final Memory mem) {
    return (extractFlags(mem) & EMPTY_BIT_MASK) != 0;
  }

  static boolean extractLevelZeroSortedFlag(final Memory mem) {
    return (extractFlags(mem) & LEVEL_ZERO_SORTED_BIT_MASK) != 0;
  }

  static boolean extractSingleItemFlag(final Memory mem) {
    return (extractFlags(mem) & SINGLE_ITEM_BIT_MASK) != 0;
  }

  static boolean extractDoubleSketchFlag(final Memory mem) {
    return (extractFlags(mem) & DOUBLES_SKETCH_BIT_MASK) != 0;
  }

  static boolean extractUpdatableFlag(final Memory mem) {
    return (extractFlags(mem) & UPDATABLE_BIT_MASK) != 0;
  }

  static int extractK(final Memory mem) {
    return mem.getShort(K_SHORT_ADR) & 0XFFFF;
  }

  static int extractM(final Memory mem) {
    return mem.getByte(M_BYTE_ADR) & 0XFF;
  }

  static long extractN(final Memory mem) {
    return mem.getLong(N_LONG_ADR);
  }

  static int extractDyMinK(final Memory mem) {
    return mem.getShort(DY_MIN_K_SHORT_ADR) & 0XFFFF;
  }

  static int extractNumLevels(final Memory mem) {
    return mem.getByte(NUM_LEVELS_BYTE_ADR) & 0XFF;
  }

  static void insertPreInts(final WritableMemory wmem, final int value) {
    wmem.putByte(PREAMBLE_INTS_BYTE_ADR, (byte) value);
  }

  static void insertSerVer(final WritableMemory wmem, final int value) {
    wmem.putByte(SER_VER_BYTE_ADR, (byte) value);
  }

  static void insertFamilyID(final WritableMemory wmem, final int value) {
    wmem.putByte(FAMILY_BYTE_ADR, (byte) value);
  }

  static void insertFlags(final WritableMemory wmem, final int value) {
    wmem.putByte(FLAGS_BYTE_ADR, (byte) value);
  }

  static void insertEmptyFlag(final WritableMemory wmem,  final boolean empty) {
    final int flags = extractFlags(wmem);
    insertFlags(wmem, empty ? flags | EMPTY_BIT_MASK : flags & ~EMPTY_BIT_MASK);
  }

  static void insertLevelZeroSortedFlag(final WritableMemory wmem,  final boolean levelZeroSorted) {
    final int flags = extractFlags(wmem);
    insertFlags(wmem, levelZeroSorted ? flags | LEVEL_ZERO_SORTED_BIT_MASK : flags & ~LEVEL_ZERO_SORTED_BIT_MASK);
  }

  static void insertSingleItemFlag(final WritableMemory wmem,  final boolean singleItem) {
    final int flags = extractFlags(wmem);
    insertFlags(wmem, singleItem ? flags | SINGLE_ITEM_BIT_MASK : flags & ~SINGLE_ITEM_BIT_MASK);
  }

  static void insertDoubleSketchFlag(final WritableMemory wmem,  final boolean doubleSketch) {
    final int flags = extractFlags(wmem);
    insertFlags(wmem, doubleSketch ? flags | DOUBLES_SKETCH_BIT_MASK : flags & ~DOUBLES_SKETCH_BIT_MASK);
  }

  static void insertUpdatableFlag(final WritableMemory wmem,  final boolean updatable) {
    final int flags = extractFlags(wmem);
    insertFlags(wmem, updatable ? flags | UPDATABLE_BIT_MASK : flags & ~UPDATABLE_BIT_MASK);
  }

  static void insertK(final WritableMemory wmem, final int value) {
    wmem.putShort(K_SHORT_ADR, (short) value);
  }

  static void insertM(final WritableMemory wmem, final int value) {
    wmem.putByte(M_BYTE_ADR, (byte) value);
  }

  static void insertN(final WritableMemory wmem, final long value) {
    wmem.putLong(N_LONG_ADR, value);
  }

  static void insertDyMinK(final WritableMemory wmem, final int value) {
    wmem.putShort(DY_MIN_K_SHORT_ADR, (short) value);
  }

  static void insertNumLevels(final WritableMemory wmem, final int value) {
    wmem.putByte(NUM_LEVELS_BYTE_ADR, (byte) value);
  }

}

