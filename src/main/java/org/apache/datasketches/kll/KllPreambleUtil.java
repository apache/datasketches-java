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

import static org.apache.datasketches.Family.idToFamily;

import org.apache.datasketches.Family;
import org.apache.datasketches.SketchesArgumentException;
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
 *      ||   23    |   22  |   21   |   20   |   19   |    18   |   17   |      16      |
 *  2   ||<--------------data----------------| unused |numLevels|-------min K-----------|
 *
 * Serialized float sketch layout, Empty (8 bytes) and Single Item (12 bytes):
 *  Adr:
 *      ||    7    |   6   |    5   |    4   |    3   |    2    |    1   |      0       |
 *  0   || unused  |   M   |--------K--------|  Flags |  FamID  | SerVer | PreambleInts |
 *      ||   15    |   14  |   13   |   12   |   11   |   10    |    9   |      8       |
 *  1   ||                                   |-------------------data-------------------|
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
 *  2   ||--------------unused------------------------|numLevels|-------min K-----------|
 *      ||                                                               |      24      |
 *  3   ||<---------------------------------data----------------------------------------|
 *
 * Serialized double sketch layout, Empty (8 bytes) and Single Item (16 bytes):
 *  Adr:
 *      ||    7    |   6   |    5   |    4   |    3   |    2    |    1   |      0       |
 *  0   || unused  |   M   |--------K--------|  Flags |  FamID  | SerVer | PreambleInts |
 *      ||                                                               |      8       |
 *  1   ||----------------------------------data----------------------------------------|
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
  static final int DEFAULT_M = 8;
  static final int MIN_K = DEFAULT_M;
  static final int MAX_K = (1 << 16) - 1; // serialized as an unsigned short

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
  static final int DATA_START_ADR_DOUBLE      = 24; // double sketch, not single item

  // Other static values
  static final byte SERIAL_VERSION_EMPTY_FULL = 1; // Empty or full preamble, NOT single item format
  static final byte SERIAL_VERSION_SINGLE     = 2; // only single-item format
  static final int PREAMBLE_INTS_EMPTY_SINGLE = 2; // for empty or single item
  static final int PREAMBLE_INTS_FLOAT        = 5; // not empty nor single item, full preamble float
  static final int PREAMBLE_INTS_DOUBLE       = 6; // not empty nor single item, full preamble double

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
    return null; //memoryToString(mem);
  }






  static class MemoryCheck {
    final int preInts; // = extractPreInts(srcMem);
    final int serVer;
    final int familyID;
    final String famName;
    final int flags;
    final boolean empty;
    final boolean level0Sorted;
    final boolean singleItem;
    final boolean doublesSketch;
    final boolean updatable;
    final int k;
    final int m;
    long n;
    int dyMinK;
    int dataStart;
    int numLevels;
    int[] levels;
    Layout layout;

    MemoryCheck(final Memory srcMem) {
      preInts = extractPreInts(srcMem);
      serVer = extractSerVer(srcMem);
      familyID = extractFamilyID(srcMem);
      flags = extractFlags(srcMem);
      empty = (flags & EMPTY_BIT_MASK) > 0;
      level0Sorted  = (flags & LEVEL_ZERO_SORTED_BIT_MASK) > 0;
      singleItem    = (flags & SINGLE_ITEM_BIT_MASK) > 0;
      doublesSketch = (flags & DOUBLES_SKETCH_BIT_MASK) > 0;
      updatable    = (flags & UPDATABLE_BIT_MASK) > 0;
      k = extractK(srcMem);
      m = extractM(srcMem);

      KllHelper.checkK(k);
      if (m != 8) { throwCustom(7, m); }
      if (familyID != Family.KLL.getID()) { throwCustom(0, familyID); }
      famName = idToFamily(familyID).toString();
      if (famName != "KLL") { throwCustom(23, 0); }

      final int checkFlags = (empty ? 1 : 0) | (singleItem ? 4 : 0) | (doublesSketch ? 8 : 0);
      if ((checkFlags & 5) == 5) { throwCustom(20, flags); }

      switch (checkFlags) {
        case 0: { //not empty, not single item, float full
          if (preInts != PREAMBLE_INTS_FLOAT) { throwCustom(6, preInts); }
          if (serVer != SERIAL_VERSION_EMPTY_FULL) { throwCustom(2, serVer); }
          layout = updatable ? Layout.FLOAT_UPDATABLE : Layout.FLOAT_FULL_COMPACT;
          n = extractN(srcMem);
          dyMinK = extractDyMinK(srcMem);
          numLevels = extractNumLevels(srcMem);
          dataStart = DATA_START_ADR_FLOAT;
          break;
        }
        case 1: { //empty, not single item, float empty
          if (preInts != PREAMBLE_INTS_EMPTY_SINGLE) { throwCustom(1, preInts); }
          if (serVer != SERIAL_VERSION_EMPTY_FULL) { throwCustom(2, serVer); }
          if (updatable) {
            layout = Layout.FLOAT_UPDATABLE;
            n = extractN(srcMem);
            if (n != 0) { throwCustom(21, (int) n); }
            dyMinK = extractDyMinK(srcMem);
            numLevels = extractNumLevels(srcMem);
            dataStart = DATA_START_ADR_FLOAT;
          } else {
            layout = Layout.FLOAT_EMPTY_COMPACT;
            n = 0;
            dyMinK = k;
            numLevels = 1;
            dataStart = DATA_START_ADR_SINGLE_ITEM; //ignore if empty
          }
          break;
        }
        case 4: { //not empty, single item, float single item
          if (preInts != PREAMBLE_INTS_EMPTY_SINGLE) { throwCustom(1, preInts); }
          if (serVer != SERIAL_VERSION_SINGLE) { throwCustom(4, serVer); }
          if (updatable) {
            layout = Layout.FLOAT_UPDATABLE;
            n = extractN(srcMem);
            if (n != 1) { throwCustom(22, (int)n); }
            dyMinK = extractDyMinK(srcMem);
            numLevels = extractNumLevels(srcMem);
            dataStart = DATA_START_ADR_FLOAT;
          } else {
            layout = Layout.FLOAT_SINGLE_COMPACT;
            n = 1;
            dyMinK = k;
            numLevels = 1;
            dataStart = DATA_START_ADR_SINGLE_ITEM;
          }
          break;
        }
        case 8: { //not empty, not single item, double full
          if (preInts != PREAMBLE_INTS_DOUBLE) { throwCustom(5, preInts); }
          if (serVer != SERIAL_VERSION_EMPTY_FULL) { throwCustom(2, serVer); }
          layout = updatable ? Layout.DOUBLE_UPDATABLE : Layout.DOUBLE_FULL_COMPACT;
          n = extractN(srcMem);
          dyMinK = extractDyMinK(srcMem);
          numLevels = extractNumLevels(srcMem);
          dataStart = DATA_START_ADR_DOUBLE;
          break;
        }
        case 9: { //empty, not single item, double empty
          if (preInts != PREAMBLE_INTS_EMPTY_SINGLE) { throwCustom(1, preInts); }
          if (serVer != SERIAL_VERSION_EMPTY_FULL) { throwCustom(2, serVer); }
          if (updatable) {
            layout = Layout.DOUBLE_UPDATABLE;
            n = extractN(srcMem);
            if (n != 0) { throwCustom(21, (int) n); }
            dyMinK = extractDyMinK(srcMem);
            numLevels = extractNumLevels(srcMem);
            dataStart = DATA_START_ADR_DOUBLE;
          } else {
            layout = Layout.DOUBLE_EMPTY_COMPACT;
            n = 0;
            dyMinK = k;
            numLevels = 1;
            dataStart = DATA_START_ADR_SINGLE_ITEM; //ignore if empty
          }
          break;
        }
        case 12: { //not empty, single item, double single item
          if (preInts != PREAMBLE_INTS_EMPTY_SINGLE) { throwCustom(1, preInts); }
          if (serVer != SERIAL_VERSION_SINGLE) { throwCustom(4, serVer); }
          if (updatable) {
            layout = Layout.DOUBLE_UPDATABLE;
            n = extractN(srcMem);
            if (n != 1) { throwCustom(22, (int)n); }
            dyMinK = extractDyMinK(srcMem);
            numLevels = extractNumLevels(srcMem);
            dataStart = DATA_START_ADR_DOUBLE;
          } else {
            layout = Layout.DOUBLE_SINGLE_COMPACT;
            n = 1;
            dyMinK = k;
            numLevels = 1;
            dataStart = DATA_START_ADR_SINGLE_ITEM;
          }
          break;
        }
      }
    }

    private static void throwCustom(final int errNo, final int value) {
      String msg = "";
      switch (errNo) {
        case 0: msg = "FamilyID Field must be: " + Family.KLL.getID() + ", NOT: " + value; break;
        case 1: msg = "Empty Bit: 1 -> PreInts: " + PREAMBLE_INTS_EMPTY_SINGLE + ", NOT: " + value; break;
        case 2: msg = "Empty Bit: 1 -> SerVer: " + SERIAL_VERSION_EMPTY_FULL + ", NOT: " + value; break;
        case 3: msg = "Single Item Bit: 1 -> PreInts: " + PREAMBLE_INTS_EMPTY_SINGLE + ", NOT: " + value; break;
        case 4: msg = "Single Item Bit: 1 -> SerVer: " + SERIAL_VERSION_SINGLE + ", NOT: " + value; break;
        case 5: msg = "Double Sketch Bit: 1 -> PreInts: " + PREAMBLE_INTS_DOUBLE + ", NOT: " + value; break;
        case 6: msg = "Double Sketch Bit: 0 -> PreInts: " + PREAMBLE_INTS_FLOAT + ", NOT: " + value; break;
        case 7: msg = "The M field must be set to " + DEFAULT_M + ", NOT: " + value; break;
        case 20: msg = "Empty flag bit and SingleItem flag bit cannot both be set. Flags: " + value; break;
        case 21: msg = "N != 0 and empty bit is set. N: " + value; break;
        case 22: msg = "N != 1 and single item bit is set. N: " + value; break;
        case 23: msg = "Family name is not KLL"; break;
      }
      throw new SketchesArgumentException(msg);
    }
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

  static void insertK(final WritableMemory wmem, final int value) {
    wmem.putShort(K_SHORT_ADR, (short) value);
  }

  static void insertM(final WritableMemory wmem, final int value) {
    wmem.putByte(M_BYTE_ADR, (byte) value);
  }

  static void insertN(final WritableMemory wmem, final long value) {
    wmem.putLong(N_LONG_ADR, value);
  }

  static void insertMinK(final WritableMemory wmem, final int value) {
    wmem.putShort(DY_MIN_K_SHORT_ADR, (short) value);
  }

  static void insertNumLevels(final WritableMemory wmem, final int value) {
    wmem.putByte(NUM_LEVELS_BYTE_ADR, (byte) value);
  }

}

