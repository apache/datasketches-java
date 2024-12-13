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

package org.apache.datasketches.theta;

import static org.apache.datasketches.common.Util.LS;
import static org.apache.datasketches.common.Util.zeroPad;

import java.nio.ByteOrder;

import org.apache.datasketches.common.Family;
import org.apache.datasketches.common.ResizeFactor;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.Util;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.datasketches.thetacommon.ThetaUtil;

//@formatter:off

/**
 * This class defines the preamble data structure and provides basic utilities for some of the key
 * fields.
 *
 * <p>The intent of the design of this class was to isolate the detailed knowledge of the bit and
 * byte layout of the serialized form of the sketches derived from the Sketch class into one place.
 * This allows the possibility of the introduction of different serialization
 * schemes with minimal impact on the rest of the library.</p>
 *
 * <p>
 * MAP: Low significance bytes of this <i>long</i> data structure are on the right. However, the
 * multi-byte integers (<i>int</i> and <i>long</i>) are stored in native byte order. The
 * <i>byte</i> values are treated as unsigned.</p>
 *
 * <p>An empty CompactSketch only requires 8 bytes.
 * Flags: notSI, Ordered*, Compact, Empty*, ReadOnly, LE.
 * (*) Earlier versions did not set these.</p>
 *
 * <pre>
 * Long || Start Byte Adr:
 * Adr:
 *      ||    7   |    6   |    5   |    4   |    3   |    2   |    1   |     0              |
 *  0   ||    Seed Hash    | Flags  |        |        | FamID  | SerVer |     PreLongs = 1   |
 * </pre>
 *
 * <p>A SingleItemSketch (extends CompactSketch) requires an 8 byte preamble plus a single
 * hash item of 8 bytes. Flags: SingleItem*, Ordered, Compact, notEmpty, ReadOnly, LE.
 * (*) Earlier versions did not set these.</p>
 *
 * <pre>
 * Long || Start Byte Adr:
 * Adr:
 *      ||    7   |    6   |    5   |    4   |    3   |    2   |    1   |     0              |
 *  0   ||    Seed Hash    | Flags  |        |        | FamID  | SerVer |     PreLongs = 1   |
 *
 *      ||   15   |   14   |   13   |   12   |   11   |   10   |    9   |     8              |
 *  1   ||---------------------------Single long hash----------------------------------------|
 * </pre>
 *
 * <p>An exact (non-estimating) CompactSketch requires 16 bytes of preamble plus a compact array of
 * longs.</p>
 *
 * <pre>
 * Long || Start Byte Adr:
 * Adr:
 *      ||    7   |    6   |    5   |    4   |    3   |    2   |    1   |     0              |
 *  0   ||    Seed Hash    | Flags  |        |        | FamID  | SerVer |     PreLongs = 2   |
 *
 *      ||   15   |   14   |   13   |   12   |   11   |   10   |    9   |     8              |
 *  1   ||-----------------p-----------------|----------Retained Entries Count---------------|
 *
 *      ||   23   |   22   |   21    |  20   |   19   |   18   |   17   |    16              |
 *  2   ||----------------------Start of Compact Long Array----------------------------------|
 * </pre>
 *
 * <p>An estimating CompactSketch requires 24 bytes of preamble plus a compact array of longs.</p>
 *
 * <pre>
 * Long || Start Byte Adr:
 * Adr:
 *      ||    7   |    6   |    5   |    4   |    3   |    2   |    1   |     0              |
 *  0   ||    Seed Hash    | Flags  |        |        | FamID  | SerVer |     PreLongs = 3   |
 *
 *      ||   15   |   14   |   13   |   12   |   11   |   10   |    9   |     8              |
 *  1   ||-----------------p-----------------|----------Retained Entries Count---------------|
 *
 *      ||   23   |   22   |   21    |  20   |   19   |   18   |   17   |    16              |
 *  2   ||------------------------------THETA_LONG-------------------------------------------|
 *
 *      ||   31   |   30   |   29   |   28   |   27   |   26   |   25   |    24              |
 *  3   ||----------------------Start of Compact Long Array----------------------------------|
 *  </pre>
 *
 * <p>The UpdateSketch and AlphaSketch require 24 bytes of preamble followed by a non-compact
 * array of longs representing a hash table.</p>
 *
 * <p>The following table applies to both the Theta UpdateSketch and the Alpha Sketch</p>
 * <pre>
 * Long || Start Byte Adr:
 * Adr:
 *      ||    7   |    6   |    5   |    4   |    3   |    2   |    1   |     0              |
 *  0   ||    Seed Hash    | Flags  |  LgArr |  lgNom | FamID  | SerVer | RF, PreLongs = 3   |
 *
 *      ||   15   |   14   |   13   |   12   |   11   |   10   |    9   |     8              |
 *  1   ||-----------------p-----------------|----------Retained Entries Count---------------|
 *
 *      ||   23   |   22   |   21    |  20   |   19   |   18   |   17   |    16              |
 *  2   ||------------------------------THETA_LONG-------------------------------------------|
 *
 *      ||   31   |   30   |   29   |   28   |   27   |   26   |   25   |    24              |
 *  3   ||----------------------Start of Hash Table of longs---------------------------------|
 *  </pre>
 *
 * <p>Union objects require 32 bytes of preamble plus a non-compact array of longs representing a
 * hash table.</p>
 *
 * <pre>
 * Long || Start Byte Adr:
 * Adr:
 *      ||    7   |    6   |    5   |    4   |    3   |    2   |    1   |     0              |
 *  0   ||    Seed Hash    | Flags  |  LgArr |  lgNom | FamID  | SerVer | RF, PreLongs = 4   |
 *
 *      ||   15   |   14   |   13   |   12   |   11   |   10   |    9   |     8              |
 *  1   ||-----------------p-----------------|----------Retained Entries Count---------------|
 *
 *      ||   23   |   22   |   21    |  20   |   19   |   18   |   17   |    16              |
 *  2   ||------------------------------THETA_LONG-------------------------------------------|
 *
 *      ||   31   |   30   |   29   |   28   |   27   |   26   |   25   |    24              |
 *  3   ||---------------------------UNION THETA LONG----------------------------------------|
 *
 *      ||   39   |   38   |   37   |   36   |   35   |   34   |   33   |    32              |
 *  4   ||----------------------Start of Hash Table of longs---------------------------------|
 *
 *  </pre>
 *
 *  @author Lee Rhodes
 */
final class PreambleUtil {

  private PreambleUtil() {}

  // ###### DO NOT MESS WITH THIS FROM HERE ...
  // Preamble byte Addresses
  static final int PREAMBLE_LONGS_BYTE        = 0; //lower 6 bits in byte.
  static final int LG_RESIZE_FACTOR_BIT       = 6; //upper 2 bits in byte. Not used by compact, direct
  static final int SER_VER_BYTE               = 1;
  static final int FAMILY_BYTE                = 2; //SerVer1,2 was SKETCH_TYPE_BYTE
  static final int LG_NOM_LONGS_BYTE          = 3; //not used by compact
  static final int LG_ARR_LONGS_BYTE          = 4; //not used by compact
  static final int FLAGS_BYTE                 = 5;
  static final int SEED_HASH_SHORT            = 6;  //byte 6,7
  static final int RETAINED_ENTRIES_INT       = 8;  //8 byte aligned
  static final int P_FLOAT                    = 12; //4 byte aligned, not used by compact
  static final int THETA_LONG                 = 16; //8-byte aligned
  static final int UNION_THETA_LONG           = 24; //8-byte aligned, only used by Union

  // flag bit masks
  static final int BIG_ENDIAN_FLAG_MASK = 1; //SerVer 1, 2, 3
  static final int READ_ONLY_FLAG_MASK  = 2; //Set but not read. Reserved. SerVer 1, 2, 3
  static final int EMPTY_FLAG_MASK      = 4; //SerVer 2, 3
  static final int COMPACT_FLAG_MASK    = 8; //SerVer 2 was NO_REBUILD_FLAG_MASK, 3
  static final int ORDERED_FLAG_MASK    = 16;//SerVer 2 was UNORDERED_FLAG_MASK, 3
  static final int SINGLEITEM_FLAG_MASK = 32;//SerVer 3
  //The last 2 bits of the flags byte are reserved and assumed to be zero, for now.

  //Backward compatibility: SerVer1 preamble always 3 longs, SerVer2 preamble: 1, 2, 3 longs
  //               SKETCH_TYPE_BYTE             2  //SerVer1, SerVer2
  //  V1, V2 types:  Alpha = 1, QuickSelect = 2, SetSketch = 3; V3 only: Buffered QS = 4
  static final int LG_RESIZE_RATIO_BYTE_V1    = 5; //used by SerVer 1
  static final int FLAGS_BYTE_V1              = 6; //used by SerVer 1

  //Other constants
  static final int SER_VER                    = 3;

  // serial version 4 compressed ordered sketch, not empty, not single item
  static final int ENTRY_BITS_BYTE_V4   = 3; // number of bits packed in deltas between hashes
  static final int NUM_ENTRIES_BYTES_BYTE_V4 = 4; // number of bytes used for the number of entries
  static final int THETA_LONG_V4             = 8; //8-byte aligned

  static final boolean NATIVE_ORDER_IS_BIG_ENDIAN  =
      (ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN);

  /**
   * Computes the number of bytes required for a non-full sized sketch in hash-table form.
   * This can be used to compute current storage size for heap sketches, or current off-heap memory
   * required for off-heap (direct) sketches. This does not apply for compact sketches.
   * @param lgArrLongs log2(current hash-table size)
   * @param preambleLongs current preamble size
   * @return the size in bytes
   */
  static final int getMemBytes(final int lgArrLongs, final int preambleLongs) {
    return (8 << lgArrLongs) + (preambleLongs << 3);
  }

  // STRINGS

  /**
   * Returns a human readable string summary of the preamble state of the given byte array.
   * Used primarily in testing.
   *
   * @param byteArr the given byte array.
   * @return the summary preamble string.
   */
  static String preambleToString(final byte[] byteArr) {
    final Memory mem = Memory.wrap(byteArr);
    return preambleToString(mem);
  }

  /**
   * Returns a human readable string summary of the preamble state of the given Memory.
   * Note: other than making sure that the given Memory size is large
   * enough for just the preamble, this does not do much value checking of the contents of the
   * preamble as this is primarily a tool for debugging the preamble visually.
   *
   * @param mem the given Memory.
   * @return the summary preamble string.
   */
  static String preambleToString(final Memory mem) {
    final int preLongs = getAndCheckPreLongs(mem);
    final int rfId = extractLgResizeFactor(mem);
    final ResizeFactor rf = ResizeFactor.getRF(rfId);
    final int serVer = extractSerVer(mem);
    final int familyId = extractFamilyID(mem);
    final Family family = Family.idToFamily(familyId);
    final int lgNomLongs = extractLgNomLongs(mem);
    final int lgArrLongs = extractLgArrLongs(mem);

    //Flags
    final int flags = extractFlags(mem);
    final String flagsStr = (flags) + ", 0x" + (Integer.toHexString(flags)) + ", "
        + zeroPad(Integer.toBinaryString(flags), 8);
    final String nativeOrder = ByteOrder.nativeOrder().toString();
    final boolean bigEndian = (flags & BIG_ENDIAN_FLAG_MASK) > 0;
    final boolean readOnly = (flags & READ_ONLY_FLAG_MASK) > 0;
    final boolean empty = (flags & EMPTY_FLAG_MASK) > 0;
    final boolean compact = (flags & COMPACT_FLAG_MASK) > 0;
    final boolean ordered = (flags & ORDERED_FLAG_MASK) > 0;
    final boolean singleItem = (flags & SINGLEITEM_FLAG_MASK) > 0; //!empty && (preLongs == 1);

    final int seedHash = extractSeedHash(mem);

    //assumes preLongs == 1; empty or singleItem
    int curCount = singleItem ? 1 : 0;
    float p = (float) 1.0;            //preLongs 1 or 2
    long thetaLong = Long.MAX_VALUE;  //preLongs 1 or 2
    long thetaULong = thetaLong;      //preLongs 1, 2 or 3

    if (preLongs == 2) { //exact (non-estimating) CompactSketch
      curCount = extractCurCount(mem);
      p = extractP(mem);
    }
    else if (preLongs == 3) { //Update Sketch
      curCount = extractCurCount(mem);
      p = extractP(mem);
      thetaLong = extractThetaLong(mem);
      thetaULong = thetaLong;
    }
    else if (preLongs == 4) { //Union
      curCount = extractCurCount(mem);
      p = extractP(mem);
      thetaLong = extractThetaLong(mem);
      thetaULong = extractUnionThetaLong(mem);
    }
    //else the same as an empty sketch or singleItem

    final double thetaDbl = thetaLong / Util.LONG_MAX_VALUE_AS_DOUBLE;
    final String thetaHex = zeroPad(Long.toHexString(thetaLong), 16);
    final double thetaUDbl = thetaULong / Util.LONG_MAX_VALUE_AS_DOUBLE;
    final String thetaUHex = zeroPad(Long.toHexString(thetaULong), 16);

    final StringBuilder sb = new StringBuilder();
    sb.append(LS);
    sb.append("### SKETCH PREAMBLE SUMMARY:").append(LS);
    sb.append("Native Byte Order             : ").append(nativeOrder).append(LS);
    sb.append("Byte  0: Preamble Longs       : ").append(preLongs).append(LS);
    sb.append("Byte  0: ResizeFactor         : ").append(rfId + ", " + rf.toString()).append(LS);
    sb.append("Byte  1: Serialization Version: ").append(serVer).append(LS);
    sb.append("Byte  2: Family               : ").append(familyId + ", " + family.toString()).append(LS);
    sb.append("Byte  3: LgNomLongs           : ").append(lgNomLongs).append(LS);
    sb.append("Byte  4: LgArrLongs           : ").append(lgArrLongs).append(LS);
    sb.append("Byte  5: Flags Field          : ").append(flagsStr).append(LS);
    sb.append("  Bit Flag Name               : State:").append(LS);
    sb.append("    0 BIG_ENDIAN_STORAGE      : ").append(bigEndian).append(LS);
    sb.append("    1 READ_ONLY               : ").append(readOnly).append(LS);
    sb.append("    2 EMPTY                   : ").append(empty).append(LS);
    sb.append("    3 COMPACT                 : ").append(compact).append(LS);
    sb.append("    4 ORDERED                 : ").append(ordered).append(LS);
    sb.append("    5 SINGLE_ITEM             : ").append(singleItem).append(LS);
    sb.append("Bytes 6-7  : Seed Hash Hex    : ").append(Integer.toHexString(seedHash)).append(LS);
    if (preLongs == 1) {
      sb.append(" --ABSENT FIELDS, ASSUMED:").append(LS);
      sb.append("Bytes 8-11 : CurrentCount     : ").append(curCount).append(LS);
      sb.append("Bytes 12-15: P                : ").append(p).append(LS);
      sb.append("Bytes 16-23: Theta (double)   : ").append(thetaDbl).append(LS);
      sb.append("             Theta (long)     : ").append(thetaLong).append(LS);
      sb.append("             Theta (long,hex) : ").append(thetaHex).append(LS);
    }
    else if (preLongs == 2) {
      sb.append("Bytes 8-11 : CurrentCount     : ").append(curCount).append(LS);
      sb.append("Bytes 12-15: P                : ").append(p).append(LS);
      sb.append(" --ABSENT, ASSUMED:").append(LS);
      sb.append("Bytes 16-23: Theta (double)   : ").append(thetaDbl).append(LS);
      sb.append("             Theta (long)     : ").append(thetaLong).append(LS);
      sb.append("             Theta (long,hex) : ").append(thetaHex).append(LS);
    }
    else if (preLongs == 3) {
      sb.append("Bytes 8-11 : CurrentCount     : ").append(curCount).append(LS);
      sb.append("Bytes 12-15: P                : ").append(p).append(LS);
      sb.append("Bytes 16-23: Theta (double)   : ").append(thetaDbl).append(LS);
      sb.append("             Theta (long)     : ").append(thetaLong).append(LS);
      sb.append("             Theta (long,hex) : ").append(thetaHex).append(LS);
    }
    else { //preLongs == 4
      sb.append("Bytes 8-11 : CurrentCount     : ").append(curCount).append(LS);
      sb.append("Bytes 12-15: P                : ").append(p).append(LS);
      sb.append("Bytes 16-23: Theta (double)   : ").append(thetaDbl).append(LS);
      sb.append("             Theta (long)     : ").append(thetaLong).append(LS);
      sb.append("             Theta (long,hex) : ").append(thetaHex).append(LS);
      sb.append("Bytes 25-31: ThetaU (double)  : ").append(thetaUDbl).append(LS);
      sb.append("             ThetaU (long)    : ").append(thetaULong).append(LS);
      sb.append("             ThetaU (long,hex): ").append(thetaUHex).append(LS);
    }
    sb.append(  "Preamble Bytes                : ").append(preLongs * 8).append(LS);
    sb.append(  "Data Bytes                    : ").append(curCount * 8).append(LS);
    sb.append(  "TOTAL Sketch Bytes            : ").append((preLongs + curCount) * 8).append(LS);
    sb.append(  "TOTAL Capacity Bytes          : ").append(mem.getCapacity()).append(LS);
    sb.append("### END SKETCH PREAMBLE SUMMARY").append(LS);
    return sb.toString();
  }

  //@formatter:on

  static int extractPreLongs(final Memory mem) {
    return mem.getByte(PREAMBLE_LONGS_BYTE) & 0X3F;
  }

  static int extractLgResizeFactor(final Memory mem) {
    return (mem.getByte(PREAMBLE_LONGS_BYTE) >>> LG_RESIZE_FACTOR_BIT) & 0X3;
  }

  static int extractLgResizeRatioV1(final Memory mem) {
    return mem.getByte(LG_RESIZE_RATIO_BYTE_V1) & 0X3;
  }

  static int extractSerVer(final Memory mem) {
    return mem.getByte(SER_VER_BYTE) & 0XFF;
  }

  static int extractFamilyID(final Memory mem) {
    return mem.getByte(FAMILY_BYTE) & 0XFF;
  }

  static int extractLgNomLongs(final Memory mem) {
    return mem.getByte(LG_NOM_LONGS_BYTE) & 0XFF;
  }

  static int extractLgArrLongs(final Memory mem) {
    return mem.getByte(LG_ARR_LONGS_BYTE) & 0XFF;
  }

  static int extractFlags(final Memory mem) {
    return mem.getByte(FLAGS_BYTE) & 0XFF;
  }

  static int extractFlagsV1(final Memory mem) {
    return mem.getByte(FLAGS_BYTE_V1) & 0XFF;
  }

  static int extractSeedHash(final Memory mem) {
    return mem.getShort(SEED_HASH_SHORT) & 0XFFFF;
  }

  static int extractCurCount(final Memory mem) {
    return mem.getInt(RETAINED_ENTRIES_INT);
  }

  static float extractP(final Memory mem) {
    return mem.getFloat(P_FLOAT);
  }

  static long extractThetaLong(final Memory mem) {
    return mem.getLong(THETA_LONG);
  }

  static long extractUnionThetaLong(final Memory mem) {
    return mem.getLong(UNION_THETA_LONG);
  }

  static int extractEntryBitsV4(final Memory mem) {
    return mem.getByte(ENTRY_BITS_BYTE_V4) & 0XFF;
  }

  static int extractNumEntriesBytesV4(final Memory mem) {
    return mem.getByte(NUM_ENTRIES_BYTES_BYTE_V4) & 0XFF;
  }

  static long extractThetaLongV4(final Memory mem) {
    return mem.getLong(THETA_LONG_V4);
  }

  /**
   * Sets PreLongs in the low 6 bits and sets LgRF in the upper 2 bits = 0.
   * @param wmem the target WritableMemory
   * @param preLongs the given number of preamble longs
   */
  static void insertPreLongs(final WritableMemory wmem, final int preLongs) {
    wmem.putByte(PREAMBLE_LONGS_BYTE, (byte) (preLongs & 0X3F));
  }

  /**
   * Sets the top 2 lgRF bits and does not affect the lower 6 bits (PreLongs).
   * To work properly, this should be called after insertPreLongs().
   * @param wmem the target WritableMemory
   * @param rf the given lgRF bits
   */
  static void insertLgResizeFactor(final WritableMemory wmem, final int rf) {
    final int curByte = wmem.getByte(PREAMBLE_LONGS_BYTE) & 0xFF;
    final int shift = LG_RESIZE_FACTOR_BIT; // shift in bits
    final int mask = 3;
    final byte newByte = (byte) (((rf & mask) << shift) | (~(mask << shift) & curByte));
    wmem.putByte(PREAMBLE_LONGS_BYTE, newByte);
  }

  static void insertSerVer(final WritableMemory wmem, final int serVer) {
    wmem.putByte(SER_VER_BYTE, (byte) serVer);
  }

  static void insertFamilyID(final WritableMemory wmem, final int famId) {
    wmem.putByte(FAMILY_BYTE, (byte) famId);
  }

  static void insertLgNomLongs(final WritableMemory wmem, final int lgNomLongs) {
    wmem.putByte(LG_NOM_LONGS_BYTE, (byte) lgNomLongs);
  }

  static void insertLgArrLongs(final WritableMemory wmem, final int lgArrLongs) {
    wmem.putByte(LG_ARR_LONGS_BYTE, (byte) lgArrLongs);
  }

  static void insertFlags(final WritableMemory wmem, final int flags) {
    wmem.putByte(FLAGS_BYTE, (byte) flags);
  }

  static void insertSeedHash(final WritableMemory wmem, final int seedHash) {
    wmem.putShort(SEED_HASH_SHORT, (short) seedHash);
  }

  static void insertCurCount(final WritableMemory wmem, final int curCount) {
    wmem.putInt(RETAINED_ENTRIES_INT, curCount);
  }

  static void insertP(final WritableMemory wmem, final float p) {
    wmem.putFloat(P_FLOAT, p);
  }

  static void insertThetaLong(final WritableMemory wmem, final long thetaLong) {
    wmem.putLong(THETA_LONG, thetaLong);
  }

  static void insertUnionThetaLong(final WritableMemory wmem, final long unionThetaLong) {
    wmem.putLong(UNION_THETA_LONG, unionThetaLong);
  }

  static void setEmpty(final WritableMemory wmem) {
    int flags = wmem.getByte(FLAGS_BYTE) & 0XFF;
    flags |= EMPTY_FLAG_MASK;
    wmem.putByte(FLAGS_BYTE, (byte) flags);
  }

  static void clearEmpty(final WritableMemory wmem) {
    int flags = wmem.getByte(FLAGS_BYTE) & 0XFF;
    flags &= ~EMPTY_FLAG_MASK;
    wmem.putByte(FLAGS_BYTE, (byte) flags);
  }

  static boolean isEmptyFlag(final Memory mem) {
    return ((extractFlags(mem) & EMPTY_FLAG_MASK) > 0);
  }

  /**
   * Checks Memory for capacity to hold the preamble and returns the extracted preLongs.
   * @param mem the given Memory
   * @return the extracted prelongs value.
   */
  static int getAndCheckPreLongs(final Memory mem) {
    final long cap = mem.getCapacity();
    if (cap < 8) {
      throwNotBigEnough(cap, 8);
    }
    final int preLongs = extractPreLongs(mem);
    final int required = Math.max(preLongs << 3, 8);
    if (cap < required) {
      throwNotBigEnough(cap, required);
    }
    return preLongs;
  }

  static final short checkMemorySeedHash(final Memory mem, final long seed) {
    final short seedHashMem = (short) extractSeedHash(mem);
    ThetaUtil.checkSeedHashes(seedHashMem, ThetaUtil.computeSeedHash(seed)); //throws if bad seedHash
    return seedHashMem;
  }

  private static void throwNotBigEnough(final long cap, final int required) {
    throw new SketchesArgumentException(
        "Possible Corruption: Size of byte array or Memory not large enough: Size: " + cap
        + ", Required: " + required);
  }

}
