/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the Apache License 2.0. See LICENSE file
 * at the project root for terms.
 */

package com.yahoo.sketches.frequencies;

import static com.yahoo.sketches.Util.LS;
import static com.yahoo.sketches.Util.zeroPad;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.SketchesArgumentException;

// @formatter:off

/**
 * This class defines the preamble data structure and provides basic utilities for some of the key
 * fields.
 * <p>
 * The intent of the design of this class was to isolate the detailed knowledge of the bit and byte
 * layout of the serialized form of the sketches derived from the Sketch class into one place. This
 * allows the possibility of the introduction of different serialization schemes with minimal impact
 * on the rest of the library.
 * </p>
 *
 * <p>
 * MAP: Low significance bytes of this <i>long</i> data structure are on the right. However, the
 * multi-byte integers (<i>int</i> and <i>long</i>) are stored in native byte order. The <i>byte</i>
 * values are treated as unsigned.
 * </p>
 *
 * <p>
 * An empty FrequentItems only requires 8 bytes. All others require 32 bytes of preamble.
 * </p>
 *
 * <pre>
 *  * Long || Start Byte Adr:
 * Adr:
 *      ||    7     |    6   |    5   |    4   |    3   |    2   |    1   |     0          |
 *  0   ||-------SerDeId-----|-Flags--|-LgCur--| LgMax  | FamID  | SerVer | PreambleLongs  |
 *      ||    15    |   14   |   13   |   12   |   11   |   10   |    9   |     8          |
 *  1   ||------------(unused)-----------------|--------ActiveItems------------------------|
 *      ||    23    |   22   |   21   |   20   |   19   |   18   |   17   |    16          |
 *  2   ||-----------------------------------streamLength----------------------------------|
 *      ||    31    |   30   |   29   |   28   |   27   |   26   |   25   |    24          |
 *  3   ||---------------------------------offset------------------------------------------|
 *      ||    39    |   38   |   37   |   36   |   35   |   34   |   33   |    32          |
 *  5   ||----------start of values buffer, followed by keys buffer------------------------|
 * </pre>
 *
 * @author Lee Rhodes
 */
final class PreambleUtil {

  private PreambleUtil() {}

  // ###### DO NOT MESS WITH THIS FROM HERE ...
  // Preamble byte Addresses
  static final int PREAMBLE_LONGS_BYTE       = 0; // either 1 or 4
  static final int SER_VER_BYTE              = 1;
  static final int FAMILY_BYTE               = 2;
  static final int LG_MAX_MAP_SIZE_BYTE      = 3;
  static final int LG_CUR_MAP_SIZE_BYTE      = 4;
  static final int FLAGS_BYTE                = 5;
  static final int SER_DE_ID_SHORT           = 6;  // to 7
  static final int ACTIVE_ITEMS_INT          = 8;  // to 11 : 0 to 4 in pre1
  static final int STREAMLENGTH_LONG         = 16; // to 23 : pre2
  static final int OFFSET_LONG               = 24; // to 31 : pre3

  // flag bit masks
  static final int EMPTY_FLAG_MASK      = 4;

  // Specific values for this implementation
  static final int SER_VER = 1;

  /**
   * Returns a human readable string summary of the preamble state of the given Memory.
   * Note: other than making sure that the given Memory size is large
   * enough for just the preamble, this does not do much value checking of the contents of the
   * preamble as this is primarily a tool for debugging the preamble visually.
   *
   * @param srcMem the given Memory.
   * @return the summary preamble string.
   */
  public static String preambleToString(final Memory srcMem) {
    final long pre0 = checkPreambleSize(srcMem); //make sure we can get the assumed preamble
    final int preLongs = extractPreLongs(pre0);   //byte 0
    final int serVer = extractSerVer(pre0);       //byte 1
    final Family family = Family.idToFamily(extractFamilyID(pre0)); //byte 2
    final int lgMaxMapSize = extractLgMaxMapSize(pre0); //byte 3
    final int lgCurMapSize = extractLgCurMapSize(pre0); //byte 4
    final int flags = extractFlags(pre0);         //byte 5
    final int type = extractSerDeId(pre0);        //byte 6

    final String flagsStr = zeroPad(Integer.toBinaryString(flags), 8) + ", " + (flags);
    final boolean empty = (flags & EMPTY_FLAG_MASK) > 0;
    final int maxMapSize = 1 << lgMaxMapSize;
    final int curMapSize = 1 << lgCurMapSize;
    final int maxPreLongs = Family.FREQUENCY.getMaxPreLongs();

    //Assumed if preLongs == 1
    int activeItems = 0;
    long streamLength = 0;
    long offset = 0;

    //Assumed if preLongs == maxPreLongs

    if (preLongs == maxPreLongs) {
      //get full preamble
      final long[] preArr = new long[preLongs];
      srcMem.getLongArray(0, preArr, 0, preLongs);
      activeItems =  extractActiveItems(preArr[1]);
      streamLength = preArr[2];
      offset = preArr[3];
    }

    final StringBuilder sb = new StringBuilder();
    sb.append(LS)
      .append("### FREQUENCY SKETCH PREAMBLE SUMMARY:").append(LS)
      .append("Byte  0: Preamble Longs       : ").append(preLongs).append(LS)
      .append("Byte  1: Serialization Version: ").append(serVer).append(LS)
      .append("Byte  2: Family               : ").append(family.toString()).append(LS)
      .append("Byte  3: MaxMapSize           : ").append(maxMapSize).append(LS)
      .append("Byte  4: CurMapSize           : ").append(curMapSize).append(LS)
      .append("Byte  5: Flags Field          : ").append(flagsStr).append(LS)
      .append("  EMPTY                       : ").append(empty).append(LS)
      .append("Byte  6: Freq Sketch Type     : ").append(type).append(LS);

    if (preLongs == 1) {
      sb.append(" --ABSENT, ASSUMED:").append(LS);
    } else { //preLongs == maxPreLongs
      sb.append("Bytes 8-11 : ActiveItems      : ").append(activeItems).append(LS);
      sb.append("Bytes 16-23: StreamLength     : ").append(streamLength).append(LS)
        .append("Bytes 24-31: Offset           : ").append(offset).append(LS);
    }

    sb.append(  "Preamble Bytes                : ").append(preLongs * 8).append(LS);
    sb.append(  "TOTAL Sketch Bytes            : ").append((preLongs + (activeItems * 2)) << 3)
      .append(LS)
      .append("### END FREQUENCY SKETCH PREAMBLE SUMMARY").append(LS);
    return sb.toString();
  }

  // @formatter:on

  static int extractPreLongs(final long pre0) { //Byte 0
    final long mask = 0X3FL; //Lower 6 bits
    return (int) (pre0 & mask);
  }

  static int extractSerVer(final long pre0) { //Byte 1
    final int shift = SER_VER_BYTE << 3;
    final long mask = 0XFFL;
    return (int) ((pre0 >>> shift) & mask);
  }

  static int extractFamilyID(final long pre0) { //Byte 2
    final int shift = FAMILY_BYTE << 3;
    final long mask = 0XFFL;
    return (int) ((pre0 >>> shift) & mask);
  }

  static int extractLgMaxMapSize(final long pre0) { //Byte 3
    final int shift = LG_MAX_MAP_SIZE_BYTE << 3;
    final long mask = 0XFFL;
    return (int) ((pre0 >>> shift) & mask);
  }

  static int extractLgCurMapSize(final long pre0) { //Byte 4
    final int shift = LG_CUR_MAP_SIZE_BYTE << 3;
    final long mask = 0XFFL;
    return (int) ((pre0 >>> shift) & mask);
  }

  static int extractFlags(final long pre0) { //Byte 5
    final int shift = FLAGS_BYTE << 3;
    final long mask = 0XFFL;
    return (int) ((pre0 >>> shift) & mask);
  }

  static short extractSerDeId(final long pre0) { //Byte 6,7
    final int shift = SER_DE_ID_SHORT << 3;
    final long mask = 0XFFFFL;
    return (short) ((pre0 >>> shift) & mask);
  }

  static int extractActiveItems(final long pre1) { //Bytes 8 to 11
    final long mask = 0XFFFFFFFFL;
    return (int) (pre1 & mask) ;
  }

  static long insertPreLongs(final int preLongs, final long pre0) { //Byte 0
    final long mask = 0X3FL; //Lower 6 bits
    return (preLongs & mask) | (~mask & pre0);
  }

  static long insertSerVer(final int serVer, final long pre0) { //Byte 1
    final int shift = SER_VER_BYTE << 3;
    final long mask = 0XFFL;
    return ((serVer & mask) << shift) | (~(mask << shift) & pre0);
  }

  static long insertFamilyID(final int familyID, final long pre0) { //Byte 2
    final int shift = FAMILY_BYTE << 3;
    final long mask = 0XFFL;
    return ((familyID & mask) << shift) | (~(mask << shift) & pre0);
  }

  static long insertLgMaxMapSize(final int lgMaxMapSize, final long pre0) { //Byte 3
    final int shift = LG_MAX_MAP_SIZE_BYTE << 3;
    final long mask = 0XFFL;
    return ((lgMaxMapSize & mask) << shift) | (~(mask << shift) & pre0);
  }

  static long insertLgCurMapSize(final int lgCurMapSize, final long pre0) { //Byte 4
    final int shift = LG_CUR_MAP_SIZE_BYTE << 3;
    final long mask = 0XFFL;
    return ((lgCurMapSize & mask) << shift) | (~(mask << shift) & pre0);
  }

  static long insertFlags(final int flags, final long pre0) { //Byte 5
    final int shift = FLAGS_BYTE << 3;
    final long mask = 0XFFL;
    return ((flags & mask) << shift) | (~(mask << shift) & pre0);
  }

  static long insertSerDeId(final short serDeId, final long pre0) { //Byte 6,7
    final int shift = SER_DE_ID_SHORT << 3;
    final long mask = 0XFFFFL;
    return ((serDeId & mask) << shift) | (~(mask << shift) & pre0);
  }

  static long insertActiveItems(final int activeItems, final long pre1) { //Bytes 8 to 11
    final long mask = 0XFFFFFFFFL;
    return (activeItems & mask) | (~mask & pre1);
  }

  /**
   * Checks Memory for capacity to hold the preamble and returns the first 8 bytes.
   * @param mem the given Memory
   * @return the first 8 bytes of preamble as a long.
   */
  static long checkPreambleSize(final Memory mem) {
    final long cap = mem.getCapacity();
    if (cap < 8) { throwNotBigEnough(cap, 8); }
    final long pre0 = mem.getLong(0);
    final int preLongs = (int) (pre0 & 0X3FL); //lower 6 bits
    final int required = Math.max(preLongs << 3, 8);
    if (cap < required) { throwNotBigEnough(cap, required); }
    return pre0;
  }

  private static void throwNotBigEnough(final long cap, final int required) {
    throw new SketchesArgumentException(
        "Possible Corruption: "
            + "Size of byte array or Memory not large enough for Preamble: Size: " + cap
            + ", Required: " + required);
  }

}
