/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.sampling;

import static com.yahoo.sketches.Util.LS;
import static com.yahoo.sketches.Util.TAB;
import static com.yahoo.sketches.Util.zeroPad;

import java.nio.ByteOrder;

import com.yahoo.memory.Memory;
import com.yahoo.memory.NativeMemory;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.ResizeFactor;
import com.yahoo.sketches.SketchesArgumentException;

//@formatter:off

/**
 * This class defines the preamble data structure and provides basic utilities for some of the key
 * fields.
 *
 * <p>
 * MAP: Low significance bytes of this <i>long</i> data structure are on the right. However, the
 * multi-byte integers (<i>int</i> and <i>long</i>) are stored in native byte order. The
 * <i>byte</i> values are treated as unsigned.</p>
 *
 * <p>The count of items seen is limited to 48 bits (~256 trillion) even though there are adjacent unused preamble
 * bits. The acceptance probability for an item is a double in the range [0,1), limiting us to 53 bits of randomness
 * due to details of the IEEE floating point format. To ensure meaningful probabilities as the items seen count
 * approaches capacity, we intentionally use slightly fewer bits.
 * </p>
 * 
 * <p>An empty sampling sketch only requires 8 bytes. A non-empty sampling sketch requires 16 bytes of preamble.</p>
 *
 * <pre>
 * Long || Start Byte Adr:
 * Adr:
 *      ||    7   |    6   |    5   |    4   |    3   |    2   |    1   |     0              |
 *  0   ||-----SerDe ID----|--Reservoir Size-|  Flags | FamID  | SerVer |   Preamble_Longs   |
 *
 *      ||   15   |   14   |   13   |   12   |   11   |   10   |    9   |     8              |
 *  1   ||-----(empty)-----|-------------------Items Seen Count------------------------------|
 *  </pre>
 *
 *  @author Jon Malkin
 *  @author Lee Rhodes
 */
final class PreambleUtil {

  private PreambleUtil() {}
  
  // ###### DO NOT MESS WITH THIS FROM HERE ...
  // Preamble byte Addresses
  static final int PREAMBLE_LONGS_BYTE   = 0; // Only low 6 bits used
  static final int LG_RESIZE_FACTOR_BITS = 6; // upper 2 bits. Not used by compact or direct.
  static final int SER_VER_BYTE          = 1;
  static final int FAMILY_BYTE           = 2;
  static final int FLAGS_BYTE            = 3;
  static final int RESERVOIR_SIZE_SHORT  = 4;
  static final int SERDE_ID_SHORT        = 6;
  static final int ITEMS_SEEN_BYTE       = 8;

  // flag bit masks
  //static final int BIG_ENDIAN_FLAG_MASK = 1;
  //static final int READ_ONLY_FLAG_MASK  = 2;
  static final int EMPTY_FLAG_MASK      = 4;
  //static final int COMPACT_FLAG_MASK    = 8;
  //static final int ORDERED_FLAG_MASK    = 16;

  //Other constants
  static final int SER_VER                    = 1;
  
  static final boolean NATIVE_ORDER_IS_BIG_ENDIAN  = 
      (ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN);
  

  // STRINGS

  /**
   * Returns a human readable string summary of the preamble state of the given byte array. 
   * Used primarily in testing.
   * 
   * @param byteArr the given byte array.
   * @return the summary preamble string.
   */
  public static String preambleToString(byte[] byteArr) {
    Memory mem = new NativeMemory(byteArr);
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
  public static String preambleToString(Memory mem) {
    int preLongs = getAndCheckPreLongs(mem);  //make sure we can get the assumed preamble
    long pre0 = mem.getLong(0);
    long pre1 = mem.getLong(8);

    ResizeFactor rf = ResizeFactor.getRF(extractResizeFactor(pre0));
    int serVer = extractSerVer(pre0);
    Family family = Family.idToFamily(extractFamilyID(pre0));

    //Flags
    int flags = extractFlags(pre0);
    String flagsStr = zeroPad(Integer.toBinaryString(flags), 8) + ", " + (flags);
    //boolean bigEndian = (flags & BIG_ENDIAN_FLAG_MASK) > 0;
    String nativeOrder = ByteOrder.nativeOrder().toString();
    //boolean compact = (flags & COMPACT_FLAG_MASK) > 0;
    //boolean ordered = (flags & ORDERED_FLAG_MASK) > 0;
    //boolean readOnly = (flags & READ_ONLY_FLAG_MASK) > 0;
    boolean isEmpty = (flags & EMPTY_FLAG_MASK) > 0;

    short encResSize = extractReservoirSize(pre0);
    int resSize = ReservoirSize.decodeValue(encResSize);
    long itemsSeen = isEmpty ? 0 : extractItemsSeenCount(pre1);
    int serDeId = extractSerDeId(pre0);

    StringBuilder sb = new StringBuilder();
    sb.append(LS)
      .append("### SKETCH PREAMBLE SUMMARY:").append(LS)
      .append("Byte  0: Preamble Longs       : ").append(preLongs).append(LS)
      .append("Byte  0: ResizeFactor         : ").append(rf.toString()).append(LS)
      .append("Byte  1: Serialization Version: ").append(serVer).append(LS)
      .append("Byte  2: Family               : ").append(family.toString()).append(LS)
      .append("Byte  3: Flags Field          : ").append(flagsStr).append(LS)
      //.append("  BIG_ENDIAN_STORAGE          : ").append(bigEndian).append(LS)
      .append("  (Native Byte Order)         : ").append(nativeOrder).append(LS)
      //.append("  READ_ONLY                   : ").append(readOnly).append(LS)
      .append("  EMPTY                       : ").append(isEmpty).append(LS)
      .append("Bytes 4-5   : Reservoir Size  : ").append(resSize).append(TAB + "(")
            .append(Integer.toHexString(encResSize)).append(")").append(LS)
      .append("Bytes 6-7   : SerDe ID        : ").append(serDeId).append(LS);
    if (!isEmpty) {
      sb.append("Bytes 8-13  : Items Seen      : ").append(itemsSeen).append(LS);
    }

    sb.append("Preamble Bytes                : ").append(preLongs << 3).append(LS);
    //sb.append(  "Data Bytes                    : ").append(curCount * 8).append(LS);
    //sb.append(  "TOTAL Sketch Bytes            : ").append(mem.getCapacity()).append(LS)
    sb.append("### END SKETCH PREAMBLE SUMMARY").append(LS);
    return sb.toString();
  }
  
  //Extract from long and insert into long methods
  
  static int extractPreLongs(final long long0) {
    long mask = 0X3FL;
    return (int) (long0 & mask);
  }

  static int extractResizeFactor(final long long0) {
    int shift = LG_RESIZE_FACTOR_BITS; // units in bits
    long mask = 0X3L;
    return (int) ((long0 >>> shift) & mask);
  }

  static int extractSerVer(final long long0) {
    int shift = SER_VER_BYTE << 3;
    long mask = 0XFFL;
    return (int) ((long0 >>> shift) & mask);
  }
  
  static int extractFamilyID(final long long0) {
    int shift = FAMILY_BYTE << 3;
    long mask = 0XFFL;
    return (int) ((long0 >>> shift) & mask);
  }

  static int extractFlags(final long long0) {
    int shift = FLAGS_BYTE << 3;
    long mask = 0XFFL;
    return (int) ((long0 >>> shift) & mask);
  }

  static short extractReservoirSize(final long long0) {
    int shift = RESERVOIR_SIZE_SHORT << 3;
    long mask = 0XFFFFL;
    return (short) ((long0 >>> shift) & mask);
  }

  static long extractItemsSeenCount(final long long1) {
    long mask = 0XFFFFFFFFFFFFL;
    return (long1 & mask);
  }

  static short extractSerDeId(final long long0) {
    int shift = SERDE_ID_SHORT << 3;
    long mask = 0XFFFFL;
    return (short) ((long0 >>> shift) & mask);
  }

  static long insertPreLongs(final int preLongs, final long long0) {
    long mask = 0X3FL;
    return (preLongs & mask) | (~mask & long0);
  }

  static long insertResizeFactor(final int rf, final long long0) {
    int shift = LG_RESIZE_FACTOR_BITS; // units in bits
    long mask = 3L;
    return ((rf & mask) << shift) | (~(mask << shift) & long0);
  }

  static long insertSerVer(final int serVer, final long long0) {
    int shift = SER_VER_BYTE << 3;
    long mask = 0XFFL;
    return ((serVer & mask) << shift) | (~(mask << shift) & long0);
  }
  
  static long insertFamilyID(final int familyID, final long long0) {
    int shift = FAMILY_BYTE << 3;
    long mask = 0XFFL;
    return ((familyID & mask) << shift) | (~(mask << shift) & long0);
  }
  
  static long insertFlags(final int flags, final long long0) {
    int shift = FLAGS_BYTE << 3;
    long mask = 0XFFL;
    return ((flags & mask) << shift) | (~(mask << shift) & long0);
  }

  static long insertReservoirSize(final short reservoirSize, final long long0) {
    int shift = RESERVOIR_SIZE_SHORT << 3;
    long mask = 0XFFFFL;
    return ((reservoirSize & mask) << shift) | (~(mask << shift) & long0);
  }

  static long insertItemsSeenCount(final long totalSeen, final long long1) {
    long mask = 0XFFFFFFFFFFFFL;
    return (totalSeen & mask) | (~mask & long1);
  }

  static long insertSerDeId(final int serDeId, final long long0) {
    int shift = SERDE_ID_SHORT << 3;
    long mask = 0XFFFFL;
    return ((serDeId & mask) << shift) | (~(mask << shift) & long0);
  }
  
  /**
   * Checks Memory for capacity to hold the preamble and returns the extracted preLongs.
   * @param mem the given Memory
   * @return the extracted prelongs value.
   */
  static int getAndCheckPreLongs(Memory mem) {
    long cap = mem.getCapacity();
    if (cap < 8) { throwNotBigEnough(cap, 8); }
    long pre0 = mem.getLong(0);
    int preLongs = extractPreLongs(pre0);
    int required = Math.max(preLongs << 3, 8);
    if (cap < required) { throwNotBigEnough(cap, required); }
    return preLongs;
  }
  
  private static void throwNotBigEnough(long cap, int required) {
    throw new SketchesArgumentException(
        "Possible Corruption: Size of byte array or Memory not large enough: Size: " + cap 
        + ", Required: " + required);
  }

}
