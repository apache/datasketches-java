/*
 * Copyright 2016-17, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.sampling;

import static com.yahoo.memory.UnsafeUtil.unsafe;
import static com.yahoo.sketches.Util.LS;
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
 * <p><strong>Sketch:</strong> The count of items seen is limited to 48 bits (~256 trillion) even
 * though there are adjacent unused preamble bits. The acceptance probability for an item is a
 * double in the range [0,1), limiting us to 53 bits of randomness due to details of the IEEE
 * floating point format. To ensure meaningful probabilities as the items seen count approaches
 * capacity, we intentionally use slightly fewer bits.</p>
 *
 * <p>An empty reservoir sampling sketch only requires 8 bytes. A non-empty sampling sketch
 * requires 16 bytes of preamble.</p>
 *
 * <pre>
 * Long || Start Byte Adr:
 * Adr:
 *      ||    7   |    6   |    5   |    4   |    3   |    2   |    1   |     0              |
 *  0   ||--------Reservoir Size (K)---------|  Flags | FamID  | SerVer |   Preamble_Longs   |
 *
 *      ||   15   |   14   |   13   |   12   |   11   |   10   |    9   |     8              |
 *  1   ||-----(empty)-----|-------------------Items Seen Count------------------------------|
 *  </pre>
 *
 * <p><strong>Union:</strong> The reservoir union has fewer internal parameters to track and uses
 * a slightly different preamble structure. The maximum reservoir size intentionally occupies the
 * same byte range as the reservoir size in the sketch preamble, allowing the same methods to be
 * used for reading and writing the values.</p>
 *
 * <p>An empty union only requires 8 bytes. A non-empty union requires 8 bytes of preamble.</p>
 *
 * <pre>
 * Long || Start Byte Adr:
 * Adr:
 *      ||    7   |    6   |    5   |    4   |    3   |    2   |    1   |     0              |
 *  0   ||---------Max Res. Size (K)---------|  Flags | FamID  | SerVer |   Preamble_Longs   |
 * </pre>
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
  static final int RESERVOIR_SIZE_SHORT  = 4; // used in ser_ver 1
  static final int RESERVOIR_SIZE_INT    = 4;
  static final int SERDE_ID_SHORT        = 6; // used in ser_ver 1
  static final int ITEMS_SEEN_LONG       = 8;

  //static final int MAX_K_SHORT           = 4; // used in Union only, ser_ver 1
  //static final int MAX_K_INT             = 4; // used in Union only

  // flag bit masks
  //static final int BIG_ENDIAN_FLAG_MASK = 1;
  //static final int READ_ONLY_FLAG_MASK  = 2;
  static final int EMPTY_FLAG_MASK      = 4;
  //static final int COMPACT_FLAG_MASK    = 8;
  //static final int ORDERED_FLAG_MASK    = 16;

  //Other constants
  static final int SER_VER                    = 2;

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
  public static String preambleToString(final byte[] byteArr) {
    final Memory mem = new NativeMemory(byteArr);
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
  public static String preambleToString(final Memory mem) {
    final int preLongs = getAndCheckPreLongs(mem);  // make sure we can get the assumed preamble

    final Object memObj = mem.array(); // may be null
    final long memAddr = mem.getCumulativeOffset(0L);
    final Family family = Family.idToFamily(extractFamilyID(memObj, memAddr));

    switch (family) {
      case RESERVOIR:
        return sketchPreambleToString(mem, family, preLongs);
      case RESERVOIR_UNION:
        return unionPreambleToString(mem, family, preLongs);
      default:
        throw new SketchesArgumentException("Inspecting preamble with Sampling family's "
                + "PreambleUtil with object of family " + family.getFamilyName());
    }
  }

  private static String sketchPreambleToString(final Memory mem,
                                               final Family family,
                                               final int preLongs) {
    final Object memObj = mem.array(); // may be null
    final long memAddr = mem.getCumulativeOffset(0L);

    final ResizeFactor rf = ResizeFactor.getRF(extractResizeFactor(memObj, memAddr));
    final int serVer = extractSerVer(memObj, memAddr);

    // Flags
    final int flags = extractFlags(memObj, memAddr);
    final String flagsStr = zeroPad(Integer.toBinaryString(flags), 8) + ", " + (flags);
    //final boolean bigEndian = (flags & BIG_ENDIAN_FLAG_MASK) > 0;
    //final String nativeOrder = ByteOrder.nativeOrder().toString();
    //final boolean readOnly = (flags & READ_ONLY_FLAG_MASK) > 0;
    final boolean isEmpty = (flags & EMPTY_FLAG_MASK) > 0;

    final int resSize;
    if (serVer == 1) {
      final short encResSize = extractEncodedReservoirSize(memObj, memAddr);
      resSize = ReservoirSize.decodeValue(encResSize);
    } else {
      resSize = extractReservoirSize(memObj, memAddr);
    }

    long itemsSeen = 0;
    if (!isEmpty) {
      itemsSeen = extractItemsSeenCount(memObj, memAddr);
    }
    final long dataBytes = mem.getCapacity() - (preLongs << 3);

    final StringBuilder sb = new StringBuilder();
    sb.append(LS)
      .append("### END ")
      .append(family.getFamilyName().toUpperCase())
      .append(" PREAMBLE SUMMARY").append(LS)
      .append("Byte  0: Preamble Longs       : ").append(preLongs).append(LS)
      .append("Byte  0: ResizeFactor         : ").append(rf.toString()).append(LS)
      .append("Byte  1: Serialization Version: ").append(serVer).append(LS)
      .append("Byte  2: Family               : ").append(family.toString()).append(LS)
      .append("Byte  3: Flags Field          : ").append(flagsStr).append(LS)
      //.append("  BIG_ENDIAN_STORAGE          : ").append(bigEndian).append(LS)
      //.append("  (Native Byte Order)         : ").append(nativeOrder).append(LS)
      //.append("  READ_ONLY                   : ").append(readOnly).append(LS)
      .append("  EMPTY                       : ").append(isEmpty).append(LS)
      .append("Bytes  4-7: Sketch Size (k)   : ").append(resSize).append(LS);
    if (!isEmpty) {
      sb.append("Bytes 8-13: Items Seen (n)    : ").append(itemsSeen).append(LS);
    }

    sb.append("TOTAL Sketch Bytes            : ").append(mem.getCapacity()).append(LS)
      .append("  Preamble Bytes              : ").append(preLongs << 3).append(LS)
      .append("  Data Bytes                  : ").append(dataBytes).append(LS)
      .append("### END ")
      .append(family.getFamilyName().toUpperCase())
      .append(" PREAMBLE SUMMARY").append(LS);
    return sb.toString();
  }

  private static String unionPreambleToString(final Memory mem,
                                              final Family family,
                                              final int preLongs) {
    final Object memObj = mem.array(); // may be null
    final long memAddr = mem.getCumulativeOffset(0L);

    final ResizeFactor rf = ResizeFactor.getRF(extractResizeFactor(memObj, memAddr));
    final int serVer = extractSerVer(memObj, memAddr);

    // Flags
    final int flags = extractFlags(memObj, memAddr);
    final String flagsStr = zeroPad(Integer.toBinaryString(flags), 8) + ", " + (flags);
    //final boolean bigEndian = (flags & BIG_ENDIAN_FLAG_MASK) > 0;
    //final String nativeOrder = ByteOrder.nativeOrder().toString();
    //final boolean readOnly = (flags & READ_ONLY_FLAG_MASK) > 0;
    final boolean isEmpty = (flags & EMPTY_FLAG_MASK) > 0;

    final int resSize;
    if (serVer == 1) {
      final short encResSize = extractEncodedReservoirSize(memObj, memAddr);
      resSize = ReservoirSize.decodeValue(encResSize);
    } else {
      resSize = extractReservoirSize(memObj, memAddr);
    }

    final long dataBytes = mem.getCapacity() - (preLongs << 3);

    return LS
            + "### END " + family.getFamilyName().toUpperCase() + " PREAMBLE SUMMARY" + LS
            + "Byte  0: Preamble Longs           : " + preLongs + LS
            + "Byte  0: ResizeFactor             : " + rf.toString() + LS
            + "Byte  1: Serialization Version    : " + serVer + LS
            + "Byte  2: Family                   : " + family.toString() + LS
            + "Byte  3: Flags Field              : " + flagsStr + LS
            //+ "  BIG_ENDIAN_STORAGE              : " + bigEndian + LS
            //+ "  (Native Byte Order)             : " + nativeOrder + LS
            //+ "  READ_ONLY                       : " + readOnly + LS
            + "  EMPTY                           : " + isEmpty + LS
            + "Bytes  4-7: Max Sketch Size (maxK): " + resSize + LS
            + "TOTAL Sketch Bytes                : " + mem.getCapacity() + LS
            + "  Preamble Bytes                  : " + (preLongs << 3) + LS
            + "  Sketch Bytes                    : " + dataBytes + LS
            + "### END " + family.getFamilyName().toUpperCase() + " PREAMBLE SUMMARY" + LS;
  }

  static int extractPreLongs(final Object memObj, final long memAddr) {
    return unsafe.getByte(memObj, memAddr + PREAMBLE_LONGS_BYTE) & 0x3F;
  }

  static int extractResizeFactor(final Object memObj, final long memAddr) {
    return (unsafe.getByte(memObj, memAddr + PREAMBLE_LONGS_BYTE) >> LG_RESIZE_FACTOR_BITS) & 0x3;
  }

  static int extractSerVer(final Object memObj, final long memAddr) {
    return unsafe.getByte(memObj, memAddr + SER_VER_BYTE) & 0xFF;
  }

  static int extractFamilyID(final Object memObj, final long memAddr) {
    return unsafe.getByte(memObj, memAddr + FAMILY_BYTE) & 0xFF;
  }

  static int extractFlags(final Object memObj, final long memAddr) {
    return unsafe.getByte(memObj, memAddr + FLAGS_BYTE) & 0xFF;
  }

  static short extractEncodedReservoirSize(final Object memObj, final long memAddr) {
    return unsafe.getShort(memObj, memAddr + RESERVOIR_SIZE_SHORT);
  }

  static int extractReservoirSize(final Object memObj, final long memAddr) {
    return unsafe.getInt(memObj, memAddr + RESERVOIR_SIZE_INT);
  }

  static int extractMaxK(final Object memObj, final long memAddr) {
    return extractReservoirSize(memObj, memAddr);
  }

  @Deprecated
  static short extractSerDeId(final Object memObj, final long memAddr) {
    return unsafe.getShort(memObj, memAddr + SERDE_ID_SHORT);
  }

  static long extractItemsSeenCount(final Object memObj, final long memAddr) {
    return unsafe.getLong(memObj, memAddr + ITEMS_SEEN_LONG);
  }

  static void insertPreLongs(final Object memObj, final long memAddr, final int preLongs) {
    final int curByte = unsafe.getByte(memObj, memAddr + PREAMBLE_LONGS_BYTE);
    final int mask = 0x3F;
    final byte newByte = (byte) ((preLongs & mask) | (~mask & curByte));
    unsafe.putByte(memObj, memAddr + PREAMBLE_LONGS_BYTE, newByte);
  }

  static void insertLgResizeFactor(final Object memObj, final long memAddr, final int rf) {
    final int curByte = unsafe.getByte(memObj, memAddr + PREAMBLE_LONGS_BYTE);
    final int shift = LG_RESIZE_FACTOR_BITS; // shift in bits
    final int mask = 3;
    final byte newByte = (byte) (((rf & mask) << shift) | (~(mask << shift) & curByte));
    unsafe.putByte(memObj, memAddr + PREAMBLE_LONGS_BYTE, newByte);
  }

  static void insertSerVer(final Object memObj, final long memAddr, final int serVer) {
    unsafe.putByte(memObj, memAddr + SER_VER_BYTE, (byte) serVer);
  }

  static void insertFamilyID(final Object memObj, final long memAddr, final int famId) {
    unsafe.putByte(memObj, memAddr + FAMILY_BYTE, (byte) famId);
  }

  static void insertFlags(final Object memObj, final long memAddr, final int flags) {
    unsafe.putByte(memObj, memAddr + FLAGS_BYTE, (byte) flags);
  }

  static void insertReservoirSize(final Object memObj, final long memAddr, final int k) {
    unsafe.putInt(memObj, memAddr + RESERVOIR_SIZE_INT, k);
  }

  static void insertMaxK(final Object memObj, final long memAddr, final int maxK) {
    insertReservoirSize(memObj, memAddr, maxK);
  }

  @Deprecated
  static void insertSerDeId(final Object memObj, final long memAddr, final short serDeId) {
    unsafe.putShort(memObj, memAddr + SERDE_ID_SHORT, serDeId);
  }

  static void insertItemsSeenCount(final Object memObj, final long memAddr, final long totalSeen) {
    unsafe.putLong(memObj, memAddr + ITEMS_SEEN_LONG, totalSeen);
  }

  /**
   * Checks Memory for capacity to hold the preamble and returns the extracted preLongs.
   * @param mem the given Memory
   * @return the extracted prelongs value.
   */
  static int getAndCheckPreLongs(final Memory mem) {
    final Object memObj = mem.array(); //may be null
    final long memAddr = mem.getCumulativeOffset(0L);

    final long cap = mem.getCapacity();
    if (cap < 8) { throwNotBigEnough(cap, 8); }
    final int preLongs = extractPreLongs(memObj, memAddr);
    final int required = Math.max(preLongs << 3, 8);
    if (cap < required) { throwNotBigEnough(cap, required); }
    return preLongs;
  }

  private static void throwNotBigEnough(final long cap, final int required) {
    throw new SketchesArgumentException(
        "Possible Corruption: Size of byte array or Memory not large enough: Size: " + cap
        + ", Required: " + required);
  }
}
