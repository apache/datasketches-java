/*
 * Copyright 2016-17, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.sampling;

import static com.yahoo.sketches.Util.LS;
import static com.yahoo.sketches.Util.zeroPad;

import java.nio.ByteOrder;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.ResizeFactor;
import com.yahoo.sketches.SketchesArgumentException;

//@formatter:off

/**
 * This class defines the preamble items structure and provides basic utilities for some of the key
 * fields.
 *
 * <p>
 * MAP: Low significance bytes of this <i>long</i> items structure are on the right. However, the
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
 *  1   ||------------------------------Items Seen Count (N)---------------------------------|
 *  </pre>
 *
 * <p><strong>Union:</strong> The reservoir union has fewer internal parameters to track and uses
 * a slightly different preamble structure. The maximum reservoir size intentionally occupies the
 * same byte range as the reservoir size in the sketch preamble, allowing the same methods to be
 * used for reading and writing the values. The varopt union takes advantage of the same format.
 * The items in the union are stored in a reservoir sketch-compatible format after the union
 * preamble.
 * </p>
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
 * <p><strong>VarOpt:</strong> A VarOpt sketch has a more complex internal items structure and
 * requires a larger preamble. Values serving a similar purpose in both reservoir and varopt sampling
 * share the same byte ranges, allowing method re-use where practical.</p>
 *
 * <p>An empty varopt sample requires 8 bytes. A non-empty sketch requires 16 bytes of preamble
 * for an under-full sample and otherwise 32 bytes of preamble.</p>
 *
 * <pre>
 * Long || Start Byte Adr:
 * Adr:
 *      ||    7   |    6   |    5   |    4   |    3   |    2   |    1   |     0              |
 *  0   ||--------Reservoir Size (K)---------|  Flags | FamID  | SerVer |   Preamble_Longs   |
 *
 *      ||   15   |   14   |   13   |   12   |   11   |   10   |    9   |     8              |
 *  1   ||------------------------------Items Seen Count (N)---------------------------------|
 *
 *      ||   23   |   22   |   21   |   20   |   19   |   18   |   17   |    16              |
 *  2   ||---------Item Count in R-----------|-----------Item Count in H---------------------|
 *
 *      ||   31   |   30   |   29   |   28   |   27   |   26   |   25   |    24              |
 *  3   ||--------------------------------Total Weight in R----------------------------------|
 *  </pre>
 *
 * <p><strong>VarOpt Union:</strong> VarOpt unions also store more information than a reservoir
 * sketch. As before, we keep values with similar o hte same meaning in corresponding locations
 * actoss sketch and union formats. The items in the union are stored in a varopt sketch-compatible
 * format after the union preamble.</p>
 *
 * <p>An empty union only requires 8 bytes. A non-empty union requires 32 bytes of preamble.</p>
 *
 * <pre>
 * Long || Start Byte Adr:
 * Adr:
 *      ||    7   |    6   |    5   |    4   |    3   |    2   |    1   |     0              |
 *  0   ||---------Max Res. Size (K)---------|  Flags | FamID  | SerVer |   Preamble_Longs   |
 *
 *      ||   15   |   14   |   13   |   12   |   11   |   10   |    9   |     8              |
 *  1   ||------------------------------Items Seen Count (N)---------------------------------|
 *
 *      ||   23   |   22   |   21   |   20   |   19   |   18   |   17   |    16              |
 *  2   ||---------------------------Outer Tau Numerator (double)----------------------------|
 *
 *      ||   31   |   30   |   29   |   28   |   27   |   26   |   25   |    24              |
 *  3   ||---------------------------Outer Tau Denominator (long)----------------------------|
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
  static final int LG_RESIZE_FACTOR_BIT  = 6; // upper 2 bits. Not used by compact or direct.
  static final int SER_VER_BYTE          = 1;
  static final int FAMILY_BYTE           = 2;
  static final int FLAGS_BYTE            = 3;
  static final int RESERVOIR_SIZE_SHORT  = 4; // used in ser_ver 1
  static final int RESERVOIR_SIZE_INT    = 4;
  static final int SERDE_ID_SHORT        = 6; // used in ser_ver 1
  static final int ITEMS_SEEN_LONG       = 8;

  static final int MAX_K_SIZE_INT        = 4; // used in Union only
  static final int OUTER_TAU_NUM_DOUBLE  = 16; // used in Varopt Union only
  static final int OUTER_TAU_DENOM_LONG  = 24; // used in Varopt Union only

  // constants and addresses used in varopt
  static final int ITEM_COUNT_H_INT      = 16;
  static final int ITEM_COUNT_R_INT      = 20;
  static final int TOTAL_WEIGHT_R_DOUBLE = 24;
  static final int VO_WARMUP_PRELONGS    = 3;   // Doesn't match min or max prelongs in Family

  // flag bit masks
  //static final int BIG_ENDIAN_FLAG_MASK = 1;
  //static final int READ_ONLY_FLAG_MASK  = 2;
  static final int EMPTY_FLAG_MASK      = 4;
  static final int GADGET_FLAG_MASK     = 128;

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
    final int preLongs = getAndCheckPreLongs(mem);  // make sure we can get the assumed preamble

    final Family family = Family.idToFamily(mem.getByte(FAMILY_BYTE));

    switch (family) {
      case RESERVOIR:
      case VAROPT:
        return sketchPreambleToString(mem, family, preLongs);
      case RESERVOIR_UNION:
      case VAROPT_UNION:
        return unionPreambleToString(mem, family, preLongs);
      default:
        throw new SketchesArgumentException("Inspecting preamble with Sampling family's "
                + "PreambleUtil with object of family " + family.getFamilyName());
    }
  }

  private static String sketchPreambleToString(final Memory mem,
                                               final Family family,
                                               final int preLongs) {
    final ResizeFactor rf = ResizeFactor.getRF(extractResizeFactor(mem));
    final int serVer = extractSerVer(mem);

    // Flags
    final int flags = extractFlags(mem);
    final String flagsStr = zeroPad(Integer.toBinaryString(flags), 8) + ", " + (flags);
    //final boolean bigEndian = (flags & BIG_ENDIAN_FLAG_MASK) > 0;
    //final String nativeOrder = ByteOrder.nativeOrder().toString();
    //final boolean readOnly = (flags & READ_ONLY_FLAG_MASK) > 0;
    final boolean isEmpty = (flags & EMPTY_FLAG_MASK) > 0;
    final boolean isGadget = (flags & GADGET_FLAG_MASK) > 0;

    final int k;
    if (serVer == 1) {
      final short encK = extractEncodedReservoirSize(mem);
      k = ReservoirSize.decodeValue(encK);
    } else {
      k = extractK(mem);
    }

    long n = 0;
    if (!isEmpty) {
      n = extractN(mem);
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
      .append("  EMPTY                       : ").append(isEmpty).append(LS);
    if (family == Family.VAROPT) {
      sb.append("  GADGET                      : ").append(isGadget).append(LS);
    }
    sb.append("Bytes  4-7: Sketch Size (k)   : ").append(k).append(LS);
    if (!isEmpty) {
      sb.append("Bytes 8-15: Items Seen (n)    : ").append(n).append(LS);
    }
    if ((family == Family.VAROPT) && !isEmpty) {
      final int hCount = extractHRegionItemCount(mem);
      final int rCount = extractRRegionItemCount(mem);
      final double totalRWeight = extractTotalRWeight(mem);
      sb.append("Bytes 16-19: H region count   : ").append(hCount).append(LS)
        .append("Bytes 20-23: R region count   : ").append(rCount).append(LS);
      if (rCount > 0) {
        sb.append("Bytes 24-31: R region weight  : ").append(totalRWeight).append(LS);
      }
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
    final ResizeFactor rf = ResizeFactor.getRF(extractResizeFactor(mem));
    final int serVer = extractSerVer(mem);

    // Flags
    final int flags = extractFlags(mem);
    final String flagsStr = zeroPad(Integer.toBinaryString(flags), 8) + ", " + (flags);
    //final boolean bigEndian = (flags & BIG_ENDIAN_FLAG_MASK) > 0;
    //final String nativeOrder = ByteOrder.nativeOrder().toString();
    //final boolean readOnly = (flags & READ_ONLY_FLAG_MASK) > 0;
    final boolean isEmpty = (flags & EMPTY_FLAG_MASK) > 0;

    final int k;
    if (serVer == 1) {
      final short encK = extractEncodedReservoirSize(mem);
      k = ReservoirSize.decodeValue(encK);
    } else {
      k = extractK(mem);
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
            + "Bytes  4-7: Max Sketch Size (maxK): " + k + LS
            + "TOTAL Sketch Bytes                : " + mem.getCapacity() + LS
            + "  Preamble Bytes                  : " + (preLongs << 3) + LS
            + "  Sketch Bytes                    : " + dataBytes + LS
            + "### END " + family.getFamilyName().toUpperCase() + " PREAMBLE SUMMARY" + LS;
  }

  // Extraction methods

  static int extractPreLongs(final Memory mem) {
    return mem.getByte(PREAMBLE_LONGS_BYTE) & 0x3F;
  }

  static int extractResizeFactor(final Memory mem) {
    return (mem.getByte(PREAMBLE_LONGS_BYTE) >>> LG_RESIZE_FACTOR_BIT) & 0x3;
  }

  static int extractSerVer(final Memory mem) {
    return mem.getByte(SER_VER_BYTE) & 0xFF;
  }

  static int extractFamilyID(final Memory mem) {
    return mem.getByte(FAMILY_BYTE) & 0xFF;
  }

  static int extractFlags(final Memory mem) {
    return mem.getByte(FLAGS_BYTE) & 0xFF;
  }

  static short extractEncodedReservoirSize(final Memory mem) {
    return mem.getShort(RESERVOIR_SIZE_SHORT);
  }

  static int extractK(final Memory mem) {
    return mem.getInt(RESERVOIR_SIZE_INT);
  }

  static int extractMaxK(final Memory mem) {
    return extractK(mem);
  }

  static long extractN(final Memory mem) {
    return mem.getLong(ITEMS_SEEN_LONG);
  }

  static int extractHRegionItemCount(final Memory mem) {
    return mem.getInt(ITEM_COUNT_H_INT);
  }

  static int extractRRegionItemCount(final Memory mem) {
    return mem.getInt(ITEM_COUNT_R_INT);
  }

  static double extractTotalRWeight(final Memory mem) {
    return mem.getDouble(TOTAL_WEIGHT_R_DOUBLE);
  }

  static double extractOuterTauNumerator(final Memory mem) {
    return mem.getDouble(OUTER_TAU_NUM_DOUBLE);
  }

  static long extractOuterTauDenominator(final Memory mem) {
    return mem.getLong(OUTER_TAU_DENOM_LONG);
  }

  // Insertion methods

  static void insertPreLongs(final WritableMemory wmem, final int preLongs) {
    final int curByte = wmem.getByte(PREAMBLE_LONGS_BYTE);
    final int mask = 0x3F;
    final byte newByte = (byte) ((preLongs & mask) | (~mask & curByte));
    wmem.putByte(PREAMBLE_LONGS_BYTE, newByte);
  }

  static void insertLgResizeFactor(final WritableMemory wmem, final int rf) {
    final int curByte = wmem.getByte(PREAMBLE_LONGS_BYTE);
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

  static void insertFlags(final WritableMemory wmem, final int flags) {
    wmem.putByte(FLAGS_BYTE,  (byte) flags);
  }

  static void insertK(final WritableMemory wmem, final int k) {
    wmem.putInt(RESERVOIR_SIZE_INT, k);
  }

  static void insertMaxK(final WritableMemory wmem, final int maxK) {
    insertK(wmem, maxK);
  }

  static void insertN(final WritableMemory wmem, final long totalSeen) {
    wmem.putLong(ITEMS_SEEN_LONG, totalSeen);
  }

  static void insertHRegionItemCount(final WritableMemory wmem, final int hCount) {
    wmem.putInt(ITEM_COUNT_H_INT, hCount);
  }

  static void insertRRegionItemCount(final WritableMemory wmem, final int rCount) {
    wmem.putInt(ITEM_COUNT_R_INT, rCount);
  }

  static void insertTotalRWeight(final WritableMemory wmem, final double weight) {
    wmem.putDouble(TOTAL_WEIGHT_R_DOUBLE, weight);
  }

  static void insertOuterTauNumerator(final WritableMemory wmem, final double numer) {
    wmem.putDouble(OUTER_TAU_NUM_DOUBLE, numer);
  }

  static void insertOuterTauDenominator(final WritableMemory wmem, final long denom) {
    wmem.putLong(OUTER_TAU_DENOM_LONG, denom);
  }

  /**
   * Checks Memory for capacity to hold the preamble and returns the extracted preLongs.
   * @param mem the given Memory
   * @return the extracted prelongs value.
   */
  static int getAndCheckPreLongs(final Memory mem) {
    final long cap = mem.getCapacity();
    if (cap < 8) { throwNotBigEnough(cap, 8); }
    final int preLongs = mem.getByte(0) & 0x3F;
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
