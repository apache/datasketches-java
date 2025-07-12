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

package org.apache.datasketches.sampling;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_DOUBLE_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_INT_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_SHORT_UNALIGNED;
import static org.apache.datasketches.common.Util.LS;
import static org.apache.datasketches.common.Util.zeroPad;

import java.lang.foreign.MemorySegment;
import java.nio.ByteOrder;
import java.util.Locale;

import org.apache.datasketches.common.Family;
import org.apache.datasketches.common.ResizeFactor;
import org.apache.datasketches.common.SketchesArgumentException;

//@formatter:off

/**
 * This class defines the preamble items structure and provides basic utilities for some of the key
 * fields. Fields are presented in Little Endian format, but multi-byte values (int, long, double)
 * are stored in native byte order. All <tt>byte</tt> values are treated as unsigned.
 *
 * <h1>Reservoir Sampling</h1>
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
 *      ||       0        |    1   |    2   |    3   |    4   |    5   |    6   |    7   |
 *  0   || Preamble_Longs | SerVer | FamID  |  Flags |---------Max Res. Size (K)---------|
 *
 *      ||       8        |    9   |   10   |   11   |   12   |   13   |   14   |   15   |
 *  1   ||----------------------------Items Seen Count (N)-------------------------------|
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
 *      ||       0        |    1   |    2   |    3   |    4   |    5   |    6   |    7   |
 *  0   || Preamble_Longs | SerVer | FamID  |  Flags |---------Max Res. Size (K)---------|
 * </pre>
 *
 *
 * <h1>VarOpt Sampling</h1>
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
 *      ||       0        |    1   |    2   |    3   |    4   |    5   |    6   |    7   |
 *  0   || Preamble_Longs | SerVer | FamID  |  Flags |---------Max Res. Size (K)---------|
 *
 *      ||       8        |    9   |   10   |   11   |   12   |   13   |   14   |   15   |
 *  1   ||----------------------------Items Seen Count (N)-------------------------------|
 *
 *      ||      16        |   17   |   18   |   19   |   20   |   21   |   22   |   23   |
 *  2   ||--------Item Count in R-----------|-----------Item Count in H------------------|
 *
 *      ||      24        |   25   |   26   |   27   |   28   |   29   |   30   |   31   |
 *  3   ||------------------------------Total Weight in R--------------------------------|
 *  </pre>
 *
 * <p><strong>VarOpt Union:</strong> VarOpt unions also store more information than a reservoir
 * sketch. As before, we keep values with similar to the same meaning in corresponding locations
 * actoss sketch and union formats. The items in the union are stored in a varopt sketch-compatible
 * format after the union preamble.</p>
 *
 * <p>An empty union only requires 8 bytes. A non-empty union requires 32 bytes of preamble.</p>
 *
 * <pre>
 * Long || Start Byte Adr:
 * Adr:
 *      ||       0        |    1   |    2   |    3   |    4   |    5   |    6   |    7   |
 *  0   || Preamble_Longs | SerVer | FamID  |  Flags |---------Max Res. Size (K)---------|
 *
 *      ||       8        |    9   |   10   |   11   |   12   |   13   |   14   |   15   |
 *  1   ||----------------------------Items Seen Count (N)-------------------------------|
 *
 *      ||      16        |   17   |   18   |   19   |   20   |   21   |   22   |   23   |
 *  2   ||-------------------------Outer Tau Numerator (double)--------------------------|
 *
 *      ||      24        |   25   |   26   |   27   |   28   |   29   |   30   |   31   |
 *  3   ||-------------------------Outer Tau Denominator (long)--------------------------|
 *  </pre>
 *
 *
 * <h1>EPPS Sampling</h1>
 *
 * <p>An empty sketch requires 8 bytes.
 *
 * <pre>
 * Long || Start Byte Adr:
 * Adr:
 *      ||       0        |    1   |    2   |    3   |    4   |    5   |    6   |    7   |
 *  0   || Preamble_Longs | SerVer | FamID  |  Flags |---------Max Res. Size (K)---------|
 * </pre>
 *
 * <p>A non-empty sketch requires 40 bytes of preamble. C looks like part of
 * the preamble but is treated as part of the sample state.
 *
 * <p>The count of items seen is not used but preserved as the value seems like a useful
 * count to track.
 *
 * <pre>
 * Long || Start Byte Adr:
 * Adr:
 *      ||       0        |    1   |    2   |    3   |    4   |    5   |    6   |    7   |
 *  0   || Preamble_Longs | SerVer | FamID  |  Flags |---------Max Res. Size (K)---------|
 *
 *      ||       8        |    9   |   10   |   11   |   12   |   13   |   14   |   15   |
 *  1   ||---------------------------Items Seen Count (N)--------------------------------|
 *
 *      ||      16        |   17   |   18   |   19   |   20   |   21   |   22   |   23   |
 *  2   ||----------------------------Cumulative Weight----------------------------------|
 *
 *      ||      24        |   25   |   26   |   27   |   28   |   29   |   30   |   31   |
 *  3   ||-----------------------------Max Item Weight-----------------------------------|
 *
 *      ||      32        |   33   |   34   |   35   |   36   |   37   |   38   |   39   |
 *  4   ||----------------------------------Rho------------------------------------------|
 *
 *      ||      40        |   41   |   42   |   43   |   44   |   45   |   46   |   47   |
 *  5   ||-----------------------------------C-------------------------------------------|
 *
 *      ||      40+                      |
 *  6+  ||  {Items Array}                |
 *      ||  {Optional Item (if needed)}  |
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
  static final int VO_PRELONGS_EMPTY     = Family.VAROPT.getMinPreLongs();
  static final int VO_PRELONGS_WARMUP    = 3;   // Doesn't match min or max prelongs in Family
  static final int VO_PRELONGS_FULL      = Family.VAROPT.getMaxPreLongs();

  // constants and addresses used in EBPPS
  static final int EBPPS_CUM_WT_DOUBLE   = 16;
  static final int EBPPS_MAX_WT_DOUBLE   = 24;
  static final int EBPPS_RHO_DOUBLE      = 32;

  // flag bit masks
  //static final int BIG_ENDIAN_FLAG_MASK = 1;
  //static final int READ_ONLY_FLAG_MASK  = 2;
  static final int EMPTY_FLAG_MASK      = 4;
  static final int HAS_PARTIAL_ITEM_MASK = 8; // EBPPS only
  static final int GADGET_FLAG_MASK     = 128;

  //Other constants
  static final int RESERVOIR_SER_VER    = 2;
  static final int VAROPT_SER_VER       = 2;
  static final int EBPPS_SER_VER        = 1;

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
    final MemorySegment seg = MemorySegment.ofArray(byteArr);
    return preambleToString(seg);
  }

  /**
   * Returns a human readable string summary of the preamble state of the given MemorySegment.
   * Note: other than making sure that the given MemorySegment size is large
   * enough for just the preamble, this does not do much value checking of the contents of the
   * preamble as this is primarily a tool for debugging the preamble visually.
   *
   * @param seg the given MemorySegment.
   * @return the summary preamble string.
   */
  static String preambleToString(final MemorySegment seg) {
    final int preLongs = getAndCheckPreLongs(seg);  // make sure we can get the assumed preamble

    final Family family = Family.idToFamily(seg.get(JAVA_BYTE, FAMILY_BYTE));

    switch (family) {
      case RESERVOIR:
      case VAROPT:
        return sketchPreambleToString(seg, family, preLongs);
      case RESERVOIR_UNION:
      case VAROPT_UNION:
        return unionPreambleToString(seg, family, preLongs);
      default:
        throw new SketchesArgumentException("Inspecting preamble with Sampling family's "
                + "PreambleUtil with object of family " + family.getFamilyName());
    }
  }

  private static String sketchPreambleToString(final MemorySegment seg,
                                               final Family family,
                                               final int preLongs) {
    final ResizeFactor rf = ResizeFactor.getRF(extractResizeFactor(seg));
    final int serVer = extractSerVer(seg);

    // Flags
    final int flags = extractFlags(seg);
    final String flagsStr = zeroPad(Integer.toBinaryString(flags), 8) + ", " + (flags);
    //final boolean bigEndian = (flags & BIG_ENDIAN_FLAG_MASK) > 0;
    //final String nativeOrder = ByteOrder.nativeOrder().toString();
    //final boolean readOnly = (flags & READ_ONLY_FLAG_MASK) > 0;
    final boolean isEmpty = (flags & EMPTY_FLAG_MASK) > 0;
    final boolean isGadget = (flags & GADGET_FLAG_MASK) > 0;

    final int k;
    if (serVer == 1) {
      final short encK = extractEncodedReservoirSize(seg);
      k = ReservoirSize.decodeValue(encK);
    } else {
      k = extractK(seg);
    }

    long n = 0;
    if (!isEmpty) {
      n = extractN(seg);
    }
    final long dataBytes = seg.byteSize() - (preLongs << 3);

    final StringBuilder sb = new StringBuilder();
    sb.append(LS)
      .append("### END ")
      .append(family.getFamilyName().toUpperCase(Locale.US))
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
      final int hCount = extractHRegionItemCount(seg);
      final int rCount = extractRRegionItemCount(seg);
      final double totalRWeight = extractTotalRWeight(seg);
      sb.append("Bytes 16-19: H region count   : ").append(hCount).append(LS)
        .append("Bytes 20-23: R region count   : ").append(rCount).append(LS);
      if (rCount > 0) {
        sb.append("Bytes 24-31: R region weight  : ").append(totalRWeight).append(LS);
      }
    }

    sb.append("TOTAL Sketch Bytes            : ").append(seg.byteSize()).append(LS)
      .append("  Preamble Bytes              : ").append(preLongs << 3).append(LS)
      .append("  Data Bytes                  : ").append(dataBytes).append(LS)
      .append("### END ")
      .append(family.getFamilyName().toUpperCase(Locale.US))
      .append(" PREAMBLE SUMMARY").append(LS);
    return sb.toString();
  }

  private static String unionPreambleToString(final MemorySegment seg,
                                              final Family family,
                                              final int preLongs) {
    final ResizeFactor rf = ResizeFactor.getRF(extractResizeFactor(seg));
    final int serVer = extractSerVer(seg);

    // Flags
    final int flags = extractFlags(seg);
    final String flagsStr = zeroPad(Integer.toBinaryString(flags), 8) + ", " + (flags);
    //final boolean bigEndian = (flags & BIG_ENDIAN_FLAG_MASK) > 0;
    //final String nativeOrder = ByteOrder.nativeOrder().toString();
    //final boolean readOnly = (flags & READ_ONLY_FLAG_MASK) > 0;
    final boolean isEmpty = (flags & EMPTY_FLAG_MASK) > 0;

    final int k;
    if (serVer == 1) {
      final short encK = extractEncodedReservoirSize(seg);
      k = ReservoirSize.decodeValue(encK);
    } else {
      k = extractK(seg);
    }

    final long dataBytes = seg.byteSize() - (preLongs << 3);

    return LS
            + "### END " + family.getFamilyName().toUpperCase(Locale.US) + " PREAMBLE SUMMARY" + LS
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
            + "TOTAL Sketch Bytes                : " + seg.byteSize() + LS
            + "  Preamble Bytes                  : " + (preLongs << 3) + LS
            + "  Sketch Bytes                    : " + dataBytes + LS
            + "### END " + family.getFamilyName().toUpperCase(Locale.US) + " PREAMBLE SUMMARY" + LS;
  }

  // Extraction methods

  static int extractPreLongs(final MemorySegment seg) {
    return seg.get(JAVA_BYTE, PREAMBLE_LONGS_BYTE) & 0x3F;
  }

  static int extractResizeFactor(final MemorySegment seg) {
    return (seg.get(JAVA_BYTE, PREAMBLE_LONGS_BYTE) >>> LG_RESIZE_FACTOR_BIT) & 0x3;
  }

  static int extractSerVer(final MemorySegment seg) {
    return seg.get(JAVA_BYTE, SER_VER_BYTE) & 0xFF;
  }

  static int extractFamilyID(final MemorySegment seg) {
    return seg.get(JAVA_BYTE, FAMILY_BYTE) & 0xFF;
  }

  static int extractFlags(final MemorySegment seg) {
    return seg.get(JAVA_BYTE, FLAGS_BYTE) & 0xFF;
  }

  static short extractEncodedReservoirSize(final MemorySegment seg) {
    return seg.get(JAVA_SHORT_UNALIGNED, RESERVOIR_SIZE_SHORT);
  }

  static int extractK(final MemorySegment seg) {
    return seg.get(JAVA_INT_UNALIGNED, RESERVOIR_SIZE_INT);
  }

  static int extractMaxK(final MemorySegment seg) {
    return extractK(seg);
  }

  static long extractN(final MemorySegment seg) {
    return seg.get(JAVA_LONG_UNALIGNED, ITEMS_SEEN_LONG);
  }

  static int extractHRegionItemCount(final MemorySegment seg) {
    return seg.get(JAVA_INT_UNALIGNED, ITEM_COUNT_H_INT);
  }

  static int extractRRegionItemCount(final MemorySegment seg) {
    return seg.get(JAVA_INT_UNALIGNED, ITEM_COUNT_R_INT);
  }

  static double extractTotalRWeight(final MemorySegment seg) {
    return seg.get(JAVA_DOUBLE_UNALIGNED, TOTAL_WEIGHT_R_DOUBLE);
  }

  static double extractOuterTauNumerator(final MemorySegment seg) {
    return seg.get(JAVA_DOUBLE_UNALIGNED, OUTER_TAU_NUM_DOUBLE);
  }

  static long extractOuterTauDenominator(final MemorySegment seg) {
    return seg.get(JAVA_LONG_UNALIGNED, OUTER_TAU_DENOM_LONG);
  }

  static double extractEbppsCumulativeWeight(final MemorySegment seg) {
    return seg.get(JAVA_DOUBLE_UNALIGNED, EBPPS_CUM_WT_DOUBLE);
  }

  static double extractEbppsMaxWeight(final MemorySegment seg) {
    return seg.get(JAVA_DOUBLE_UNALIGNED, EBPPS_MAX_WT_DOUBLE);
  }

  static double extractEbppsRho(final MemorySegment seg) {
    return seg.get(JAVA_DOUBLE_UNALIGNED, EBPPS_RHO_DOUBLE);
  }

  // Insertion methods

  static void insertPreLongs(final MemorySegment wseg, final int preLongs) {
    final int curByte = wseg.get(JAVA_BYTE, PREAMBLE_LONGS_BYTE);
    final int mask = 0x3F;
    final byte newByte = (byte) ((preLongs & mask) | (~mask & curByte));
    wseg.set(JAVA_BYTE, PREAMBLE_LONGS_BYTE, newByte);
  }

  static void insertLgResizeFactor(final MemorySegment wseg, final int rf) {
    final int curByte = wseg.get(JAVA_BYTE, PREAMBLE_LONGS_BYTE);
    final int shift = LG_RESIZE_FACTOR_BIT; // shift in bits
    final int mask = 3;
    final byte newByte = (byte) (((rf & mask) << shift) | (~(mask << shift) & curByte));
    wseg.set(JAVA_BYTE, PREAMBLE_LONGS_BYTE, newByte);
  }

  static void insertSerVer(final MemorySegment wseg, final int serVer) {
    wseg.set(JAVA_BYTE, SER_VER_BYTE, (byte) serVer);
  }

  static void insertFamilyID(final MemorySegment wseg, final int famId) {
    wseg.set(JAVA_BYTE, FAMILY_BYTE, (byte) famId);
  }

  static void insertFlags(final MemorySegment wseg, final int flags) {
    wseg.set(JAVA_BYTE, FLAGS_BYTE,  (byte) flags);
  }

  static void insertK(final MemorySegment wseg, final int k) {
    wseg.set(JAVA_INT_UNALIGNED, RESERVOIR_SIZE_INT, k);
  }

  static void insertMaxK(final MemorySegment wseg, final int maxK) {
    insertK(wseg, maxK);
  }

  static void insertN(final MemorySegment wseg, final long totalSeen) {
    wseg.set(JAVA_LONG_UNALIGNED, ITEMS_SEEN_LONG, totalSeen);
  }

  static void insertHRegionItemCount(final MemorySegment wseg, final int hCount) {
    wseg.set(JAVA_INT_UNALIGNED, ITEM_COUNT_H_INT, hCount);
  }

  static void insertRRegionItemCount(final MemorySegment wseg, final int rCount) {
    wseg.set(JAVA_INT_UNALIGNED, ITEM_COUNT_R_INT, rCount);
  }

  static void insertTotalRWeight(final MemorySegment wseg, final double weight) {
    wseg.set(JAVA_DOUBLE_UNALIGNED, TOTAL_WEIGHT_R_DOUBLE, weight);
  }

  static void insertOuterTauNumerator(final MemorySegment wseg, final double numer) {
    wseg.set(JAVA_DOUBLE_UNALIGNED, OUTER_TAU_NUM_DOUBLE, numer);
  }

  static void insertOuterTauDenominator(final MemorySegment wseg, final long denom) {
    wseg.set(JAVA_LONG_UNALIGNED, OUTER_TAU_DENOM_LONG, denom);
  }

  static void insertEbppsCumulativeWeight(final MemorySegment wseg, final double cumWt) {
    wseg.set(JAVA_DOUBLE_UNALIGNED, EBPPS_CUM_WT_DOUBLE, cumWt);
  }

  static void insertEbppsMaxWeight(final MemorySegment wseg, final double maxWt) {
    wseg.set(JAVA_DOUBLE_UNALIGNED, EBPPS_MAX_WT_DOUBLE, maxWt);
  }

  static void insertEbppsRho(final MemorySegment wseg, final double rho) {
    wseg.set(JAVA_DOUBLE_UNALIGNED, EBPPS_RHO_DOUBLE, rho);
  }

  /**
   * Checks MemorySegment for capacity to hold the preamble and returns the extracted preLongs.
   * @param seg the given MemorySegment
   * @return the extracted prelongs value.
   */
  static int getAndCheckPreLongs(final MemorySegment seg) {
    final long cap = seg.byteSize();
    if (cap < 8) { throwNotBigEnough(cap, 8); }
    final int preLongs = seg.get(JAVA_BYTE, 0) & 0x3F;
    final int required = Math.max(preLongs << 3, 8);
    if (cap < required) { throwNotBigEnough(cap, required); }
    return preLongs;
  }

  private static void throwNotBigEnough(final long cap, final int required) {
    throw new SketchesArgumentException(
        "Possible Corruption: Size of byte array or MemorySegment not large enough: Size: " + cap
        + ", Required: " + required);
  }
}
