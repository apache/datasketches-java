/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.quantiles;

import static com.yahoo.memory.UnsafeUtil.unsafe;
import static com.yahoo.sketches.Family.idToFamily;
import static com.yahoo.sketches.quantiles.Util.LS;
import static com.yahoo.sketches.quantiles.Util.computeRetainedItems;

import java.nio.ByteOrder;

import com.yahoo.memory.Memory;
import com.yahoo.memory.NativeMemory;

//@formatter:off

/**
 * This class defines the preamble data structure and provides basic utilities for some of the key
 * fields.
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
 * <p>An empty QuantilesSketch only requires 8 bytes. All others require 24 bytes of preamble.</p>
 *
 * <pre>
 * Long || Start Byte Adr: Common for both DoublesSketch and ItemsSketch
 * Adr:
 *      ||    7   |    6   |    5   |    4   |    3   |    2   |    1   |     0          |
 *  0   ||------unused-----|--------K--------|  Flags | FamID  | SerVer | Preamble_Longs |
 *
 *      ||   15   |   14   |   13   |   12   |   11   |   10   |    9   |     8          |
 *  1   ||-----------------------------------N_LONG--------------------------------------|
 *
 *  Applies only to DoublesSketch:
 *
 *      ||   23   |   22   |   21   |   20   |   19   |   18   |   17   |    16          |
 *  2   ||---------------------------START OF DATA, MIN_DOUBLE---------------------------|
 *
 *      ||   31   |   30   |   29   |   28   |   27   |   26   |   25   |    24          |
 *  3   ||----------------------------------MAX_DOUBLE-----------------------------------|
 *
 *      ||   39   |   38   |   37   |   36   |   35   |   34   |   33   |    32          |
 *  4   ||---------------------------------REST OF DATA----------------------------------|
 *  </pre>
 *
 *  @author Lee Rhodes
 */
final class PreambleUtil {

  private PreambleUtil() {}

  // ###### DO NOT MESS WITH THIS FROM HERE ...
  // Preamble byte Addresses
  static final int PREAMBLE_LONGS_BYTE        = 0;
  static final int SER_VER_BYTE               = 1;
  static final int FAMILY_BYTE                = 2;
  static final int FLAGS_BYTE                 = 3;
  static final int K_SHORT                    = 4;  //to 5
  static final int N_LONG                     = 8;  //to 15

  //After Preamble:
  static final int MIN_DOUBLE                 = 16; //to 23 (Only for DoublesSketch)
  static final int MAX_DOUBLE                 = 24; //to 31 (Only for DoublesSketch)
  static final int COMBINED_BUFFER            = 32; //to 39 (Only for DoublesSketch)

  // flag bit masks
  static final int BIG_ENDIAN_FLAG_MASK       = 1;
  static final int READ_ONLY_FLAG_MASK        = 2;
  static final int EMPTY_FLAG_MASK            = 4;
  static final int COMPACT_FLAG_MASK          = 8;
  static final int ORDERED_FLAG_MASK          = 16;

  static final boolean NATIVE_ORDER_IS_BIG_ENDIAN  =
      (ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN);

  // STRINGS
  /**
   * Returns a human readable string summary of the internal state of the given byte array.
   * Used primarily in testing.
   *
   * @param byteArr the given byte array.
   * @param isDoublesSketch flag to indicate that the byte array represents DoublesSketch
   * to print min and max value in the summary
   * @return the summary string.
   */
  static String toString(final byte[] byteArr, final boolean isDoublesSketch) {
    final Memory mem = new NativeMemory(byteArr);
    return toString(mem, isDoublesSketch);
  }

  /**
   * Returns a human readable string summary of the Preamble of the given Memory. If this Memory
   * image is from a DoublesSketch, the MinValue and MaxValue will also be output.
   * Used primarily in testing.
   *
   * @param mem the given Memory
   * @param isDoublesSketch flag to indicate that the byte array represents DoublesSketch
   * to print min and max value in the summary
   * @return the summary string.
   */
  static String toString(final Memory mem, final boolean isDoublesSketch) {
    return memoryToString(mem, isDoublesSketch);
  }

  private static String memoryToString(final Memory mem, final boolean isDoublesSketch) {
    final Object memObj = mem.array(); //may be null
    final long memAdd = mem.getCumulativeOffset(0L);

    final int preLongs = extractPreLongs(memObj, memAdd); //either 1 or 2
    final int serVer = extractSerVer(memObj, memAdd);
    final int familyID = extractFamilyID(memObj, memAdd);
    final String famName = idToFamily(familyID).toString();
    final int flags = extractFlags(memObj, memAdd);
    final boolean bigEndian = (flags & BIG_ENDIAN_FLAG_MASK) > 0;
    final String nativeOrder = ByteOrder.nativeOrder().toString();
    final boolean readOnly  = (flags & READ_ONLY_FLAG_MASK) > 0;
    final boolean empty = (flags & EMPTY_FLAG_MASK) > 0;
    final boolean compact = (flags & COMPACT_FLAG_MASK) > 0;
    final boolean ordered = (flags & ORDERED_FLAG_MASK) > 0;
    final int k = extractK(memObj, memAdd);

    final long n = (preLongs == 1) ? 0L : extractN(memObj, memAdd);
    double minDouble = Double.POSITIVE_INFINITY;
    double maxDouble = Double.NEGATIVE_INFINITY;
    if ((preLongs > 1) && isDoublesSketch) { // preLongs = 2 or 3
      minDouble = extractMinDouble(memObj, memAdd);
      maxDouble = extractMaxDouble(memObj, memAdd);
    }

    final StringBuilder sb = new StringBuilder();
    sb.append(LS);
    sb.append("### QUANTILES SKETCH PREAMBLE SUMMARY:").append(LS);
    sb.append("Byte  0: Preamble Longs       : ").append(preLongs).append(LS);
    sb.append("Byte  1: Serialization Version: ").append(serVer).append(LS);
    sb.append("Byte  2: Family               : ").append(famName).append(LS);
    sb.append("Byte  3: Flags Field          : ").append(String.format("%02o", flags)).append(LS);
    sb.append("  BIG ENDIAN                  : ").append(bigEndian).append(LS);
    sb.append("  (Native Byte Order)         : ").append(nativeOrder).append(LS);
    sb.append("  READ ONLY                   : ").append(readOnly).append(LS);
    sb.append("  EMPTY                       : ").append(empty).append(LS);
    sb.append("  COMPACT                     : ").append(compact).append(LS);
    sb.append("  ORDERED                     : ").append(ordered).append(LS);
    sb.append("Bytes  4-5  : K               : ").append(k).append(LS);
    if (preLongs == 1) {
      sb.append(" --ABSENT, ASSUMED:").append(LS);
    }
    sb.append("Bytes  8-15 : N                : ").append(n).append(LS);
    if (isDoublesSketch) {
      sb.append("MinDouble                      : ").append(minDouble).append(LS);
      sb.append("MaxDouble                      : ").append(maxDouble).append(LS);
    }
    sb.append("Retained Items                 : ").append(computeRetainedItems(k, n)).append(LS);
    sb.append("Total Bytes                    : ").append(mem.getCapacity()).append(LS);
    sb.append("### END SKETCH PREAMBLE SUMMARY").append(LS);
    return sb.toString();
  }

  //@formatter:on

  static int extractPreLongs(final Object memObj, final long memAdd) {
    return unsafe.getByte(memObj, memAdd + PREAMBLE_LONGS_BYTE) & 0XFF;
  }

  static int extractSerVer(final Object memObj, final long memAdd) {
    return unsafe.getByte(memObj, memAdd + SER_VER_BYTE) & 0XFF;
  }

  static int extractFamilyID(final Object memObj, final long memAdd) {
    return unsafe.getByte(memObj, memAdd + FAMILY_BYTE) & 0XFF;
  }

  static int extractFlags(final Object memObj, final long memAdd) {
    return unsafe.getByte(memObj, memAdd + FLAGS_BYTE) & 0XFF;
  }

  static int extractK(final Object memObj, final long memAdd) {
    return unsafe.getShort(memObj, memAdd + K_SHORT) & 0XFFFF;
  }

  static long extractN(final Object memObj, final long memAdd) {
    return unsafe.getLong(memObj, memAdd + N_LONG);
  }

  static double extractMinDouble(final Object memObj, final long memAdd) {
    return unsafe.getDouble(memObj, memAdd + MIN_DOUBLE);
  }

  static double extractMaxDouble(final Object memObj, final long memAdd) {
    return unsafe.getDouble(memObj, memAdd + MAX_DOUBLE);
  }

  static void insertPreLongs(final Object memObj, final long memAdd, final int value) {
    unsafe.putByte(memObj, memAdd + PREAMBLE_LONGS_BYTE, (byte) value);
  }

  static void insertSerVer(final Object memObj, final long memAdd, final int value) {
    unsafe.putByte(memObj, memAdd + SER_VER_BYTE, (byte) value);
  }

  static void insertFamilyID(final Object memObj, final long memAdd, final int value) {
    unsafe.putByte(memObj, memAdd + FAMILY_BYTE, (byte) value);
  }

  static void insertFlags(final Object memObj, final long memAdd, final int value) {
    unsafe.putByte(memObj, memAdd + FLAGS_BYTE, (byte) value);
  }

  static void insertK(final Object memObj, final long memAdd, final int value) {
    unsafe.putShort(memObj, memAdd + K_SHORT, (short) value);
  }

  static void insertN(final Object memObj, final long memAdd, final long value) {
    unsafe.putLong(memObj, memAdd + N_LONG, value);
  }

  static void insertMinDouble(final Object memObj, final long memAdd, final double value) {
    unsafe.putDouble(memObj, memAdd + MIN_DOUBLE, value);
  }

  static void insertMaxDouble(final Object memObj, final long memAdd, final double value) {
    unsafe.putDouble(memObj, memAdd + MAX_DOUBLE, value);
  }

  static void insertIntoBaseBuffer(final Object memObj, final long memAdd, final int bbOffset,
      final double value) {
    unsafe.putDouble(memObj, memAdd + COMBINED_BUFFER + bbOffset * 8L, value);
  }
}
