/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.cpc;

import static com.yahoo.sketches.Util.zeroPad;

import java.nio.ByteOrder;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.SketchesArgumentException;

//@formatter:off
/**
 * <pre>
 * EMPTY Layout, MODE = 0 = 0000
 * Long adr || Big Endian Illustration
 *          ||    7   |    6   |    5   |    4   |    3   |    2   |    1   |    0   |
 *      0   ||--Mode--|        |-Flags--|        |---lgK--|-FamID--|-SerVer-|--PI=2--|
 *
 *
 * SPARSE_MERGE Layout, MODE = 3 = 0011
 * HYBRID_MERGE Layout, MODE = 5 = 0101
 * No HIP Registers
 * Long adr || Big Endian Illustration
 *          ||    7   |    6   |    5   |    4   |    3   |    2   |    1   |    0   |
 *      0   ||--Mode--|        |-Flags--|        |---lgK--|-FamID--|-SerVer-|--PI=4--|
 *
 *          ||   15   |   14   |   13   |   12   |   11   |   10   |    9   |    8   |
 *      1   ||-------------csvLength-------------|------------numCoupons-------------|
 *
 *          ||   23   |   22   |   21   |   20   |   19   |   18   |   17   |   16   |
 *      2   ||                                   |<---------Start of csv bit stream--|
 *
 *
 * SPARSE_HIP Layout, MODE = 2 = 0010
 * HYBRID_HIP Layout, MODE = 4 = 0100
 * With HIP Registers
 * Long adr || Big Endian Illustration
 *          ||    7   |    6   |    5   |    4   |    3   |    2   |    1   |    0   |
 *      0   ||--Mode--|        |-Flags--|        |---lgK--|-FamID--|-SerVer-|--PI=8--|
 *
 *          ||   15   |   14   |   13   |   12   |   11   |   10   |    9   |    8   |
 *      1   ||-------------csvLength-------------|------------numCoupons-------------|
 *
 *          ||   23   |   22   |   21   |   20   |   19   |   18   |   17   |   16   |
 *      2   ||----------------------------------KxP----------------------------------|
 *
 *          ||   31   |   30   |   29   |   28   |   27   |   26   |   25   |   24   |
 *      3   ||-------------------------------HIP Accum-------------------------------|
 *
 *          ||   39   |   38   |   37   |   36   |   35   |   34   |   33   |   32   |
 *      4   ||                                   |<---------Start of csv bit stream--|
 *
 *
 * PINNED_MERGE  Layout, MODE = 7 = 0111
 * SLIDING_MERGE Layout, MODE = 9 = 1001
 * No HIP Registers
 * Long adr || Big Endian Illustration
 *          ||    7   |    6   |    5   |    4   |    3   |    2   |    1   |    0   |
 *      0   ||--Mode--|-offset-|-Flags--|-FIcol--|---lgK--|-FamID--|-SerVer-|--PI=5--|
 *
 *          ||   15   |   14   |   13   |   12   |   11   |   10   |    9   |    8   |
 *      1   ||-------------csvLength-------------|------------numCoupons-------------|
 *
 *          ||   23   |   22   |   21   |   20   |   19   |   18   |   17   |   16   |
 *      2   ||<---------Start of csv bit stream--|--------------cwLength-------------|
 *
 *          ||   XX   |   XX   |   XX   |   XX   |   XX   |   XX   |   XX   |   XX   |
 *      X   ||                                   |<-------Start of cw bit stream-----|
 *
 *
 *
 * PINNED_HIP  Layout, MODE = 6 = 0110
 * SLIDING_HIP Layout, MODE = 8 = 1000
 * With HIP Registers
 * Long adr || Big Endian Illustration
 *          ||    7   |    6   |    5   |    4   |    3   |    2   |    1   |    0   |
 *      0   ||--Mode--|-offset-|-Flags--|-FIcol--|---lgK--|-FamID--|-SerVer-|--PI=9--|
 *
 *          ||   15   |   14   |   13   |   12   |   11   |   10   |    9   |    8   |
 *      1   ||-------------csvLength-------------|------------numCoupons-------------|
 *
 *          ||   23   |   22   |   21   |   20   |   19   |   18   |   17   |   16   |
 *      2   ||----------------------------------KxP----------------------------------|
 *
 *          ||   31   |   30   |   29   |   28   |   27   |   26   |   25   |   24   |
 *      3   ||-------------------------------HIP Accum-------------------------------|
 *
 *          ||   39   |   38   |   37   |   36   |   35   |   34   |   33   |   32   |
 *      4   ||<---------Start of csv bit stream--|--------------cwLength-------------|
 *
 *          ||   XX   |   XX   |   XX   |   XX   |   XX   |   XX   |   XX   |   XX   |
 *      X   ||                                   |<-------Start of cw bit stream-----|
 * </pre>
 *
 * @author Lee Rhodes
 * @author Kevin Lang
 */
final class PreambleUtil {

  private PreambleUtil() {}

  public static final String LS = System.getProperty("line.separator");

  //Flag bit masks, Byte 5
  static final int BIG_ENDIAN_FLAG_MASK     = 1; //Reserved.
  static final int READ_ONLY_FLAG_MASK      = 2; //Reserved.
  static final int EMPTY_FLAG_MASK          = 4;
  static final int MERGE_FLAG_MASK          = 8; //Merge occured, inherited

  // Preamble byte start addresses
  // First 8 Bytes:
  static int PREAMBLE_INTS_BYTE             = 0; //PI
  static int SER_VER_BYTE                   = 1;
  static int FAMILY_BYTE                    = 2;
  static int LG_K_BYTE                      = 3;
  static int FI_COL_BYTE                    = 4; //First Interesting Column
  static int FLAGS_BYTE                     = 5;
  static int WINDOW_OFFSET_BYTE             = 6;
  static int MODE_BYTE                      = 7;

  static int NUM_COUPONS_INT                = 8;
  static int CSV_LENGTH_INT                 = 12;

  //If MERGE_FLAG is NOT set, use HIP:
  static int KXP_DOUBLE                     = 16;
  static int HIP_ACCUM_DOUBLE               = 24;
  static int HIP_CW_LEN_INT                 = 32;
  static int HIP_CSV_STREAM_OFFSET          = 36;

  //If MERGE_FLAG is set, cannot use HIP:
  static int MERGE_CW_LEN_INT               = 16;
  static int MERGE_CSV_STREAM_OFFSET        = 20;
  //NOTE: cwStream byte offset = 4 (PI + csvLength) = 4 (9 + csvLength)

  //Modes
  static int EMPTY_MODE                     =  0;

  //Other constants
  static final int SER_VER                  = 1;
  static final int FAMILY_ID                = 16;
  static final int EMPTY_PREINTS            = 2;
  static final int SPARSE_MERGE_PREINTS     = 4;
  static final int HYBRID_MERGE_PREINTS     = 4;
  static final int SPARSE_HIP_PREINTS       = 8;
  static final int HYBRID_HIP_PREINTS       = 8;
  static final int PINNED_MERGE_PREINTS     = 5;
  static final int SLIDING_MERGE_PREINTS    = 5;
  static final int PINNED_HIP_PREINTS       = 9;
  static final int SLIDING_HIP_PREINTS      = 9;

  static final boolean NATIVE_ORDER_IS_BIG_ENDIAN  =
      (ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN);

  public static String toString(final byte[] byteArr) {
    final Memory mem = Memory.wrap(byteArr);
    return toString(mem);
  }

  public static String toString(final Memory mem) {
    //First 8 bytes
    final int preInts = mem.getByte(PREAMBLE_INTS_BYTE) & 0xff;
    final int serVer = mem.getByte(SER_VER_BYTE) & 0xff;
    final Family family = Family.idToFamily(mem.getByte(FAMILY_BYTE) & 0xff);
    final int lgK = mem.getByte(LG_K_BYTE) & 0xff;
    final int fiCol = mem.getByte(FI_COL_BYTE) & 0xff;
    final int flags = mem.getByte(FLAGS_BYTE) & 0xff;
    final int wOff = mem.getByte(WINDOW_OFFSET_BYTE) & 0xff;
    final Mode mode = Mode.idToMode(mem.getByte(MODE_BYTE) & 0xff);
    //Flags
    final String flagsStr = zeroPad(Integer.toBinaryString(flags), 8) + ", " + (flags);
    final boolean bigEndian = (flags & BIG_ENDIAN_FLAG_MASK) > 0;
    final String nativeOrder = ByteOrder.nativeOrder().toString();
    final boolean readOnly = (flags & READ_ONLY_FLAG_MASK) > 0;
    final boolean empty = (flags & EMPTY_FLAG_MASK) > 0;
    final boolean merge = (flags & MERGE_FLAG_MASK) > 0;

    final long numCoupons = getNumCoupons(mem);
    final long csvLength = getCsvLength(mem);
    final long cwLength = getCwLength(mem);
    final double kxp = getKxP(mem);
    final double hipAccum = getHipAccum(mem);
    final int csvOffset = getCsvOffset(mem);
    final int cwOffset = getCwOffset(mem);

    final StringBuilder sb = new StringBuilder();
    sb.append(LS);
    sb.append("### CPC SKETCH PREAMBLE:").append(LS);
    sb.append("Byte 0: Preamble Ints           : ").append(preInts).append(LS);
    sb.append("Byte 1: SerVer                  : ").append(serVer).append(LS);
    sb.append("Byte 2: Family                  : ").append(family).append(LS);
    sb.append("Byte 3: lgK                     : ").append(lgK).append(LS);
    sb.append("Byte 4: First Interesting Col   : ").append(fiCol).append(LS);
    sb.append("Byte 5: Flags                   : ").append(flagsStr).append(LS);
    sb.append("  BIG_ENDIAN_STORAGE            : ").append(bigEndian).append(LS);
    sb.append("  (Native Byte Order)           : ").append(nativeOrder).append(LS);
    sb.append("  READ_ONLY                     : ").append(readOnly).append(LS);
    sb.append("  EMPTY                         : ").append(empty).append(LS);
    sb.append("  MERGE FLAG                    : ").append(merge).append(LS);

    sb.append("Byte 6: Window Offset           : ").append(wOff).append(LS);
    sb.append("Byte 7: Mode                    : ").append(mode).append(LS);

    sb.append("Num Coupons                     : ").append(numCoupons).append(LS);
    sb.append("Compressed SV Length (ints)     : ").append(csvLength).append(LS);
    sb.append("Compressed SV Offset (bytes)    : ").append(csvOffset).append(LS);
    sb.append("Compressed Window Length (ints) : ").append(cwLength).append(LS);
    sb.append("Compressed Window Offset (bytes): ").append(cwOffset).append(LS);
    sb.append("HIP KxP                         : ").append(kxp).append(LS);
    sb.append("HIP Accum                       : ").append(hipAccum).append(LS);
    sb.append("### END CPC SKETCH PREAMBLE").append(LS);
    return sb.toString();
  }

  //First 8 bytes of preamble
  static int getPreInts(final Memory mem) {
    return mem.getByte(PREAMBLE_INTS_BYTE) & 0XFF;
  }

  static void putPreInts(final WritableMemory wmem, final int preInts) {
    wmem.putByte(PREAMBLE_INTS_BYTE, (byte) (preInts & 0XFF));
  }

  static int getSerVer(final Memory mem) {
    return mem.getByte(SER_VER_BYTE) & 0XFF;
  }

  static void putSerVer(final WritableMemory wmem) {
    wmem.putByte(SER_VER_BYTE, (byte) SER_VER);
  }

  static int getFamilyId(final Memory mem) {
    return mem.getByte(FAMILY_BYTE) & 0XFF;
  }

  static void putFamilyId(final WritableMemory wmem) {
    wmem.putByte(FAMILY_BYTE, (byte) FAMILY_ID);
  }

  static int getLgK(final Memory mem) {
    return mem.getByte(LG_K_BYTE) & 0XFF;
  }

  static void putLgK(final WritableMemory wmem, final int lgK) {
    wmem.putByte(LG_K_BYTE, (byte) lgK);
  }

  static int getFiCol(final Memory mem) { //return 0 if mode < 6?
    return mem.getByte(FI_COL_BYTE) & 0XFF;
  }

  static void putFiCol(final WritableMemory wmem, final int fiCol) {
    wmem.putByte(FI_COL_BYTE, (byte) fiCol);
  }

  static int getWinOffset(final Memory mem) { //return 0 if mode < 6?
    return mem.getByte(WINDOW_OFFSET_BYTE) & 0XFF;
  }

  static void putWinOffset(final WritableMemory wmem, final int winOffset) {
    wmem.putByte(WINDOW_OFFSET_BYTE, (byte) winOffset);
  }

  static int getMode(final Memory mem) {
    return mem.getByte(MODE_BYTE) & 0XFF;
  }

  static void putMode(final WritableMemory wmem, final int mode) {
    wmem.putByte(MODE_BYTE, (byte) mode);
  }

  //Variable-sized preamble

  static long getNumCoupons(final Memory mem) {
    return (getMode(mem) > 0) ? mem.getInt(NUM_COUPONS_INT) & 0XFFFFFFFF : 0;
  }

  static void putNumCoupons(final WritableMemory wmem, final long numCoupons) {
    final int mode = getMode(wmem);
    if (mode == 0) { error(mode); }
    wmem.putInt(NUM_COUPONS_INT, (int) numCoupons);
  }

  static int getCsvLength(final Memory mem) {
    return  (getMode(mem) > 0) ? mem.getInt(CSV_LENGTH_INT) : 0;
  }

  static void putCsvLength(final WritableMemory wmem, final long csvLen) {
    final int mode = getMode(wmem);
    if (mode == 0) { error(mode); }
    wmem.putInt(CSV_LENGTH_INT, (int) csvLen);
  }

  static int getCwLength(final Memory mem) {
    final int mode = getMode(mem);
    if (mode < 6) { return 0; }
    final int add =  ((mode & 1) == 0) ? HIP_CW_LEN_INT : MERGE_CW_LEN_INT;
    return mem.getInt(add);
  }

  static void putCwLength(final WritableMemory wmem, final long cwLen) {
    final int mode = getMode(wmem);
    if ((mode >>> 1) < 3) { error(mode); }
    final int add =  ((mode & 1) == 0) ? HIP_CW_LEN_INT : MERGE_CW_LEN_INT;
    wmem.putInt(add, (int) cwLen);
  }

  static double getKxP(final Memory mem) {
    final int mode = getMode(mem);
    if ((mode < 2) || ((mode & 1) == 1)) { return 0; }
    return mem.getDouble(KXP_DOUBLE);
  }

  static void putKxP(final WritableMemory wmem, final double kxp) {
    final int mode = getMode(wmem);
    if ((mode < 2) || ((mode & 1) == 1)) { error(mode); }
    wmem.putDouble(KXP_DOUBLE, kxp);
  }

  static double getHipAccum(final Memory mem) {
    final int mode = getMode(mem);
    if ((mode < 2) || ((mode & 1) == 1)) { return 0; }
    return mem.getDouble(HIP_ACCUM_DOUBLE);
  }

  static void putHipAccum(final WritableMemory wmem, final double hipAccum) {
    final int mode = getMode(wmem);
    if ((mode < 2) || ((mode & 1) == 1)) { error(mode); }
    wmem.putDouble(HIP_ACCUM_DOUBLE, hipAccum);
  }

  static int getCsvOffset(final Memory mem) {
    final int mode = getMode(mem);
    return ((mode & 1) == 1) ? MERGE_CSV_STREAM_OFFSET : HIP_CSV_STREAM_OFFSET;
  }

  static int getCwOffset(final Memory mem) {
    final int mode = getMode(mem);
    if (mode < 6) { error(mode); }
    return 4 * (getPreInts(mem) + getCsvLength(mem));
  }

  static void error(final int mode) {
    throw new SketchesArgumentException(
        "Operation is illegal in this mode: " + Mode.idToMode(mode));
  }

}
//@formatter:on
