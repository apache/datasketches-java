/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import static com.yahoo.sketches.Util.ceilingPowerOf2;
import static com.yahoo.sketches.Util.simpleIntLog2;
import static com.yahoo.sketches.Util.zeroPad;
import static com.yahoo.sketches.hll.HllUtil.LG_AUX_ARR_INTS;
import static com.yahoo.sketches.hll.HllUtil.LG_INIT_SET_SIZE;
import static com.yahoo.sketches.hll.HllUtil.RESIZE_DENOM;
import static com.yahoo.sketches.hll.HllUtil.RESIZE_NUMER;

import java.nio.ByteOrder;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.Family;

//@formatter:off
/**
 * <pre>
 * CouponList Layout
 * Long || Start Byte Adr, Big Endian Illustration
 * Adr:
 *      ||    7   |    6   |    5   |    4   |    3   |    2   |    1   |    0   |
 *  0   ||  Mode  | ListCnt| Flags  |  LgArr |   lgK  | FamID  | SerVer |  PI=2  |
 *
 *      ||   15   |   14   |   13   |   12   |   11   |   10   |    9   |    8   |
 *  1   ||                                   |------Coupon Int List Start--------|
 * </pre>
 *
 *  <pre>
 * CouponHashSet Layout
 * Long || Start Byte Adr, Big Endian Illustration
 * Adr:
 *      ||    7   |    6   |    5   |    4   |    3   |    2   |    1   |    0   |
 *  0   ||  Mode  |        | Flags  |  LgArr |   lgK  | FamID  | SerVer |  PI=3  |
 *
 *      ||   15   |   14   |   13   |   12   |   11   |   10   |    9   |    8   |
 *  1   ||-----Coupon Int Hash Set Start-----|---------Hash Set Count------------|
 * </pre>
 *
 * <pre>
 * HllArray Layout
 * Long || Start Byte Adr, Big Endian Illustration
 * Adr:
 *      ||    7   |    6   |    5   |    4   |    3   |    2   |    1   |    0   |
 *  0   ||  Mode  | CurMin | Flags  |  LgArr |   lgK  | FamID  | SerVer | PI=10  |
 *
 *      ||   15   |   14   |   13   |   12   |   11   |   10   |    9   |    8   |
 *  1   ||-------------------------------HIP Accum-------------------------------|
 *
 *      ||   23   |   22   |   21   |   20   |   19   |   18   |   17   |   16   |
 *  2   ||----------------------------------KxQ0---------------------------------|
 *
 *      ||   31   |   30   |   29   |   28   |   27   |   26   |   25   |   24   |
 *  3   ||----------------------------------KxQ1---------------------------------|
 *
 *      ||   39   |   38   |   37   |   36   |   35   |   34   |   33   |   32   |
 *  4   ||-------------Aux Count-------------|----------Num At Cur Min-----------|
 *
 *      ||   47   |   46   |   45   |   44   |   43   |   42   |   41   |   40   |
 *  5   ||...................................|------Start of HLL_X Byte Array----|
 *
 *  N   ||----End of Byte Array for HLL_4----|...................................|
 *  N+1 ||...................................|-----Start of Aux Array for HLL_4--|
 * </pre>
 * If in compact form exceptions array will be compacted.
 *
 * @author Lee Rhodes
 */
final class PreambleUtil {

  private PreambleUtil() {}

  private static final String LS = System.getProperty("line.separator");

  // ###### DO NOT MESS WITH THIS ...
  // Preamble byte start addresses
  // First 8 Bytes:
  static int PREAMBLE_INTS_BYTE             = 0;
  static int SER_VER_BYTE                   = 1;
  static int FAMILY_BYTE                    = 2;
  static int LG_K_BYTE                      = 3;
  static int LG_ARR_BYTE                    = 4; //used for LIST, SET & HLL_4
  static int FLAGS_BYTE                     = 5;
  static int LIST_COUNT_BYTE                = 6;
  static int HLL_CUR_MIN_BYTE               = 6;
  static int MODE_BYTE                      = 7; //lo2bits = curMode, next 2 bits = tgtHllType
  //mode encoding of combined CurMode and TgtHllType:
  // Dec  Lo4Bits TgtHllType, CurMode
  //   0     0000      HLL_4,    LIST
  //   1     0001      HLL_4,     SET
  //   2     0010      HLL_4,     HLL
  //   4     0100      HLL_6,    LIST
  //   5     0101      HLL_6,     SET
  //   6     0110      HLL_6,     HLL
  //   8     1000      HLL_8,    LIST
  //   9     1001      HLL_8,     SET
  //  10     1010      HLL_8,     HLL

  //Coupon List
  static int LIST_INT_ARR_START             = 8;

  //Coupon Hash Set
  static int HASH_SET_COUNT_INT             = 8;
  static int HASH_SET_INT_ARR_START         = 12;

  //HLL
  static int HIP_ACCUM_DOUBLE               = 8;
  static int KXQ0_DOUBLE                    = 16;
  static int KXQ1_DOUBLE                    = 24;
  static int CUR_MIN_COUNT_INT              = 32;
  static int AUX_COUNT_INT                  = 36;
  static int HLL_BYTE_ARR_START             = 40;

  //Flag bit masks
  static final int BIG_ENDIAN_FLAG_MASK     = 1; //Set but not read. Reserved.
  static final int READ_ONLY_FLAG_MASK      = 2; //Set but not read. Reserved.
  static final int EMPTY_FLAG_MASK          = 4;
  static final int COMPACT_FLAG_MASK        = 8;
  static final int OUT_OF_ORDER_FLAG_MASK   = 16;

  //Mode byte masks
  static final int CUR_MODE_MASK            = 3;
  static final int TGT_HLL_TYPE_MASK        = 12;

  //Other constants
  static final int SER_VER                  = 1;
  static final int FAMILY_ID                = 7;
  static final int LIST_PREINTS             = 2;
  static final int HASH_SET_PREINTS         = 3;
  static final int HLL_PREINTS              = 10;
  static final boolean NATIVE_ORDER_IS_BIG_ENDIAN  =
      (ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN);

  static String toString(final byte[] byteArr) {
    final Memory mem = Memory.wrap(byteArr);
    return toString(mem);
  }

  static String toString(final Memory mem) {
    //First 8 bytes
    final int preInts = mem.getByte(PREAMBLE_INTS_BYTE);
    final int serVer = mem.getByte(SER_VER_BYTE);
    final Family family = Family.idToFamily(mem.getByte(FAMILY_BYTE));
    final int lgK = mem.getByte(LG_K_BYTE);
    final int lgArr = mem.getByte(LG_ARR_BYTE);
    final int flags = mem.getByte(FLAGS_BYTE);
    //Flags
    final String flagsStr = zeroPad(Integer.toBinaryString(flags), 8) + ", " + (flags);
    final boolean bigEndian = (flags & BIG_ENDIAN_FLAG_MASK) > 0;
    final String nativeOrder = ByteOrder.nativeOrder().toString();
    final boolean compact = (flags & COMPACT_FLAG_MASK) > 0;
    final boolean oooFlag = (flags & OUT_OF_ORDER_FLAG_MASK) > 0;
    final boolean readOnly = (flags & READ_ONLY_FLAG_MASK) > 0;
    final boolean empty = (flags & EMPTY_FLAG_MASK) > 0;

    final int hllCurMin = mem.getByte(HLL_CUR_MIN_BYTE);
    final int listCount = hllCurMin;
    final int modeByte = mem.getByte(MODE_BYTE);
    final CurMode curMode = CurMode.fromOrdinal(modeByte & 3);
    final TgtHllType tgtHllType = TgtHllType.fromOrdinal((modeByte >>> 2) & 3);

    double hipAccum = 0;
    double kxq0 = 0;
    double kxq1 = 0;
    int hashSetCount = 0;
    int curMinCount = 0;
    int exceptionCount = 0;

    if (curMode == CurMode.SET) {
      hashSetCount = mem.getInt(HASH_SET_COUNT_INT);
    }
    else if (curMode == CurMode.HLL) {
      hipAccum = mem.getDouble(HIP_ACCUM_DOUBLE);
      kxq0 = mem.getDouble(KXQ0_DOUBLE);
      kxq1 = mem.getDouble(KXQ1_DOUBLE);
      curMinCount = mem.getInt(CUR_MIN_COUNT_INT);
      exceptionCount = mem.getInt(AUX_COUNT_INT);
    }

    final StringBuilder sb = new StringBuilder();
    sb.append(LS);
    sb.append("### HLL SKETCH PREAMBLE:").append(LS);
    sb.append("Byte 0: Preamble Ints         : ").append(preInts).append(LS);
    sb.append("Byte 1: SerVer                : ").append(serVer).append(LS);
    sb.append("Byte 2: Family                : ").append(family).append(LS);
    sb.append("Byte 3: lgK                   : ").append(lgK).append(LS);
    //expand byte 4: LgArr
    if (curMode == CurMode.LIST) {
      sb.append("Byte 4: LgArr: List Arr       : ").append(lgArr).append(LS);
    }
    if (curMode == CurMode.SET) {
      sb.append("Byte 4: LgArr: Hash Set Arr   : ").append(lgArr).append(LS);
    }
    if (curMode == CurMode.HLL) {
      sb.append("Byte 4: LgArr or Aux LgArr    : ").append(lgArr).append(LS);
    }
    //expand byte 5: Flags
    sb.append("Byte 5: Flags:                : ").append(flagsStr).append(LS);
    sb.append("  BIG_ENDIAN_STORAGE          : ").append(bigEndian).append(LS);
    sb.append("  (Native Byte Order)         : ").append(nativeOrder).append(LS);
    sb.append("  READ_ONLY                   : ").append(readOnly).append(LS);
    sb.append("  EMPTY                       : ").append(empty).append(LS);
    sb.append("  COMPACT                     : ").append(compact).append(LS);
    sb.append("  OUT_OF_ORDER                : ").append(oooFlag).append(LS);
    //expand byte 6: ListCount, CurMin
    if (curMode == CurMode.LIST) {
      sb.append("Byte 6: List Count/CurMin     : ").append(listCount).append(LS);
    }
    if (curMode == CurMode.SET) {
      sb.append("Byte 6: (not used)            : ").append(LS);
    }
    if (curMode == CurMode.HLL) {
      sb.append("Byte 6: Cur Min               : ").append(curMinCount).append(LS);
    }
    final String modes = curMode.toString() + ", " + tgtHllType.toString();
    sb.append("Byte 7: Mode                  : ").append(modes).append(LS);
    if (curMode == CurMode.SET) {
      sb.append("Hash Set Count                : ").append(hashSetCount).append(LS);
    }
    if (curMode == CurMode.HLL) {
      sb.append("HIP Accum                     : ").append(hipAccum).append(LS);
      sb.append("KxQ0                          : ").append(kxq0).append(LS);
      sb.append("KxQ1                          : ").append(kxq1).append(LS);
      sb.append("Num At Cur Min                : ").append(curMinCount).append(LS);
      sb.append("Aux Count                     : ").append(exceptionCount).append(LS);
    }
    sb.append("### END HLL SKETCH PREAMBLE").append(LS);
    return sb.toString();
  }
  //@formatter:on

  static int extractPreInts(final Memory mem) {
    return mem.getByte(PREAMBLE_INTS_BYTE) & 0X3F;
  }

  static void insertPreInts(final WritableMemory wmem, final int preInts) {
    wmem.putByte(PREAMBLE_INTS_BYTE, (byte) (preInts & 0X3F));
  }

  static int extractSerVer(final Memory mem) {
    return mem.getByte(SER_VER_BYTE) & 0XFF;
  }

  static void insertSerVer(final WritableMemory wmem) {
    wmem.putByte(SER_VER_BYTE, (byte) SER_VER);
  }

  static int extractFamilyId(final Memory mem) {
    return mem.getByte(FAMILY_BYTE) & 0XFF;
  }

  static void insertFamilyId(final WritableMemory wmem) {
    wmem.putByte(FAMILY_BYTE, (byte) FAMILY_ID);
  }

  static int extractLgK(final Memory mem) {
    return mem.getByte(LG_K_BYTE) & 0XFF;
  }

  static void insertLgK(final WritableMemory wmem, final int lgK) {
    wmem.putByte(LG_K_BYTE, (byte) lgK);
  }

  static int extractLgArr(final Memory mem) {
    final int lgArr = mem.getByte(LG_ARR_BYTE) & 0XFF;
    return lgArr;
  }

  static void insertLgArr(final WritableMemory wmem, final int lgArr) {
    wmem.putByte(LG_ARR_BYTE, (byte) lgArr);
  }

  static int extractListCount(final Memory mem) {
    return mem.getByte(LIST_COUNT_BYTE) & 0XFF;
  }

  static void insertListCount(final WritableMemory wmem, final int listCnt) {
    wmem.putByte(LIST_COUNT_BYTE, (byte) listCnt);
  }

  static int extractCurMin(final Memory mem) {
    return mem.getByte(HLL_CUR_MIN_BYTE) & 0XFF;
  }

  static void insertCurMin(final WritableMemory wmem, final int curMin) {
    wmem.putByte(HLL_CUR_MIN_BYTE, (byte) curMin);
  }

  static double extractHipAccum(final Memory mem) {
    return mem.getDouble(HIP_ACCUM_DOUBLE);
  }

  static void insertHipAccum(final WritableMemory wmem, final double hipAccum) {
    wmem.putDouble(HIP_ACCUM_DOUBLE, hipAccum);
  }

  static double extractKxQ0(final Memory mem) {
    return mem.getDouble(KXQ0_DOUBLE);
  }

  static void insertKxQ0(final WritableMemory wmem, final double kxq0) {
    wmem.putDouble(KXQ0_DOUBLE, kxq0);
  }

  static double extractKxQ1(final Memory mem) {
    return mem.getDouble(KXQ1_DOUBLE);
  }

  static void insertKxQ1(final WritableMemory wmem, final double kxq1) {
    wmem.putDouble(KXQ1_DOUBLE, kxq1);
  }

  static int extractHashSetCount(final Memory mem) {
    return mem.getInt(HASH_SET_COUNT_INT);
  }

  static void insertHashSetCount(final WritableMemory wmem, final int hashSetCnt) {
    wmem.putInt(HASH_SET_COUNT_INT, hashSetCnt);
  }

  static int extractNumAtCurMin(final Memory mem) {
    return mem.getInt(CUR_MIN_COUNT_INT);
  }

  static void insertNumAtCurMin(final WritableMemory wmem, final int numAtCurMin) {
    wmem.putInt(CUR_MIN_COUNT_INT, numAtCurMin);
  }

  static int extractAuxCount(final Memory mem) {
    return mem.getInt(AUX_COUNT_INT);
  }

  static void insertAuxCount(final WritableMemory wmem, final int auxCount) {
    wmem.putInt(AUX_COUNT_INT, auxCount);
  }

  //Mode bits
  static void insertCurMode(final WritableMemory wmem, final CurMode curMode) {
    final int curModeId = curMode.ordinal();
    int mode = wmem.getByte(MODE_BYTE)  & ~CUR_MODE_MASK; //strip bits 0, 1
    mode |= (curModeId & CUR_MODE_MASK);
    wmem.putByte(MODE_BYTE, (byte) mode);
  }

  static CurMode extractCurMode(final Memory mem) {
    final int curModeId = mem.getByte(MODE_BYTE) & CUR_MODE_MASK;
    return CurMode.fromOrdinal(curModeId);
  }

  static void insertTgtHllType(final WritableMemory wmem, final TgtHllType tgtHllType) {
    final int typeId = tgtHllType.ordinal();
    int mode = wmem.getByte(MODE_BYTE) & ~TGT_HLL_TYPE_MASK; //strip bits 2, 3
    mode |= (typeId << 2) & TGT_HLL_TYPE_MASK;
    wmem.putByte(MODE_BYTE, (byte) mode);
  }

  static TgtHllType extractTgtHllType(final Memory mem) {
    final int typeId = mem.getByte(MODE_BYTE) & TGT_HLL_TYPE_MASK;
    return TgtHllType.fromOrdinal(typeId >>> 2);
  }

  static void insertModes(final WritableMemory wmem, final TgtHllType tgtHllType,
      final CurMode curMode) {
    final int curModeId = curMode.ordinal() & 3;
    final int typeId = (tgtHllType.ordinal() & 3) << 2;
    final int mode = typeId | curModeId;
    wmem.putByte(MODE_BYTE, (byte) mode);
  }

  //Flags
  static void insertEmptyFlag(final WritableMemory wmem, final boolean empty) {
    int flags = wmem.getByte(FLAGS_BYTE);
    if (empty) { flags |= EMPTY_FLAG_MASK; }
    else { flags &= ~EMPTY_FLAG_MASK; }
    wmem.putByte(FLAGS_BYTE, (byte) flags);
  }

  static boolean extractEmptyFlag(final Memory mem) {
    final int flags = mem.getByte(FLAGS_BYTE);
    return (flags & EMPTY_FLAG_MASK) > 0;
  }

  static void insertCompactFlag(final WritableMemory wmem, final boolean compact) {
    int flags = wmem.getByte(FLAGS_BYTE);
    if (compact) { flags |= COMPACT_FLAG_MASK; }
    else { flags &= ~COMPACT_FLAG_MASK; }
    wmem.putByte(FLAGS_BYTE, (byte) flags);
  }

  static boolean extractCompactFlag(final Memory mem) {
    final int flags = mem.getByte(FLAGS_BYTE);
    return (flags & COMPACT_FLAG_MASK) > 0;
  }

  static void insertOooFlag(final WritableMemory wmem, final boolean oooFlag) {
    int flags = wmem.getByte(FLAGS_BYTE);
    if (oooFlag) { flags |= OUT_OF_ORDER_FLAG_MASK; }
    else { flags &= ~OUT_OF_ORDER_FLAG_MASK; }
    wmem.putByte(FLAGS_BYTE, (byte) flags);
  }

  static boolean extractOooFlag(final Memory mem) {
    final int flags = mem.getByte(FLAGS_BYTE);
    return (flags & OUT_OF_ORDER_FLAG_MASK) > 0;
  }

  static void insertFlags(final WritableMemory wmem, final int flags) {
    wmem.putByte(FLAGS_BYTE, (byte) flags);
  }

  static int extractFlags(final Memory mem) {
    return mem.getByte(FLAGS_BYTE) & 0XFF;
  }

  //Other
  static int extractInt(final Memory mem, final long byteOffset) {
    return mem.getInt(byteOffset);
  }

  static void insertInt(final WritableMemory wmem, final long byteOffset, final int value) {
    wmem.putInt(byteOffset, value);
  }

  static int computeLgArr(final Memory mem, final int count, final int lgConfigK) {
    //value is missing, recompute
    final CurMode curMode = extractCurMode(mem);
    if (curMode == CurMode.LIST) { return HllUtil.LG_INIT_LIST_SIZE; }
    int ceilPwr2 = ceilingPowerOf2(count);
    if ((RESIZE_DENOM * count) > (RESIZE_NUMER * ceilPwr2)) { ceilPwr2 <<= 1; }
    if (curMode == CurMode.SET) {
      return Math.max(LG_INIT_SET_SIZE, simpleIntLog2(ceilPwr2));
    }
    //only used for HLL4
    return Math.max(LG_AUX_ARR_INTS[lgConfigK], simpleIntLog2(ceilPwr2));
  }

}
