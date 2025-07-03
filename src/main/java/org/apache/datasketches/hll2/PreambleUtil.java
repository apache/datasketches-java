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

package org.apache.datasketches.hll2;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_DOUBLE_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_INT_UNALIGNED;
import static org.apache.datasketches.common.Util.LS;
import static org.apache.datasketches.common.Util.ceilingPowerOf2;
import static org.apache.datasketches.common.Util.exactLog2OfLong;
import static org.apache.datasketches.common.Util.zeroPad;
import static org.apache.datasketches.hll2.HllUtil.LG_AUX_ARR_INTS;
import static org.apache.datasketches.hll2.HllUtil.LG_INIT_SET_SIZE;
import static org.apache.datasketches.hll2.HllUtil.RESIZE_DENOM;
import static org.apache.datasketches.hll2.HllUtil.RESIZE_NUMER;

import java.lang.foreign.MemorySegment;
import java.nio.ByteOrder;

import org.apache.datasketches.common.Family;

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
  static final int REBUILD_CURMIN_NUM_KXQ_MASK = 32; //used only by Union

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
    final MemorySegment seg = MemorySegment.ofArray(byteArr);
    return toString(seg);
  }

  static String toString(final MemorySegment seg) {
    //First 8 bytes
    final int preInts = seg.get(JAVA_BYTE, PREAMBLE_INTS_BYTE);
    final int serVer = seg.get(JAVA_BYTE, SER_VER_BYTE);
    final Family family = Family.idToFamily(seg.get(JAVA_BYTE, FAMILY_BYTE));
    final int lgK = seg.get(JAVA_BYTE, LG_K_BYTE);
    final int lgArr = seg.get(JAVA_BYTE, LG_ARR_BYTE);
    final int flags = seg.get(JAVA_BYTE, FLAGS_BYTE);
    //Flags
    final String flagsStr = zeroPad(Integer.toBinaryString(flags), 8) + ", " + (flags);
    final boolean bigEndian = (flags & BIG_ENDIAN_FLAG_MASK) > 0;
    final String nativeOrder = ByteOrder.nativeOrder().toString();
    final boolean compact = (flags & COMPACT_FLAG_MASK) > 0;
    final boolean oooFlag = (flags & OUT_OF_ORDER_FLAG_MASK) > 0;
    final boolean readOnly = (flags & READ_ONLY_FLAG_MASK) > 0;
    final boolean empty = (flags & EMPTY_FLAG_MASK) > 0;
    final boolean rebuildKxQ = (flags & REBUILD_CURMIN_NUM_KXQ_MASK) > 0;

    final int hllCurMin = seg.get(JAVA_BYTE, HLL_CUR_MIN_BYTE);
    final int listCount = hllCurMin;
    final int modeByte = seg.get(JAVA_BYTE, MODE_BYTE);
    final CurMode curMode = CurMode.fromOrdinal(modeByte & 3);
    final TgtHllType tgtHllType = TgtHllType.fromOrdinal((modeByte >>> 2) & 3);

    double hipAccum = 0;
    double kxq0 = 0;
    double kxq1 = 0;
    int hashSetCount = 0;
    int curMinCount = 0;
    int exceptionCount = 0;

    if (curMode == CurMode.SET) {
      hashSetCount = seg.get(JAVA_INT_UNALIGNED, HASH_SET_COUNT_INT);
    }
    else if (curMode == CurMode.HLL) {
      hipAccum = seg.get(JAVA_DOUBLE_UNALIGNED, HIP_ACCUM_DOUBLE);
      kxq0 = seg.get(JAVA_DOUBLE_UNALIGNED, KXQ0_DOUBLE);
      kxq1 = seg.get(JAVA_DOUBLE_UNALIGNED, KXQ1_DOUBLE);
      curMinCount = seg.get(JAVA_INT_UNALIGNED, CUR_MIN_COUNT_INT);
      exceptionCount = seg.get(JAVA_INT_UNALIGNED, AUX_COUNT_INT);
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
    sb.append("  REBUILD_KXQ                 : ").append(rebuildKxQ).append(LS);
    //expand byte 6: ListCount, CurMin
    if (curMode == CurMode.LIST) {
      sb.append("Byte 6: List Count/CurMin     : ").append(listCount).append(LS);
    }
    if (curMode == CurMode.SET) {
      sb.append("Byte 6: (not used)            : ").append(LS);
    }
    if (curMode == CurMode.HLL) {
      sb.append("Byte 6: Cur Min               : ").append(hllCurMin).append(LS);
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

  static int extractPreInts(final MemorySegment seg) {
    return seg.get(JAVA_BYTE, PREAMBLE_INTS_BYTE) & 0X3F;
  }

  static void insertPreInts(final MemorySegment wseg, final int preInts) {
    wseg.set(JAVA_BYTE, PREAMBLE_INTS_BYTE, (byte) (preInts & 0X3F));
  }

  static int extractSerVer(final MemorySegment seg) {
    return seg.get(JAVA_BYTE, SER_VER_BYTE) & 0XFF;
  }

  static void insertSerVer(final MemorySegment wseg) {
    wseg.set(JAVA_BYTE, SER_VER_BYTE, (byte) SER_VER);
  }

  static int extractFamilyId(final MemorySegment seg) {
    return seg.get(JAVA_BYTE, FAMILY_BYTE) & 0XFF;
  }

  static void insertFamilyId(final MemorySegment wseg) {
    wseg.set(JAVA_BYTE, FAMILY_BYTE, (byte) FAMILY_ID);
  }

  static int extractLgK(final MemorySegment seg) {
    return seg.get(JAVA_BYTE, LG_K_BYTE) & 0XFF;
  }

  static void insertLgK(final MemorySegment wseg, final int lgK) {
    wseg.set(JAVA_BYTE, LG_K_BYTE, (byte) lgK);
  }

  static int extractLgArr(final MemorySegment seg) {
    final int lgArr = seg.get(JAVA_BYTE, LG_ARR_BYTE) & 0XFF;
    return lgArr;
  }

  static void insertLgArr(final MemorySegment wseg, final int lgArr) {
    wseg.set(JAVA_BYTE, LG_ARR_BYTE, (byte) lgArr);
  }

  static int extractListCount(final MemorySegment seg) {
    return seg.get(JAVA_BYTE, LIST_COUNT_BYTE) & 0XFF;
  }

  static void insertListCount(final MemorySegment wseg, final int listCnt) {
    wseg.set(JAVA_BYTE, LIST_COUNT_BYTE, (byte) listCnt);
  }

  static int extractCurMin(final MemorySegment seg) {
    return seg.get(JAVA_BYTE, HLL_CUR_MIN_BYTE) & 0XFF;
  }

  static void insertCurMin(final MemorySegment wseg, final int curMin) {
    wseg.set(JAVA_BYTE, HLL_CUR_MIN_BYTE, (byte) curMin);
  }

  static double extractHipAccum(final MemorySegment seg) {
    return seg.get(JAVA_DOUBLE_UNALIGNED, HIP_ACCUM_DOUBLE);
  }

  static void insertHipAccum(final MemorySegment wseg, final double hipAccum) {
    wseg.set(JAVA_DOUBLE_UNALIGNED, HIP_ACCUM_DOUBLE, hipAccum);
  }

  static double extractKxQ0(final MemorySegment seg) {
    return seg.get(JAVA_DOUBLE_UNALIGNED, KXQ0_DOUBLE);
  }

  static void insertKxQ0(final MemorySegment wseg, final double kxq0) {
    wseg.set(JAVA_DOUBLE_UNALIGNED, KXQ0_DOUBLE, kxq0);
  }

  static double extractKxQ1(final MemorySegment seg) {
    return seg.get(JAVA_DOUBLE_UNALIGNED, KXQ1_DOUBLE);
  }

  static void insertKxQ1(final MemorySegment wseg, final double kxq1) {
    wseg.set(JAVA_DOUBLE_UNALIGNED, KXQ1_DOUBLE, kxq1);
  }

  static int extractHashSetCount(final MemorySegment seg) {
    return seg.get(JAVA_INT_UNALIGNED, HASH_SET_COUNT_INT);
  }

  static void insertHashSetCount(final MemorySegment wseg, final int hashSetCnt) {
    wseg.set(JAVA_INT_UNALIGNED, HASH_SET_COUNT_INT, hashSetCnt);
  }

  static int extractNumAtCurMin(final MemorySegment seg) {
    return seg.get(JAVA_INT_UNALIGNED, CUR_MIN_COUNT_INT);
  }

  static void insertNumAtCurMin(final MemorySegment wseg, final int numAtCurMin) {
    wseg.set(JAVA_INT_UNALIGNED, CUR_MIN_COUNT_INT, numAtCurMin);
  }

  static int extractAuxCount(final MemorySegment seg) {
    return seg.get(JAVA_INT_UNALIGNED, AUX_COUNT_INT);
  }

  static void insertAuxCount(final MemorySegment wseg, final int auxCount) {
    wseg.set(JAVA_INT_UNALIGNED, AUX_COUNT_INT, auxCount);
  }

  //Mode bits
  static void insertCurMode(final MemorySegment wseg, final CurMode curMode) {
    final int curModeId = curMode.ordinal();
    int mode = wseg.get(JAVA_BYTE, MODE_BYTE)  & ~CUR_MODE_MASK; //strip bits 0, 1
    mode |= (curModeId & CUR_MODE_MASK);
    wseg.set(JAVA_BYTE, MODE_BYTE, (byte) mode);
  }

  static CurMode extractCurMode(final MemorySegment seg) {
    final int curModeId = seg.get(JAVA_BYTE, MODE_BYTE) & CUR_MODE_MASK;
    return CurMode.fromOrdinal(curModeId);
  }

  static void insertTgtHllType(final MemorySegment wseg, final TgtHllType tgtHllType) {
    final int typeId = tgtHllType.ordinal();
    int mode = wseg.get(JAVA_BYTE, MODE_BYTE) & ~TGT_HLL_TYPE_MASK; //strip bits 2, 3
    mode |= (typeId << 2) & TGT_HLL_TYPE_MASK;
    wseg.set(JAVA_BYTE, MODE_BYTE, (byte) mode);
  }

  static TgtHllType extractTgtHllType(final MemorySegment seg) {
    final int typeId = seg.get(JAVA_BYTE, MODE_BYTE) & TGT_HLL_TYPE_MASK;
    return TgtHllType.fromOrdinal(typeId >>> 2);
  }

  static void insertModes(final MemorySegment wseg, final TgtHllType tgtHllType,
      final CurMode curMode) {
    final int curModeId = curMode.ordinal() & 3;
    final int typeId = (tgtHllType.ordinal() & 3) << 2;
    final int mode = typeId | curModeId;
    wseg.set(JAVA_BYTE, MODE_BYTE, (byte) mode);
  }

  //Flags
  static void insertEmptyFlag(final MemorySegment wseg, final boolean empty) {
    int flags = wseg.get(JAVA_BYTE, FLAGS_BYTE);
    if (empty) { flags |= EMPTY_FLAG_MASK; }
    else { flags &= ~EMPTY_FLAG_MASK; }
    wseg.set(JAVA_BYTE, FLAGS_BYTE, (byte) flags);
  }

  static boolean extractEmptyFlag(final MemorySegment seg) {
    final int flags = seg.get(JAVA_BYTE, FLAGS_BYTE);
    return (flags & EMPTY_FLAG_MASK) > 0;
  }

  static void insertCompactFlag(final MemorySegment wseg, final boolean compact) {
    int flags = wseg.get(JAVA_BYTE, FLAGS_BYTE);
    if (compact) { flags |= COMPACT_FLAG_MASK; }
    else { flags &= ~COMPACT_FLAG_MASK; }
    wseg.set(JAVA_BYTE, FLAGS_BYTE, (byte) flags);
  }

  static boolean extractCompactFlag(final MemorySegment seg) {
    final int flags = seg.get(JAVA_BYTE, FLAGS_BYTE);
    return (flags & COMPACT_FLAG_MASK) > 0;
  }

  static void insertOooFlag(final MemorySegment wseg, final boolean oooFlag) {
    int flags = wseg.get(JAVA_BYTE, FLAGS_BYTE);
    if (oooFlag) { flags |= OUT_OF_ORDER_FLAG_MASK; }
    else { flags &= ~OUT_OF_ORDER_FLAG_MASK; }
    wseg.set(JAVA_BYTE, FLAGS_BYTE, (byte) flags);
  }

  static boolean extractOooFlag(final MemorySegment seg) {
    final int flags = seg.get(JAVA_BYTE, FLAGS_BYTE);
    return (flags & OUT_OF_ORDER_FLAG_MASK) > 0;
  }

  static void insertRebuildCurMinNumKxQFlag(final MemorySegment wseg, final boolean rebuild) {
    int flags = wseg.get(JAVA_BYTE, FLAGS_BYTE);
    if (rebuild) { flags |= REBUILD_CURMIN_NUM_KXQ_MASK; }
    else { flags &= ~REBUILD_CURMIN_NUM_KXQ_MASK; }
    wseg.set(JAVA_BYTE, FLAGS_BYTE, (byte) flags);
  }

  static boolean extractRebuildCurMinNumKxQFlag(final MemorySegment seg) {
    final int flags = seg.get(JAVA_BYTE, FLAGS_BYTE);
    return (flags & REBUILD_CURMIN_NUM_KXQ_MASK) > 0;
  }

  static void insertFlags(final MemorySegment wseg, final int flags) {
    wseg.set(JAVA_BYTE, FLAGS_BYTE, (byte) flags);
  }

  static int extractFlags(final MemorySegment seg) {
    return seg.get(JAVA_BYTE, FLAGS_BYTE) & 0XFF;
  }

  //Other
  static int extractInt(final MemorySegment seg, final long byteOffset) {
    return seg.get(JAVA_INT_UNALIGNED, byteOffset);
  }

  static void insertInt(final MemorySegment wseg, final long byteOffset, final int value) {
    wseg.set(JAVA_INT_UNALIGNED, byteOffset, value);
  }

  static int computeLgArr(final MemorySegment seg, final int count, final int lgConfigK) {
    //value is missing, recompute
    final CurMode curMode = extractCurMode(seg);
    if (curMode == CurMode.LIST) { return HllUtil.LG_INIT_LIST_SIZE; }
    int ceilPwr2 = ceilingPowerOf2(count);
    if ((RESIZE_DENOM * count) > (RESIZE_NUMER * ceilPwr2)) { ceilPwr2 <<= 1; }
    if (curMode == CurMode.SET) {
      return Math.max(LG_INIT_SET_SIZE, exactLog2OfLong(ceilPwr2));
    }
    //only used for HLL4
    return Math.max(LG_AUX_ARR_INTS[lgConfigK], exactLog2OfLong(ceilPwr2));
  }

}
