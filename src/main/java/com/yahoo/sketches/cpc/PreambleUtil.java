/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.cpc;

import static com.yahoo.sketches.Util.zeroPad;

import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.Map;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.SketchesStateException;

//@formatter:off
/**
 * All formats are illustrated as Big-Endian, LSB on the right.
 *
 * <pre>
 * Format = EMPTY: NoWindow, NoSV, NoHIP = 0 = 000.  Common for all Formats.
 * PI = 2, FIcol = 0
 * Long adr ||
 *          ||    7   |    6   |    5   |    4   |    3   |    2   |    1   |    0   |
 *      0   ||---SEED HASH-----|-Flags--|-FIcol--|---lgK--|-FamID--|-SerVer-|---PI---|
 *
 *
 * Format = SPARSE_HYBRID_MERGED: {NoWindow, SV, NoHIP} = 2 = 010.
 * PI = 4, FIcol = 0
 * Long adr ||
 *          ||   15   |   14   |   13   |   12   |   11   |   10   |    9   |    8   |
 *      1   ||-------------csvLength-------------|--------numCoupons = numSV---------|
 *
 *          ||   23   |   22   |   21   |   20   |   19   |   18   |   17   |   16   |
 *      2   ||                                   |<---------Start of csv bit stream--|
 *
 *
 * Format = SPARSE_HYBRID_HIP: {NoWindow, SV, HIP} = 3 = 011
 * PI = 8, FIcol = 0
 * Long adr ||
 *          ||   15   |   14   |   13   |   12   |   11   |   10   |    9   |    8   |
 *      1   ||-------------csvLength-------------|--------numCoupons = numSV---------|
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
 * Format = PINNED_SLIDING_MERGED_NOSV: {Window, NoSV, NoHIP} = 4 = 100
 * PI = 4, FIcol = valid
 * Long adr ||
 *          ||   15   |   14   |   13   |   12   |   11   |   10   |    9   |    8   |
 *      1   ||-------------cwLength--------------|------------numCoupons-------------|
 *
 *          ||   23   |   22   |   21   |   20   |   19   |   18   |   17   |   16   |
 *      2   ||                                   |<-------Start of cw bit stream-----|
 *
 *
 * Format = PINNED_SLIDING_HIP_NOSV: {Window, NoSV, HIP} = 5 = 101
 * PI = 8, FIcol = valid
 * Long adr ||
 *          ||   15   |   14   |   13   |   12   |   11   |   10   |    9   |    8   |
 *      1   ||-------------cwLength--------------|------------numCoupons-------------|
 *
 *          ||   23   |   22   |   21   |   20   |   19   |   18   |   17   |   16   |
 *      2   ||----------------------------------KxP----------------------------------|
 *
 *          ||   31   |   30   |   29   |   28   |   27   |   26   |   25   |   24   |
 *      3   ||-------------------------------HIP Accum-------------------------------|
 *
 *          ||   39   |   38   |   37   |   36   |   35   |   34   |   33   |   32   |
 *      4   ||                                   |<-------Start of cw bit stream-----|
 *
 *
 * Format = PINNED_SLIDING_MERGED: {Window, SV, NoHIP} = 6 = 110
 * PI = 6, FIcol = valid
 * Long adr ||
 *          ||   15   |   14   |   13   |   12   |   11   |   10   |    9   |    8   |
 *      1   ||---------------numSV---------------|------------numCoupons-------------|
 *
 *          ||   23   |   22   |   21   |   20   |   19   |   18   |   17   |   16   |
 *      2   ||-------------cwLength--------------|-------------csvLength-------------|
 *
 *          ||   XX   |   XX   |   XX   |   XX   |   27   |   26   |   25   |   24   |
 *      3   ||<-------Start of cw bit stream-----|<-------Start of csv bit stream----|
 *
 *
 * Format = PINNED_SLIDING_HIP: {Window, SV, HIP} = 7 = 111
 * PI = 10, FIcol = valid
 * Long adr ||
 *          ||   15   |   14   |   13   |   12   |   11   |   10   |    9   |    8   |
 *      1   ||---------------numSV---------------|------------numCoupons-------------|
 *
 *          ||   23   |   22   |   21   |   20   |   19   |   18   |   17   |   16   |
 *      2   ||----------------------------------KxP----------------------------------|
 *
 *          ||   31   |   30   |   29   |   28   |   27   |   26   |   25   |   24   |
 *      3   ||-------------------------------HIP Accum-------------------------------|
 *
 *          ||   39   |   38   |   37   |   36   |   35   |   34   |   33   |   32   |
 *      4   ||-------------cwLength--------------|-------------csvLength-------------|
 *
 *          ||   XX   |   XX   |   XX   |   XX   |   43   |   42   |   41   |   40   |
 *      5   ||<-------Start of cw bit stream-----|<-------Start of csv bit stream----|
 * </pre>
 *
 * @author Lee Rhodes
 * @author Kevin Lang
 */
final class PreambleUtil {

  private PreambleUtil() {}

  static {
    if (ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN) {
      throw new SketchesStateException("This sketch will not work on Big Endian CPUs.");
    }
  }

  private static final String LS = System.getProperty("line.separator");
  private static final String fmt = "%10d%10x";

  /**
   *  The serialization version for the set of serialization formats defined in this class
   */
  static final byte SER_VER = 1;

  //Flag bit masks, Byte 5
  static final int BIG_ENDIAN_FLAG_MASK     = 1; //Reserved.
  static final int READ_ONLY_FLAG_MASK      = 2; //Reserved.
  static final int HIP_FLAG_MASK            = 4;
  static final int SUP_VAL_FLAG_MASK        = 8;
  static final int WINDOW_FLAG_MASK         = 16;

  //PREAMBLE SIZE

  /**
   * This defines the preamble space required by each of the formats in units of 4-byte integers.
   */
  private static final byte[] preIntDefs = { 2, 0, 4, 8, 4, 8, 6, 10 };

  /**
   * Returns the defined size of the preamble in units of integers (4 bytes) given the
   * <i>Format</i>.
   * @param format the given <i>Format</i>.
   * @return the defined size of the preamble in units of integers (4 bytes) given the <i>Format</i>.
   */
  static byte getDefinedPreInts(final Format format) {
    return preIntDefs[format.ordinal()];
  }

  //PREAMBLE LO_FIELD DEFINITIONS, OFFSETS, AND GETS

  /**
   * This defines the seven fields of the first eight bytes of the preamble.
   * The ordinal of these values defines the byte offset.
   * Do not change the order.
   */
  enum LoField { PRE_INTS, SER_VERSION, FAMILY, LG_K, FI_COL, FLAGS, SEED_HASH }

  /**
   * Returns the defined byte offset from the start of the preamble given a <i>LoField</i>.
   * This only applies to the first 8 bytes of the preamble.
   * @param loField the given <i>LoField</i>.
   * @return the defined byte offset from the start of the preamble given a <i>LoField</i>.
   */
  static int getLoFieldOffset(final LoField loField) {
    return loField.ordinal();
  }

  static int getPreInts(final Memory mem) {
    return mem.getByte(getLoFieldOffset(LoField.PRE_INTS)) & 0XFF;
  }

  static int getSerVer(final Memory mem) {
    return mem.getByte(getLoFieldOffset(LoField.SER_VERSION)) & 0XFF;
  }

  static Family getFamily(final Memory mem) {
    final int fam = mem.getByte(getLoFieldOffset(LoField.FAMILY)) & 0XFF;
    return Family.idToFamily(fam);
  }

  static int getLgK(final Memory mem) {
    return mem.getByte(getLoFieldOffset(LoField.LG_K)) & 0XFF;
  }

  static int getFiCol(final Memory mem) {
    return mem.getByte(getLoFieldOffset(LoField.FI_COL)) & 0XFF;
  }

  static int getFlags(final Memory mem) {
    return mem.getByte(getLoFieldOffset(LoField.FLAGS)) & 0XFF;
  }

  static short getSeedHash(final Memory mem) {
    return mem.getShort(getLoFieldOffset(LoField.SEED_HASH));
  }

  //PREAMBLE FORMAT DEFINITIONS

  /**
   * There are seven different preamble formats, which determine the layout of the <i>HiField</i>
   * variables after the first 8 bytes of the preamble.
   * Do not change the order.
   */
  enum Format { EMPTY, NONE, SPARSE_HYBRID_MERGED, SPARSE_HYBRID_HIP, PINNED_SLIDING_MERGED_NOSV,
    PINNED_SLIDING_HIP_NOSV, PINNED_SLIDING_MERGED, PINNED_SLIDING_HIP ;

    private static final Map<Integer, Format> lookupID = new HashMap<>();

    static {
      for (Format f : values()) {
        lookupID.put(f.ordinal(), f);
      }
    }

    /**
     * Returns the Format given its enum ordinal
     * @param ordinal the given enum ordinal
     * @return the Format given its enum ordinal
     */
    static Format ordinalToFormat(final int ordinal) {
      final Format f = lookupID.get(ordinal);
      return f;
    }
  } //end enum Format

  //COMMON FORMAT GETS

  static int getFormatOrdinal(final Memory mem) {
    final int flags = getFlags(mem);
    return (flags >>> 2) & 0x7;
  }

  static Format getFormat(final Memory mem) {
    final int ordinal = getFormatOrdinal(mem);
    return Format.ordinalToFormat(ordinal);
  }

  static boolean hasHip(final Memory mem) {
    return (getFlags(mem) & HIP_FLAG_MASK) > 0;
  }

  static final boolean hasSV(final Memory mem) {
    return (getFlags(mem) & SUP_VAL_FLAG_MASK) > 0;
  }

  static final boolean hasWindow(final Memory mem) {
    return (getFlags(mem) & WINDOW_FLAG_MASK) > 0;
  }

  //PREAMBLE HI_FIELD DEFINITIONS

  /**
   * This defines the eight additional preamble fields located after the <i>LoField</i>.
   * Do not change the order.
   */
  enum HiField { NUM_COUPONS, NUM_SV, KXP, HIP_ACCUM, CSV_LENGTH, CW_LENGTH, CSV_STREAM,
    CW_STREAM }

  //PREAMBLE HI_FIELD OFFSETS

  /**
   * This defines the byte offset for eac of the 8 <i>HiFields</i>
   * given the Format ordinal (1st dimension) and the HiField ordinal (2nd dimension).
   */
  private static final byte[][] hiFieldOffset = //[Format][HiField]
    { {0,  0,  0,  0,  0,  0,  0,  0},
      {0,  0,  0,  0,  0,  0,  0,  0},
      {8,  0,  0,  0, 12,  0, 16,  0},
      {8,  0, 16, 24, 12,  0, 32,  0},
      {8,  0,  0,  0,  0, 12,  0, 16},
      {8,  0, 16, 24,  0, 12,  0, 32},
      {8, 12,  0,  0, 16, 20, 24, 24},
      {8, 12, 16, 24, 32, 36, 40, 40}
    };

  /**
   * Returns the defined byte offset from the start of the preamble given the <i>HiField</i>
   * and the <i>Format</i>.
   * Note this can not be used to obtain the stream offsets.
   * @param format the desired <i>Format</i>
   * @param hiField the desired preamble <i>HiField</i> after the first eight bytes.
   * @return the defined byte offset from the start of the preamble for the given <i>HiField</i>
   * and the <i>Format</i>.
   */
  static long getHiFieldOffset(final Format format, final HiField hiField) {
    final int formatIdx = format.ordinal();
    final int hiFieldIdx = hiField.ordinal();
    final long fieldOffset = hiFieldOffset[formatIdx][hiFieldIdx] & 0xFF; //initially a byte
    if (fieldOffset == 0) {
      throw new SketchesStateException("Undefined preamble field given the Format: "
          + "Format: " + format.toString() + ", HiField: " + hiField.toString());
    }
    return fieldOffset;
  }

  //PREAMBLE HI_FIELD GETS

  static long getNumCoupons(final Memory mem) {
    final Format format = getFormat(mem);
    final HiField hiField = HiField.NUM_COUPONS;
    final long offset = getHiFieldOffset(format, hiField);
    return mem.getInt(offset) & 0xFFFF_FFFFL;
  }

  static long getNumSV(final Memory mem) {
    final Format format = getFormat(mem);
    final HiField hiField = HiField.NUM_SV;
    final long offset = getHiFieldOffset(format, hiField);
    return mem.getInt(offset) & 0xFFFF_FFFFL;
  }

  static long getCsvLength(final Memory mem) {
    final Format format = getFormat(mem);
    final HiField hiField = HiField.CSV_LENGTH;
    final long offset = getHiFieldOffset(format, hiField);
    return mem.getInt(offset) & 0xFFFF_FFFFL;
  }

  static long getCwLength(final Memory mem) {
    final Format format = getFormat(mem);
    final HiField hiField = HiField.CW_LENGTH;
    final long offset = getHiFieldOffset(format, hiField);
    return mem.getInt(offset) & 0xFFFF_FFFFL;
  }

  static double getKxP(final Memory mem) {
    final Format format = getFormat(mem);
    final HiField hiField = HiField.KXP;
    final long offset = getHiFieldOffset(format, hiField);
    return mem.getDouble(offset);
  }

  static double getHipAccum(final Memory mem) {
    final Format format = getFormat(mem);
    final HiField hiField = HiField.HIP_ACCUM;
    final long offset = getHiFieldOffset(format, hiField);
    return mem.getDouble(offset);
  }

  static long getCsvStreamOffset(final Memory mem) {
    final Format format = getFormat(mem);
    final HiField csvLenField = HiField.CSV_LENGTH;
    if (!hasSV(mem)) { fieldError(format, csvLenField); }
    final long csvLength = mem.getInt(getHiFieldOffset(format, csvLenField)) & 0XFFFF_FFFFL;
    if (csvLength == 0) {
      throw new SketchesStateException("CsvLength cannot be zero");
    }
    return 4L * getPreInts(mem);
  }

  static long getCwStreamOffset(final Memory mem) {
    final Format format = getFormat(mem);
    final HiField cwLenField = HiField.CW_LENGTH;
    if (!hasWindow(mem))  { fieldError(format, cwLenField); }
    long csvLength = 0;
    if (hasSV(mem)) {
      csvLength = mem.getInt(getHiFieldOffset(format, HiField.CSV_LENGTH)) & 0XFFFF_FFFFL;
      if (csvLength == 0) {
        throw new SketchesStateException("CsvLength cannot be zero");
      }
    }
    return 4L * (getPreInts(mem) + csvLength);
  }

  // PUT INTO MEMORY

  static void putEmpty(final WritableMemory wmem,
      final byte lgK,
      final short seedHash) {
    final Format format = Format.EMPTY;
    final byte preInts = getDefinedPreInts(format);
    final byte fiCol = (byte) 0;
    final byte flags = (byte) ((format.ordinal() << 2) | READ_ONLY_FLAG_MASK);
    checkCapacity(wmem.getCapacity(), 8);
    putFirst8(wmem, preInts, lgK, fiCol, flags, seedHash);
  }

  static void putSparseHybridMerged(final WritableMemory wmem,
      final byte lgK,
      final long numCoupons,
      final long csvLength,
      final short seedHash,
      final int[] csvStream) {
    final Format format = Format.SPARSE_HYBRID_MERGED;
    final byte preInts = getDefinedPreInts(format);
    final byte fiCol = (byte) 0;
    final byte flags = (byte) ((format.ordinal() << 2) | READ_ONLY_FLAG_MASK);
    checkCapacity(wmem.getCapacity(), 4L * (preInts + csvLength));
    putFirst8(wmem, preInts, lgK, fiCol, flags, seedHash);

    wmem.putInt(getHiFieldOffset(format, HiField.NUM_COUPONS), (int) numCoupons);
    wmem.putInt(getHiFieldOffset(format, HiField.CSV_LENGTH), (int) csvLength);
    wmem.putIntArray(getCsvStreamOffset(wmem), csvStream, 0, (int) csvLength);
  }

  static void putSparseHybridHip(final WritableMemory wmem,
      final byte lgK,
      final long numCoupons,
      final long csvLength,
      final double kxp,
      final double hipAccum,
      final short seedHash,
      final int[] csvStream) {
    final Format format = Format.SPARSE_HYBRID_HIP;
    final byte preInts = getDefinedPreInts(format);
    final byte fiCol = (byte) 0;
    final byte flags = (byte) ((format.ordinal() << 2) | READ_ONLY_FLAG_MASK);
    checkCapacity(wmem.getCapacity(), 4L * (preInts + csvLength));
    putFirst8(wmem, preInts, lgK, fiCol, flags, seedHash);

    wmem.putInt(getHiFieldOffset(format, HiField.NUM_COUPONS), (int) numCoupons);
    wmem.putInt(getHiFieldOffset(format, HiField.CSV_LENGTH), (int) csvLength);
    wmem.putDouble(getHiFieldOffset(format, HiField.KXP), kxp);
    wmem.putDouble(getHiFieldOffset(format, HiField.HIP_ACCUM), hipAccum);
    wmem.putIntArray(getCsvStreamOffset(wmem), csvStream, 0, (int) csvLength);
  }

  static void putPinnedSlidingMergedNoSv(final WritableMemory wmem,
      final byte lgK,
      final byte fiCol,
      final long numCoupons,
      final long cwLength,
      final short seedHash,
      final int[] cwStream) {
    final Format format = Format.PINNED_SLIDING_MERGED_NOSV;
    final byte preInts = getDefinedPreInts(format);
    final byte flags = (byte) ((format.ordinal() << 2) | READ_ONLY_FLAG_MASK);
    checkCapacity(wmem.getCapacity(), 4L * (preInts + cwLength));
    putFirst8(wmem, preInts, lgK, fiCol, flags, seedHash);

    wmem.putInt(getHiFieldOffset(format, HiField.NUM_COUPONS), (int) numCoupons);
    wmem.putInt(getHiFieldOffset(format, HiField.CW_LENGTH), (int) cwLength);
    wmem.putIntArray(getCwStreamOffset(wmem), cwStream, 0, (int) cwLength);
  }

  static void putPinnedSlidingHipNoSv(final WritableMemory wmem,
      final byte lgK,
      final byte fiCol,
      final long numCoupons,
      final long cwLength,
      final double kxp,
      final double hipAccum,
      final short seedHash,
      final int[] cwStream) {
    final Format format = Format.PINNED_SLIDING_HIP_NOSV;
    final byte preInts = getDefinedPreInts(format);
    final byte flags = (byte) ((format.ordinal() << 2) | READ_ONLY_FLAG_MASK);
    checkCapacity(wmem.getCapacity(), 4L * (preInts + cwLength));
    putFirst8(wmem, preInts, lgK, fiCol, flags, seedHash);

    wmem.putInt(getHiFieldOffset(format, HiField.NUM_COUPONS), (int) numCoupons);
    wmem.putInt(getHiFieldOffset(format, HiField.CW_LENGTH), (int) cwLength);
    wmem.putDouble(getHiFieldOffset(format, HiField.KXP), kxp);
    wmem.putDouble(getHiFieldOffset(format, HiField.HIP_ACCUM), hipAccum);
    wmem.putIntArray(getCwStreamOffset(wmem), cwStream, 0, (int) cwLength);
  }

  static void putPinnedSlidingMerged(final WritableMemory wmem,
      final byte lgK,
      final byte fiCol,
      final long numCoupons,
      final long numSV,
      final long csvLength,
      final long cwLength,
      final short seedHash,
      final int[] csvStream,
      final int[] cwStream) {
    final Format format = Format.PINNED_SLIDING_MERGED;
    final byte preInts = getDefinedPreInts(format);
    final byte flags = (byte) ((format.ordinal() << 2) | READ_ONLY_FLAG_MASK);
    checkCapacity(wmem.getCapacity(), 4L * (preInts + csvLength + cwLength));
    putFirst8(wmem, preInts, lgK, fiCol, flags, seedHash);

    wmem.putInt(getHiFieldOffset(format, HiField.NUM_COUPONS), (int) numCoupons);
    wmem.putInt(getHiFieldOffset(format, HiField.NUM_SV), (int) numSV);
    wmem.putInt(getHiFieldOffset(format, HiField.CSV_LENGTH), (int) csvLength);
    wmem.putInt(getHiFieldOffset(format, HiField.CW_LENGTH), (int) cwLength);
    wmem.putIntArray(getCsvStreamOffset(wmem), csvStream, 0, (int) csvLength);
    wmem.putIntArray(getCwStreamOffset(wmem), cwStream, 0, (int) cwLength);
  }

  static void putPinnedSlidingHip(final WritableMemory wmem,
      final byte lgK,
      final byte fiCol,
      final long numCoupons,
      final long numSV,
      final double kxp,
      final double hipAccum,
      final long csvLength,
      final long cwLength,
      final short seedHash,
      final int[] csvStream,
      final int[] cwStream) {
    final Format format = Format.PINNED_SLIDING_HIP;
    final byte preInts = getDefinedPreInts(format);
    final byte flags = (byte) ((format.ordinal() << 2) | READ_ONLY_FLAG_MASK);
    checkCapacity(wmem.getCapacity(), 4L * (preInts + csvLength + cwLength));
    putFirst8(wmem, preInts, lgK, fiCol, flags, seedHash);

    wmem.putInt(getHiFieldOffset(format, HiField.NUM_COUPONS), (int) numCoupons);
    wmem.putInt(getHiFieldOffset(format, HiField.NUM_SV), (int) numSV);
    wmem.putDouble(getHiFieldOffset(format, HiField.KXP), kxp);
    wmem.putDouble(getHiFieldOffset(format, HiField.HIP_ACCUM), hipAccum);
    wmem.putInt(getHiFieldOffset(format, HiField.CSV_LENGTH), (int) csvLength);
    wmem.putInt(getHiFieldOffset(format, HiField.CW_LENGTH), (int) cwLength);
    wmem.putIntArray(getCsvStreamOffset(wmem), csvStream, 0, (int) csvLength);
    wmem.putIntArray(getCwStreamOffset(wmem), cwStream, 0, (int) cwLength);
  }

  private static void putFirst8(final WritableMemory wmem, final byte preInts, final byte lgK,
      final byte fiCol, final byte flags, final short seedHash) {
    wmem.clear(0, 4 * preInts);
    wmem.putByte(getLoFieldOffset(LoField.PRE_INTS), preInts);
    wmem.putByte(getLoFieldOffset(LoField.SER_VERSION), SER_VER);
    wmem.putByte(getLoFieldOffset(LoField.FAMILY), (byte) Family.CPC.getID());
    wmem.putByte(getLoFieldOffset(LoField.LG_K), lgK);
    wmem.putByte(getLoFieldOffset(LoField.FI_COL), fiCol);
    wmem.putByte(getLoFieldOffset(LoField.FLAGS), flags);
    wmem.putShort(getLoFieldOffset(LoField.SEED_HASH), seedHash);
  }

  //TO STRING

  public static String toString(final byte[] byteArr, final boolean data) {
    final Memory mem = Memory.wrap(byteArr);
    return toString(mem, data);
  }

  public static String toString(final Memory mem, final boolean data) {
    //Lo Fields Preamble, first 7 fields, first 8 bytes
    final int preInts = mem.getByte(getLoFieldOffset(LoField.PRE_INTS)) & 0xFF;
    final int serVer = mem.getByte(getLoFieldOffset(LoField.SER_VERSION)) & 0xFF;
    final Family family = Family.idToFamily(mem.getByte(getLoFieldOffset(LoField.FAMILY)) & 0xFF);
    final int lgK = mem.getByte(getLoFieldOffset(LoField.LG_K)) & 0xFF;
    final int fiCol = mem.getByte(getLoFieldOffset(LoField.FI_COL)) & 0xFF;
    final int flags = mem.getByte(getLoFieldOffset(LoField.FLAGS)) & 0XFF;
    final int seedHash = mem.getShort(getLoFieldOffset(LoField.SEED_HASH)) & 0XFFFF;
    final String seedHashStr = Integer.toHexString(seedHash);

    //Flags of the Flags byte
    final String flagsStr = zeroPad(Integer.toBinaryString(flags), 8) + ", " + (flags);
    final boolean bigEndian = (flags & BIG_ENDIAN_FLAG_MASK) > 0;
    final boolean readOnly = (flags & READ_ONLY_FLAG_MASK) > 0;
    final boolean hasHip = (flags & HIP_FLAG_MASK) > 0;
    final boolean hasSV = (flags & SUP_VAL_FLAG_MASK) > 0;
    final boolean hasWindow = (flags & WINDOW_FLAG_MASK) > 0;

    final int formatOrdinal = (flags >>> 2) & 0x7;
    final Format format = Format.ordinalToFormat(formatOrdinal);

    final String nativeOrderStr = ByteOrder.nativeOrder().toString();

    long numCoupons = 0;
    long winOffset = 0;
    long csvLength = 0;
    long cwLength = 0;
    double kxp = 0;
    double hipAccum = 0;
    long csvStreamStart = 0;
    long cwStreamStart = 0;

    final StringBuilder sb = new StringBuilder();
    sb.append(LS);
    sb.append("### CPC SKETCH PREAMBLE:").append(LS);
    sb.append("Format                          : ").append(format.name()).append(LS);
    sb.append("Byte 0: Preamble Ints           : ").append(preInts).append(LS);
    sb.append("Byte 1: SerVer                  : ").append(serVer).append(LS);
    sb.append("Byte 2: Family                  : ").append(family).append(LS);
    sb.append("Byte 3: lgK                     : ").append(lgK).append(LS);
    sb.append("Byte 4: First Interesting Col   : ").append(fiCol).append(LS);
    sb.append("Byte 5: Flags                   : ").append(flagsStr).append(LS);
    sb.append("  BIG_ENDIAN_STORAGE            : ").append(bigEndian).append(LS);
    sb.append("  (Native Byte Order)           : ").append(nativeOrderStr).append(LS);
    sb.append("  READ_ONLY                     : ").append(readOnly).append(LS);
    sb.append("  Has HIP                       : ").append(hasHip).append(LS);
    sb.append("  Has Surprising Values         : ").append(hasSV).append(LS);
    sb.append("  Has Window                    : ").append(hasWindow).append(LS);
    sb.append("Byte 6, 7: Seed Hash            : ").append(seedHashStr).append(LS);

    switch (format) {
      case EMPTY :
      case NONE : break;
      case SPARSE_HYBRID_MERGED : {
        numCoupons = mem.getInt(getHiFieldOffset(format, HiField.NUM_COUPONS)) & 0xFFFF_FFFFL;
        csvLength = mem.getInt(getHiFieldOffset(format, HiField.CSV_LENGTH)) & 0xFFFF_FFFFL;
        csvStreamStart = getCsvStreamOffset(mem);
        sb.append("NumCoupons                      : ").append(numCoupons).append(LS);
        sb.append("Window Offset                   : ").append(winOffset).append(LS);
        sb.append("CsvLength                       : ").append(csvLength).append(LS);
        sb.append("CSV Stream Start                : ").append(csvStreamStart).append(LS);
        if (data) {
          sb.append(LS).append("CSV Stream:").append(LS);
          listData(mem, csvStreamStart, csvLength, sb);
        }
        break;
      }
      case SPARSE_HYBRID_HIP : {
        numCoupons = mem.getInt(getHiFieldOffset(format, HiField.NUM_COUPONS)) & 0xFFFF_FFFFL;
        csvLength = mem.getInt(getHiFieldOffset(format, HiField.CSV_LENGTH)) & 0xFFFF_FFFFL;
        kxp = mem.getDouble(getHiFieldOffset(format, HiField.KXP));
        hipAccum = mem.getDouble(getHiFieldOffset(format, HiField.HIP_ACCUM));
        csvStreamStart = getCsvStreamOffset(mem);
        sb.append("NumCoupons                      : ").append(numCoupons).append(LS);
        sb.append("Window Offset                   : ").append(winOffset).append(LS);
        sb.append("CsvLength                       : ").append(csvLength).append(LS);
        sb.append("KxP                             : ").append(kxp).append(LS);
        sb.append("HipAccum                        : ").append(hipAccum).append(LS);
        sb.append("CSV Stream Start                : ").append(csvStreamStart).append(LS);
        if (data) {
          sb.append(LS).append("CSV Stream:").append(LS);
          listData(mem, csvStreamStart, csvLength, sb);
        }
        break;
      }
      case PINNED_SLIDING_MERGED_NOSV : {
        numCoupons = mem.getInt(getHiFieldOffset(format, HiField.NUM_COUPONS)) & 0xFFFF_FFFFL;
        winOffset = determineCorrectOffset(lgK, numCoupons);
        cwLength = mem.getInt(getHiFieldOffset(format, HiField.CW_LENGTH)) & 0xFFFF_FFFFL;
        cwStreamStart = getCwStreamOffset(mem);
        sb.append("NumCoupons                      : ").append(numCoupons).append(LS);
        sb.append("Window Offset                   : ").append(winOffset).append(LS);
        sb.append("CwLength                        : ").append(cwLength).append(LS);
        sb.append("CwStreamStart                   : ").append(cwStreamStart).append(LS);
        if (data) {
          sb.append(LS).append("CW Stream:").append(LS);
          listData(mem, cwStreamStart, cwLength, sb);
        }
        break;
      }
      case PINNED_SLIDING_HIP_NOSV : {
        numCoupons = mem.getInt(getHiFieldOffset(format, HiField.NUM_COUPONS)) & 0xFFFF_FFFFL;
        winOffset = determineCorrectOffset(lgK, numCoupons);
        cwLength = mem.getInt(getHiFieldOffset(format, HiField.CW_LENGTH)) & 0xFFFF_FFFFL;
        kxp = mem.getDouble(getHiFieldOffset(format, HiField.KXP));
        hipAccum = mem.getDouble(getHiFieldOffset(format, HiField.HIP_ACCUM));
        cwStreamStart = getCwStreamOffset(mem);
        sb.append("NumCoupons                      : ").append(numCoupons).append(LS);
        sb.append("Window Offset                   : ").append(winOffset).append(LS);
        sb.append("CwLength                        : ").append(cwLength).append(LS);
        sb.append("KxP                             : ").append(kxp).append(LS);
        sb.append("HipAccum                        : ").append(hipAccum).append(LS);
        sb.append("CwStreamStart                   : ").append(cwStreamStart).append(LS);
        if (data) {
          sb.append(LS).append("CW Stream:").append(LS);
          listData(mem, cwStreamStart, cwLength, sb);
        }
        break;
      }
      case PINNED_SLIDING_MERGED : {
        numCoupons = mem.getInt(getHiFieldOffset(format, HiField.NUM_COUPONS) & 0xFFFF_FFFFL);
        winOffset = determineCorrectOffset(lgK, numCoupons);
        csvLength = mem.getInt(getHiFieldOffset(format, HiField.CSV_LENGTH)) & 0xFFFF_FFFFL;
        cwLength = mem.getInt(getHiFieldOffset(format, HiField.CW_LENGTH)) & 0xFFFF_FFFFL;
        csvStreamStart = getCsvStreamOffset(mem);
        cwStreamStart = (csvLength == 0) ? -1L : getCwStreamOffset(mem);
        sb.append("NumCoupons                      : ").append(numCoupons).append(LS);
        sb.append("Window Offset                   : ").append(winOffset).append(LS);
        sb.append("CsvLength                       : ").append(csvLength).append(LS);
        sb.append("CwLength                        : ").append(cwLength).append(LS);
        sb.append("CsvStreamStart                  : ").append(csvStreamStart).append(LS);
        sb.append("CwStreamStart                   : ").append(cwStreamStart).append(LS);
        if (data) {
          sb.append(LS).append("CSV Stream:").append(LS);
          listData(mem, csvStreamStart, csvLength, sb);
          sb.append(LS).append("CW Stream:").append(LS);
          listData(mem, cwStreamStart, cwLength, sb);
        }
        break;
      }
      case PINNED_SLIDING_HIP : {
        numCoupons = mem.getInt(getHiFieldOffset(format, HiField.NUM_COUPONS) & 0xFFFF_FFFFL);
        winOffset = determineCorrectOffset(lgK, numCoupons);
        csvLength = mem.getInt(getHiFieldOffset(format, HiField.CSV_LENGTH)) & 0xFFFF_FFFFL;
        cwLength = mem.getInt(getHiFieldOffset(format, HiField.CW_LENGTH)) & 0xFFFF_FFFFL;
        kxp = mem.getDouble(getHiFieldOffset(format, HiField.KXP));
        hipAccum = mem.getDouble(getHiFieldOffset(format, HiField.HIP_ACCUM));
        csvStreamStart = getCsvStreamOffset(mem);
        cwStreamStart = (csvLength == 0) ? -1L : getCwStreamOffset(mem);
        sb.append("NumCoupons                      : ").append(numCoupons).append(LS);
        sb.append("Window Offset                   : ").append(winOffset).append(LS);
        sb.append("CsvLength                       : ").append(csvLength).append(LS);
        sb.append("CwLength                        : ").append(cwLength).append(LS);
        sb.append("KxP                             : ").append(kxp).append(LS);
        sb.append("HipAccum                        : ").append(hipAccum).append(LS);
        sb.append("CsvStreamStart                  : ").append(csvStreamStart).append(LS);
        sb.append("CwStreamStart                   : ").append(cwStreamStart).append(LS);
        if (data) {
          sb.append(LS).append("CSV Stream:").append(LS);
          listData(mem, csvStreamStart, csvLength, sb);
          sb.append(LS).append("CW Stream:").append(LS);
          listData(mem, cwStreamStart, cwLength, sb);
        }
        break;
      }
    }
    sb.append("### END CPC SKETCH PREAMBLE").append(LS);
    return sb.toString();
  } //end toString(mem)

  private static void listData(final Memory mem, final long offset, final long length,
      final StringBuilder sb) {
    for (int i = 0; i < length; i++) {
      sb.append(String.format(fmt, i, mem.getInt(offset + (4 * i)))).append(LS);
    }
  }


  static void fieldError(final Format format, final HiField hiField) {
    throw new SketchesArgumentException(
        "Operation is illegal: Format = " + format.name() + ", HiField = " + hiField);
  }

  static void checkCapacity(final long memCap, final long expectedCap) {
    if (memCap < expectedCap) {
      throw new SketchesArgumentException(
          "Insufficient WritableMemory Capacity = " + memCap + ", Expected = " + expectedCap);
    }
  }

  //TODO move to a common util class
  static long determineCorrectOffset(final long lgK, final long c) {
    final long k = (1L << lgK);
    final long tmp = (c << 3) - (19 * k);        // 8C - 19K
    if (tmp < 0) { return 0; }
    return tmp >> (lgK + 3L); // tmp / 8K
  }

}
//@formatter:on
