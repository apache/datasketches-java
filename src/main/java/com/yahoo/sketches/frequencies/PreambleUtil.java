/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the Apache License 2.0. See LICENSE file
 * at the project root for terms.
 */

package com.yahoo.sketches.frequencies;

//import static com.yahoo.sketches.Util.LS;
//import static com.yahoo.sketches.Util.zeroPad;
//
//import java.nio.ByteOrder;
//
//import com.yahoo.sketches.Family;
//import com.yahoo.sketches.ResizeFactor;
import com.yahoo.sketches.memory.Memory;
//import com.yahoo.sketches.memory.NativeMemory;

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
 * An empty FrequentItems only requires 8 bytes. All others require 40 bytes of preamble.
 * </p>
 * 
 * <pre>
 *  * Long || Start Byte Adr:
 * Adr: 
 *      ||    7     |    6   |    5   |    4   |    3   |    2   |    1   |     0          |
 *  0   ||----------|--Type--|-Flags--|-LgCur--| LgMax  | FamID  | SerVer | PreambleLongs  |
 *      ||    15    |   14   |   13   |   12   |   11   |   10   |    9   |     8          |
 *  1   ||------------(unused)-----------------|--------ActiveItems------------------------|
 *      ||    23    |   22   |   21   |   20   |   19   |   18   |   17   |    16          |
 *  2   ||-----------------------------------streamLength----------------------------------|
 *      ||    31    |   30   |   29   |   28   |   27   |   26   |   25   |    24          |
 *  3   ||---------------------------------offset------------------------------------------|
 *      ||    39    |   38   |   37   |   36   |   35   |   34   |   33   |    32          |
 *  4   ||---------------------------------mergeError--------------------------------------|
 *      ||    47    |   46   |   45   |   44   |   43   |   42   |   41   |    40          |
 *  5   ||----------start of values buffer, followed by keys buffer------------------------|
 * </pre>
 * 
 * @author Justin Thaler
 */
final class PreambleUtil {

  private PreambleUtil() {}

  // ###### DO NOT MESS WITH THIS FROM HERE ...
  // Preamble byte Addresses
  static final int PREAMBLE_LONGS_BYTE       = 0; // either 1 or 6
  static final int SER_VER_BYTE              = 1;
  static final int FAMILY_BYTE               = 2;
  static final int LG_MAX_MAP_SIZE_BYTE      = 3;
  static final int LG_CUR_MAP_SIZE_BYTE      = 4;
  static final int FLAGS_BYTE                = 5;
  static final int FREQ_SKETCH_TYPE_BYTE     = 6;
  static final int ACTIVE_ITEMS_INT          = 8;  // to 11 : 0 to 4 in pre1
  static final int STREAMLENGTH_LONG         = 16; // to 23 : pre2
  static final int OFFSET_LONG               = 24; // to 31 : pre3
  static final int MERGE_ERROR_LONG          = 32; // to 39 : pre4
  
  
  // flag bit masks
  static final int EMPTY_FLAG_MASK      = 4;
  
  
  // Specific values for this implementation
  static final int SER_VER = 1;
  static final int FREQ_SKETCH_TYPE = 1;

  
  /**
   * Returns a human readable string summary of the preamble state of the given Memory. 
   * Note: other than making sure that the given Memory size is large
   * enough for just the preamble, this does not do much value checking of the contents of the 
   * preamble as this is primarily a tool for debugging the preamble visually.
   * 
   * @param mem the given Memory.
   * @return the summary preamble string.
   */
//  public static String preambleToString(Memory mem) {
//    int preLongs = getAndCheckPreLongs(mem);  //make sure we can get the assumed preamble
//    long pre0 = mem.getLong(0);
//    int serVer = extractSerVer(pre0);
//    Family family = Family.idToFamily(extractFamilyID(pre0));
//    
//    //Flags
//    int flags = extractFlags(pre0);
//    String flagsStr = zeroPad(Integer.toBinaryString(flags), 8) + ", " + (flags);
//    boolean empty = (flags & EMPTY_FLAG_MASK) > 0;
//    
//    int maxMapSize = extractMaxMapSize(pre0);
//    
//    //Assumed if preLongs == 1
//    long mergeError = 0;
//    long offset = 0;
//    long streamLength = 0;
//    int curMapSize = 
//    //Assumed if preLongs == 1 or 2
//    
//    //Assumed if preLongs == 1 or 2 or 3
//
//    
//    if (preLongs == 2) {
//      long pre1 = mem.getLong(1);
//      curCount = extractCurCount(pre1);
//      p = extractP(pre1);
//      thetaLong = (long)(p * MAX_THETA_LONG_AS_DOUBLE);
//      thetaULong = thetaLong;
//    } 
//    else if (preLongs == 3){
//      long pre1 = mem.getLong(1);
//      curCount = extractCurCount(pre1);
//      p = extractP(pre1);
//      thetaLong = mem.getLong(THETA_LONG);
//      thetaULong = thetaLong;
//    } 
//    else if (preLongs == 4) {
//      long pre1 = mem.getLong(1);
//      curCount = extractCurCount(pre1);
//      p = extractP(pre1);
//      thetaLong = mem.getLong(THETA_LONG);
//      thetaULong = mem.getLong(UNION_THETA_LONG);
//    } //else: the same as preLongs == 1
//    double thetaDbl = thetaLong / MAX_THETA_LONG_AS_DOUBLE;
//    String thetaHex = zeroPad(Long.toHexString(thetaLong), 16);
//    double thetaUDbl = thetaULong / MAX_THETA_LONG_AS_DOUBLE;
//    String thetaUHex = zeroPad(Long.toHexString(thetaULong), 16);
//    
//    StringBuilder sb = new StringBuilder();
//    sb.append(LS)
//      .append("### SKETCH PREAMBLE SUMMARY:").append(LS)
//      .append("Byte  0: Preamble Longs       : ").append(preLongs).append(LS)
//      .append("Byte  0: ResizeFactor         : ").append(rf.toString()).append(LS)
//      .append("Byte  1: Serialization Version: ").append(serVer).append(LS)
//      .append("Byte  2: Family               : ").append(family.toString()).append(LS)
//      .append("Byte  3: LgNomLongs           : ").append(lgNomLongs).append(LS)
//      .append("Byte  4: LgArrLongs           : ").append(lgArrLongs).append(LS)
//      .append("Byte  5: Flags Field          : ").append(flagsStr).append(LS)
//      .append("  BIG_ENDIAN_STORAGE          : ").append(bigEndian).append(LS)
//      .append("  (Native Byte Order)         : ").append(nativeOrder).append(LS)
//      .append("  READ_ONLY                   : ").append(readOnly).append(LS)
//      .append("  EMPTY                       : ").append(empty).append(LS)
//      .append("  COMPACT                     : ").append(compact).append(LS)
//      .append("  ORDERED                     : ").append(ordered).append(LS)
//      .append("Bytes 6-7  : Seed Hash        : ").append(Integer.toHexString(seedHash)).append(LS);
//    if (preLongs == 1) {
//      sb.append(" --ABSENT, ASSUMED:").append(LS);
//      sb.append("Bytes 8-11 : CurrentCount     : ").append(curCount).append(LS)
//        .append("Bytes 12-15: P                : ").append(p).append(LS);
//      sb.append("Bytes 16-23: Theta (double)   : ").append(thetaDbl).append(LS)
//        .append("             Theta (long)     : ").append(thetaLong).append(LS)
//        .append("             Theta (long,hex) : ").append(thetaHex).append(LS);
//    }
//    if (preLongs == 2) {
//      sb.append("Bytes 8-11 : CurrentCount     : ").append(curCount).append(LS)
//        .append("Bytes 12-15: P                : ").append(p).append(LS);
//      sb.append(" --ABSENT, ASSUMED:").append(LS);
//      sb.append("Bytes 16-23: Theta (double)   : ").append(thetaDbl).append(LS)
//        .append("             Theta (long)     : ").append(thetaLong).append(LS)
//        .append("             Theta (long,hex) : ").append(thetaHex).append(LS);
//    }
//    if (preLongs == 3) {
//      sb.append("Bytes 8-11 : CurrentCount     : ").append(curCount).append(LS)
//        .append("Bytes 12-15: P                : ").append(p).append(LS);
//      sb.append("Bytes 16-23: Theta (double)   : ").append(thetaDbl).append(LS)
//        .append("             Theta (long)     : ").append(thetaLong).append(LS)
//        .append("             Theta (long,hex) : ").append(thetaHex).append(LS);
//    }
//    if (preLongs == 4) {
//      sb.append("Bytes 8-11 : CurrentCount     : ").append(curCount).append(LS)
//        .append("Bytes 12-15: P                : ").append(p).append(LS);
//      sb.append("Bytes 16-23: Theta (double)   : ").append(thetaDbl).append(LS)
//        .append("             Theta (long)     : ").append(thetaLong).append(LS)
//        .append("             Theta (long,hex) : ").append(thetaHex).append(LS);
//      sb.append("Bytes 25-31: ThetaU (double)  : ").append(thetaUDbl).append(LS)
//        .append("             ThetaU (long)    : ").append(thetaULong).append(LS)
//        .append("             ThetaU (long,hex): ").append(thetaUHex).append(LS);
//    }
//    sb.append(  "Preamble Bytes                : ").append(preLongs * 8).append(LS);
//    sb.append(  "Data Bytes                    : ").append(curCount * 8).append(LS);
//    sb.append(  "TOTAL Sketch Bytes            : ").append(mem.getCapacity()).append(LS)
//      .append("### END SKETCH PREAMBLE SUMMARY").append(LS);
//    return sb.toString();
//  }

// @formatter:on
  
  static int extractPreLongs(final long pre0) { //Byte 0
    long mask = 0XFFL;
    return (int) (pre0 & mask);
  }

  static int extractSerVer(final long pre0) { //Byte 1
    int shift = SER_VER_BYTE << 3;
    long mask = 0XFFL;
    return (int) ((pre0 >>> shift) & mask);
  }

  static int extractFamilyID(final long pre0) { //Byte 2
    int shift = FAMILY_BYTE << 3;
    long mask = 0XFFL;
    return (int) ((pre0 >>> shift) & mask);
  }

  static int extractLgMaxMapSize(final long pre0) { //Byte 3
    int shift = LG_MAX_MAP_SIZE_BYTE << 3;
    long mask = 0XFFL;
    return (int) ((pre0 >>> shift) & mask);
  }
  
  static int extractLgCurMapSize(final long pre0) { //Byte 4
    int shift = LG_CUR_MAP_SIZE_BYTE << 3;
    long mask = 0XFFL;
    return (int) ((pre0 >>> shift) & mask);
  }
  
  static int extractFlags(final long pre0) { //Byte 5
    int shift = FLAGS_BYTE << 3;
    long mask = 0XFFL;
    return (int) ((pre0 >>> shift) & mask);
  }
  
  static int extractFreqSketchType(final long pre0) { //Byte 7
    int shift = FREQ_SKETCH_TYPE_BYTE << 3;
    long mask = 0XFFL;
    return (int) ((pre0 >>> shift) & mask);
  }
  
  static int extractActiveItems(final long pre1) { //Bytes 8 to 11
    long mask = 0XFFFFFFFFL;
    return (int) (pre1 & mask) ;
  }

  static long insertPreLongs(final int preLongs, final long pre0) { //Byte 0
    long mask = 0XFFL;
    return (preLongs & mask) | (~mask & pre0);
  }

  static long insertSerVer(final int serVer, final long pre0) { //Byte 1
    int shift = SER_VER_BYTE << 3;
    long mask = 0XFFL;
    return ((serVer & mask) << shift) | (~(mask << shift) & pre0); 
  }

  static long insertFamilyID(final int familyID, final long pre0) { //Byte 2
    int shift = FAMILY_BYTE << 3;
    long mask = 0XFFL;
    return ((familyID & mask) << shift) | (~(mask << shift) & pre0);
  }

  static long insertLgMaxMapSize(final int lgMaxMapSize, final long pre0) { //Byte 3
    int shift = LG_MAX_MAP_SIZE_BYTE << 3;
    long mask = 0XFFL;
    return ((lgMaxMapSize & mask) << shift) | (~(mask << shift) & pre0);
  }

  static long insertLgCurMapSize(final int lgCurMapSize, final long pre0) { //Byte 4
    int shift = LG_CUR_MAP_SIZE_BYTE << 3;
    long mask = 0XFFL;
    return ((lgCurMapSize & mask) << shift) | (~(mask << shift) & pre0);
  }

  static long insertFlags(final int flags, final long pre0) { //Byte 5
    int shift = FLAGS_BYTE << 3;
    long mask = 0XFFL;
    return ((flags & mask) << shift) | (~(mask << shift) & pre0);
  }

  static long insertFreqSketchType(final int freqSketchType, final long pre0) { //Byte 7
    int shift = FREQ_SKETCH_TYPE_BYTE << 3;
    long mask = 0XFFL;
    return ((freqSketchType & mask) << shift) | (~(mask << shift) & pre0);
  }
  
  static long insertActiveItems(final int activeItems, final long pre1) { //Bytes 8 to 11
    long mask = 0XFFFFFFFFL;
    return (activeItems & mask) | (~mask & pre1);
  }

  /**
   * Checks Memory for capacity to hold the preamble and returns the first 8 bytes.
   * @param mem the given Memory
   * @param max the max value for preLongs
   * @return the first 8 bytes of preamble as a long.
   */
  static long getAndCheckPreLongs(Memory mem) {
    long cap = mem.getCapacity();
    if (cap < 8) { throwNotBigEnough(cap, 8); }
    long pre0 = mem.getLong(0);
    int preLongs = extractPreLongs(pre0);
    int required = Math.max(preLongs << 3, 8);
    if (cap < required) { throwNotBigEnough(cap, required); }
    return pre0;
  }
  
  private static void throwNotBigEnough(long cap, int required) {
    throw new IllegalArgumentException(
        "Possible Corruption: Size of byte array or Memory not large enough: Size: " + cap 
        + ", Required: " + required);
  }
  
}
