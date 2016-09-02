/*
 * Copyright 2016, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.quantiles;

import com.yahoo.memory.Memory;
import com.yahoo.memory.NativeMemory;

import java.nio.ByteOrder;

import static com.yahoo.sketches.Family.idToFamily;
import static com.yahoo.sketches.quantiles.Util.LS;
import static com.yahoo.sketches.quantiles.Util.computeRetainedItems;

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
 *  0   ||------SerDeId----|--------K--------|  Flags | FamID  | SerVer | Preamble_Longs |
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
  static final int SER_DE_ID_SHORT            = 6;  //to 7 
  static final int N_LONG                     = 8;  //to 15
  
  //After Preamble:
  static final int MIN_DOUBLE                 = 16; //to 23 (Only for DoublesSketch)
  static final int MAX_DOUBLE                 = 24; //to 31 (Only for DoublesSketch)
  
  //Specific values for this implementation
  static final int SER_VER                    = 2;

  // flag bit masks
  static final int BIG_ENDIAN_FLAG_MASK       = 1;
  //static final int READ_ONLY_FLAG_MASK        = 2;   //reserved
  static final int EMPTY_FLAG_MASK            = 4;
  //static final int COMPACT_FLAG_MASK          = 8;   //reserved
  //static final int ORDERED_FLAG_MASK          = 16;  //reserved
  
  static final boolean NATIVE_ORDER_IS_BIG_ENDIAN  = 
      (ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN);
  
  // STRINGS
  /**
   * Returns a human readable string summary of the internal state of the given byte array. 
   * Used primarily in testing.
   * 
   * @param byteArr the given byte array.
   * @return the summary string.
   */
  public static String toString(byte[] byteArr) {
    Memory mem = new NativeMemory(byteArr);
    return toString(mem);
  }
  
  /**
   * Returns a human readable string summary of the internal state of the given Memory. 
   * Used primarily in testing.
   * 
   * @param mem the given Memory
   * @return the summary string.
   */
  public static String toString(Memory mem) {
    return memoryToString(mem);
  }

  private static String memoryToString(Memory mem) {
    //pre0
    int preLongs = (mem.getByte(PREAMBLE_LONGS_BYTE)) & 0XFF; //either 1 or 2
    int serVer = mem.getByte(SER_VER_BYTE);
    int familyID = mem.getByte(FAMILY_BYTE);
    String famName = idToFamily(familyID).toString();
    int flags = mem.getByte(FLAGS_BYTE);
    boolean bigEndian = (flags & BIG_ENDIAN_FLAG_MASK) > 0;
    String nativeOrder = ByteOrder.nativeOrder().toString();
    boolean empty = (flags & EMPTY_FLAG_MASK) > 0;
    int k = mem.getShort(K_SHORT);
    short type = mem.getShort(SER_DE_ID_SHORT);
    
    long n;
    if (preLongs == 1) {
      n = 0;
    } else { // preLongs == 2
      n = mem.getLong(N_LONG);
    } 
    
    StringBuilder sb = new StringBuilder();
    sb.append(LS);
    sb.append("### QUANTILES SKETCH PREAMBLE SUMMARY:").append(LS);
    sb.append("Byte  0: Preamble Longs       : ").append(preLongs).append(LS);
    sb.append("Byte  1: Serialization Version: ").append(serVer).append(LS);
    sb.append("Byte  2: Family               : ").append(famName).append(LS);
    sb.append("Byte  3: Flags Field          : ").append(String.format("%02o", flags)).append(LS);
    sb.append("  BIG_ENDIAN_STORAGE          : ").append(bigEndian).append(LS);
    sb.append("  (Native Byte Order)         : ").append(nativeOrder).append(LS);
    sb.append("  EMPTY                       : ").append(empty).append(LS);
    sb.append("Bytes  4-5  : K               : ").append(k).append(LS);
    sb.append("Byte  6: SKETCH_TYPE          : ").append(type).append(LS);
    //Byte 7 not used
    if (preLongs == 1) {
      sb.append(" --ABSENT, ASSUMED:").append(LS);
    }
    sb.append("Bytes  8-15 : N                : ").append(n).append(LS);
    sb.append("Retained Items                 : ").append(computeRetainedItems(k, n)).append(LS);
    sb.append("Total Bytes                    : ").append(mem.getCapacity()).append(LS);
    sb.append("### END SKETCH PREAMBLE SUMMARY").append(LS);
    return sb.toString();
  }
  
//@formatter:on
  
  static int extractPreLongs(final long pre0) {
    long mask = 0XFFL;
    return (int) (pre0 & mask);
  }
  
  static int extractSerVer(final long pre0) {
    int shift = SER_VER_BYTE << 3;
    long mask = 0XFFL;
    return (int) ((pre0 >>> shift) & mask);
  }
  
  static int extractFamilyID(final long pre0) {
    int shift = FAMILY_BYTE << 3;
    long mask = 0XFFL;
    return (int) ((pre0 >>> shift) & mask);
  }
  
  static int extractFlags(final long pre0) {
    int shift = FLAGS_BYTE << 3;
    long mask = 0XFFL;
    return (int) ((pre0 >>> shift) & mask);
  }
  
  static int extractK(final long pre1) {
    int shift = K_SHORT << 3;
    long mask = 0XFFFFL;
    return (int) ((pre1 >>> shift) & mask);
  }

  static short extractSerDeId(final long pre0) {
    final int shift = SER_DE_ID_SHORT << 3;
    final long mask = 0XFFFFL;
    return (short) ((pre0 >>> shift) & mask);
  }

  static long insertPreLongs(final int preLongs, final long pre0) {
    long mask = 0XFFL;
    return (preLongs & mask) | (~mask & pre0);
  }

  static long insertSerVer(final int serVer, final long pre0) {
    int shift = SER_VER_BYTE << 3;
    long mask = 0XFFL;
    return ((serVer & mask) << shift) | (~(mask << shift) & pre0);
  }

  static long insertFamilyID(final int familyID, final long pre0) {
    int shift = FAMILY_BYTE << 3;
    long mask = 0XFFL;
    return ((familyID & mask) << shift) | (~(mask << shift) & pre0);
  }

  static long insertFlags(final int flags, final long pre0) {
    int shift = FLAGS_BYTE << 3;
    long mask = 0XFFL;
    return ((flags & mask) << shift) | (~(mask << shift) & pre0);
  }

  static long insertK(final int k, final long pre0) {
    int shift = K_SHORT << 3;
    long mask = 0XFFFFL;
    return ((k & mask) << shift) | (~(mask << shift) & pre0);
  }

  static long insertSerDeId(final short serDeId, final long pre0) {
    final int shift = SER_DE_ID_SHORT << 3;
    final long mask = 0XFFFFL;
    return ((serDeId & mask) << shift) | (~(mask << shift) & pre0);
  }

}
