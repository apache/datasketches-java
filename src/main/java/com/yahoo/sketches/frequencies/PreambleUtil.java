/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the Apache License 2.0. See LICENSE file
 * at the project root for terms.
 */

package com.yahoo.sketches.frequencies;

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
 * Long || Start Byte Adr:
 * Adr: 
 *      ||    7     |    6   |    5   |    4   |    3   |    2   |    1   |     0          |
 *  0   |||--------k---------------------------|--flag--| FamID  | SerVer | Preamble_Longs |
 *      ||    15    |   14   |   13   |   12   |   11   |   10   |    9   |     8          |
 *  1   ||---------------------------------mergeError--------------------------------------|
 *      ||    23    |   22   |   21   |   20   |   19   |   18   |   17   |    16          |
 *  2   ||---------------------------------offset------------------------------------------|
 *      ||    31    |   30   |   29   |   28   |   27   |   26   |   25   |    24          |
 *  3   ||-----------------------------------streamLength----------------------------------|
 *      ||    39    |   38   |   37   |   36   |   35   |   34   |   33   |    32          |
 *  4   ||------initialSize--------------------|-------------------K-----------------------|
 *      ||    47    |   46   |   45   |   44   |   43   |   42   |   41   |   40           |
 *  5   ||------------(unused)-----------------|--------bufferlength-----------------------|
 *      ||    55    |   54   |   53   |   52   |   51   |   50   |   49   |   48           |
 *  6   ||----------start of keys buffer, followed by values buffer------------------------|
 * 
 * </pre>
 * 
 * @author Justin Thaler
 */
// @formatter:on
final class PreambleUtil {

  private PreambleUtil() {}

  // ###### DO NOT MESS WITH THIS FROM HERE ...
  // Preamble byte Addresses
  static final int PREAMBLE_LONGS_BYTE = 0; // either 1 or 5
  static final int SER_VER_BYTE = 1;
  static final int FAMILY_BYTE = 2;
  static final int FLAG_START = 3;
  static final int LOWER_K_START = 4; // to 7
  static final int MERGE_ERROR_START = 8; // to 15
  static final int OFFSET_START = 16; // to 23
  static final int STREAMLENGTH_START = 24; // to 31
  static final int UPPER_K_START = 32; // to 35
  static final int INITIALSIZE_START = 36; // to 39
  static final int BUFFERLENGTH_START = 40; // to 43
  // Specific values for this implementation
  static final int SER_VER = 1;

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

  static int extractEmptyFlag(final long pre0) {
    int shift = FLAG_START << 3;
    long mask = 0XFFL;
    return (int) ((pre0 >>> shift) & mask);
  }


  static int extractLowerK(final long pre1) {
    int shift = LOWER_K_START << 3;
    long mask = 0XFFFFFFFFL;
    return (int) ((pre1 >>> shift) & mask);
  }

  static int extractUpperK(final long pre1) {
    long mask = 0XFFFFFFFFL;
    return (int) (pre1 & mask);
  }

  static int extractBufferLength(final long pre2) {
    long mask = 0XFFFFFFFFL;
    return (int) (pre2 & mask);
  }

  static int extractInitialSize(final long pre1) {
    long mask = 0XFFFFFFFFL;
    int shift = (INITIALSIZE_START - UPPER_K_START) << 3;
    return (int) ((pre1 >>> shift) & mask);
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

  static long insertEmptyFlag(final int flag, final long pre0) {
    int shift = FLAG_START << 3;
    long mask = 0XFFL;
    return ((flag & mask) << shift) | (~(mask << shift) & pre0);
  }

  static long insertLowerK(final int k, final long pre0) {
    int shift = LOWER_K_START << 3;
    long mask = 0XFFFFFFFFL;
    return ((k & mask) << shift) | (~(mask << shift) & pre0);
  }

  static long insertUpperK(final int K, final long pre1) {
    long mask = 0XFFFFFFFFL;
    return (K & mask) | (~mask & pre1);
  }

  static long insertInitialSize(final int initialSize, final long pre1) {
    long mask = 0XFFFFFFFFL;
    int shift = (INITIALSIZE_START - UPPER_K_START) << 3;
    return ((initialSize & mask) << shift) | (~(mask << shift) & pre1);
  }

  static long insertBufferLength(final int bufferLength, final long pre2) {
    long mask = 0XFFFFFFFFL;
    return (bufferLength & mask) | (~mask & pre2);
  }

}
