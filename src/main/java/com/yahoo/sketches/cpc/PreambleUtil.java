/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.cpc;

//@formatter:off
/**
 * <pre>
 * EMPTY Layout
 * Long adr || Big Endian Illustration
 *          ||    7   |    6   |    5   |    4   |    3   |    2   |    1   |    0   |
 *      0   ||                 |-Flags--|        |---lgK--|-FamID--|-SerVer-|--PI=2--|
 *
 *
 * SPARSE/HYBRID Layout (without HIP registers)
 * Long adr || Big Endian Illustration
 *          ||    7   |    6   |    5   |    4   |    3   |    2   |    1   |    0   |
 *      0   ||                 |-Flags--|        |---lgK--|-FamID--|-SerVer-|--PI=4--|
 *
 *          ||   15   |   14   |   13   |   12   |   11   |   10   |    9   |    8   |
 *      1   ||-------------csvLength-------------|------------numCoupons-------------|
 *
 *          ||   23   |   22   |   21   |   20   |   19   |   18   |   17   |   16   |
 *      2   ||                                   |<---------Start of csv bit stream--|
 *
 *
 * SPARSE/HYBRID Layout (with HIP registers)
 * Long adr || Big Endian Illustration
 *          ||    7   |    6   |    5   |    4   |    3   |    2   |    1   |    0   |
 *      0   ||                 |-Flags--|        |---lgK--|-FamID--|-SerVer-|--PI=8--|
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
 * PINNED/SLIDING Layout (without HIP registers)
 * Long adr || Big Endian Illustration
 *          ||    7   |    6   |    5   |    4   |    3   |    2   |    1   |    0   |
 *      0   ||        |-offset-|-Flags--|-FIcol--|---lgK--|-FamID--|-SerVer-|--PI=5--|
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
 * PINNED/SLIDING Layout (with HIP registers)
 * Long adr || Big Endian Illustration
 *          ||    7   |    6   |    5   |    4   |    3   |    2   |    1   |    0   |
 *      0   ||        |-offset-|-Flags--|-FIcol--|---lgK--|-FamID--|-SerVer-|--PI=9--|
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
 */



/**
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
  static final int MERGE_FLAG_MASK          = 8;

  // Preamble byte start addresses
  // First 8 Bytes:
  static int PREAMBLE_INTS_BYTE             = 0; //PI
  static int SER_VER_BYTE                   = 1;
  static int FAMILY_BYTE                    = 2;
  static int LG_K_BYTE                      = 3;
  static int FI_COL_BYTE                    = 4; //First Interesting Column
  static int FLAGS_BYTE                     = 5;
  static int WINDOW_OFFSET_BYTE             = 6;

  static int NUM_COUPONS_INT                = 8;
  static int CSV_LENGTH_INT                 = 12;

  //If MERGE_FLAG is NOT set:
  static int HIP_KxP_DOUBLE                 = 16;
  static int HIP_ACCUM_DOUBLE               = 24;
  static int CW_LEN_HIP_INT                 = 32;
  static int CSV_STREAM_HIP                 = 36;

  //If MERGE_FLAG is set:
  static int CW_LEN_NO_HIP_INT              = 16;
  static int CSV_STREAM_NO_HIP              = 20;
  //NOTE: cwStream start = PI + 4 * csvLength
}

//@formatter:on
