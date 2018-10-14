/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.cpc;

/**
 * The 23 length-limited Huffman codes in this file were created by the ocaml program
 * "generateHuffmanCodes.ml", which was compiled and run as follows:
 *
 * <p>~/ocaml-4.03.0/bin/ocamlopt -o generateHuffmanCodes columnProbabilities.ml generateHuffmanCodes.ml
 *
 * <p>./generateHuffmanCodes &gt; raw-encoding-tables.c
 *
 * <p>Some manual cutting and pasting was then done to transfer the contents
 * of that file into this one.
 *
 * <p>Only the encoding tables are defined by this file. The decoding tables
 * (which are exact inverses) are created at library startup time by the function
 * makeDecodingTable (), which is defined in fm85Compression.c.
 *
 * @author Lee Rhodes
 * @author Kevin Lang
 */
final class CompressionData {

  private static byte[] makeInversePermutation(final byte[]encodePermu) {
    final int length = encodePermu.length;
    final byte[] inverse = new byte[length];

    for (int i = 0; i < length; i++) {
      inverse[encodePermu[i]] = (byte) i;
    }
    for (int i = 0; i < length; i++) {
      assert ((encodePermu[inverse[i]] & 0XFF) == i);
    }
    return inverse;
  }

  /**
   * Given an encoding table that maps unsigned bytes to codewords
   *  of length at most 12, this builds a size-4096 decoding table.
   *
   *  <p>The second argument is typically 256, but can be other values such as 65.
   *  @param encodingTable unsigned
   *  @param numByteValues size of encoding table
   *  @return one segment of the decoding table
   */
  private static short[] makeDecodingTable(final short[] encodingTable, final int numByteValues) {
    final short[] decodingTable = new short[4096];

    for (int byteValue = 0; byteValue < numByteValues; byteValue++) {
      final int encodingEntry = encodingTable[byteValue] & 0xFFFF;
      final int codeValue = encodingEntry & 0xfff;
      final int codeLength = encodingEntry >> 12;
      final int decodingEntry = (codeLength << 8) | byteValue;
      final int garbageLength = 12 - codeLength;
      final int numCopies = 1 << garbageLength;

      for (int garbageBits = 0; garbageBits < numCopies; garbageBits++) {
        final int extendedCodeValue = codeValue | (garbageBits << codeLength);
        decodingTable[extendedCodeValue & 0xfff] = (short) decodingEntry;
      }
    }
    return (decodingTable);
  }

  /**
   * These short arrays are being treated as unsigned
   * @param decodingTable unsigned
   * @param encodingTable unsigned
   */
  static void validateDecodingTable(final short[] decodingTable, final short[] encodingTable) {
    for (int decodeThis = 0; decodeThis < 4096; decodeThis++) {
      final int tmpD = decodingTable[decodeThis] & 0xFFFF;
      final int decodedByte   = tmpD & 0xff;
      final int decodedLength = tmpD >> 8;

      final int tmpE = encodingTable[decodedByte] & 0xFFFF;
      final int encodedBitpattern = tmpE & 0xfff;
      final int encodedLength = tmpE >> 12;

      // encodedBitpattern++; // uncomment this line to force failure when testing this method
      // encodedLength++;     // uncomment this line to force failure when testing this method

      assert (decodedLength == encodedLength)
        : "deLen: " + decodedLength + ", enLen: " + encodedLength;
      assert (encodedBitpattern == (decodeThis & ((1 << decodedLength) - 1)));
    }
  }

  private static void makeTheDecodingTables() {
    lengthLimitedUnaryDecodingTable65 = makeDecodingTable(lengthLimitedUnaryEncodingTable65, 65);
    validateDecodingTable(lengthLimitedUnaryDecodingTable65, lengthLimitedUnaryEncodingTable65);

    for (int i = 0; i < (16 + 6); i++) {
      decodingTablesForHighEntropyByte[i] = makeDecodingTable(encodingTablesForHighEntropyByte[i], 256);
      validateDecodingTable(decodingTablesForHighEntropyByte[i], encodingTablesForHighEntropyByte[i]);
    }

    for (int i = 0; i < 16; i++) {
      columnPermutationsForDecoding[i] = makeInversePermutation(columnPermutationsForEncoding[i]);
    }
  }

  /**
   * These decoding tables are created at library startup time by inverting the encoding tables.
   * Sixteen tables for the steady state (chosen based on the "phase" of C/K).
   * Six more tables for the gradual transition between warmup mode and the steady state.
   */
  static short[][] decodingTablesForHighEntropyByte = new short[22][];

  /**
   * Sixteen Encoding Tables for the Steady State.
   */
  static short[][] encodingTablesForHighEntropyByte = new short[][] //[22][256]
  {
    // (table 0 of 22) (steady 0 of 16) (phase = 0.031250000 = 1.0 / 32.0)
    // entropy:    4.4619200780464778333
    // avg_length: 4.5415773046232610355; max_length = 12; num_symbols = 256
    {
      //table, // (4 bits,  12 bits) symbol
      //entry, // (length, codeword) [byte]
      (short) 0x9017, // ( 9,   23)   0
      (short) 0x5009, // ( 5,    9)   1
      (short) 0x7033, // ( 7,   51)   2
      (short) 0x3002, // ( 3,    2)   3
      (short) 0x9117, // ( 9,  279)   4
      (short) 0x5019, // ( 5,   25)   5
      (short) 0x7073, // ( 7,  115)   6
      (short) 0x2000, // ( 2,    0)   7
      (short) 0xa177, // (10,  375)   8
      (short) 0x601d, // ( 6,   29)   9
      (short) 0x803b, // ( 8,   59)  10
      (short) 0x4001, // ( 4,    1)  11
      (short) 0xa377, // (10,  887)  12
      (short) 0x5005, // ( 5,    5)  13
      (short) 0x80bb, // ( 8,  187)  14
      (short) 0x3006, // ( 3,    6)  15
      (short) 0xb0cf, // (11,  207)  16
      (short) 0x700b, // ( 7,   11)  17
      (short) 0xa0f7, // (10,  247)  18
      (short) 0x5015, // ( 5,   21)  19
      (short) 0xb4cf, // (11, 1231)  20
      (short) 0x704b, // ( 7,   75)  21
      (short) 0x9097, // ( 9,  151)  22
      (short) 0x500d, // ( 5,   13)  23
      (short) 0xc4af, // (12, 1199)  24
      (short) 0x807b, // ( 8,  123)  25
      (short) 0xa2f7, // (10,  759)  26
      (short) 0x603d, // ( 6,   61)  27
      (short) 0xccaf, // (12, 3247)  28
      (short) 0x80fb, // ( 8,  251)  29
      (short) 0xa1f7, // (10,  503)  30
      (short) 0x6003, // ( 6,    3)  31
      (short) 0xc2af, // (12,  687)  32
      (short) 0x8007, // ( 8,    7)  33
      (short) 0xb2cf, // (11,  719)  34
      (short) 0x6023, // ( 6,   35)  35
      (short) 0xcaaf, // (12, 2735)  36
      (short) 0x8087, // ( 8,  135)  37
      (short) 0xa3f7, // (10, 1015)  38
      (short) 0x6013, // ( 6,   19)  39
      (short) 0xc6af, // (12, 1711)  40
      (short) 0x9197, // ( 9,  407)  41
      (short) 0xceaf, // (12, 3759)  42
      (short) 0x702b, // ( 7,   43)  43
      (short) 0xc1af, // (12,  431)  44
      (short) 0x9057, // ( 9,   87)  45
      (short) 0xb6cf, // (11, 1743)  46
      (short) 0x706b, // ( 7,  107)  47
      (short) 0xc9af, // (12, 2479)  48
      (short) 0xa00f, // (10,   15)  49
      (short) 0xc5af, // (12, 1455)  50
      (short) 0x8047, // ( 8,   71)  51
      (short) 0xcdaf, // (12, 3503)  52
      (short) 0xa20f, // (10,  527)  53
      (short) 0xc3af, // (12,  943)  54
      (short) 0x80c7, // ( 8,  199)  55
      (short) 0xcbaf, // (12, 2991)  56
      (short) 0xb1cf, // (11,  463)  57
      (short) 0xc7af, // (12, 1967)  58
      (short) 0x9157, // ( 9,  343)  59
      (short) 0xcfaf, // (12, 4015)  60
      (short) 0xb5cf, // (11, 1487)  61
      (short) 0xc06f, // (12,  111)  62
      (short) 0x90d7, // ( 9,  215)  63
      (short) 0xc86f, // (12, 2159)  64
      (short) 0x91d7, // ( 9,  471)  65
      (short) 0xc46f, // (12, 1135)  66
      (short) 0x701b, // ( 7,   27)  67
      (short) 0xcc6f, // (12, 3183)  68
      (short) 0x9037, // ( 9,   55)  69
      (short) 0xb3cf, // (11,  975)  70
      (short) 0x705b, // ( 7,   91)  71
      (short) 0xc26f, // (12,  623)  72
      (short) 0xa10f, // (10,  271)  73
      (short) 0xca6f, // (12, 2671)  74
      (short) 0x8027, // ( 8,   39)  75
      (short) 0xc66f, // (12, 1647)  76
      (short) 0xa30f, // (10,  783)  77
      (short) 0xce6f, // (12, 3695)  78
      (short) 0x80a7, // ( 8,  167)  79
      (short) 0xc16f, // (12,  367)  80
      (short) 0xb7cf, // (11, 1999)  81
      (short) 0xc96f, // (12, 2415)  82
      (short) 0x9137, // ( 9,  311)  83
      (short) 0xc56f, // (12, 1391)  84
      (short) 0xb02f, // (11,   47)  85
      (short) 0xcd6f, // (12, 3439)  86
      (short) 0x90b7, // ( 9,  183)  87
      (short) 0xc36f, // (12,  879)  88
      (short) 0xcb6f, // (12, 2927)  89
      (short) 0xc76f, // (12, 1903)  90
      (short) 0xa08f, // (10,  143)  91
      (short) 0xcf6f, // (12, 3951)  92
      (short) 0xc0ef, // (12,  239)  93
      (short) 0xc8ef, // (12, 2287)  94
      (short) 0xa28f, // (10,  655)  95
      (short) 0xc4ef, // (12, 1263)  96
      (short) 0xccef, // (12, 3311)  97
      (short) 0xc2ef, // (12,  751)  98
      (short) 0xa18f, // (10,  399)  99
      (short) 0xcaef, // (12, 2799) 100
      (short) 0xc6ef, // (12, 1775) 101
      (short) 0xceef, // (12, 3823) 102
      (short) 0xa38f, // (10,  911) 103
      (short) 0xc1ef, // (12,  495) 104
      (short) 0xc9ef, // (12, 2543) 105
      (short) 0xc5ef, // (12, 1519) 106
      (short) 0xb42f, // (11, 1071) 107
      (short) 0xcdef, // (12, 3567) 108
      (short) 0xc3ef, // (12, 1007) 109
      (short) 0xcbef, // (12, 3055) 110
      (short) 0xb22f, // (11,  559) 111
      (short) 0xc7ef, // (12, 2031) 112
      (short) 0xcfef, // (12, 4079) 113
      (short) 0xc01f, // (12,   31) 114
      (short) 0xc81f, // (12, 2079) 115
      (short) 0xc41f, // (12, 1055) 116
      (short) 0xcc1f, // (12, 3103) 117
      (short) 0xc21f, // (12,  543) 118
      (short) 0xca1f, // (12, 2591) 119
      (short) 0xc61f, // (12, 1567) 120
      (short) 0xce1f, // (12, 3615) 121
      (short) 0xc11f, // (12,  287) 122
      (short) 0xc91f, // (12, 2335) 123
      (short) 0xc51f, // (12, 1311) 124
      (short) 0xcd1f, // (12, 3359) 125
      (short) 0xc31f, // (12,  799) 126
      (short) 0xcb1f, // (12, 2847) 127
      (short) 0xc71f, // (12, 1823) 128
      (short) 0xa04f, // (10,   79) 129
      (short) 0xcf1f, // (12, 3871) 130
      (short) 0x8067, // ( 8,  103) 131
      (short) 0xc09f, // (12,  159) 132
      (short) 0xa24f, // (10,  591) 133
      (short) 0xc89f, // (12, 2207) 134
      (short) 0x80e7, // ( 8,  231) 135
      (short) 0xc49f, // (12, 1183) 136
      (short) 0xb62f, // (11, 1583) 137
      (short) 0xcc9f, // (12, 3231) 138
      (short) 0x91b7, // ( 9,  439) 139
      (short) 0xc29f, // (12,  671) 140
      (short) 0xb12f, // (11,  303) 141
      (short) 0xca9f, // (12, 2719) 142
      (short) 0x9077, // ( 9,  119) 143
      (short) 0xc69f, // (12, 1695) 144
      (short) 0xce9f, // (12, 3743) 145
      (short) 0xc19f, // (12,  415) 146
      (short) 0xa14f, // (10,  335) 147
      (short) 0xc99f, // (12, 2463) 148
      (short) 0xc59f, // (12, 1439) 149
      (short) 0xcd9f, // (12, 3487) 150
      (short) 0xa34f, // (10,  847) 151
      (short) 0xc39f, // (12,  927) 152
      (short) 0xcb9f, // (12, 2975) 153
      (short) 0xc79f, // (12, 1951) 154
      (short) 0xb52f, // (11, 1327) 155
      (short) 0xcf9f, // (12, 3999) 156
      (short) 0xc05f, // (12,   95) 157
      (short) 0xc85f, // (12, 2143) 158
      (short) 0xb32f, // (11,  815) 159
      (short) 0xc45f, // (12, 1119) 160
      (short) 0xcc5f, // (12, 3167) 161
      (short) 0xc25f, // (12,  607) 162
      (short) 0xb72f, // (11, 1839) 163
      (short) 0xca5f, // (12, 2655) 164
      (short) 0xc65f, // (12, 1631) 165
      (short) 0xce5f, // (12, 3679) 166
      (short) 0xb0af, // (11,  175) 167
      (short) 0xc15f, // (12,  351) 168
      (short) 0xc95f, // (12, 2399) 169
      (short) 0xc55f, // (12, 1375) 170
      (short) 0xcd5f, // (12, 3423) 171
      (short) 0xc35f, // (12,  863) 172
      (short) 0xcb5f, // (12, 2911) 173
      (short) 0xc75f, // (12, 1887) 174
      (short) 0xcf5f, // (12, 3935) 175
      (short) 0xc0df, // (12,  223) 176
      (short) 0xc8df, // (12, 2271) 177
      (short) 0xc4df, // (12, 1247) 178
      (short) 0xccdf, // (12, 3295) 179
      (short) 0xc2df, // (12,  735) 180
      (short) 0xcadf, // (12, 2783) 181
      (short) 0xc6df, // (12, 1759) 182
      (short) 0xcedf, // (12, 3807) 183
      (short) 0xc1df, // (12,  479) 184
      (short) 0xc9df, // (12, 2527) 185
      (short) 0xc5df, // (12, 1503) 186
      (short) 0xcddf, // (12, 3551) 187
      (short) 0xc3df, // (12,  991) 188
      (short) 0xcbdf, // (12, 3039) 189
      (short) 0xc7df, // (12, 2015) 190
      (short) 0xcfdf, // (12, 4063) 191
      (short) 0xc03f, // (12,   63) 192
      (short) 0xc83f, // (12, 2111) 193
      (short) 0xc43f, // (12, 1087) 194
      (short) 0xcc3f, // (12, 3135) 195
      (short) 0xc23f, // (12,  575) 196
      (short) 0xca3f, // (12, 2623) 197
      (short) 0xc63f, // (12, 1599) 198
      (short) 0xce3f, // (12, 3647) 199
      (short) 0xc13f, // (12,  319) 200
      (short) 0xc93f, // (12, 2367) 201
      (short) 0xc53f, // (12, 1343) 202
      (short) 0xcd3f, // (12, 3391) 203
      (short) 0xc33f, // (12,  831) 204
      (short) 0xcb3f, // (12, 2879) 205
      (short) 0xc73f, // (12, 1855) 206
      (short) 0xcf3f, // (12, 3903) 207
      (short) 0xc0bf, // (12,  191) 208
      (short) 0xc8bf, // (12, 2239) 209
      (short) 0xc4bf, // (12, 1215) 210
      (short) 0xccbf, // (12, 3263) 211
      (short) 0xc2bf, // (12,  703) 212
      (short) 0xcabf, // (12, 2751) 213
      (short) 0xc6bf, // (12, 1727) 214
      (short) 0xcebf, // (12, 3775) 215
      (short) 0xc1bf, // (12,  447) 216
      (short) 0xc9bf, // (12, 2495) 217
      (short) 0xc5bf, // (12, 1471) 218
      (short) 0xcdbf, // (12, 3519) 219
      (short) 0xc3bf, // (12,  959) 220
      (short) 0xcbbf, // (12, 3007) 221
      (short) 0xc7bf, // (12, 1983) 222
      (short) 0xcfbf, // (12, 4031) 223
      (short) 0xc07f, // (12,  127) 224
      (short) 0xc87f, // (12, 2175) 225
      (short) 0xc47f, // (12, 1151) 226
      (short) 0xcc7f, // (12, 3199) 227
      (short) 0xc27f, // (12,  639) 228
      (short) 0xca7f, // (12, 2687) 229
      (short) 0xc67f, // (12, 1663) 230
      (short) 0xce7f, // (12, 3711) 231
      (short) 0xc17f, // (12,  383) 232
      (short) 0xc97f, // (12, 2431) 233
      (short) 0xc57f, // (12, 1407) 234
      (short) 0xcd7f, // (12, 3455) 235
      (short) 0xc37f, // (12,  895) 236
      (short) 0xcb7f, // (12, 2943) 237
      (short) 0xc77f, // (12, 1919) 238
      (short) 0xcf7f, // (12, 3967) 239
      (short) 0xc0ff, // (12,  255) 240
      (short) 0xc8ff, // (12, 2303) 241
      (short) 0xc4ff, // (12, 1279) 242
      (short) 0xccff, // (12, 3327) 243
      (short) 0xc2ff, // (12,  767) 244
      (short) 0xcaff, // (12, 2815) 245
      (short) 0xc6ff, // (12, 1791) 246
      (short) 0xceff, // (12, 3839) 247
      (short) 0xc1ff, // (12,  511) 248
      (short) 0xc9ff, // (12, 2559) 249
      (short) 0xc5ff, // (12, 1535) 250
      (short) 0xcdff, // (12, 3583) 251
      (short) 0xc3ff, // (12, 1023) 252
      (short) 0xcbff, // (12, 3071) 253
      (short) 0xc7ff, // (12, 2047) 254
      (short) 0xcfff  // (12, 4095) 255
    },

    // (table 1 of 22) (steady 1 of 16) (phase = 0.093750000 = 3.0 / 32.0)
    // entropy:    4.4574755684414029133
    // avg_length: 4.5336306265208552446; max_length = 12; num_symbols = 256
    {
      //table, // (4 bits,  12 bits) symbol
      //entry, // (length, codeword) [byte]
      (short) 0xa177, // (10,  375)   0
      (short) 0x5009, // ( 5,    9)   1
      (short) 0x803b, // ( 8,   59)   2
      (short) 0x3002, // ( 3,    2)   3
      (short) 0x9017, // ( 9,   23)   4
      (short) 0x5019, // ( 5,   25)   5
      (short) 0x700b, // ( 7,   11)   6
      (short) 0x2000, // ( 2,    0)   7
      (short) 0xb34f, // (11,  847)   8
      (short) 0x601d, // ( 6,   29)   9
      (short) 0x9117, // ( 9,  279)  10
      (short) 0x4001, // ( 4,    1)  11
      (short) 0xa377, // (10,  887)  12
      (short) 0x603d, // ( 6,   61)  13
      (short) 0x80bb, // ( 8,  187)  14
      (short) 0x3006, // ( 3,    6)  15
      (short) 0xc4af, // (12, 1199)  16
      (short) 0x704b, // ( 7,   75)  17
      (short) 0xa0f7, // (10,  247)  18
      (short) 0x5005, // ( 5,    5)  19
      (short) 0xb74f, // (11, 1871)  20
      (short) 0x702b, // ( 7,   43)  21
      (short) 0x9097, // ( 9,  151)  22
      (short) 0x5015, // ( 5,   21)  23
      (short) 0xccaf, // (12, 3247)  24
      (short) 0x807b, // ( 8,  123)  25
      (short) 0xb0cf, // (11,  207)  26
      (short) 0x6003, // ( 6,    3)  27
      (short) 0xc2af, // (12,  687)  28
      (short) 0x80fb, // ( 8,  251)  29
      (short) 0xa2f7, // (10,  759)  30
      (short) 0x500d, // ( 5,   13)  31
      (short) 0xcaaf, // (12, 2735)  32
      (short) 0x8007, // ( 8,    7)  33
      (short) 0xb4cf, // (11, 1231)  34
      (short) 0x6023, // ( 6,   35)  35
      (short) 0xc6af, // (12, 1711)  36
      (short) 0x8087, // ( 8,  135)  37
      (short) 0xa1f7, // (10,  503)  38
      (short) 0x6013, // ( 6,   19)  39
      (short) 0xceaf, // (12, 3759)  40
      (short) 0x9197, // ( 9,  407)  41
      (short) 0xc1af, // (12,  431)  42
      (short) 0x706b, // ( 7,  107)  43
      (short) 0xc9af, // (12, 2479)  44
      (short) 0x9057, // ( 9,   87)  45
      (short) 0xb2cf, // (11,  719)  46
      (short) 0x6033, // ( 6,   51)  47
      (short) 0xc5af, // (12, 1455)  48
      (short) 0xa3f7, // (10, 1015)  49
      (short) 0xcdaf, // (12, 3503)  50
      (short) 0x8047, // ( 8,   71)  51
      (short) 0xc3af, // (12,  943)  52
      (short) 0xa00f, // (10,   15)  53
      (short) 0xcbaf, // (12, 2991)  54
      (short) 0x80c7, // ( 8,  199)  55
      (short) 0xc7af, // (12, 1967)  56
      (short) 0xb6cf, // (11, 1743)  57
      (short) 0xcfaf, // (12, 4015)  58
      (short) 0x9157, // ( 9,  343)  59
      (short) 0xc06f, // (12,  111)  60
      (short) 0xb1cf, // (11,  463)  61
      (short) 0xc86f, // (12, 2159)  62
      (short) 0x90d7, // ( 9,  215)  63
      (short) 0xc46f, // (12, 1135)  64
      (short) 0x91d7, // ( 9,  471)  65
      (short) 0xcc6f, // (12, 3183)  66
      (short) 0x701b, // ( 7,   27)  67
      (short) 0xc26f, // (12,  623)  68
      (short) 0x9037, // ( 9,   55)  69
      (short) 0xb5cf, // (11, 1487)  70
      (short) 0x705b, // ( 7,   91)  71
      (short) 0xca6f, // (12, 2671)  72
      (short) 0xa20f, // (10,  527)  73
      (short) 0xc66f, // (12, 1647)  74
      (short) 0x8027, // ( 8,   39)  75
      (short) 0xce6f, // (12, 3695)  76
      (short) 0xa10f, // (10,  271)  77
      (short) 0xc16f, // (12,  367)  78
      (short) 0x80a7, // ( 8,  167)  79
      (short) 0xc96f, // (12, 2415)  80
      (short) 0xb3cf, // (11,  975)  81
      (short) 0xc56f, // (12, 1391)  82
      (short) 0x9137, // ( 9,  311)  83
      (short) 0xcd6f, // (12, 3439)  84
      (short) 0xb7cf, // (11, 1999)  85
      (short) 0xc36f, // (12,  879)  86
      (short) 0x90b7, // ( 9,  183)  87
      (short) 0xcb6f, // (12, 2927)  88
      (short) 0xc76f, // (12, 1903)  89
      (short) 0xcf6f, // (12, 3951)  90
      (short) 0xa30f, // (10,  783)  91
      (short) 0xc0ef, // (12,  239)  92
      (short) 0xc8ef, // (12, 2287)  93
      (short) 0xc4ef, // (12, 1263)  94
      (short) 0xa08f, // (10,  143)  95
      (short) 0xccef, // (12, 3311)  96
      (short) 0xc2ef, // (12,  751)  97
      (short) 0xcaef, // (12, 2799)  98
      (short) 0xa28f, // (10,  655)  99
      (short) 0xc6ef, // (12, 1775) 100
      (short) 0xceef, // (12, 3823) 101
      (short) 0xc1ef, // (12,  495) 102
      (short) 0xa18f, // (10,  399) 103
      (short) 0xc9ef, // (12, 2543) 104
      (short) 0xc5ef, // (12, 1519) 105
      (short) 0xcdef, // (12, 3567) 106
      (short) 0xb02f, // (11,   47) 107
      (short) 0xc3ef, // (12, 1007) 108
      (short) 0xcbef, // (12, 3055) 109
      (short) 0xc7ef, // (12, 2031) 110
      (short) 0xb42f, // (11, 1071) 111
      (short) 0xcfef, // (12, 4079) 112
      (short) 0xc01f, // (12,   31) 113
      (short) 0xc81f, // (12, 2079) 114
      (short) 0xc41f, // (12, 1055) 115
      (short) 0xcc1f, // (12, 3103) 116
      (short) 0xc21f, // (12,  543) 117
      (short) 0xca1f, // (12, 2591) 118
      (short) 0xc61f, // (12, 1567) 119
      (short) 0xce1f, // (12, 3615) 120
      (short) 0xc11f, // (12,  287) 121
      (short) 0xc91f, // (12, 2335) 122
      (short) 0xc51f, // (12, 1311) 123
      (short) 0xcd1f, // (12, 3359) 124
      (short) 0xc31f, // (12,  799) 125
      (short) 0xcb1f, // (12, 2847) 126
      (short) 0xc71f, // (12, 1823) 127
      (short) 0xcf1f, // (12, 3871) 128
      (short) 0xa38f, // (10,  911) 129
      (short) 0xc09f, // (12,  159) 130
      (short) 0x8067, // ( 8,  103) 131
      (short) 0xc89f, // (12, 2207) 132
      (short) 0xa04f, // (10,   79) 133
      (short) 0xc49f, // (12, 1183) 134
      (short) 0x80e7, // ( 8,  231) 135
      (short) 0xcc9f, // (12, 3231) 136
      (short) 0xb22f, // (11,  559) 137
      (short) 0xc29f, // (12,  671) 138
      (short) 0x91b7, // ( 9,  439) 139
      (short) 0xca9f, // (12, 2719) 140
      (short) 0xb62f, // (11, 1583) 141
      (short) 0xc69f, // (12, 1695) 142
      (short) 0x9077, // ( 9,  119) 143
      (short) 0xce9f, // (12, 3743) 144
      (short) 0xc19f, // (12,  415) 145
      (short) 0xc99f, // (12, 2463) 146
      (short) 0xa24f, // (10,  591) 147
      (short) 0xc59f, // (12, 1439) 148
      (short) 0xcd9f, // (12, 3487) 149
      (short) 0xc39f, // (12,  927) 150
      (short) 0xa14f, // (10,  335) 151
      (short) 0xcb9f, // (12, 2975) 152
      (short) 0xc79f, // (12, 1951) 153
      (short) 0xcf9f, // (12, 3999) 154
      (short) 0xb12f, // (11,  303) 155
      (short) 0xc05f, // (12,   95) 156
      (short) 0xc85f, // (12, 2143) 157
      (short) 0xc45f, // (12, 1119) 158
      (short) 0xb52f, // (11, 1327) 159
      (short) 0xcc5f, // (12, 3167) 160
      (short) 0xc25f, // (12,  607) 161
      (short) 0xca5f, // (12, 2655) 162
      (short) 0xb32f, // (11,  815) 163
      (short) 0xc65f, // (12, 1631) 164
      (short) 0xce5f, // (12, 3679) 165
      (short) 0xc15f, // (12,  351) 166
      (short) 0xb72f, // (11, 1839) 167
      (short) 0xc95f, // (12, 2399) 168
      (short) 0xc55f, // (12, 1375) 169
      (short) 0xcd5f, // (12, 3423) 170
      (short) 0xc35f, // (12,  863) 171
      (short) 0xcb5f, // (12, 2911) 172
      (short) 0xc75f, // (12, 1887) 173
      (short) 0xcf5f, // (12, 3935) 174
      (short) 0xb0af, // (11,  175) 175
      (short) 0xc0df, // (12,  223) 176
      (short) 0xc8df, // (12, 2271) 177
      (short) 0xc4df, // (12, 1247) 178
      (short) 0xccdf, // (12, 3295) 179
      (short) 0xc2df, // (12,  735) 180
      (short) 0xcadf, // (12, 2783) 181
      (short) 0xc6df, // (12, 1759) 182
      (short) 0xcedf, // (12, 3807) 183
      (short) 0xc1df, // (12,  479) 184
      (short) 0xc9df, // (12, 2527) 185
      (short) 0xc5df, // (12, 1503) 186
      (short) 0xcddf, // (12, 3551) 187
      (short) 0xc3df, // (12,  991) 188
      (short) 0xcbdf, // (12, 3039) 189
      (short) 0xc7df, // (12, 2015) 190
      (short) 0xcfdf, // (12, 4063) 191
      (short) 0xc03f, // (12,   63) 192
      (short) 0xc83f, // (12, 2111) 193
      (short) 0xc43f, // (12, 1087) 194
      (short) 0xcc3f, // (12, 3135) 195
      (short) 0xc23f, // (12,  575) 196
      (short) 0xca3f, // (12, 2623) 197
      (short) 0xc63f, // (12, 1599) 198
      (short) 0xce3f, // (12, 3647) 199
      (short) 0xc13f, // (12,  319) 200
      (short) 0xc93f, // (12, 2367) 201
      (short) 0xc53f, // (12, 1343) 202
      (short) 0xcd3f, // (12, 3391) 203
      (short) 0xc33f, // (12,  831) 204
      (short) 0xcb3f, // (12, 2879) 205
      (short) 0xc73f, // (12, 1855) 206
      (short) 0xcf3f, // (12, 3903) 207
      (short) 0xc0bf, // (12,  191) 208
      (short) 0xc8bf, // (12, 2239) 209
      (short) 0xc4bf, // (12, 1215) 210
      (short) 0xccbf, // (12, 3263) 211
      (short) 0xc2bf, // (12,  703) 212
      (short) 0xcabf, // (12, 2751) 213
      (short) 0xc6bf, // (12, 1727) 214
      (short) 0xcebf, // (12, 3775) 215
      (short) 0xc1bf, // (12,  447) 216
      (short) 0xc9bf, // (12, 2495) 217
      (short) 0xc5bf, // (12, 1471) 218
      (short) 0xcdbf, // (12, 3519) 219
      (short) 0xc3bf, // (12,  959) 220
      (short) 0xcbbf, // (12, 3007) 221
      (short) 0xc7bf, // (12, 1983) 222
      (short) 0xcfbf, // (12, 4031) 223
      (short) 0xc07f, // (12,  127) 224
      (short) 0xc87f, // (12, 2175) 225
      (short) 0xc47f, // (12, 1151) 226
      (short) 0xcc7f, // (12, 3199) 227
      (short) 0xc27f, // (12,  639) 228
      (short) 0xca7f, // (12, 2687) 229
      (short) 0xc67f, // (12, 1663) 230
      (short) 0xce7f, // (12, 3711) 231
      (short) 0xc17f, // (12,  383) 232
      (short) 0xc97f, // (12, 2431) 233
      (short) 0xc57f, // (12, 1407) 234
      (short) 0xcd7f, // (12, 3455) 235
      (short) 0xc37f, // (12,  895) 236
      (short) 0xcb7f, // (12, 2943) 237
      (short) 0xc77f, // (12, 1919) 238
      (short) 0xcf7f, // (12, 3967) 239
      (short) 0xc0ff, // (12,  255) 240
      (short) 0xc8ff, // (12, 2303) 241
      (short) 0xc4ff, // (12, 1279) 242
      (short) 0xccff, // (12, 3327) 243
      (short) 0xc2ff, // (12,  767) 244
      (short) 0xcaff, // (12, 2815) 245
      (short) 0xc6ff, // (12, 1791) 246
      (short) 0xceff, // (12, 3839) 247
      (short) 0xc1ff, // (12,  511) 248
      (short) 0xc9ff, // (12, 2559) 249
      (short) 0xc5ff, // (12, 1535) 250
      (short) 0xcdff, // (12, 3583) 251
      (short) 0xc3ff, // (12, 1023) 252
      (short) 0xcbff, // (12, 3071) 253
      (short) 0xc7ff, // (12, 2047) 254
      (short) 0xcfff  // (12, 4095) 255
    },

    // (table 2 of 22) (steady 2 of 16) (phase = 0.156250000 = 5.0 / 32.0)
    // entropy:    4.4520619712441886762
    // avg_length: 4.5253989110544479146; max_length = 12; num_symbols = 256
    {
      //table, // (4 bits,  12 bits) symbol
      //entry, // (length, codeword) [byte]
      (short) 0xa177, // (10,  375)   0
      (short) 0x5009, // ( 5,    9)   1
      (short) 0x803b, // ( 8,   59)   2
      (short) 0x3002, // ( 3,    2)   3
      (short) 0xa377, // (10,  887)   4
      (short) 0x5019, // ( 5,   25)   5
      (short) 0x80bb, // ( 8,  187)   6
      (short) 0x2000, // ( 2,    0)   7
      (short) 0xb34f, // (11,  847)   8
      (short) 0x601d, // ( 6,   29)   9
      (short) 0x9057, // ( 9,   87)  10
      (short) 0x4001, // ( 4,    1)  11
      (short) 0xb74f, // (11, 1871)  12
      (short) 0x603d, // ( 6,   61)  13
      (short) 0x807b, // ( 8,  123)  14
      (short) 0x3006, // ( 3,    6)  15
      (short) 0xc72f, // (12, 1839)  16
      (short) 0x700b, // ( 7,   11)  17
      (short) 0xa0f7, // (10,  247)  18
      (short) 0x5005, // ( 5,    5)  19
      (short) 0xcf2f, // (12, 3887)  20
      (short) 0x704b, // ( 7,   75)  21
      (short) 0xa2f7, // (10,  759)  22
      (short) 0x5015, // ( 5,   21)  23
      (short) 0xc0af, // (12,  175)  24
      (short) 0x80fb, // ( 8,  251)  25
      (short) 0xb0cf, // (11,  207)  26
      (short) 0x6003, // ( 6,    3)  27
      (short) 0xc8af, // (12, 2223)  28
      (short) 0x8007, // ( 8,    7)  29
      (short) 0xa1f7, // (10,  503)  30
      (short) 0x500d, // ( 5,   13)  31
      (short) 0xc4af, // (12, 1199)  32
      (short) 0x8087, // ( 8,  135)  33
      (short) 0xb4cf, // (11, 1231)  34
      (short) 0x6023, // ( 6,   35)  35
      (short) 0xccaf, // (12, 3247)  36
      (short) 0x8047, // ( 8,   71)  37
      (short) 0xb2cf, // (11,  719)  38
      (short) 0x6013, // ( 6,   19)  39
      (short) 0xc2af, // (12,  687)  40
      (short) 0x9157, // ( 9,  343)  41
      (short) 0xcaaf, // (12, 2735)  42
      (short) 0x702b, // ( 7,   43)  43
      (short) 0xc6af, // (12, 1711)  44
      (short) 0x90d7, // ( 9,  215)  45
      (short) 0xceaf, // (12, 3759)  46
      (short) 0x6033, // ( 6,   51)  47
      (short) 0xc1af, // (12,  431)  48
      (short) 0xa3f7, // (10, 1015)  49
      (short) 0xc9af, // (12, 2479)  50
      (short) 0x80c7, // ( 8,  199)  51
      (short) 0xc5af, // (12, 1455)  52
      (short) 0xa00f, // (10,   15)  53
      (short) 0xcdaf, // (12, 3503)  54
      (short) 0x8027, // ( 8,   39)  55
      (short) 0xc3af, // (12,  943)  56
      (short) 0xb6cf, // (11, 1743)  57
      (short) 0xcbaf, // (12, 2991)  58
      (short) 0x91d7, // ( 9,  471)  59
      (short) 0xc7af, // (12, 1967)  60
      (short) 0xb1cf, // (11,  463)  61
      (short) 0xcfaf, // (12, 4015)  62
      (short) 0x80a7, // ( 8,  167)  63
      (short) 0xc06f, // (12,  111)  64
      (short) 0x9037, // ( 9,   55)  65
      (short) 0xc86f, // (12, 2159)  66
      (short) 0x706b, // ( 7,  107)  67
      (short) 0xc46f, // (12, 1135)  68
      (short) 0x9137, // ( 9,  311)  69
      (short) 0xcc6f, // (12, 3183)  70
      (short) 0x701b, // ( 7,   27)  71
      (short) 0xc26f, // (12,  623)  72
      (short) 0xa20f, // (10,  527)  73
      (short) 0xca6f, // (12, 2671)  74
      (short) 0x8067, // ( 8,  103)  75
      (short) 0xc66f, // (12, 1647)  76
      (short) 0xa10f, // (10,  271)  77
      (short) 0xce6f, // (12, 3695)  78
      (short) 0x705b, // ( 7,   91)  79
      (short) 0xc16f, // (12,  367)  80
      (short) 0xb5cf, // (11, 1487)  81
      (short) 0xc96f, // (12, 2415)  82
      (short) 0x90b7, // ( 9,  183)  83
      (short) 0xc56f, // (12, 1391)  84
      (short) 0xb3cf, // (11,  975)  85
      (short) 0xcd6f, // (12, 3439)  86
      (short) 0x91b7, // ( 9,  439)  87
      (short) 0xc36f, // (12,  879)  88
      (short) 0xcb6f, // (12, 2927)  89
      (short) 0xc76f, // (12, 1903)  90
      (short) 0xa30f, // (10,  783)  91
      (short) 0xcf6f, // (12, 3951)  92
      (short) 0xc0ef, // (12,  239)  93
      (short) 0xc8ef, // (12, 2287)  94
      (short) 0xa08f, // (10,  143)  95
      (short) 0xc4ef, // (12, 1263)  96
      (short) 0xccef, // (12, 3311)  97
      (short) 0xc2ef, // (12,  751)  98
      (short) 0xa28f, // (10,  655)  99
      (short) 0xcaef, // (12, 2799) 100
      (short) 0xc6ef, // (12, 1775) 101
      (short) 0xceef, // (12, 3823) 102
      (short) 0xa18f, // (10,  399) 103
      (short) 0xc1ef, // (12,  495) 104
      (short) 0xc9ef, // (12, 2543) 105
      (short) 0xc5ef, // (12, 1519) 106
      (short) 0xb7cf, // (11, 1999) 107
      (short) 0xcdef, // (12, 3567) 108
      (short) 0xc3ef, // (12, 1007) 109
      (short) 0xcbef, // (12, 3055) 110
      (short) 0xb02f, // (11,   47) 111
      (short) 0xc7ef, // (12, 2031) 112
      (short) 0xcfef, // (12, 4079) 113
      (short) 0xc01f, // (12,   31) 114
      (short) 0xc81f, // (12, 2079) 115
      (short) 0xc41f, // (12, 1055) 116
      (short) 0xcc1f, // (12, 3103) 117
      (short) 0xc21f, // (12,  543) 118
      (short) 0xca1f, // (12, 2591) 119
      (short) 0xc61f, // (12, 1567) 120
      (short) 0xce1f, // (12, 3615) 121
      (short) 0xc11f, // (12,  287) 122
      (short) 0xc91f, // (12, 2335) 123
      (short) 0xc51f, // (12, 1311) 124
      (short) 0xcd1f, // (12, 3359) 125
      (short) 0xc31f, // (12,  799) 126
      (short) 0xcb1f, // (12, 2847) 127
      (short) 0xc71f, // (12, 1823) 128
      (short) 0xa38f, // (10,  911) 129
      (short) 0xcf1f, // (12, 3871) 130
      (short) 0x80e7, // ( 8,  231) 131
      (short) 0xc09f, // (12,  159) 132
      (short) 0xa04f, // (10,   79) 133
      (short) 0xc89f, // (12, 2207) 134
      (short) 0x8017, // ( 8,   23) 135
      (short) 0xc49f, // (12, 1183) 136
      (short) 0xb42f, // (11, 1071) 137
      (short) 0xcc9f, // (12, 3231) 138
      (short) 0x9077, // ( 9,  119) 139
      (short) 0xc29f, // (12,  671) 140
      (short) 0xb22f, // (11,  559) 141
      (short) 0xca9f, // (12, 2719) 142
      (short) 0x8097, // ( 8,  151) 143
      (short) 0xc69f, // (12, 1695) 144
      (short) 0xce9f, // (12, 3743) 145
      (short) 0xc19f, // (12,  415) 146
      (short) 0xa24f, // (10,  591) 147
      (short) 0xc99f, // (12, 2463) 148
      (short) 0xc59f, // (12, 1439) 149
      (short) 0xcd9f, // (12, 3487) 150
      (short) 0xa14f, // (10,  335) 151
      (short) 0xc39f, // (12,  927) 152
      (short) 0xcb9f, // (12, 2975) 153
      (short) 0xc79f, // (12, 1951) 154
      (short) 0xb62f, // (11, 1583) 155
      (short) 0xcf9f, // (12, 3999) 156
      (short) 0xc05f, // (12,   95) 157
      (short) 0xc85f, // (12, 2143) 158
      (short) 0xb12f, // (11,  303) 159
      (short) 0xc45f, // (12, 1119) 160
      (short) 0xcc5f, // (12, 3167) 161
      (short) 0xc25f, // (12,  607) 162
      (short) 0xb52f, // (11, 1327) 163
      (short) 0xca5f, // (12, 2655) 164
      (short) 0xc65f, // (12, 1631) 165
      (short) 0xce5f, // (12, 3679) 166
      (short) 0xb32f, // (11,  815) 167
      (short) 0xc15f, // (12,  351) 168
      (short) 0xc95f, // (12, 2399) 169
      (short) 0xc55f, // (12, 1375) 170
      (short) 0xcd5f, // (12, 3423) 171
      (short) 0xc35f, // (12,  863) 172
      (short) 0xcb5f, // (12, 2911) 173
      (short) 0xc75f, // (12, 1887) 174
      (short) 0xcf5f, // (12, 3935) 175
      (short) 0xc0df, // (12,  223) 176
      (short) 0xc8df, // (12, 2271) 177
      (short) 0xc4df, // (12, 1247) 178
      (short) 0xccdf, // (12, 3295) 179
      (short) 0xc2df, // (12,  735) 180
      (short) 0xcadf, // (12, 2783) 181
      (short) 0xc6df, // (12, 1759) 182
      (short) 0xcedf, // (12, 3807) 183
      (short) 0xc1df, // (12,  479) 184
      (short) 0xc9df, // (12, 2527) 185
      (short) 0xc5df, // (12, 1503) 186
      (short) 0xcddf, // (12, 3551) 187
      (short) 0xc3df, // (12,  991) 188
      (short) 0xcbdf, // (12, 3039) 189
      (short) 0xc7df, // (12, 2015) 190
      (short) 0xcfdf, // (12, 4063) 191
      (short) 0xc03f, // (12,   63) 192
      (short) 0xc83f, // (12, 2111) 193
      (short) 0xc43f, // (12, 1087) 194
      (short) 0xcc3f, // (12, 3135) 195
      (short) 0xc23f, // (12,  575) 196
      (short) 0xca3f, // (12, 2623) 197
      (short) 0xc63f, // (12, 1599) 198
      (short) 0xce3f, // (12, 3647) 199
      (short) 0xc13f, // (12,  319) 200
      (short) 0xc93f, // (12, 2367) 201
      (short) 0xc53f, // (12, 1343) 202
      (short) 0xcd3f, // (12, 3391) 203
      (short) 0xc33f, // (12,  831) 204
      (short) 0xcb3f, // (12, 2879) 205
      (short) 0xc73f, // (12, 1855) 206
      (short) 0xcf3f, // (12, 3903) 207
      (short) 0xc0bf, // (12,  191) 208
      (short) 0xc8bf, // (12, 2239) 209
      (short) 0xc4bf, // (12, 1215) 210
      (short) 0xccbf, // (12, 3263) 211
      (short) 0xc2bf, // (12,  703) 212
      (short) 0xcabf, // (12, 2751) 213
      (short) 0xc6bf, // (12, 1727) 214
      (short) 0xcebf, // (12, 3775) 215
      (short) 0xc1bf, // (12,  447) 216
      (short) 0xc9bf, // (12, 2495) 217
      (short) 0xc5bf, // (12, 1471) 218
      (short) 0xcdbf, // (12, 3519) 219
      (short) 0xc3bf, // (12,  959) 220
      (short) 0xcbbf, // (12, 3007) 221
      (short) 0xc7bf, // (12, 1983) 222
      (short) 0xcfbf, // (12, 4031) 223
      (short) 0xc07f, // (12,  127) 224
      (short) 0xc87f, // (12, 2175) 225
      (short) 0xc47f, // (12, 1151) 226
      (short) 0xcc7f, // (12, 3199) 227
      (short) 0xc27f, // (12,  639) 228
      (short) 0xca7f, // (12, 2687) 229
      (short) 0xc67f, // (12, 1663) 230
      (short) 0xce7f, // (12, 3711) 231
      (short) 0xc17f, // (12,  383) 232
      (short) 0xc97f, // (12, 2431) 233
      (short) 0xc57f, // (12, 1407) 234
      (short) 0xcd7f, // (12, 3455) 235
      (short) 0xc37f, // (12,  895) 236
      (short) 0xcb7f, // (12, 2943) 237
      (short) 0xc77f, // (12, 1919) 238
      (short) 0xcf7f, // (12, 3967) 239
      (short) 0xc0ff, // (12,  255) 240
      (short) 0xc8ff, // (12, 2303) 241
      (short) 0xc4ff, // (12, 1279) 242
      (short) 0xccff, // (12, 3327) 243
      (short) 0xc2ff, // (12,  767) 244
      (short) 0xcaff, // (12, 2815) 245
      (short) 0xc6ff, // (12, 1791) 246
      (short) 0xceff, // (12, 3839) 247
      (short) 0xc1ff, // (12,  511) 248
      (short) 0xc9ff, // (12, 2559) 249
      (short) 0xc5ff, // (12, 1535) 250
      (short) 0xcdff, // (12, 3583) 251
      (short) 0xc3ff, // (12, 1023) 252
      (short) 0xcbff, // (12, 3071) 253
      (short) 0xc7ff, // (12, 2047) 254
      (short) 0xcfff  // (12, 4095) 255
    },

    // (table 3 of 22) (steady 3 of 16) (phase = 0.218750000 = 7.0 / 32.0)
    // entropy:    4.4457680500675866853
    // avg_length: 4.5181192844586535173; max_length = 12; num_symbols = 256
    {
      //table, // (4 bits,  12 bits) symbol
      //entry, // (length, codeword) [byte]
      (short) 0xb24f, // (11,  591)   0
      (short) 0x601d, // ( 6,   29)   1
      (short) 0x9097, // ( 9,  151)   2
      (short) 0x3002, // ( 3,    2)   3
      (short) 0xa1f7, // (10,  503)   4
      (short) 0x5005, // ( 5,    5)   5
      (short) 0x807b, // ( 8,  123)   6
      (short) 0x2000, // ( 2,    0)   7
      (short) 0xc52f, // (12, 1327)   8
      (short) 0x603d, // ( 6,   61)   9
      (short) 0x9197, // ( 9,  407)  10
      (short) 0x4001, // ( 4,    1)  11
      (short) 0xb64f, // (11, 1615)  12
      (short) 0x6003, // ( 6,    3)  13
      (short) 0x9057, // ( 9,   87)  14
      (short) 0x3006, // ( 3,    6)  15
      (short) 0xcd2f, // (12, 3375)  16
      (short) 0x80fb, // ( 8,  251)  17
      (short) 0xb14f, // (11,  335)  18
      (short) 0x5015, // ( 5,   21)  19
      (short) 0xc32f, // (12,  815)  20
      (short) 0x702b, // ( 7,   43)  21
      (short) 0xa3f7, // (10, 1015)  22
      (short) 0x4009, // ( 4,    9)  23
      (short) 0xcb2f, // (12, 2863)  24
      (short) 0x8007, // ( 8,    7)  25
      (short) 0xb54f, // (11, 1359)  26
      (short) 0x6023, // ( 6,   35)  27
      (short) 0xc72f, // (12, 1839)  28
      (short) 0x8087, // ( 8,  135)  29
      (short) 0xb34f, // (11,  847)  30
      (short) 0x500d, // ( 5,   13)  31
      (short) 0xcf2f, // (12, 3887)  32
      (short) 0x9157, // ( 9,  343)  33
      (short) 0xc0af, // (12,  175)  34
      (short) 0x6013, // ( 6,   19)  35
      (short) 0xc8af, // (12, 2223)  36
      (short) 0x8047, // ( 8,   71)  37
      (short) 0xb74f, // (11, 1871)  38
      (short) 0x6033, // ( 6,   51)  39
      (short) 0xc4af, // (12, 1199)  40
      (short) 0x90d7, // ( 9,  215)  41
      (short) 0xccaf, // (12, 3247)  42
      (short) 0x706b, // ( 7,  107)  43
      (short) 0xc2af, // (12,  687)  44
      (short) 0x91d7, // ( 9,  471)  45
      (short) 0xcaaf, // (12, 2735)  46
      (short) 0x600b, // ( 6,   11)  47
      (short) 0xc6af, // (12, 1711)  48
      (short) 0xb0cf, // (11,  207)  49
      (short) 0xceaf, // (12, 3759)  50
      (short) 0x80c7, // ( 8,  199)  51
      (short) 0xc1af, // (12,  431)  52
      (short) 0xa00f, // (10,   15)  53
      (short) 0xc9af, // (12, 2479)  54
      (short) 0x8027, // ( 8,   39)  55
      (short) 0xc5af, // (12, 1455)  56
      (short) 0xb4cf, // (11, 1231)  57
      (short) 0xcdaf, // (12, 3503)  58
      (short) 0x9037, // ( 9,   55)  59
      (short) 0xc3af, // (12,  943)  60
      (short) 0xb2cf, // (11,  719)  61
      (short) 0xcbaf, // (12, 2991)  62
      (short) 0x80a7, // ( 8,  167)  63
      (short) 0xc7af, // (12, 1967)  64
      (short) 0xa20f, // (10,  527)  65
      (short) 0xcfaf, // (12, 4015)  66
      (short) 0x701b, // ( 7,   27)  67
      (short) 0xc06f, // (12,  111)  68
      (short) 0x9137, // ( 9,  311)  69
      (short) 0xc86f, // (12, 2159)  70
      (short) 0x705b, // ( 7,   91)  71
      (short) 0xc46f, // (12, 1135)  72
      (short) 0xb6cf, // (11, 1743)  73
      (short) 0xcc6f, // (12, 3183)  74
      (short) 0x8067, // ( 8,  103)  75
      (short) 0xc26f, // (12,  623)  76
      (short) 0xa10f, // (10,  271)  77
      (short) 0xca6f, // (12, 2671)  78
      (short) 0x703b, // ( 7,   59)  79
      (short) 0xc66f, // (12, 1647)  80
      (short) 0xce6f, // (12, 3695)  81
      (short) 0xc16f, // (12,  367)  82
      (short) 0x90b7, // ( 9,  183)  83
      (short) 0xc96f, // (12, 2415)  84
      (short) 0xb1cf, // (11,  463)  85
      (short) 0xc56f, // (12, 1391)  86
      (short) 0x91b7, // ( 9,  439)  87
      (short) 0xcd6f, // (12, 3439)  88
      (short) 0xc36f, // (12,  879)  89
      (short) 0xcb6f, // (12, 2927)  90
      (short) 0xa30f, // (10,  783)  91
      (short) 0xc76f, // (12, 1903)  92
      (short) 0xcf6f, // (12, 3951)  93
      (short) 0xc0ef, // (12,  239)  94
      (short) 0x9077, // ( 9,  119)  95
      (short) 0xc8ef, // (12, 2287)  96
      (short) 0xc4ef, // (12, 1263)  97
      (short) 0xccef, // (12, 3311)  98
      (short) 0xa08f, // (10,  143)  99
      (short) 0xc2ef, // (12,  751) 100
      (short) 0xcaef, // (12, 2799) 101
      (short) 0xc6ef, // (12, 1775) 102
      (short) 0xa28f, // (10,  655) 103
      (short) 0xceef, // (12, 3823) 104
      (short) 0xc1ef, // (12,  495) 105
      (short) 0xc9ef, // (12, 2543) 106
      (short) 0xb5cf, // (11, 1487) 107
      (short) 0xc5ef, // (12, 1519) 108
      (short) 0xcdef, // (12, 3567) 109
      (short) 0xc3ef, // (12, 1007) 110
      (short) 0xb3cf, // (11,  975) 111
      (short) 0xcbef, // (12, 3055) 112
      (short) 0xc7ef, // (12, 2031) 113
      (short) 0xcfef, // (12, 4079) 114
      (short) 0xc01f, // (12,   31) 115
      (short) 0xc81f, // (12, 2079) 116
      (short) 0xc41f, // (12, 1055) 117
      (short) 0xcc1f, // (12, 3103) 118
      (short) 0xc21f, // (12,  543) 119
      (short) 0xca1f, // (12, 2591) 120
      (short) 0xc61f, // (12, 1567) 121
      (short) 0xce1f, // (12, 3615) 122
      (short) 0xc11f, // (12,  287) 123
      (short) 0xc91f, // (12, 2335) 124
      (short) 0xc51f, // (12, 1311) 125
      (short) 0xcd1f, // (12, 3359) 126
      (short) 0xc31f, // (12,  799) 127
      (short) 0xcb1f, // (12, 2847) 128
      (short) 0xb7cf, // (11, 1999) 129
      (short) 0xc71f, // (12, 1823) 130
      (short) 0x80e7, // ( 8,  231) 131
      (short) 0xcf1f, // (12, 3871) 132
      (short) 0xa18f, // (10,  399) 133
      (short) 0xc09f, // (12,  159) 134
      (short) 0x8017, // ( 8,   23) 135
      (short) 0xc89f, // (12, 2207) 136
      (short) 0xc49f, // (12, 1183) 137
      (short) 0xcc9f, // (12, 3231) 138
      (short) 0x9177, // ( 9,  375) 139
      (short) 0xc29f, // (12,  671) 140
      (short) 0xb02f, // (11,   47) 141
      (short) 0xca9f, // (12, 2719) 142
      (short) 0x90f7, // ( 9,  247) 143
      (short) 0xc69f, // (12, 1695) 144
      (short) 0xce9f, // (12, 3743) 145
      (short) 0xc19f, // (12,  415) 146
      (short) 0xa38f, // (10,  911) 147
      (short) 0xc99f, // (12, 2463) 148
      (short) 0xc59f, // (12, 1439) 149
      (short) 0xcd9f, // (12, 3487) 150
      (short) 0xa04f, // (10,   79) 151
      (short) 0xc39f, // (12,  927) 152
      (short) 0xcb9f, // (12, 2975) 153
      (short) 0xc79f, // (12, 1951) 154
      (short) 0xb42f, // (11, 1071) 155
      (short) 0xcf9f, // (12, 3999) 156
      (short) 0xc05f, // (12,   95) 157
      (short) 0xc85f, // (12, 2143) 158
      (short) 0xb22f, // (11,  559) 159
      (short) 0xc45f, // (12, 1119) 160
      (short) 0xcc5f, // (12, 3167) 161
      (short) 0xc25f, // (12,  607) 162
      (short) 0xb62f, // (11, 1583) 163
      (short) 0xca5f, // (12, 2655) 164
      (short) 0xc65f, // (12, 1631) 165
      (short) 0xce5f, // (12, 3679) 166
      (short) 0xb12f, // (11,  303) 167
      (short) 0xc15f, // (12,  351) 168
      (short) 0xc95f, // (12, 2399) 169
      (short) 0xc55f, // (12, 1375) 170
      (short) 0xcd5f, // (12, 3423) 171
      (short) 0xc35f, // (12,  863) 172
      (short) 0xcb5f, // (12, 2911) 173
      (short) 0xc75f, // (12, 1887) 174
      (short) 0xcf5f, // (12, 3935) 175
      (short) 0xc0df, // (12,  223) 176
      (short) 0xc8df, // (12, 2271) 177
      (short) 0xc4df, // (12, 1247) 178
      (short) 0xccdf, // (12, 3295) 179
      (short) 0xc2df, // (12,  735) 180
      (short) 0xcadf, // (12, 2783) 181
      (short) 0xc6df, // (12, 1759) 182
      (short) 0xcedf, // (12, 3807) 183
      (short) 0xc1df, // (12,  479) 184
      (short) 0xc9df, // (12, 2527) 185
      (short) 0xc5df, // (12, 1503) 186
      (short) 0xcddf, // (12, 3551) 187
      (short) 0xc3df, // (12,  991) 188
      (short) 0xcbdf, // (12, 3039) 189
      (short) 0xc7df, // (12, 2015) 190
      (short) 0xcfdf, // (12, 4063) 191
      (short) 0xc03f, // (12,   63) 192
      (short) 0xc83f, // (12, 2111) 193
      (short) 0xc43f, // (12, 1087) 194
      (short) 0xcc3f, // (12, 3135) 195
      (short) 0xc23f, // (12,  575) 196
      (short) 0xca3f, // (12, 2623) 197
      (short) 0xc63f, // (12, 1599) 198
      (short) 0xce3f, // (12, 3647) 199
      (short) 0xc13f, // (12,  319) 200
      (short) 0xc93f, // (12, 2367) 201
      (short) 0xc53f, // (12, 1343) 202
      (short) 0xcd3f, // (12, 3391) 203
      (short) 0xc33f, // (12,  831) 204
      (short) 0xcb3f, // (12, 2879) 205
      (short) 0xc73f, // (12, 1855) 206
      (short) 0xcf3f, // (12, 3903) 207
      (short) 0xc0bf, // (12,  191) 208
      (short) 0xc8bf, // (12, 2239) 209
      (short) 0xc4bf, // (12, 1215) 210
      (short) 0xccbf, // (12, 3263) 211
      (short) 0xc2bf, // (12,  703) 212
      (short) 0xcabf, // (12, 2751) 213
      (short) 0xc6bf, // (12, 1727) 214
      (short) 0xcebf, // (12, 3775) 215
      (short) 0xc1bf, // (12,  447) 216
      (short) 0xc9bf, // (12, 2495) 217
      (short) 0xc5bf, // (12, 1471) 218
      (short) 0xcdbf, // (12, 3519) 219
      (short) 0xc3bf, // (12,  959) 220
      (short) 0xcbbf, // (12, 3007) 221
      (short) 0xc7bf, // (12, 1983) 222
      (short) 0xcfbf, // (12, 4031) 223
      (short) 0xc07f, // (12,  127) 224
      (short) 0xc87f, // (12, 2175) 225
      (short) 0xc47f, // (12, 1151) 226
      (short) 0xcc7f, // (12, 3199) 227
      (short) 0xc27f, // (12,  639) 228
      (short) 0xca7f, // (12, 2687) 229
      (short) 0xc67f, // (12, 1663) 230
      (short) 0xce7f, // (12, 3711) 231
      (short) 0xc17f, // (12,  383) 232
      (short) 0xc97f, // (12, 2431) 233
      (short) 0xc57f, // (12, 1407) 234
      (short) 0xcd7f, // (12, 3455) 235
      (short) 0xc37f, // (12,  895) 236
      (short) 0xcb7f, // (12, 2943) 237
      (short) 0xc77f, // (12, 1919) 238
      (short) 0xcf7f, // (12, 3967) 239
      (short) 0xc0ff, // (12,  255) 240
      (short) 0xc8ff, // (12, 2303) 241
      (short) 0xc4ff, // (12, 1279) 242
      (short) 0xccff, // (12, 3327) 243
      (short) 0xc2ff, // (12,  767) 244
      (short) 0xcaff, // (12, 2815) 245
      (short) 0xc6ff, // (12, 1791) 246
      (short) 0xceff, // (12, 3839) 247
      (short) 0xc1ff, // (12,  511) 248
      (short) 0xc9ff, // (12, 2559) 249
      (short) 0xc5ff, // (12, 1535) 250
      (short) 0xcdff, // (12, 3583) 251
      (short) 0xc3ff, // (12, 1023) 252
      (short) 0xcbff, // (12, 3071) 253
      (short) 0xc7ff, // (12, 2047) 254
      (short) 0xcfff  // (12, 4095) 255
    },

    // (table 4 of 22) (steady 4 of 16) (phase = 0.281250000 = 9.0 / 32.0)
    // entropy:    4.4386754570568340839
    // avg_length: 4.5071584786605640716; max_length = 12; num_symbols = 256
    {
      //table, // (4 bits,  12 bits) symbol
      //entry, // (length, codeword) [byte]
      (short) 0xb24f, // (11,  591)   0
      (short) 0x601d, // ( 6,   29)   1
      (short) 0x9057, // ( 9,   87)   2
      (short) 0x3002, // ( 3,    2)   3
      (short) 0xb64f, // (11, 1615)   4
      (short) 0x5005, // ( 5,    5)   5
      (short) 0x807b, // ( 8,  123)   6
      (short) 0x2000, // ( 2,    0)   7
      (short) 0xc32f, // (12,  815)   8
      (short) 0x700b, // ( 7,   11)   9
      (short) 0xa0f7, // (10,  247)  10
      (short) 0x4001, // ( 4,    1)  11
      (short) 0xb14f, // (11,  335)  12
      (short) 0x603d, // ( 6,   61)  13
      (short) 0x9157, // ( 9,  343)  14
      (short) 0x3006, // ( 3,    6)  15
      (short) 0xcb2f, // (12, 2863)  16
      (short) 0x80fb, // ( 8,  251)  17
      (short) 0xb54f, // (11, 1359)  18
      (short) 0x5015, // ( 5,   21)  19
      (short) 0xc72f, // (12, 1839)  20
      (short) 0x704b, // ( 7,   75)  21
      (short) 0xa2f7, // (10,  759)  22
      (short) 0x4009, // ( 4,    9)  23
      (short) 0xcf2f, // (12, 3887)  24
      (short) 0x8007, // ( 8,    7)  25
      (short) 0xb34f, // (11,  847)  26
      (short) 0x6003, // ( 6,    3)  27
      (short) 0xc0af, // (12,  175)  28
      (short) 0x8087, // ( 8,  135)  29
      (short) 0xb74f, // (11, 1871)  30
      (short) 0x500d, // ( 5,   13)  31
      (short) 0xc8af, // (12, 2223)  32
      (short) 0x90d7, // ( 9,  215)  33
      (short) 0xc4af, // (12, 1199)  34
      (short) 0x6023, // ( 6,   35)  35
      (short) 0xccaf, // (12, 3247)  36
      (short) 0x8047, // ( 8,   71)  37
      (short) 0xb0cf, // (11,  207)  38
      (short) 0x6013, // ( 6,   19)  39
      (short) 0xc2af, // (12,  687)  40
      (short) 0xa1f7, // (10,  503)  41
      (short) 0xcaaf, // (12, 2735)  42
      (short) 0x702b, // ( 7,   43)  43
      (short) 0xc6af, // (12, 1711)  44
      (short) 0x91d7, // ( 9,  471)  45
      (short) 0xceaf, // (12, 3759)  46
      (short) 0x6033, // ( 6,   51)  47
      (short) 0xc1af, // (12,  431)  48
      (short) 0xb4cf, // (11, 1231)  49
      (short) 0xc9af, // (12, 2479)  50
      (short) 0x80c7, // ( 8,  199)  51
      (short) 0xc5af, // (12, 1455)  52
      (short) 0xa3f7, // (10, 1015)  53
      (short) 0xcdaf, // (12, 3503)  54
      (short) 0x706b, // ( 7,  107)  55
      (short) 0xc3af, // (12,  943)  56
      (short) 0xb2cf, // (11,  719)  57
      (short) 0xcbaf, // (12, 2991)  58
      (short) 0x9037, // ( 9,   55)  59
      (short) 0xc7af, // (12, 1967)  60
      (short) 0xb6cf, // (11, 1743)  61
      (short) 0xcfaf, // (12, 4015)  62
      (short) 0x8027, // ( 8,   39)  63
      (short) 0xc06f, // (12,  111)  64
      (short) 0xa00f, // (10,   15)  65
      (short) 0xc86f, // (12, 2159)  66
      (short) 0x701b, // ( 7,   27)  67
      (short) 0xc46f, // (12, 1135)  68
      (short) 0x9137, // ( 9,  311)  69
      (short) 0xcc6f, // (12, 3183)  70
      (short) 0x705b, // ( 7,   91)  71
      (short) 0xc26f, // (12,  623)  72
      (short) 0xb1cf, // (11,  463)  73
      (short) 0xca6f, // (12, 2671)  74
      (short) 0x80a7, // ( 8,  167)  75
      (short) 0xc66f, // (12, 1647)  76
      (short) 0xa20f, // (10,  527)  77
      (short) 0xce6f, // (12, 3695)  78
      (short) 0x703b, // ( 7,   59)  79
      (short) 0xc16f, // (12,  367)  80
      (short) 0xc96f, // (12, 2415)  81
      (short) 0xc56f, // (12, 1391)  82
      (short) 0x90b7, // ( 9,  183)  83
      (short) 0xcd6f, // (12, 3439)  84
      (short) 0xb5cf, // (11, 1487)  85
      (short) 0xc36f, // (12,  879)  86
      (short) 0x8067, // ( 8,  103)  87
      (short) 0xcb6f, // (12, 2927)  88
      (short) 0xc76f, // (12, 1903)  89
      (short) 0xcf6f, // (12, 3951)  90
      (short) 0xa10f, // (10,  271)  91
      (short) 0xc0ef, // (12,  239)  92
      (short) 0xc8ef, // (12, 2287)  93
      (short) 0xc4ef, // (12, 1263)  94
      (short) 0x91b7, // ( 9,  439)  95
      (short) 0xccef, // (12, 3311)  96
      (short) 0xc2ef, // (12,  751)  97
      (short) 0xcaef, // (12, 2799)  98
      (short) 0xa30f, // (10,  783)  99
      (short) 0xc6ef, // (12, 1775) 100
      (short) 0xceef, // (12, 3823) 101
      (short) 0xc1ef, // (12,  495) 102
      (short) 0xa08f, // (10,  143) 103
      (short) 0xc9ef, // (12, 2543) 104
      (short) 0xc5ef, // (12, 1519) 105
      (short) 0xcdef, // (12, 3567) 106
      (short) 0xb3cf, // (11,  975) 107
      (short) 0xc3ef, // (12, 1007) 108
      (short) 0xcbef, // (12, 3055) 109
      (short) 0xc7ef, // (12, 2031) 110
      (short) 0xa28f, // (10,  655) 111
      (short) 0xcfef, // (12, 4079) 112
      (short) 0xc01f, // (12,   31) 113
      (short) 0xc81f, // (12, 2079) 114
      (short) 0xc41f, // (12, 1055) 115
      (short) 0xcc1f, // (12, 3103) 116
      (short) 0xc21f, // (12,  543) 117
      (short) 0xca1f, // (12, 2591) 118
      (short) 0xb7cf, // (11, 1999) 119
      (short) 0xc61f, // (12, 1567) 120
      (short) 0xce1f, // (12, 3615) 121
      (short) 0xc11f, // (12,  287) 122
      (short) 0xc91f, // (12, 2335) 123
      (short) 0xc51f, // (12, 1311) 124
      (short) 0xcd1f, // (12, 3359) 125
      (short) 0xc31f, // (12,  799) 126
      (short) 0xcb1f, // (12, 2847) 127
      (short) 0xc71f, // (12, 1823) 128
      (short) 0xb02f, // (11,   47) 129
      (short) 0xcf1f, // (12, 3871) 130
      (short) 0x80e7, // ( 8,  231) 131
      (short) 0xc09f, // (12,  159) 132
      (short) 0xa18f, // (10,  399) 133
      (short) 0xc89f, // (12, 2207) 134
      (short) 0x8017, // ( 8,   23) 135
      (short) 0xc49f, // (12, 1183) 136
      (short) 0xcc9f, // (12, 3231) 137
      (short) 0xc29f, // (12,  671) 138
      (short) 0x9077, // ( 9,  119) 139
      (short) 0xca9f, // (12, 2719) 140
      (short) 0xb42f, // (11, 1071) 141
      (short) 0xc69f, // (12, 1695) 142
      (short) 0x8097, // ( 8,  151) 143
      (short) 0xce9f, // (12, 3743) 144
      (short) 0xc19f, // (12,  415) 145
      (short) 0xc99f, // (12, 2463) 146
      (short) 0xa38f, // (10,  911) 147
      (short) 0xc59f, // (12, 1439) 148
      (short) 0xcd9f, // (12, 3487) 149
      (short) 0xc39f, // (12,  927) 150
      (short) 0x9177, // ( 9,  375) 151
      (short) 0xcb9f, // (12, 2975) 152
      (short) 0xc79f, // (12, 1951) 153
      (short) 0xcf9f, // (12, 3999) 154
      (short) 0xb22f, // (11,  559) 155
      (short) 0xc05f, // (12,   95) 156
      (short) 0xc85f, // (12, 2143) 157
      (short) 0xc45f, // (12, 1119) 158
      (short) 0xa04f, // (10,   79) 159
      (short) 0xcc5f, // (12, 3167) 160
      (short) 0xc25f, // (12,  607) 161
      (short) 0xca5f, // (12, 2655) 162
      (short) 0xb62f, // (11, 1583) 163
      (short) 0xc65f, // (12, 1631) 164
      (short) 0xce5f, // (12, 3679) 165
      (short) 0xc15f, // (12,  351) 166
      (short) 0xb12f, // (11,  303) 167
      (short) 0xc95f, // (12, 2399) 168
      (short) 0xc55f, // (12, 1375) 169
      (short) 0xcd5f, // (12, 3423) 170
      (short) 0xc35f, // (12,  863) 171
      (short) 0xcb5f, // (12, 2911) 172
      (short) 0xc75f, // (12, 1887) 173
      (short) 0xcf5f, // (12, 3935) 174
      (short) 0xb52f, // (11, 1327) 175
      (short) 0xc0df, // (12,  223) 176
      (short) 0xc8df, // (12, 2271) 177
      (short) 0xc4df, // (12, 1247) 178
      (short) 0xccdf, // (12, 3295) 179
      (short) 0xc2df, // (12,  735) 180
      (short) 0xcadf, // (12, 2783) 181
      (short) 0xc6df, // (12, 1759) 182
      (short) 0xcedf, // (12, 3807) 183
      (short) 0xc1df, // (12,  479) 184
      (short) 0xc9df, // (12, 2527) 185
      (short) 0xc5df, // (12, 1503) 186
      (short) 0xcddf, // (12, 3551) 187
      (short) 0xc3df, // (12,  991) 188
      (short) 0xcbdf, // (12, 3039) 189
      (short) 0xc7df, // (12, 2015) 190
      (short) 0xcfdf, // (12, 4063) 191
      (short) 0xc03f, // (12,   63) 192
      (short) 0xc83f, // (12, 2111) 193
      (short) 0xc43f, // (12, 1087) 194
      (short) 0xcc3f, // (12, 3135) 195
      (short) 0xc23f, // (12,  575) 196
      (short) 0xca3f, // (12, 2623) 197
      (short) 0xc63f, // (12, 1599) 198
      (short) 0xce3f, // (12, 3647) 199
      (short) 0xc13f, // (12,  319) 200
      (short) 0xc93f, // (12, 2367) 201
      (short) 0xc53f, // (12, 1343) 202
      (short) 0xcd3f, // (12, 3391) 203
      (short) 0xc33f, // (12,  831) 204
      (short) 0xcb3f, // (12, 2879) 205
      (short) 0xc73f, // (12, 1855) 206
      (short) 0xcf3f, // (12, 3903) 207
      (short) 0xc0bf, // (12,  191) 208
      (short) 0xc8bf, // (12, 2239) 209
      (short) 0xc4bf, // (12, 1215) 210
      (short) 0xccbf, // (12, 3263) 211
      (short) 0xc2bf, // (12,  703) 212
      (short) 0xcabf, // (12, 2751) 213
      (short) 0xc6bf, // (12, 1727) 214
      (short) 0xcebf, // (12, 3775) 215
      (short) 0xc1bf, // (12,  447) 216
      (short) 0xc9bf, // (12, 2495) 217
      (short) 0xc5bf, // (12, 1471) 218
      (short) 0xcdbf, // (12, 3519) 219
      (short) 0xc3bf, // (12,  959) 220
      (short) 0xcbbf, // (12, 3007) 221
      (short) 0xc7bf, // (12, 1983) 222
      (short) 0xcfbf, // (12, 4031) 223
      (short) 0xc07f, // (12,  127) 224
      (short) 0xc87f, // (12, 2175) 225
      (short) 0xc47f, // (12, 1151) 226
      (short) 0xcc7f, // (12, 3199) 227
      (short) 0xc27f, // (12,  639) 228
      (short) 0xca7f, // (12, 2687) 229
      (short) 0xc67f, // (12, 1663) 230
      (short) 0xce7f, // (12, 3711) 231
      (short) 0xc17f, // (12,  383) 232
      (short) 0xc97f, // (12, 2431) 233
      (short) 0xc57f, // (12, 1407) 234
      (short) 0xcd7f, // (12, 3455) 235
      (short) 0xc37f, // (12,  895) 236
      (short) 0xcb7f, // (12, 2943) 237
      (short) 0xc77f, // (12, 1919) 238
      (short) 0xcf7f, // (12, 3967) 239
      (short) 0xc0ff, // (12,  255) 240
      (short) 0xc8ff, // (12, 2303) 241
      (short) 0xc4ff, // (12, 1279) 242
      (short) 0xccff, // (12, 3327) 243
      (short) 0xc2ff, // (12,  767) 244
      (short) 0xcaff, // (12, 2815) 245
      (short) 0xc6ff, // (12, 1791) 246
      (short) 0xceff, // (12, 3839) 247
      (short) 0xc1ff, // (12,  511) 248
      (short) 0xc9ff, // (12, 2559) 249
      (short) 0xc5ff, // (12, 1535) 250
      (short) 0xcdff, // (12, 3583) 251
      (short) 0xc3ff, // (12, 1023) 252
      (short) 0xcbff, // (12, 3071) 253
      (short) 0xc7ff, // (12, 2047) 254
      (short) 0xcfff  // (12, 4095) 255
    },

    // (table 5 of 22) (steady 5 of 16) (phase = 0.343750000 = 11.0 / 32.0)
    // entropy:    4.4308578632493116345
    // avg_length: 4.4996166821663301505; max_length = 12; num_symbols = 256
    {
      //table, // (4 bits,  12 bits) symbol
      //entry, // (length, codeword) [byte]
      (short) 0xc12f, // (12,  303)   0
      (short) 0x601d, // ( 6,   29)   1
      (short) 0x9057, // ( 9,   87)   2
      (short) 0x3002, // ( 3,    2)   3
      (short) 0xb14f, // (11,  335)   4
      (short) 0x5005, // ( 5,    5)   5
      (short) 0x807b, // ( 8,  123)   6
      (short) 0x2000, // ( 2,    0)   7
      (short) 0xc92f, // (12, 2351)   8
      (short) 0x700b, // ( 7,   11)   9
      (short) 0xa1f7, // (10,  503)  10
      (short) 0x4001, // ( 4,    1)  11
      (short) 0xc52f, // (12, 1327)  12
      (short) 0x603d, // ( 6,   61)  13
      (short) 0x9157, // ( 9,  343)  14
      (short) 0x3006, // ( 3,    6)  15
      (short) 0xcd2f, // (12, 3375)  16
      (short) 0x80fb, // ( 8,  251)  17
      (short) 0xb54f, // (11, 1359)  18
      (short) 0x5015, // ( 5,   21)  19
      (short) 0xc32f, // (12,  815)  20
      (short) 0x704b, // ( 7,   75)  21
      (short) 0xa3f7, // (10, 1015)  22
      (short) 0x4009, // ( 4,    9)  23
      (short) 0xcb2f, // (12, 2863)  24
      (short) 0x8007, // ( 8,    7)  25
      (short) 0xc72f, // (12, 1839)  26
      (short) 0x6003, // ( 6,    3)  27
      (short) 0xcf2f, // (12, 3887)  28
      (short) 0x8087, // ( 8,  135)  29
      (short) 0xb34f, // (11,  847)  30
      (short) 0x500d, // ( 5,   13)  31
      (short) 0xc0af, // (12,  175)  32
      (short) 0x90d7, // ( 9,  215)  33
      (short) 0xc8af, // (12, 2223)  34
      (short) 0x6023, // ( 6,   35)  35
      (short) 0xc4af, // (12, 1199)  36
      (short) 0x8047, // ( 8,   71)  37
      (short) 0xb74f, // (11, 1871)  38
      (short) 0x6013, // ( 6,   19)  39
      (short) 0xccaf, // (12, 3247)  40
      (short) 0xa00f, // (10,   15)  41
      (short) 0xc2af, // (12,  687)  42
      (short) 0x702b, // ( 7,   43)  43
      (short) 0xcaaf, // (12, 2735)  44
      (short) 0x91d7, // ( 9,  471)  45
      (short) 0xc6af, // (12, 1711)  46
      (short) 0x6033, // ( 6,   51)  47
      (short) 0xceaf, // (12, 3759)  48
      (short) 0xb0cf, // (11,  207)  49
      (short) 0xc1af, // (12,  431)  50
      (short) 0x80c7, // ( 8,  199)  51
      (short) 0xc9af, // (12, 2479)  52
      (short) 0xa20f, // (10,  527)  53
      (short) 0xc5af, // (12, 1455)  54
      (short) 0x706b, // ( 7,  107)  55
      (short) 0xcdaf, // (12, 3503)  56
      (short) 0xc3af, // (12,  943)  57
      (short) 0xcbaf, // (12, 2991)  58
      (short) 0x9037, // ( 9,   55)  59
      (short) 0xc7af, // (12, 1967)  60
      (short) 0xb4cf, // (11, 1231)  61
      (short) 0xcfaf, // (12, 4015)  62
      (short) 0x8027, // ( 8,   39)  63
      (short) 0xc06f, // (12,  111)  64
      (short) 0xa10f, // (10,  271)  65
      (short) 0xc86f, // (12, 2159)  66
      (short) 0x701b, // ( 7,   27)  67
      (short) 0xc46f, // (12, 1135)  68
      (short) 0x9137, // ( 9,  311)  69
      (short) 0xcc6f, // (12, 3183)  70
      (short) 0x705b, // ( 7,   91)  71
      (short) 0xc26f, // (12,  623)  72
      (short) 0xb2cf, // (11,  719)  73
      (short) 0xca6f, // (12, 2671)  74
      (short) 0x80a7, // ( 8,  167)  75
      (short) 0xc66f, // (12, 1647)  76
      (short) 0xa30f, // (10,  783)  77
      (short) 0xce6f, // (12, 3695)  78
      (short) 0x703b, // ( 7,   59)  79
      (short) 0xc16f, // (12,  367)  80
      (short) 0xc96f, // (12, 2415)  81
      (short) 0xc56f, // (12, 1391)  82
      (short) 0x90b7, // ( 9,  183)  83
      (short) 0xcd6f, // (12, 3439)  84
      (short) 0xb6cf, // (11, 1743)  85
      (short) 0xc36f, // (12,  879)  86
      (short) 0x8067, // ( 8,  103)  87
      (short) 0xcb6f, // (12, 2927)  88
      (short) 0xc76f, // (12, 1903)  89
      (short) 0xcf6f, // (12, 3951)  90
      (short) 0xa08f, // (10,  143)  91
      (short) 0xc0ef, // (12,  239)  92
      (short) 0xc8ef, // (12, 2287)  93
      (short) 0xc4ef, // (12, 1263)  94
      (short) 0x91b7, // ( 9,  439)  95
      (short) 0xccef, // (12, 3311)  96
      (short) 0xc2ef, // (12,  751)  97
      (short) 0xcaef, // (12, 2799)  98
      (short) 0xa28f, // (10,  655)  99
      (short) 0xc6ef, // (12, 1775) 100
      (short) 0xceef, // (12, 3823) 101
      (short) 0xc1ef, // (12,  495) 102
      (short) 0x9077, // ( 9,  119) 103
      (short) 0xc9ef, // (12, 2543) 104
      (short) 0xc5ef, // (12, 1519) 105
      (short) 0xcdef, // (12, 3567) 106
      (short) 0xb1cf, // (11,  463) 107
      (short) 0xc3ef, // (12, 1007) 108
      (short) 0xcbef, // (12, 3055) 109
      (short) 0xc7ef, // (12, 2031) 110
      (short) 0xa18f, // (10,  399) 111
      (short) 0xcfef, // (12, 4079) 112
      (short) 0xc01f, // (12,   31) 113
      (short) 0xc81f, // (12, 2079) 114
      (short) 0xc41f, // (12, 1055) 115
      (short) 0xcc1f, // (12, 3103) 116
      (short) 0xc21f, // (12,  543) 117
      (short) 0xca1f, // (12, 2591) 118
      (short) 0xb5cf, // (11, 1487) 119
      (short) 0xc61f, // (12, 1567) 120
      (short) 0xce1f, // (12, 3615) 121
      (short) 0xc11f, // (12,  287) 122
      (short) 0xc91f, // (12, 2335) 123
      (short) 0xc51f, // (12, 1311) 124
      (short) 0xcd1f, // (12, 3359) 125
      (short) 0xc31f, // (12,  799) 126
      (short) 0xcb1f, // (12, 2847) 127
      (short) 0xc71f, // (12, 1823) 128
      (short) 0xb3cf, // (11,  975) 129
      (short) 0xcf1f, // (12, 3871) 130
      (short) 0x80e7, // ( 8,  231) 131
      (short) 0xc09f, // (12,  159) 132
      (short) 0xa38f, // (10,  911) 133
      (short) 0xc89f, // (12, 2207) 134
      (short) 0x8017, // ( 8,   23) 135
      (short) 0xc49f, // (12, 1183) 136
      (short) 0xcc9f, // (12, 3231) 137
      (short) 0xc29f, // (12,  671) 138
      (short) 0x9177, // ( 9,  375) 139
      (short) 0xca9f, // (12, 2719) 140
      (short) 0xb7cf, // (11, 1999) 141
      (short) 0xc69f, // (12, 1695) 142
      (short) 0x8097, // ( 8,  151) 143
      (short) 0xce9f, // (12, 3743) 144
      (short) 0xc19f, // (12,  415) 145
      (short) 0xc99f, // (12, 2463) 146
      (short) 0xa04f, // (10,   79) 147
      (short) 0xc59f, // (12, 1439) 148
      (short) 0xcd9f, // (12, 3487) 149
      (short) 0xc39f, // (12,  927) 150
      (short) 0x90f7, // ( 9,  247) 151
      (short) 0xcb9f, // (12, 2975) 152
      (short) 0xc79f, // (12, 1951) 153
      (short) 0xcf9f, // (12, 3999) 154
      (short) 0xb02f, // (11,   47) 155
      (short) 0xc05f, // (12,   95) 156
      (short) 0xc85f, // (12, 2143) 157
      (short) 0xc45f, // (12, 1119) 158
      (short) 0xa24f, // (10,  591) 159
      (short) 0xcc5f, // (12, 3167) 160
      (short) 0xc25f, // (12,  607) 161
      (short) 0xca5f, // (12, 2655) 162
      (short) 0xb42f, // (11, 1071) 163
      (short) 0xc65f, // (12, 1631) 164
      (short) 0xce5f, // (12, 3679) 165
      (short) 0xc15f, // (12,  351) 166
      (short) 0xb22f, // (11,  559) 167
      (short) 0xc95f, // (12, 2399) 168
      (short) 0xc55f, // (12, 1375) 169
      (short) 0xcd5f, // (12, 3423) 170
      (short) 0xc35f, // (12,  863) 171
      (short) 0xcb5f, // (12, 2911) 172
      (short) 0xc75f, // (12, 1887) 173
      (short) 0xcf5f, // (12, 3935) 174
      (short) 0xb62f, // (11, 1583) 175
      (short) 0xc0df, // (12,  223) 176
      (short) 0xc8df, // (12, 2271) 177
      (short) 0xc4df, // (12, 1247) 178
      (short) 0xccdf, // (12, 3295) 179
      (short) 0xc2df, // (12,  735) 180
      (short) 0xcadf, // (12, 2783) 181
      (short) 0xc6df, // (12, 1759) 182
      (short) 0xcedf, // (12, 3807) 183
      (short) 0xc1df, // (12,  479) 184
      (short) 0xc9df, // (12, 2527) 185
      (short) 0xc5df, // (12, 1503) 186
      (short) 0xcddf, // (12, 3551) 187
      (short) 0xc3df, // (12,  991) 188
      (short) 0xcbdf, // (12, 3039) 189
      (short) 0xc7df, // (12, 2015) 190
      (short) 0xcfdf, // (12, 4063) 191
      (short) 0xc03f, // (12,   63) 192
      (short) 0xc83f, // (12, 2111) 193
      (short) 0xc43f, // (12, 1087) 194
      (short) 0xcc3f, // (12, 3135) 195
      (short) 0xc23f, // (12,  575) 196
      (short) 0xca3f, // (12, 2623) 197
      (short) 0xc63f, // (12, 1599) 198
      (short) 0xce3f, // (12, 3647) 199
      (short) 0xc13f, // (12,  319) 200
      (short) 0xc93f, // (12, 2367) 201
      (short) 0xc53f, // (12, 1343) 202
      (short) 0xcd3f, // (12, 3391) 203
      (short) 0xc33f, // (12,  831) 204
      (short) 0xcb3f, // (12, 2879) 205
      (short) 0xc73f, // (12, 1855) 206
      (short) 0xcf3f, // (12, 3903) 207
      (short) 0xc0bf, // (12,  191) 208
      (short) 0xc8bf, // (12, 2239) 209
      (short) 0xc4bf, // (12, 1215) 210
      (short) 0xccbf, // (12, 3263) 211
      (short) 0xc2bf, // (12,  703) 212
      (short) 0xcabf, // (12, 2751) 213
      (short) 0xc6bf, // (12, 1727) 214
      (short) 0xcebf, // (12, 3775) 215
      (short) 0xc1bf, // (12,  447) 216
      (short) 0xc9bf, // (12, 2495) 217
      (short) 0xc5bf, // (12, 1471) 218
      (short) 0xcdbf, // (12, 3519) 219
      (short) 0xc3bf, // (12,  959) 220
      (short) 0xcbbf, // (12, 3007) 221
      (short) 0xc7bf, // (12, 1983) 222
      (short) 0xcfbf, // (12, 4031) 223
      (short) 0xc07f, // (12,  127) 224
      (short) 0xc87f, // (12, 2175) 225
      (short) 0xc47f, // (12, 1151) 226
      (short) 0xcc7f, // (12, 3199) 227
      (short) 0xc27f, // (12,  639) 228
      (short) 0xca7f, // (12, 2687) 229
      (short) 0xc67f, // (12, 1663) 230
      (short) 0xce7f, // (12, 3711) 231
      (short) 0xc17f, // (12,  383) 232
      (short) 0xc97f, // (12, 2431) 233
      (short) 0xc57f, // (12, 1407) 234
      (short) 0xcd7f, // (12, 3455) 235
      (short) 0xc37f, // (12,  895) 236
      (short) 0xcb7f, // (12, 2943) 237
      (short) 0xc77f, // (12, 1919) 238
      (short) 0xcf7f, // (12, 3967) 239
      (short) 0xc0ff, // (12,  255) 240
      (short) 0xc8ff, // (12, 2303) 241
      (short) 0xc4ff, // (12, 1279) 242
      (short) 0xccff, // (12, 3327) 243
      (short) 0xc2ff, // (12,  767) 244
      (short) 0xcaff, // (12, 2815) 245
      (short) 0xc6ff, // (12, 1791) 246
      (short) 0xceff, // (12, 3839) 247
      (short) 0xc1ff, // (12,  511) 248
      (short) 0xc9ff, // (12, 2559) 249
      (short) 0xc5ff, // (12, 1535) 250
      (short) 0xcdff, // (12, 3583) 251
      (short) 0xc3ff, // (12, 1023) 252
      (short) 0xcbff, // (12, 3071) 253
      (short) 0xc7ff, // (12, 2047) 254
      (short) 0xcfff  // (12, 4095) 255
    },

    // (table 6 of 22) (steady 6 of 16) (phase = 0.406250000 = 13.0 / 32.0)
    // entropy:    4.4310364988500126060
    // avg_length: 4.5051134111084252254; max_length = 12; num_symbols = 256
    {
      //table, // (4 bits,  12 bits) symbol
      //entry, // (length, codeword) [byte]
      (short) 0x601d, // ( 6,   29)   0
      (short) 0x3002, // ( 3,    2)   1
      (short) 0x603d, // ( 6,   61)   2
      (short) 0x2000, // ( 2,    0)   3
      (short) 0x700b, // ( 7,   11)   4
      (short) 0x4001, // ( 4,    1)   5
      (short) 0x6003, // ( 6,    3)   6
      (short) 0x3006, // ( 3,    6)   7
      (short) 0x807b, // ( 8,  123)   8
      (short) 0x5005, // ( 5,    5)   9
      (short) 0x704b, // ( 7,   75)  10
      (short) 0x4009, // ( 4,    9)  11
      (short) 0x9097, // ( 9,  151)  12
      (short) 0x6023, // ( 6,   35)  13
      (short) 0x80fb, // ( 8,  251)  14
      (short) 0x5015, // ( 5,   21)  15
      (short) 0x9197, // ( 9,  407)  16
      (short) 0x6013, // ( 6,   19)  17
      (short) 0x8007, // ( 8,    7)  18
      (short) 0x500d, // ( 5,   13)  19
      (short) 0xa0f7, // (10,  247)  20
      (short) 0x702b, // ( 7,   43)  21
      (short) 0x9057, // ( 9,   87)  22
      (short) 0x6033, // ( 6,   51)  23
      (short) 0xb14f, // (11,  335)  24
      (short) 0x8087, // ( 8,  135)  25
      (short) 0xa2f7, // (10,  759)  26
      (short) 0x706b, // ( 7,  107)  27
      (short) 0xb54f, // (11, 1359)  28
      (short) 0x9157, // ( 9,  343)  29
      (short) 0xa1f7, // (10,  503)  30
      (short) 0x8047, // ( 8,   71)  31
      (short) 0xa3f7, // (10, 1015)  32
      (short) 0x701b, // ( 7,   27)  33
      (short) 0x90d7, // ( 9,  215)  34
      (short) 0x705b, // ( 7,   91)  35
      (short) 0xb34f, // (11,  847)  36
      (short) 0x80c7, // ( 8,  199)  37
      (short) 0xa00f, // (10,   15)  38
      (short) 0x703b, // ( 7,   59)  39
      (short) 0xc32f, // (12,  815)  40
      (short) 0x91d7, // ( 9,  471)  41
      (short) 0xb74f, // (11, 1871)  42
      (short) 0x8027, // ( 8,   39)  43
      (short) 0xcb2f, // (12, 2863)  44
      (short) 0xa20f, // (10,  527)  45
      (short) 0xb0cf, // (11,  207)  46
      (short) 0x9037, // ( 9,   55)  47
      (short) 0xc72f, // (12, 1839)  48
      (short) 0xa10f, // (10,  271)  49
      (short) 0xcf2f, // (12, 3887)  50
      (short) 0x9137, // ( 9,  311)  51
      (short) 0xc0af, // (12,  175)  52
      (short) 0xb4cf, // (11, 1231)  53
      (short) 0xc8af, // (12, 2223)  54
      (short) 0xa30f, // (10,  783)  55
      (short) 0xc4af, // (12, 1199)  56
      (short) 0xccaf, // (12, 3247)  57
      (short) 0xc2af, // (12,  687)  58
      (short) 0xb2cf, // (11,  719)  59
      (short) 0xcaaf, // (12, 2735)  60
      (short) 0xc6af, // (12, 1711)  61
      (short) 0xceaf, // (12, 3759)  62
      (short) 0xb6cf, // (11, 1743)  63
      (short) 0xb1cf, // (11,  463)  64
      (short) 0x80a7, // ( 8,  167)  65
      (short) 0xa08f, // (10,  143)  66
      (short) 0x8067, // ( 8,  103)  67
      (short) 0xc1af, // (12,  431)  68
      (short) 0x90b7, // ( 9,  183)  69
      (short) 0xb5cf, // (11, 1487)  70
      (short) 0x80e7, // ( 8,  231)  71
      (short) 0xc9af, // (12, 2479)  72
      (short) 0xa28f, // (10,  655)  73
      (short) 0xc5af, // (12, 1455)  74
      (short) 0x91b7, // ( 9,  439)  75
      (short) 0xcdaf, // (12, 3503)  76
      (short) 0xb3cf, // (11,  975)  77
      (short) 0xc3af, // (12,  943)  78
      (short) 0xa18f, // (10,  399)  79
      (short) 0xcbaf, // (12, 2991)  80
      (short) 0xb7cf, // (11, 1999)  81
      (short) 0xc7af, // (12, 1967)  82
      (short) 0xa38f, // (10,  911)  83
      (short) 0xcfaf, // (12, 4015)  84
      (short) 0xc06f, // (12,  111)  85
      (short) 0xc86f, // (12, 2159)  86
      (short) 0xb02f, // (11,   47)  87
      (short) 0xc46f, // (12, 1135)  88
      (short) 0xcc6f, // (12, 3183)  89
      (short) 0xc26f, // (12,  623)  90
      (short) 0xca6f, // (12, 2671)  91
      (short) 0xc66f, // (12, 1647)  92
      (short) 0xce6f, // (12, 3695)  93
      (short) 0xc16f, // (12,  367)  94
      (short) 0xc96f, // (12, 2415)  95
      (short) 0xc56f, // (12, 1391)  96
      (short) 0xcd6f, // (12, 3439)  97
      (short) 0xc36f, // (12,  879)  98
      (short) 0xb42f, // (11, 1071)  99
      (short) 0xcb6f, // (12, 2927) 100
      (short) 0xc76f, // (12, 1903) 101
      (short) 0xcf6f, // (12, 3951) 102
      (short) 0xc0ef, // (12,  239) 103
      (short) 0xc8ef, // (12, 2287) 104
      (short) 0xc4ef, // (12, 1263) 105
      (short) 0xccef, // (12, 3311) 106
      (short) 0xc2ef, // (12,  751) 107
      (short) 0xcaef, // (12, 2799) 108
      (short) 0xc6ef, // (12, 1775) 109
      (short) 0xceef, // (12, 3823) 110
      (short) 0xc1ef, // (12,  495) 111
      (short) 0xc9ef, // (12, 2543) 112
      (short) 0xc5ef, // (12, 1519) 113
      (short) 0xcdef, // (12, 3567) 114
      (short) 0xc3ef, // (12, 1007) 115
      (short) 0xcbef, // (12, 3055) 116
      (short) 0xc7ef, // (12, 2031) 117
      (short) 0xcfef, // (12, 4079) 118
      (short) 0xc01f, // (12,   31) 119
      (short) 0xc81f, // (12, 2079) 120
      (short) 0xc41f, // (12, 1055) 121
      (short) 0xcc1f, // (12, 3103) 122
      (short) 0xc21f, // (12,  543) 123
      (short) 0xca1f, // (12, 2591) 124
      (short) 0xc61f, // (12, 1567) 125
      (short) 0xce1f, // (12, 3615) 126
      (short) 0xc11f, // (12,  287) 127
      (short) 0xc91f, // (12, 2335) 128
      (short) 0x9077, // ( 9,  119) 129
      (short) 0xb22f, // (11,  559) 130
      (short) 0x8017, // ( 8,   23) 131
      (short) 0xc51f, // (12, 1311) 132
      (short) 0xa04f, // (10,   79) 133
      (short) 0xcd1f, // (12, 3359) 134
      (short) 0x9177, // ( 9,  375) 135
      (short) 0xc31f, // (12,  799) 136
      (short) 0xb62f, // (11, 1583) 137
      (short) 0xcb1f, // (12, 2847) 138
      (short) 0xa24f, // (10,  591) 139
      (short) 0xc71f, // (12, 1823) 140
      (short) 0xcf1f, // (12, 3871) 141
      (short) 0xc09f, // (12,  159) 142
      (short) 0xb12f, // (11,  303) 143
      (short) 0xc89f, // (12, 2207) 144
      (short) 0xc49f, // (12, 1183) 145
      (short) 0xcc9f, // (12, 3231) 146
      (short) 0xb52f, // (11, 1327) 147
      (short) 0xc29f, // (12,  671) 148
      (short) 0xca9f, // (12, 2719) 149
      (short) 0xc69f, // (12, 1695) 150
      (short) 0xce9f, // (12, 3743) 151
      (short) 0xc19f, // (12,  415) 152
      (short) 0xc99f, // (12, 2463) 153
      (short) 0xc59f, // (12, 1439) 154
      (short) 0xcd9f, // (12, 3487) 155
      (short) 0xc39f, // (12,  927) 156
      (short) 0xcb9f, // (12, 2975) 157
      (short) 0xc79f, // (12, 1951) 158
      (short) 0xcf9f, // (12, 3999) 159
      (short) 0xc05f, // (12,   95) 160
      (short) 0xc85f, // (12, 2143) 161
      (short) 0xc45f, // (12, 1119) 162
      (short) 0xcc5f, // (12, 3167) 163
      (short) 0xc25f, // (12,  607) 164
      (short) 0xca5f, // (12, 2655) 165
      (short) 0xc65f, // (12, 1631) 166
      (short) 0xce5f, // (12, 3679) 167
      (short) 0xc15f, // (12,  351) 168
      (short) 0xc95f, // (12, 2399) 169
      (short) 0xc55f, // (12, 1375) 170
      (short) 0xcd5f, // (12, 3423) 171
      (short) 0xc35f, // (12,  863) 172
      (short) 0xcb5f, // (12, 2911) 173
      (short) 0xc75f, // (12, 1887) 174
      (short) 0xcf5f, // (12, 3935) 175
      (short) 0xc0df, // (12,  223) 176
      (short) 0xc8df, // (12, 2271) 177
      (short) 0xc4df, // (12, 1247) 178
      (short) 0xccdf, // (12, 3295) 179
      (short) 0xc2df, // (12,  735) 180
      (short) 0xcadf, // (12, 2783) 181
      (short) 0xc6df, // (12, 1759) 182
      (short) 0xcedf, // (12, 3807) 183
      (short) 0xc1df, // (12,  479) 184
      (short) 0xc9df, // (12, 2527) 185
      (short) 0xc5df, // (12, 1503) 186
      (short) 0xcddf, // (12, 3551) 187
      (short) 0xc3df, // (12,  991) 188
      (short) 0xcbdf, // (12, 3039) 189
      (short) 0xc7df, // (12, 2015) 190
      (short) 0xcfdf, // (12, 4063) 191
      (short) 0xc03f, // (12,   63) 192
      (short) 0xc83f, // (12, 2111) 193
      (short) 0xc43f, // (12, 1087) 194
      (short) 0xcc3f, // (12, 3135) 195
      (short) 0xc23f, // (12,  575) 196
      (short) 0xca3f, // (12, 2623) 197
      (short) 0xc63f, // (12, 1599) 198
      (short) 0xce3f, // (12, 3647) 199
      (short) 0xc13f, // (12,  319) 200
      (short) 0xc93f, // (12, 2367) 201
      (short) 0xc53f, // (12, 1343) 202
      (short) 0xcd3f, // (12, 3391) 203
      (short) 0xc33f, // (12,  831) 204
      (short) 0xcb3f, // (12, 2879) 205
      (short) 0xc73f, // (12, 1855) 206
      (short) 0xcf3f, // (12, 3903) 207
      (short) 0xc0bf, // (12,  191) 208
      (short) 0xc8bf, // (12, 2239) 209
      (short) 0xc4bf, // (12, 1215) 210
      (short) 0xccbf, // (12, 3263) 211
      (short) 0xc2bf, // (12,  703) 212
      (short) 0xcabf, // (12, 2751) 213
      (short) 0xc6bf, // (12, 1727) 214
      (short) 0xcebf, // (12, 3775) 215
      (short) 0xc1bf, // (12,  447) 216
      (short) 0xc9bf, // (12, 2495) 217
      (short) 0xc5bf, // (12, 1471) 218
      (short) 0xcdbf, // (12, 3519) 219
      (short) 0xc3bf, // (12,  959) 220
      (short) 0xcbbf, // (12, 3007) 221
      (short) 0xc7bf, // (12, 1983) 222
      (short) 0xcfbf, // (12, 4031) 223
      (short) 0xc07f, // (12,  127) 224
      (short) 0xc87f, // (12, 2175) 225
      (short) 0xc47f, // (12, 1151) 226
      (short) 0xcc7f, // (12, 3199) 227
      (short) 0xc27f, // (12,  639) 228
      (short) 0xca7f, // (12, 2687) 229
      (short) 0xc67f, // (12, 1663) 230
      (short) 0xce7f, // (12, 3711) 231
      (short) 0xc17f, // (12,  383) 232
      (short) 0xc97f, // (12, 2431) 233
      (short) 0xc57f, // (12, 1407) 234
      (short) 0xcd7f, // (12, 3455) 235
      (short) 0xc37f, // (12,  895) 236
      (short) 0xcb7f, // (12, 2943) 237
      (short) 0xc77f, // (12, 1919) 238
      (short) 0xcf7f, // (12, 3967) 239
      (short) 0xc0ff, // (12,  255) 240
      (short) 0xc8ff, // (12, 2303) 241
      (short) 0xc4ff, // (12, 1279) 242
      (short) 0xccff, // (12, 3327) 243
      (short) 0xc2ff, // (12,  767) 244
      (short) 0xcaff, // (12, 2815) 245
      (short) 0xc6ff, // (12, 1791) 246
      (short) 0xceff, // (12, 3839) 247
      (short) 0xc1ff, // (12,  511) 248
      (short) 0xc9ff, // (12, 2559) 249
      (short) 0xc5ff, // (12, 1535) 250
      (short) 0xcdff, // (12, 3583) 251
      (short) 0xc3ff, // (12, 1023) 252
      (short) 0xcbff, // (12, 3071) 253
      (short) 0xc7ff, // (12, 2047) 254
      (short) 0xcfff  // (12, 4095) 255
    },

    // (table 7 of 22) (steady 7 of 16) (phase = 0.468750000 = 15.0 / 32.0)
    // entropy:    4.4417871821766841123
    // avg_length: 4.5206419191518980583; max_length = 12; num_symbols = 256
    {
      //table, // (4 bits,  12 bits) symbol
      //entry, // (length, codeword) [byte]
      (short) 0x700b, // ( 7,   11)   0
      (short) 0x3002, // ( 3,    2)   1
      (short) 0x601d, // ( 6,   29)   2
      (short) 0x2000, // ( 2,    0)   3
      (short) 0x704b, // ( 7,   75)   4
      (short) 0x4001, // ( 4,    1)   5
      (short) 0x603d, // ( 6,   61)   6
      (short) 0x3006, // ( 3,    6)   7
      (short) 0x8007, // ( 8,    7)   8
      (short) 0x5005, // ( 5,    5)   9
      (short) 0x702b, // ( 7,   43)  10
      (short) 0x4009, // ( 4,    9)  11
      (short) 0x9097, // ( 9,  151)  12
      (short) 0x6003, // ( 6,    3)  13
      (short) 0x8087, // ( 8,  135)  14
      (short) 0x5015, // ( 5,   21)  15
      (short) 0x9197, // ( 9,  407)  16
      (short) 0x6023, // ( 6,   35)  17
      (short) 0x8047, // ( 8,   71)  18
      (short) 0x500d, // ( 5,   13)  19
      (short) 0xa0f7, // (10,  247)  20
      (short) 0x706b, // ( 7,  107)  21
      (short) 0x9057, // ( 9,   87)  22
      (short) 0x6013, // ( 6,   19)  23
      (short) 0xb14f, // (11,  335)  24
      (short) 0x80c7, // ( 8,  199)  25
      (short) 0xa2f7, // (10,  759)  26
      (short) 0x701b, // ( 7,   27)  27
      (short) 0xc52f, // (12, 1327)  28
      (short) 0x9157, // ( 9,  343)  29
      (short) 0xb54f, // (11, 1359)  30
      (short) 0x8027, // ( 8,   39)  31
      (short) 0xa1f7, // (10,  503)  32
      (short) 0x705b, // ( 7,   91)  33
      (short) 0x90d7, // ( 9,  215)  34
      (short) 0x6033, // ( 6,   51)  35
      (short) 0xb34f, // (11,  847)  36
      (short) 0x80a7, // ( 8,  167)  37
      (short) 0xa3f7, // (10, 1015)  38
      (short) 0x703b, // ( 7,   59)  39
      (short) 0xcd2f, // (12, 3375)  40
      (short) 0x91d7, // ( 9,  471)  41
      (short) 0xb74f, // (11, 1871)  42
      (short) 0x8067, // ( 8,  103)  43
      (short) 0xc32f, // (12,  815)  44
      (short) 0xa00f, // (10,   15)  45
      (short) 0xcb2f, // (12, 2863)  46
      (short) 0x9037, // ( 9,   55)  47
      (short) 0xc72f, // (12, 1839)  48
      (short) 0xa20f, // (10,  527)  49
      (short) 0xcf2f, // (12, 3887)  50
      (short) 0x9137, // ( 9,  311)  51
      (short) 0xc0af, // (12,  175)  52
      (short) 0xb0cf, // (11,  207)  53
      (short) 0xc8af, // (12, 2223)  54
      (short) 0xa10f, // (10,  271)  55
      (short) 0xc4af, // (12, 1199)  56
      (short) 0xccaf, // (12, 3247)  57
      (short) 0xc2af, // (12,  687)  58
      (short) 0xb4cf, // (11, 1231)  59
      (short) 0xcaaf, // (12, 2735)  60
      (short) 0xc6af, // (12, 1711)  61
      (short) 0xceaf, // (12, 3759)  62
      (short) 0xb2cf, // (11,  719)  63
      (short) 0xb6cf, // (11, 1743)  64
      (short) 0x80e7, // ( 8,  231)  65
      (short) 0xa30f, // (10,  783)  66
      (short) 0x707b, // ( 7,  123)  67
      (short) 0xc1af, // (12,  431)  68
      (short) 0x90b7, // ( 9,  183)  69
      (short) 0xb1cf, // (11,  463)  70
      (short) 0x8017, // ( 8,   23)  71
      (short) 0xc9af, // (12, 2479)  72
      (short) 0xa08f, // (10,  143)  73
      (short) 0xc5af, // (12, 1455)  74
      (short) 0x91b7, // ( 9,  439)  75
      (short) 0xcdaf, // (12, 3503)  76
      (short) 0xb5cf, // (11, 1487)  77
      (short) 0xc3af, // (12,  943)  78
      (short) 0xa28f, // (10,  655)  79
      (short) 0xcbaf, // (12, 2991)  80
      (short) 0xb3cf, // (11,  975)  81
      (short) 0xc7af, // (12, 1967)  82
      (short) 0xa18f, // (10,  399)  83
      (short) 0xcfaf, // (12, 4015)  84
      (short) 0xc06f, // (12,  111)  85
      (short) 0xc86f, // (12, 2159)  86
      (short) 0xb7cf, // (11, 1999)  87
      (short) 0xc46f, // (12, 1135)  88
      (short) 0xcc6f, // (12, 3183)  89
      (short) 0xc26f, // (12,  623)  90
      (short) 0xca6f, // (12, 2671)  91
      (short) 0xc66f, // (12, 1647)  92
      (short) 0xce6f, // (12, 3695)  93
      (short) 0xc16f, // (12,  367)  94
      (short) 0xc96f, // (12, 2415)  95
      (short) 0xc56f, // (12, 1391)  96
      (short) 0xcd6f, // (12, 3439)  97
      (short) 0xc36f, // (12,  879)  98
      (short) 0xb02f, // (11,   47)  99
      (short) 0xcb6f, // (12, 2927) 100
      (short) 0xc76f, // (12, 1903) 101
      (short) 0xcf6f, // (12, 3951) 102
      (short) 0xc0ef, // (12,  239) 103
      (short) 0xc8ef, // (12, 2287) 104
      (short) 0xc4ef, // (12, 1263) 105
      (short) 0xccef, // (12, 3311) 106
      (short) 0xc2ef, // (12,  751) 107
      (short) 0xcaef, // (12, 2799) 108
      (short) 0xc6ef, // (12, 1775) 109
      (short) 0xceef, // (12, 3823) 110
      (short) 0xc1ef, // (12,  495) 111
      (short) 0xc9ef, // (12, 2543) 112
      (short) 0xc5ef, // (12, 1519) 113
      (short) 0xcdef, // (12, 3567) 114
      (short) 0xc3ef, // (12, 1007) 115
      (short) 0xcbef, // (12, 3055) 116
      (short) 0xc7ef, // (12, 2031) 117
      (short) 0xcfef, // (12, 4079) 118
      (short) 0xc01f, // (12,   31) 119
      (short) 0xc81f, // (12, 2079) 120
      (short) 0xc41f, // (12, 1055) 121
      (short) 0xcc1f, // (12, 3103) 122
      (short) 0xc21f, // (12,  543) 123
      (short) 0xca1f, // (12, 2591) 124
      (short) 0xc61f, // (12, 1567) 125
      (short) 0xce1f, // (12, 3615) 126
      (short) 0xc11f, // (12,  287) 127
      (short) 0xc91f, // (12, 2335) 128
      (short) 0xa38f, // (10,  911) 129
      (short) 0xb42f, // (11, 1071) 130
      (short) 0x9077, // ( 9,  119) 131
      (short) 0xc51f, // (12, 1311) 132
      (short) 0xa04f, // (10,   79) 133
      (short) 0xcd1f, // (12, 3359) 134
      (short) 0x9177, // ( 9,  375) 135
      (short) 0xc31f, // (12,  799) 136
      (short) 0xb22f, // (11,  559) 137
      (short) 0xcb1f, // (12, 2847) 138
      (short) 0xa24f, // (10,  591) 139
      (short) 0xc71f, // (12, 1823) 140
      (short) 0xcf1f, // (12, 3871) 141
      (short) 0xc09f, // (12,  159) 142
      (short) 0xb62f, // (11, 1583) 143
      (short) 0xc89f, // (12, 2207) 144
      (short) 0xc49f, // (12, 1183) 145
      (short) 0xcc9f, // (12, 3231) 146
      (short) 0xb12f, // (11,  303) 147
      (short) 0xc29f, // (12,  671) 148
      (short) 0xca9f, // (12, 2719) 149
      (short) 0xc69f, // (12, 1695) 150
      (short) 0xce9f, // (12, 3743) 151
      (short) 0xc19f, // (12,  415) 152
      (short) 0xc99f, // (12, 2463) 153
      (short) 0xc59f, // (12, 1439) 154
      (short) 0xcd9f, // (12, 3487) 155
      (short) 0xc39f, // (12,  927) 156
      (short) 0xcb9f, // (12, 2975) 157
      (short) 0xc79f, // (12, 1951) 158
      (short) 0xcf9f, // (12, 3999) 159
      (short) 0xc05f, // (12,   95) 160
      (short) 0xc85f, // (12, 2143) 161
      (short) 0xc45f, // (12, 1119) 162
      (short) 0xcc5f, // (12, 3167) 163
      (short) 0xc25f, // (12,  607) 164
      (short) 0xca5f, // (12, 2655) 165
      (short) 0xc65f, // (12, 1631) 166
      (short) 0xce5f, // (12, 3679) 167
      (short) 0xc15f, // (12,  351) 168
      (short) 0xc95f, // (12, 2399) 169
      (short) 0xc55f, // (12, 1375) 170
      (short) 0xcd5f, // (12, 3423) 171
      (short) 0xc35f, // (12,  863) 172
      (short) 0xcb5f, // (12, 2911) 173
      (short) 0xc75f, // (12, 1887) 174
      (short) 0xcf5f, // (12, 3935) 175
      (short) 0xc0df, // (12,  223) 176
      (short) 0xc8df, // (12, 2271) 177
      (short) 0xc4df, // (12, 1247) 178
      (short) 0xccdf, // (12, 3295) 179
      (short) 0xc2df, // (12,  735) 180
      (short) 0xcadf, // (12, 2783) 181
      (short) 0xc6df, // (12, 1759) 182
      (short) 0xcedf, // (12, 3807) 183
      (short) 0xc1df, // (12,  479) 184
      (short) 0xc9df, // (12, 2527) 185
      (short) 0xc5df, // (12, 1503) 186
      (short) 0xcddf, // (12, 3551) 187
      (short) 0xc3df, // (12,  991) 188
      (short) 0xcbdf, // (12, 3039) 189
      (short) 0xc7df, // (12, 2015) 190
      (short) 0xcfdf, // (12, 4063) 191
      (short) 0xc03f, // (12,   63) 192
      (short) 0xc83f, // (12, 2111) 193
      (short) 0xc43f, // (12, 1087) 194
      (short) 0xcc3f, // (12, 3135) 195
      (short) 0xc23f, // (12,  575) 196
      (short) 0xca3f, // (12, 2623) 197
      (short) 0xc63f, // (12, 1599) 198
      (short) 0xce3f, // (12, 3647) 199
      (short) 0xc13f, // (12,  319) 200
      (short) 0xc93f, // (12, 2367) 201
      (short) 0xc53f, // (12, 1343) 202
      (short) 0xcd3f, // (12, 3391) 203
      (short) 0xc33f, // (12,  831) 204
      (short) 0xcb3f, // (12, 2879) 205
      (short) 0xc73f, // (12, 1855) 206
      (short) 0xcf3f, // (12, 3903) 207
      (short) 0xc0bf, // (12,  191) 208
      (short) 0xc8bf, // (12, 2239) 209
      (short) 0xc4bf, // (12, 1215) 210
      (short) 0xccbf, // (12, 3263) 211
      (short) 0xc2bf, // (12,  703) 212
      (short) 0xcabf, // (12, 2751) 213
      (short) 0xc6bf, // (12, 1727) 214
      (short) 0xcebf, // (12, 3775) 215
      (short) 0xc1bf, // (12,  447) 216
      (short) 0xc9bf, // (12, 2495) 217
      (short) 0xc5bf, // (12, 1471) 218
      (short) 0xcdbf, // (12, 3519) 219
      (short) 0xc3bf, // (12,  959) 220
      (short) 0xcbbf, // (12, 3007) 221
      (short) 0xc7bf, // (12, 1983) 222
      (short) 0xcfbf, // (12, 4031) 223
      (short) 0xc07f, // (12,  127) 224
      (short) 0xc87f, // (12, 2175) 225
      (short) 0xc47f, // (12, 1151) 226
      (short) 0xcc7f, // (12, 3199) 227
      (short) 0xc27f, // (12,  639) 228
      (short) 0xca7f, // (12, 2687) 229
      (short) 0xc67f, // (12, 1663) 230
      (short) 0xce7f, // (12, 3711) 231
      (short) 0xc17f, // (12,  383) 232
      (short) 0xc97f, // (12, 2431) 233
      (short) 0xc57f, // (12, 1407) 234
      (short) 0xcd7f, // (12, 3455) 235
      (short) 0xc37f, // (12,  895) 236
      (short) 0xcb7f, // (12, 2943) 237
      (short) 0xc77f, // (12, 1919) 238
      (short) 0xcf7f, // (12, 3967) 239
      (short) 0xc0ff, // (12,  255) 240
      (short) 0xc8ff, // (12, 2303) 241
      (short) 0xc4ff, // (12, 1279) 242
      (short) 0xccff, // (12, 3327) 243
      (short) 0xc2ff, // (12,  767) 244
      (short) 0xcaff, // (12, 2815) 245
      (short) 0xc6ff, // (12, 1791) 246
      (short) 0xceff, // (12, 3839) 247
      (short) 0xc1ff, // (12,  511) 248
      (short) 0xc9ff, // (12, 2559) 249
      (short) 0xc5ff, // (12, 1535) 250
      (short) 0xcdff, // (12, 3583) 251
      (short) 0xc3ff, // (12, 1023) 252
      (short) 0xcbff, // (12, 3071) 253
      (short) 0xc7ff, // (12, 2047) 254
      (short) 0xcfff  // (12, 4095) 255
    },

    // (table 8 of 22) (steady 8 of 16) (phase = 0.531250000 = 17.0 / 32.0)
    // entropy:    4.4505873338397474726
    // avg_length: 4.5270058771550303334; max_length = 12; num_symbols = 256
    {
      //table, // (4 bits,  12 bits) symbol
      //entry, // (length, codeword) [byte]
      (short) 0x7033, // ( 7,   51)   0
      (short) 0x4006, // ( 4,    6)   1
      (short) 0x601d, // ( 6,   29)   2
      (short) 0x2000, // ( 2,    0)   3
      (short) 0x7073, // ( 7,  115)   4
      (short) 0x400e, // ( 4,   14)   5
      (short) 0x603d, // ( 6,   61)   6
      (short) 0x3002, // ( 3,    2)   7
      (short) 0x807b, // ( 8,  123)   8
      (short) 0x5005, // ( 5,    5)   9
      (short) 0x700b, // ( 7,   11)  10
      (short) 0x4001, // ( 4,    1)  11
      (short) 0x9097, // ( 9,  151)  12
      (short) 0x5015, // ( 5,   21)  13
      (short) 0x80fb, // ( 8,  251)  14
      (short) 0x4009, // ( 4,    9)  15
      (short) 0xa0f7, // (10,  247)  16
      (short) 0x6003, // ( 6,    3)  17
      (short) 0x8007, // ( 8,    7)  18
      (short) 0x500d, // ( 5,   13)  19
      (short) 0xa2f7, // (10,  759)  20
      (short) 0x704b, // ( 7,   75)  21
      (short) 0x9197, // ( 9,  407)  22
      (short) 0x6023, // ( 6,   35)  23
      (short) 0xb34f, // (11,  847)  24
      (short) 0x8087, // ( 8,  135)  25
      (short) 0xa1f7, // (10,  503)  26
      (short) 0x702b, // ( 7,   43)  27
      (short) 0xb74f, // (11, 1871)  28
      (short) 0x8047, // ( 8,   71)  29
      (short) 0xa3f7, // (10, 1015)  30
      (short) 0x706b, // ( 7,  107)  31
      (short) 0xb0cf, // (11,  207)  32
      (short) 0x701b, // ( 7,   27)  33
      (short) 0x9057, // ( 9,   87)  34
      (short) 0x6013, // ( 6,   19)  35
      (short) 0xb4cf, // (11, 1231)  36
      (short) 0x80c7, // ( 8,  199)  37
      (short) 0xa00f, // (10,   15)  38
      (short) 0x705b, // ( 7,   91)  39
      (short) 0xc72f, // (12, 1839)  40
      (short) 0x9157, // ( 9,  343)  41
      (short) 0xb2cf, // (11,  719)  42
      (short) 0x8027, // ( 8,   39)  43
      (short) 0xcf2f, // (12, 3887)  44
      (short) 0x90d7, // ( 9,  215)  45
      (short) 0xb6cf, // (11, 1743)  46
      (short) 0x80a7, // ( 8,  167)  47
      (short) 0xc0af, // (12,  175)  48
      (short) 0xa20f, // (10,  527)  49
      (short) 0xc8af, // (12, 2223)  50
      (short) 0x91d7, // ( 9,  471)  51
      (short) 0xc4af, // (12, 1199)  52
      (short) 0xa10f, // (10,  271)  53
      (short) 0xccaf, // (12, 3247)  54
      (short) 0x9037, // ( 9,   55)  55
      (short) 0xc2af, // (12,  687)  56
      (short) 0xcaaf, // (12, 2735)  57
      (short) 0xc6af, // (12, 1711)  58
      (short) 0xb1cf, // (11,  463)  59
      (short) 0xceaf, // (12, 3759)  60
      (short) 0xc1af, // (12,  431)  61
      (short) 0xc9af, // (12, 2479)  62
      (short) 0xb5cf, // (11, 1487)  63
      (short) 0xc5af, // (12, 1455)  64
      (short) 0x8067, // ( 8,  103)  65
      (short) 0xa30f, // (10,  783)  66
      (short) 0x703b, // ( 7,   59)  67
      (short) 0xcdaf, // (12, 3503)  68
      (short) 0x9137, // ( 9,  311)  69
      (short) 0xb3cf, // (11,  975)  70
      (short) 0x80e7, // ( 8,  231)  71
      (short) 0xc3af, // (12,  943)  72
      (short) 0xa08f, // (10,  143)  73
      (short) 0xcbaf, // (12, 2991)  74
      (short) 0x90b7, // ( 9,  183)  75
      (short) 0xc7af, // (12, 1967)  76
      (short) 0xa28f, // (10,  655)  77
      (short) 0xcfaf, // (12, 4015)  78
      (short) 0x91b7, // ( 9,  439)  79
      (short) 0xc06f, // (12,  111)  80
      (short) 0xb7cf, // (11, 1999)  81
      (short) 0xc86f, // (12, 2159)  82
      (short) 0xa18f, // (10,  399)  83
      (short) 0xc46f, // (12, 1135)  84
      (short) 0xb02f, // (11,   47)  85
      (short) 0xcc6f, // (12, 3183)  86
      (short) 0xa38f, // (10,  911)  87
      (short) 0xc26f, // (12,  623)  88
      (short) 0xca6f, // (12, 2671)  89
      (short) 0xc66f, // (12, 1647)  90
      (short) 0xce6f, // (12, 3695)  91
      (short) 0xc16f, // (12,  367)  92
      (short) 0xc96f, // (12, 2415)  93
      (short) 0xc56f, // (12, 1391)  94
      (short) 0xcd6f, // (12, 3439)  95
      (short) 0xc36f, // (12,  879)  96
      (short) 0xcb6f, // (12, 2927)  97
      (short) 0xc76f, // (12, 1903)  98
      (short) 0xb42f, // (11, 1071)  99
      (short) 0xcf6f, // (12, 3951) 100
      (short) 0xc0ef, // (12,  239) 101
      (short) 0xc8ef, // (12, 2287) 102
      (short) 0xb22f, // (11,  559) 103
      (short) 0xc4ef, // (12, 1263) 104
      (short) 0xccef, // (12, 3311) 105
      (short) 0xc2ef, // (12,  751) 106
      (short) 0xcaef, // (12, 2799) 107
      (short) 0xc6ef, // (12, 1775) 108
      (short) 0xceef, // (12, 3823) 109
      (short) 0xc1ef, // (12,  495) 110
      (short) 0xc9ef, // (12, 2543) 111
      (short) 0xc5ef, // (12, 1519) 112
      (short) 0xcdef, // (12, 3567) 113
      (short) 0xc3ef, // (12, 1007) 114
      (short) 0xcbef, // (12, 3055) 115
      (short) 0xc7ef, // (12, 2031) 116
      (short) 0xcfef, // (12, 4079) 117
      (short) 0xc01f, // (12,   31) 118
      (short) 0xc81f, // (12, 2079) 119
      (short) 0xc41f, // (12, 1055) 120
      (short) 0xcc1f, // (12, 3103) 121
      (short) 0xc21f, // (12,  543) 122
      (short) 0xca1f, // (12, 2591) 123
      (short) 0xc61f, // (12, 1567) 124
      (short) 0xce1f, // (12, 3615) 125
      (short) 0xc11f, // (12,  287) 126
      (short) 0xc91f, // (12, 2335) 127
      (short) 0xc51f, // (12, 1311) 128
      (short) 0x9077, // ( 9,  119) 129
      (short) 0xcd1f, // (12, 3359) 130
      (short) 0x8017, // ( 8,   23) 131
      (short) 0xc31f, // (12,  799) 132
      (short) 0xa04f, // (10,   79) 133
      (short) 0xcb1f, // (12, 2847) 134
      (short) 0x9177, // ( 9,  375) 135
      (short) 0xc71f, // (12, 1823) 136
      (short) 0xb62f, // (11, 1583) 137
      (short) 0xcf1f, // (12, 3871) 138
      (short) 0xa24f, // (10,  591) 139
      (short) 0xc09f, // (12,  159) 140
      (short) 0xb12f, // (11,  303) 141
      (short) 0xc89f, // (12, 2207) 142
      (short) 0xa14f, // (10,  335) 143
      (short) 0xc49f, // (12, 1183) 144
      (short) 0xcc9f, // (12, 3231) 145
      (short) 0xc29f, // (12,  671) 146
      (short) 0xb52f, // (11, 1327) 147
      (short) 0xca9f, // (12, 2719) 148
      (short) 0xc69f, // (12, 1695) 149
      (short) 0xce9f, // (12, 3743) 150
      (short) 0xb32f, // (11,  815) 151
      (short) 0xc19f, // (12,  415) 152
      (short) 0xc99f, // (12, 2463) 153
      (short) 0xc59f, // (12, 1439) 154
      (short) 0xcd9f, // (12, 3487) 155
      (short) 0xc39f, // (12,  927) 156
      (short) 0xcb9f, // (12, 2975) 157
      (short) 0xc79f, // (12, 1951) 158
      (short) 0xcf9f, // (12, 3999) 159
      (short) 0xc05f, // (12,   95) 160
      (short) 0xc85f, // (12, 2143) 161
      (short) 0xc45f, // (12, 1119) 162
      (short) 0xcc5f, // (12, 3167) 163
      (short) 0xc25f, // (12,  607) 164
      (short) 0xca5f, // (12, 2655) 165
      (short) 0xc65f, // (12, 1631) 166
      (short) 0xce5f, // (12, 3679) 167
      (short) 0xc15f, // (12,  351) 168
      (short) 0xc95f, // (12, 2399) 169
      (short) 0xc55f, // (12, 1375) 170
      (short) 0xcd5f, // (12, 3423) 171
      (short) 0xc35f, // (12,  863) 172
      (short) 0xcb5f, // (12, 2911) 173
      (short) 0xc75f, // (12, 1887) 174
      (short) 0xcf5f, // (12, 3935) 175
      (short) 0xc0df, // (12,  223) 176
      (short) 0xc8df, // (12, 2271) 177
      (short) 0xc4df, // (12, 1247) 178
      (short) 0xccdf, // (12, 3295) 179
      (short) 0xc2df, // (12,  735) 180
      (short) 0xcadf, // (12, 2783) 181
      (short) 0xc6df, // (12, 1759) 182
      (short) 0xcedf, // (12, 3807) 183
      (short) 0xc1df, // (12,  479) 184
      (short) 0xc9df, // (12, 2527) 185
      (short) 0xc5df, // (12, 1503) 186
      (short) 0xcddf, // (12, 3551) 187
      (short) 0xc3df, // (12,  991) 188
      (short) 0xcbdf, // (12, 3039) 189
      (short) 0xc7df, // (12, 2015) 190
      (short) 0xcfdf, // (12, 4063) 191
      (short) 0xc03f, // (12,   63) 192
      (short) 0xc83f, // (12, 2111) 193
      (short) 0xc43f, // (12, 1087) 194
      (short) 0xcc3f, // (12, 3135) 195
      (short) 0xc23f, // (12,  575) 196
      (short) 0xca3f, // (12, 2623) 197
      (short) 0xc63f, // (12, 1599) 198
      (short) 0xce3f, // (12, 3647) 199
      (short) 0xc13f, // (12,  319) 200
      (short) 0xc93f, // (12, 2367) 201
      (short) 0xc53f, // (12, 1343) 202
      (short) 0xcd3f, // (12, 3391) 203
      (short) 0xc33f, // (12,  831) 204
      (short) 0xcb3f, // (12, 2879) 205
      (short) 0xc73f, // (12, 1855) 206
      (short) 0xcf3f, // (12, 3903) 207
      (short) 0xc0bf, // (12,  191) 208
      (short) 0xc8bf, // (12, 2239) 209
      (short) 0xc4bf, // (12, 1215) 210
      (short) 0xccbf, // (12, 3263) 211
      (short) 0xc2bf, // (12,  703) 212
      (short) 0xcabf, // (12, 2751) 213
      (short) 0xc6bf, // (12, 1727) 214
      (short) 0xcebf, // (12, 3775) 215
      (short) 0xc1bf, // (12,  447) 216
      (short) 0xc9bf, // (12, 2495) 217
      (short) 0xc5bf, // (12, 1471) 218
      (short) 0xcdbf, // (12, 3519) 219
      (short) 0xc3bf, // (12,  959) 220
      (short) 0xcbbf, // (12, 3007) 221
      (short) 0xc7bf, // (12, 1983) 222
      (short) 0xcfbf, // (12, 4031) 223
      (short) 0xc07f, // (12,  127) 224
      (short) 0xc87f, // (12, 2175) 225
      (short) 0xc47f, // (12, 1151) 226
      (short) 0xcc7f, // (12, 3199) 227
      (short) 0xc27f, // (12,  639) 228
      (short) 0xca7f, // (12, 2687) 229
      (short) 0xc67f, // (12, 1663) 230
      (short) 0xce7f, // (12, 3711) 231
      (short) 0xc17f, // (12,  383) 232
      (short) 0xc97f, // (12, 2431) 233
      (short) 0xc57f, // (12, 1407) 234
      (short) 0xcd7f, // (12, 3455) 235
      (short) 0xc37f, // (12,  895) 236
      (short) 0xcb7f, // (12, 2943) 237
      (short) 0xc77f, // (12, 1919) 238
      (short) 0xcf7f, // (12, 3967) 239
      (short) 0xc0ff, // (12,  255) 240
      (short) 0xc8ff, // (12, 2303) 241
      (short) 0xc4ff, // (12, 1279) 242
      (short) 0xccff, // (12, 3327) 243
      (short) 0xc2ff, // (12,  767) 244
      (short) 0xcaff, // (12, 2815) 245
      (short) 0xc6ff, // (12, 1791) 246
      (short) 0xceff, // (12, 3839) 247
      (short) 0xc1ff, // (12,  511) 248
      (short) 0xc9ff, // (12, 2559) 249
      (short) 0xc5ff, // (12, 1535) 250
      (short) 0xcdff, // (12, 3583) 251
      (short) 0xc3ff, // (12, 1023) 252
      (short) 0xcbff, // (12, 3071) 253
      (short) 0xc7ff, // (12, 2047) 254
      (short) 0xcfff  // (12, 4095) 255
    },

    // (table 9 of 22) (steady 9 of 16) (phase = 0.593750000 = 19.0 / 32.0)
    // entropy:    4.4575203029748040606
    // avg_length: 4.5315465600684730063; max_length = 12; num_symbols = 256
    {
      //table, // (4 bits,  12 bits) symbol
      //entry, // (length, codeword) [byte]
      (short) 0x7033, // ( 7,   51)   0
      (short) 0x4006, // ( 4,    6)   1
      (short) 0x601d, // ( 6,   29)   2
      (short) 0x2000, // ( 2,    0)   3
      (short) 0x7073, // ( 7,  115)   4
      (short) 0x400e, // ( 4,   14)   5
      (short) 0x603d, // ( 6,   61)   6
      (short) 0x3002, // ( 3,    2)   7
      (short) 0x9097, // ( 9,  151)   8
      (short) 0x5005, // ( 5,    5)   9
      (short) 0x700b, // ( 7,   11)  10
      (short) 0x4001, // ( 4,    1)  11
      (short) 0x9197, // ( 9,  407)  12
      (short) 0x6003, // ( 6,    3)  13
      (short) 0x807b, // ( 8,  123)  14
      (short) 0x4009, // ( 4,    9)  15
      (short) 0xa0f7, // (10,  247)  16
      (short) 0x6023, // ( 6,   35)  17
      (short) 0x80fb, // ( 8,  251)  18
      (short) 0x5015, // ( 5,   21)  19
      (short) 0xa2f7, // (10,  759)  20
      (short) 0x704b, // ( 7,   75)  21
      (short) 0x9057, // ( 9,   87)  22
      (short) 0x500d, // ( 5,   13)  23
      (short) 0xb34f, // (11,  847)  24
      (short) 0x8007, // ( 8,    7)  25
      (short) 0xa1f7, // (10,  503)  26
      (short) 0x702b, // ( 7,   43)  27
      (short) 0xc72f, // (12, 1839)  28
      (short) 0x8087, // ( 8,  135)  29
      (short) 0xa3f7, // (10, 1015)  30
      (short) 0x706b, // ( 7,  107)  31
      (short) 0xb74f, // (11, 1871)  32
      (short) 0x701b, // ( 7,   27)  33
      (short) 0x9157, // ( 9,  343)  34
      (short) 0x6013, // ( 6,   19)  35
      (short) 0xb0cf, // (11,  207)  36
      (short) 0x8047, // ( 8,   71)  37
      (short) 0xa00f, // (10,   15)  38
      (short) 0x705b, // ( 7,   91)  39
      (short) 0xcf2f, // (12, 3887)  40
      (short) 0x90d7, // ( 9,  215)  41
      (short) 0xb4cf, // (11, 1231)  42
      (short) 0x80c7, // ( 8,  199)  43
      (short) 0xc0af, // (12,  175)  44
      (short) 0x91d7, // ( 9,  471)  45
      (short) 0xb2cf, // (11,  719)  46
      (short) 0x8027, // ( 8,   39)  47
      (short) 0xc8af, // (12, 2223)  48
      (short) 0xa20f, // (10,  527)  49
      (short) 0xc4af, // (12, 1199)  50
      (short) 0x9037, // ( 9,   55)  51
      (short) 0xccaf, // (12, 3247)  52
      (short) 0xa10f, // (10,  271)  53
      (short) 0xc2af, // (12,  687)  54
      (short) 0x9137, // ( 9,  311)  55
      (short) 0xcaaf, // (12, 2735)  56
      (short) 0xc6af, // (12, 1711)  57
      (short) 0xceaf, // (12, 3759)  58
      (short) 0xa30f, // (10,  783)  59
      (short) 0xc1af, // (12,  431)  60
      (short) 0xc9af, // (12, 2479)  61
      (short) 0xc5af, // (12, 1455)  62
      (short) 0xb6cf, // (11, 1743)  63
      (short) 0xcdaf, // (12, 3503)  64
      (short) 0x80a7, // ( 8,  167)  65
      (short) 0xb1cf, // (11,  463)  66
      (short) 0x703b, // ( 7,   59)  67
      (short) 0xc3af, // (12,  943)  68
      (short) 0x90b7, // ( 9,  183)  69
      (short) 0xb5cf, // (11, 1487)  70
      (short) 0x8067, // ( 8,  103)  71
      (short) 0xcbaf, // (12, 2991)  72
      (short) 0xa08f, // (10,  143)  73
      (short) 0xc7af, // (12, 1967)  74
      (short) 0x91b7, // ( 9,  439)  75
      (short) 0xcfaf, // (12, 4015)  76
      (short) 0xa28f, // (10,  655)  77
      (short) 0xc06f, // (12,  111)  78
      (short) 0x9077, // ( 9,  119)  79
      (short) 0xc86f, // (12, 2159)  80
      (short) 0xb3cf, // (11,  975)  81
      (short) 0xc46f, // (12, 1135)  82
      (short) 0xa18f, // (10,  399)  83
      (short) 0xcc6f, // (12, 3183)  84
      (short) 0xb7cf, // (11, 1999)  85
      (short) 0xc26f, // (12,  623)  86
      (short) 0xa38f, // (10,  911)  87
      (short) 0xca6f, // (12, 2671)  88
      (short) 0xc66f, // (12, 1647)  89
      (short) 0xce6f, // (12, 3695)  90
      (short) 0xb02f, // (11,   47)  91
      (short) 0xc16f, // (12,  367)  92
      (short) 0xc96f, // (12, 2415)  93
      (short) 0xc56f, // (12, 1391)  94
      (short) 0xcd6f, // (12, 3439)  95
      (short) 0xc36f, // (12,  879)  96
      (short) 0xcb6f, // (12, 2927)  97
      (short) 0xc76f, // (12, 1903)  98
      (short) 0xb42f, // (11, 1071)  99
      (short) 0xcf6f, // (12, 3951) 100
      (short) 0xc0ef, // (12,  239) 101
      (short) 0xc8ef, // (12, 2287) 102
      (short) 0xb22f, // (11,  559) 103
      (short) 0xc4ef, // (12, 1263) 104
      (short) 0xccef, // (12, 3311) 105
      (short) 0xc2ef, // (12,  751) 106
      (short) 0xcaef, // (12, 2799) 107
      (short) 0xc6ef, // (12, 1775) 108
      (short) 0xceef, // (12, 3823) 109
      (short) 0xc1ef, // (12,  495) 110
      (short) 0xc9ef, // (12, 2543) 111
      (short) 0xc5ef, // (12, 1519) 112
      (short) 0xcdef, // (12, 3567) 113
      (short) 0xc3ef, // (12, 1007) 114
      (short) 0xcbef, // (12, 3055) 115
      (short) 0xc7ef, // (12, 2031) 116
      (short) 0xcfef, // (12, 4079) 117
      (short) 0xc01f, // (12,   31) 118
      (short) 0xc81f, // (12, 2079) 119
      (short) 0xc41f, // (12, 1055) 120
      (short) 0xcc1f, // (12, 3103) 121
      (short) 0xc21f, // (12,  543) 122
      (short) 0xca1f, // (12, 2591) 123
      (short) 0xc61f, // (12, 1567) 124
      (short) 0xce1f, // (12, 3615) 125
      (short) 0xc11f, // (12,  287) 126
      (short) 0xc91f, // (12, 2335) 127
      (short) 0xc51f, // (12, 1311) 128
      (short) 0x9177, // ( 9,  375) 129
      (short) 0xcd1f, // (12, 3359) 130
      (short) 0x80e7, // ( 8,  231) 131
      (short) 0xc31f, // (12,  799) 132
      (short) 0xa04f, // (10,   79) 133
      (short) 0xcb1f, // (12, 2847) 134
      (short) 0x8017, // ( 8,   23) 135
      (short) 0xc71f, // (12, 1823) 136
      (short) 0xb62f, // (11, 1583) 137
      (short) 0xcf1f, // (12, 3871) 138
      (short) 0xa24f, // (10,  591) 139
      (short) 0xc09f, // (12,  159) 140
      (short) 0xb12f, // (11,  303) 141
      (short) 0xc89f, // (12, 2207) 142
      (short) 0xa14f, // (10,  335) 143
      (short) 0xc49f, // (12, 1183) 144
      (short) 0xcc9f, // (12, 3231) 145
      (short) 0xc29f, // (12,  671) 146
      (short) 0xb52f, // (11, 1327) 147
      (short) 0xca9f, // (12, 2719) 148
      (short) 0xc69f, // (12, 1695) 149
      (short) 0xce9f, // (12, 3743) 150
      (short) 0xb32f, // (11,  815) 151
      (short) 0xc19f, // (12,  415) 152
      (short) 0xc99f, // (12, 2463) 153
      (short) 0xc59f, // (12, 1439) 154
      (short) 0xcd9f, // (12, 3487) 155
      (short) 0xc39f, // (12,  927) 156
      (short) 0xcb9f, // (12, 2975) 157
      (short) 0xc79f, // (12, 1951) 158
      (short) 0xcf9f, // (12, 3999) 159
      (short) 0xc05f, // (12,   95) 160
      (short) 0xc85f, // (12, 2143) 161
      (short) 0xc45f, // (12, 1119) 162
      (short) 0xcc5f, // (12, 3167) 163
      (short) 0xc25f, // (12,  607) 164
      (short) 0xca5f, // (12, 2655) 165
      (short) 0xc65f, // (12, 1631) 166
      (short) 0xce5f, // (12, 3679) 167
      (short) 0xc15f, // (12,  351) 168
      (short) 0xc95f, // (12, 2399) 169
      (short) 0xc55f, // (12, 1375) 170
      (short) 0xcd5f, // (12, 3423) 171
      (short) 0xc35f, // (12,  863) 172
      (short) 0xcb5f, // (12, 2911) 173
      (short) 0xc75f, // (12, 1887) 174
      (short) 0xcf5f, // (12, 3935) 175
      (short) 0xc0df, // (12,  223) 176
      (short) 0xc8df, // (12, 2271) 177
      (short) 0xc4df, // (12, 1247) 178
      (short) 0xccdf, // (12, 3295) 179
      (short) 0xc2df, // (12,  735) 180
      (short) 0xcadf, // (12, 2783) 181
      (short) 0xc6df, // (12, 1759) 182
      (short) 0xcedf, // (12, 3807) 183
      (short) 0xc1df, // (12,  479) 184
      (short) 0xc9df, // (12, 2527) 185
      (short) 0xc5df, // (12, 1503) 186
      (short) 0xcddf, // (12, 3551) 187
      (short) 0xc3df, // (12,  991) 188
      (short) 0xcbdf, // (12, 3039) 189
      (short) 0xc7df, // (12, 2015) 190
      (short) 0xcfdf, // (12, 4063) 191
      (short) 0xc03f, // (12,   63) 192
      (short) 0xc83f, // (12, 2111) 193
      (short) 0xc43f, // (12, 1087) 194
      (short) 0xcc3f, // (12, 3135) 195
      (short) 0xc23f, // (12,  575) 196
      (short) 0xca3f, // (12, 2623) 197
      (short) 0xc63f, // (12, 1599) 198
      (short) 0xce3f, // (12, 3647) 199
      (short) 0xc13f, // (12,  319) 200
      (short) 0xc93f, // (12, 2367) 201
      (short) 0xc53f, // (12, 1343) 202
      (short) 0xcd3f, // (12, 3391) 203
      (short) 0xc33f, // (12,  831) 204
      (short) 0xcb3f, // (12, 2879) 205
      (short) 0xc73f, // (12, 1855) 206
      (short) 0xcf3f, // (12, 3903) 207
      (short) 0xc0bf, // (12,  191) 208
      (short) 0xc8bf, // (12, 2239) 209
      (short) 0xc4bf, // (12, 1215) 210
      (short) 0xccbf, // (12, 3263) 211
      (short) 0xc2bf, // (12,  703) 212
      (short) 0xcabf, // (12, 2751) 213
      (short) 0xc6bf, // (12, 1727) 214
      (short) 0xcebf, // (12, 3775) 215
      (short) 0xc1bf, // (12,  447) 216
      (short) 0xc9bf, // (12, 2495) 217
      (short) 0xc5bf, // (12, 1471) 218
      (short) 0xcdbf, // (12, 3519) 219
      (short) 0xc3bf, // (12,  959) 220
      (short) 0xcbbf, // (12, 3007) 221
      (short) 0xc7bf, // (12, 1983) 222
      (short) 0xcfbf, // (12, 4031) 223
      (short) 0xc07f, // (12,  127) 224
      (short) 0xc87f, // (12, 2175) 225
      (short) 0xc47f, // (12, 1151) 226
      (short) 0xcc7f, // (12, 3199) 227
      (short) 0xc27f, // (12,  639) 228
      (short) 0xca7f, // (12, 2687) 229
      (short) 0xc67f, // (12, 1663) 230
      (short) 0xce7f, // (12, 3711) 231
      (short) 0xc17f, // (12,  383) 232
      (short) 0xc97f, // (12, 2431) 233
      (short) 0xc57f, // (12, 1407) 234
      (short) 0xcd7f, // (12, 3455) 235
      (short) 0xc37f, // (12,  895) 236
      (short) 0xcb7f, // (12, 2943) 237
      (short) 0xc77f, // (12, 1919) 238
      (short) 0xcf7f, // (12, 3967) 239
      (short) 0xc0ff, // (12,  255) 240
      (short) 0xc8ff, // (12, 2303) 241
      (short) 0xc4ff, // (12, 1279) 242
      (short) 0xccff, // (12, 3327) 243
      (short) 0xc2ff, // (12,  767) 244
      (short) 0xcaff, // (12, 2815) 245
      (short) 0xc6ff, // (12, 1791) 246
      (short) 0xceff, // (12, 3839) 247
      (short) 0xc1ff, // (12,  511) 248
      (short) 0xc9ff, // (12, 2559) 249
      (short) 0xc5ff, // (12, 1535) 250
      (short) 0xcdff, // (12, 3583) 251
      (short) 0xc3ff, // (12, 1023) 252
      (short) 0xcbff, // (12, 3071) 253
      (short) 0xc7ff, // (12, 2047) 254
      (short) 0xcfff  // (12, 4095) 255
    },

    // (table 10 of 22) (steady 10 of 16) (phase = 0.656250000 = 21.0 / 32.0)
    // entropy:    4.4626765653088611430
    // avg_length: 4.5373141251902122661; max_length = 12; num_symbols = 256
    {
      //table, // (4 bits,  12 bits) symbol
      //entry, // (length, codeword) [byte]
      (short) 0x700b, // ( 7,   11)   0
      (short) 0x4006, // ( 4,    6)   1
      (short) 0x601d, // ( 6,   29)   2
      (short) 0x2000, // ( 2,    0)   3
      (short) 0x807b, // ( 8,  123)   4
      (short) 0x400e, // ( 4,   14)   5
      (short) 0x603d, // ( 6,   61)   6
      (short) 0x3002, // ( 3,    2)   7
      (short) 0x9017, // ( 9,   23)   8
      (short) 0x5005, // ( 5,    5)   9
      (short) 0x704b, // ( 7,   75)  10
      (short) 0x4001, // ( 4,    1)  11
      (short) 0x9117, // ( 9,  279)  12
      (short) 0x6003, // ( 6,    3)  13
      (short) 0x80fb, // ( 8,  251)  14
      (short) 0x4009, // ( 4,    9)  15
      (short) 0xa177, // (10,  375)  16
      (short) 0x6023, // ( 6,   35)  17
      (short) 0x9097, // ( 9,  151)  18
      (short) 0x5015, // ( 5,   21)  19
      (short) 0xa377, // (10,  887)  20
      (short) 0x702b, // ( 7,   43)  21
      (short) 0x9197, // ( 9,  407)  22
      (short) 0x500d, // ( 5,   13)  23
      (short) 0xb34f, // (11,  847)  24
      (short) 0x8007, // ( 8,    7)  25
      (short) 0xa0f7, // (10,  247)  26
      (short) 0x706b, // ( 7,  107)  27
      (short) 0xc0af, // (12,  175)  28
      (short) 0x8087, // ( 8,  135)  29
      (short) 0xa2f7, // (10,  759)  30
      (short) 0x701b, // ( 7,   27)  31
      (short) 0xb74f, // (11, 1871)  32
      (short) 0x8047, // ( 8,   71)  33
      (short) 0xa1f7, // (10,  503)  34
      (short) 0x6013, // ( 6,   19)  35
      (short) 0xb0cf, // (11,  207)  36
      (short) 0x80c7, // ( 8,  199)  37
      (short) 0xa3f7, // (10, 1015)  38
      (short) 0x6033, // ( 6,   51)  39
      (short) 0xc8af, // (12, 2223)  40
      (short) 0x9057, // ( 9,   87)  41
      (short) 0xb4cf, // (11, 1231)  42
      (short) 0x8027, // ( 8,   39)  43
      (short) 0xc4af, // (12, 1199)  44
      (short) 0x9157, // ( 9,  343)  45
      (short) 0xb2cf, // (11,  719)  46
      (short) 0x80a7, // ( 8,  167)  47
      (short) 0xccaf, // (12, 3247)  48
      (short) 0xa00f, // (10,   15)  49
      (short) 0xc2af, // (12,  687)  50
      (short) 0x90d7, // ( 9,  215)  51
      (short) 0xcaaf, // (12, 2735)  52
      (short) 0xa20f, // (10,  527)  53
      (short) 0xc6af, // (12, 1711)  54
      (short) 0x91d7, // ( 9,  471)  55
      (short) 0xceaf, // (12, 3759)  56
      (short) 0xb6cf, // (11, 1743)  57
      (short) 0xc1af, // (12,  431)  58
      (short) 0xa10f, // (10,  271)  59
      (short) 0xc9af, // (12, 2479)  60
      (short) 0xc5af, // (12, 1455)  61
      (short) 0xcdaf, // (12, 3503)  62
      (short) 0xa30f, // (10,  783)  63
      (short) 0xc3af, // (12,  943)  64
      (short) 0x9037, // ( 9,   55)  65
      (short) 0xb1cf, // (11,  463)  66
      (short) 0x705b, // ( 7,   91)  67
      (short) 0xcbaf, // (12, 2991)  68
      (short) 0x9137, // ( 9,  311)  69
      (short) 0xb5cf, // (11, 1487)  70
      (short) 0x703b, // ( 7,   59)  71
      (short) 0xc7af, // (12, 1967)  72
      (short) 0xa08f, // (10,  143)  73
      (short) 0xcfaf, // (12, 4015)  74
      (short) 0x90b7, // ( 9,  183)  75
      (short) 0xc06f, // (12,  111)  76
      (short) 0xa28f, // (10,  655)  77
      (short) 0xc86f, // (12, 2159)  78
      (short) 0x91b7, // ( 9,  439)  79
      (short) 0xc46f, // (12, 1135)  80
      (short) 0xb3cf, // (11,  975)  81
      (short) 0xcc6f, // (12, 3183)  82
      (short) 0xa18f, // (10,  399)  83
      (short) 0xc26f, // (12,  623)  84
      (short) 0xb7cf, // (11, 1999)  85
      (short) 0xca6f, // (12, 2671)  86
      (short) 0xa38f, // (10,  911)  87
      (short) 0xc66f, // (12, 1647)  88
      (short) 0xce6f, // (12, 3695)  89
      (short) 0xc16f, // (12,  367)  90
      (short) 0xb02f, // (11,   47)  91
      (short) 0xc96f, // (12, 2415)  92
      (short) 0xc56f, // (12, 1391)  93
      (short) 0xcd6f, // (12, 3439)  94
      (short) 0xb42f, // (11, 1071)  95
      (short) 0xc36f, // (12,  879)  96
      (short) 0xcb6f, // (12, 2927)  97
      (short) 0xc76f, // (12, 1903)  98
      (short) 0xb22f, // (11,  559)  99
      (short) 0xcf6f, // (12, 3951) 100
      (short) 0xc0ef, // (12,  239) 101
      (short) 0xc8ef, // (12, 2287) 102
      (short) 0xb62f, // (11, 1583) 103
      (short) 0xc4ef, // (12, 1263) 104
      (short) 0xccef, // (12, 3311) 105
      (short) 0xc2ef, // (12,  751) 106
      (short) 0xcaef, // (12, 2799) 107
      (short) 0xc6ef, // (12, 1775) 108
      (short) 0xceef, // (12, 3823) 109
      (short) 0xc1ef, // (12,  495) 110
      (short) 0xc9ef, // (12, 2543) 111
      (short) 0xc5ef, // (12, 1519) 112
      (short) 0xcdef, // (12, 3567) 113
      (short) 0xc3ef, // (12, 1007) 114
      (short) 0xcbef, // (12, 3055) 115
      (short) 0xc7ef, // (12, 2031) 116
      (short) 0xcfef, // (12, 4079) 117
      (short) 0xc01f, // (12,   31) 118
      (short) 0xc81f, // (12, 2079) 119
      (short) 0xc41f, // (12, 1055) 120
      (short) 0xcc1f, // (12, 3103) 121
      (short) 0xc21f, // (12,  543) 122
      (short) 0xca1f, // (12, 2591) 123
      (short) 0xc61f, // (12, 1567) 124
      (short) 0xce1f, // (12, 3615) 125
      (short) 0xc11f, // (12,  287) 126
      (short) 0xc91f, // (12, 2335) 127
      (short) 0xc51f, // (12, 1311) 128
      (short) 0x9077, // ( 9,  119) 129
      (short) 0xcd1f, // (12, 3359) 130
      (short) 0x8067, // ( 8,  103) 131
      (short) 0xc31f, // (12,  799) 132
      (short) 0xa04f, // (10,   79) 133
      (short) 0xcb1f, // (12, 2847) 134
      (short) 0x80e7, // ( 8,  231) 135
      (short) 0xc71f, // (12, 1823) 136
      (short) 0xb12f, // (11,  303) 137
      (short) 0xcf1f, // (12, 3871) 138
      (short) 0xa24f, // (10,  591) 139
      (short) 0xc09f, // (12,  159) 140
      (short) 0xb52f, // (11, 1327) 141
      (short) 0xc89f, // (12, 2207) 142
      (short) 0xa14f, // (10,  335) 143
      (short) 0xc49f, // (12, 1183) 144
      (short) 0xcc9f, // (12, 3231) 145
      (short) 0xc29f, // (12,  671) 146
      (short) 0xb32f, // (11,  815) 147
      (short) 0xca9f, // (12, 2719) 148
      (short) 0xc69f, // (12, 1695) 149
      (short) 0xce9f, // (12, 3743) 150
      (short) 0xb72f, // (11, 1839) 151
      (short) 0xc19f, // (12,  415) 152
      (short) 0xc99f, // (12, 2463) 153
      (short) 0xc59f, // (12, 1439) 154
      (short) 0xcd9f, // (12, 3487) 155
      (short) 0xc39f, // (12,  927) 156
      (short) 0xcb9f, // (12, 2975) 157
      (short) 0xc79f, // (12, 1951) 158
      (short) 0xcf9f, // (12, 3999) 159
      (short) 0xc05f, // (12,   95) 160
      (short) 0xc85f, // (12, 2143) 161
      (short) 0xc45f, // (12, 1119) 162
      (short) 0xcc5f, // (12, 3167) 163
      (short) 0xc25f, // (12,  607) 164
      (short) 0xca5f, // (12, 2655) 165
      (short) 0xc65f, // (12, 1631) 166
      (short) 0xce5f, // (12, 3679) 167
      (short) 0xc15f, // (12,  351) 168
      (short) 0xc95f, // (12, 2399) 169
      (short) 0xc55f, // (12, 1375) 170
      (short) 0xcd5f, // (12, 3423) 171
      (short) 0xc35f, // (12,  863) 172
      (short) 0xcb5f, // (12, 2911) 173
      (short) 0xc75f, // (12, 1887) 174
      (short) 0xcf5f, // (12, 3935) 175
      (short) 0xc0df, // (12,  223) 176
      (short) 0xc8df, // (12, 2271) 177
      (short) 0xc4df, // (12, 1247) 178
      (short) 0xccdf, // (12, 3295) 179
      (short) 0xc2df, // (12,  735) 180
      (short) 0xcadf, // (12, 2783) 181
      (short) 0xc6df, // (12, 1759) 182
      (short) 0xcedf, // (12, 3807) 183
      (short) 0xc1df, // (12,  479) 184
      (short) 0xc9df, // (12, 2527) 185
      (short) 0xc5df, // (12, 1503) 186
      (short) 0xcddf, // (12, 3551) 187
      (short) 0xc3df, // (12,  991) 188
      (short) 0xcbdf, // (12, 3039) 189
      (short) 0xc7df, // (12, 2015) 190
      (short) 0xcfdf, // (12, 4063) 191
      (short) 0xc03f, // (12,   63) 192
      (short) 0xc83f, // (12, 2111) 193
      (short) 0xc43f, // (12, 1087) 194
      (short) 0xcc3f, // (12, 3135) 195
      (short) 0xc23f, // (12,  575) 196
      (short) 0xca3f, // (12, 2623) 197
      (short) 0xc63f, // (12, 1599) 198
      (short) 0xce3f, // (12, 3647) 199
      (short) 0xc13f, // (12,  319) 200
      (short) 0xc93f, // (12, 2367) 201
      (short) 0xc53f, // (12, 1343) 202
      (short) 0xcd3f, // (12, 3391) 203
      (short) 0xc33f, // (12,  831) 204
      (short) 0xcb3f, // (12, 2879) 205
      (short) 0xc73f, // (12, 1855) 206
      (short) 0xcf3f, // (12, 3903) 207
      (short) 0xc0bf, // (12,  191) 208
      (short) 0xc8bf, // (12, 2239) 209
      (short) 0xc4bf, // (12, 1215) 210
      (short) 0xccbf, // (12, 3263) 211
      (short) 0xc2bf, // (12,  703) 212
      (short) 0xcabf, // (12, 2751) 213
      (short) 0xc6bf, // (12, 1727) 214
      (short) 0xcebf, // (12, 3775) 215
      (short) 0xc1bf, // (12,  447) 216
      (short) 0xc9bf, // (12, 2495) 217
      (short) 0xc5bf, // (12, 1471) 218
      (short) 0xcdbf, // (12, 3519) 219
      (short) 0xc3bf, // (12,  959) 220
      (short) 0xcbbf, // (12, 3007) 221
      (short) 0xc7bf, // (12, 1983) 222
      (short) 0xcfbf, // (12, 4031) 223
      (short) 0xc07f, // (12,  127) 224
      (short) 0xc87f, // (12, 2175) 225
      (short) 0xc47f, // (12, 1151) 226
      (short) 0xcc7f, // (12, 3199) 227
      (short) 0xc27f, // (12,  639) 228
      (short) 0xca7f, // (12, 2687) 229
      (short) 0xc67f, // (12, 1663) 230
      (short) 0xce7f, // (12, 3711) 231
      (short) 0xc17f, // (12,  383) 232
      (short) 0xc97f, // (12, 2431) 233
      (short) 0xc57f, // (12, 1407) 234
      (short) 0xcd7f, // (12, 3455) 235
      (short) 0xc37f, // (12,  895) 236
      (short) 0xcb7f, // (12, 2943) 237
      (short) 0xc77f, // (12, 1919) 238
      (short) 0xcf7f, // (12, 3967) 239
      (short) 0xc0ff, // (12,  255) 240
      (short) 0xc8ff, // (12, 2303) 241
      (short) 0xc4ff, // (12, 1279) 242
      (short) 0xccff, // (12, 3327) 243
      (short) 0xc2ff, // (12,  767) 244
      (short) 0xcaff, // (12, 2815) 245
      (short) 0xc6ff, // (12, 1791) 246
      (short) 0xceff, // (12, 3839) 247
      (short) 0xc1ff, // (12,  511) 248
      (short) 0xc9ff, // (12, 2559) 249
      (short) 0xc5ff, // (12, 1535) 250
      (short) 0xcdff, // (12, 3583) 251
      (short) 0xc3ff, // (12, 1023) 252
      (short) 0xcbff, // (12, 3071) 253
      (short) 0xc7ff, // (12, 2047) 254
      (short) 0xcfff  // (12, 4095) 255
    },

    // (table 11 of 22) (steady 11 of 16) (phase = 0.718750000 = 23.0 / 32.0)
    // entropy:    4.4661524304421691411
    // avg_length: 4.5443750890419041255; max_length = 12; num_symbols = 256
    {
      //table, // (4 bits,  12 bits) symbol
      //entry, // (length, codeword) [byte]
      (short) 0x803b, // ( 8,   59)   0
      (short) 0x4006, // ( 4,    6)   1
      (short) 0x601d, // ( 6,   29)   2
      (short) 0x2000, // ( 2,    0)   3
      (short) 0x80bb, // ( 8,  187)   4
      (short) 0x400e, // ( 4,   14)   5
      (short) 0x603d, // ( 6,   61)   6
      (short) 0x3002, // ( 3,    2)   7
      (short) 0x9017, // ( 9,   23)   8
      (short) 0x5005, // ( 5,    5)   9
      (short) 0x807b, // ( 8,  123)  10
      (short) 0x4001, // ( 4,    1)  11
      (short) 0x9117, // ( 9,  279)  12
      (short) 0x6003, // ( 6,    3)  13
      (short) 0x80fb, // ( 8,  251)  14
      (short) 0x4009, // ( 4,    9)  15
      (short) 0xa177, // (10,  375)  16
      (short) 0x6023, // ( 6,   35)  17
      (short) 0x9097, // ( 9,  151)  18
      (short) 0x5015, // ( 5,   21)  19
      (short) 0xa377, // (10,  887)  20
      (short) 0x702b, // ( 7,   43)  21
      (short) 0x9197, // ( 9,  407)  22
      (short) 0x500d, // ( 5,   13)  23
      (short) 0xb34f, // (11,  847)  24
      (short) 0x8007, // ( 8,    7)  25
      (short) 0xa0f7, // (10,  247)  26
      (short) 0x6013, // ( 6,   19)  27
      (short) 0xc0af, // (12,  175)  28
      (short) 0x8087, // ( 8,  135)  29
      (short) 0xa2f7, // (10,  759)  30
      (short) 0x706b, // ( 7,  107)  31
      (short) 0xb74f, // (11, 1871)  32
      (short) 0x8047, // ( 8,   71)  33
      (short) 0xa1f7, // (10,  503)  34
      (short) 0x6033, // ( 6,   51)  35
      (short) 0xb0cf, // (11,  207)  36
      (short) 0x80c7, // ( 8,  199)  37
      (short) 0xa3f7, // (10, 1015)  38
      (short) 0x600b, // ( 6,   11)  39
      (short) 0xc8af, // (12, 2223)  40
      (short) 0x9057, // ( 9,   87)  41
      (short) 0xb4cf, // (11, 1231)  42
      (short) 0x8027, // ( 8,   39)  43
      (short) 0xc4af, // (12, 1199)  44
      (short) 0x9157, // ( 9,  343)  45
      (short) 0xb2cf, // (11,  719)  46
      (short) 0x80a7, // ( 8,  167)  47
      (short) 0xccaf, // (12, 3247)  48
      (short) 0xa00f, // (10,   15)  49
      (short) 0xc2af, // (12,  687)  50
      (short) 0x90d7, // ( 9,  215)  51
      (short) 0xcaaf, // (12, 2735)  52
      (short) 0xa20f, // (10,  527)  53
      (short) 0xc6af, // (12, 1711)  54
      (short) 0x91d7, // ( 9,  471)  55
      (short) 0xceaf, // (12, 3759)  56
      (short) 0xb6cf, // (11, 1743)  57
      (short) 0xc1af, // (12,  431)  58
      (short) 0xa10f, // (10,  271)  59
      (short) 0xc9af, // (12, 2479)  60
      (short) 0xc5af, // (12, 1455)  61
      (short) 0xcdaf, // (12, 3503)  62
      (short) 0xa30f, // (10,  783)  63
      (short) 0xc3af, // (12,  943)  64
      (short) 0x9037, // ( 9,   55)  65
      (short) 0xb1cf, // (11,  463)  66
      (short) 0x701b, // ( 7,   27)  67
      (short) 0xcbaf, // (12, 2991)  68
      (short) 0x9137, // ( 9,  311)  69
      (short) 0xb5cf, // (11, 1487)  70
      (short) 0x705b, // ( 7,   91)  71
      (short) 0xc7af, // (12, 1967)  72
      (short) 0xa08f, // (10,  143)  73
      (short) 0xcfaf, // (12, 4015)  74
      (short) 0x90b7, // ( 9,  183)  75
      (short) 0xc06f, // (12,  111)  76
      (short) 0xa28f, // (10,  655)  77
      (short) 0xc86f, // (12, 2159)  78
      (short) 0x91b7, // ( 9,  439)  79
      (short) 0xc46f, // (12, 1135)  80
      (short) 0xb3cf, // (11,  975)  81
      (short) 0xcc6f, // (12, 3183)  82
      (short) 0xa18f, // (10,  399)  83
      (short) 0xc26f, // (12,  623)  84
      (short) 0xb7cf, // (11, 1999)  85
      (short) 0xca6f, // (12, 2671)  86
      (short) 0xa38f, // (10,  911)  87
      (short) 0xc66f, // (12, 1647)  88
      (short) 0xce6f, // (12, 3695)  89
      (short) 0xc16f, // (12,  367)  90
      (short) 0xb02f, // (11,   47)  91
      (short) 0xc96f, // (12, 2415)  92
      (short) 0xc56f, // (12, 1391)  93
      (short) 0xcd6f, // (12, 3439)  94
      (short) 0xb42f, // (11, 1071)  95
      (short) 0xc36f, // (12,  879)  96
      (short) 0xcb6f, // (12, 2927)  97
      (short) 0xc76f, // (12, 1903)  98
      (short) 0xb22f, // (11,  559)  99
      (short) 0xcf6f, // (12, 3951) 100
      (short) 0xc0ef, // (12,  239) 101
      (short) 0xc8ef, // (12, 2287) 102
      (short) 0xb62f, // (11, 1583) 103
      (short) 0xc4ef, // (12, 1263) 104
      (short) 0xccef, // (12, 3311) 105
      (short) 0xc2ef, // (12,  751) 106
      (short) 0xcaef, // (12, 2799) 107
      (short) 0xc6ef, // (12, 1775) 108
      (short) 0xceef, // (12, 3823) 109
      (short) 0xc1ef, // (12,  495) 110
      (short) 0xc9ef, // (12, 2543) 111
      (short) 0xc5ef, // (12, 1519) 112
      (short) 0xcdef, // (12, 3567) 113
      (short) 0xc3ef, // (12, 1007) 114
      (short) 0xcbef, // (12, 3055) 115
      (short) 0xc7ef, // (12, 2031) 116
      (short) 0xcfef, // (12, 4079) 117
      (short) 0xc01f, // (12,   31) 118
      (short) 0xc81f, // (12, 2079) 119
      (short) 0xc41f, // (12, 1055) 120
      (short) 0xcc1f, // (12, 3103) 121
      (short) 0xc21f, // (12,  543) 122
      (short) 0xca1f, // (12, 2591) 123
      (short) 0xc61f, // (12, 1567) 124
      (short) 0xce1f, // (12, 3615) 125
      (short) 0xc11f, // (12,  287) 126
      (short) 0xc91f, // (12, 2335) 127
      (short) 0xc51f, // (12, 1311) 128
      (short) 0xa04f, // (10,   79) 129
      (short) 0xcd1f, // (12, 3359) 130
      (short) 0x8067, // ( 8,  103) 131
      (short) 0xc31f, // (12,  799) 132
      (short) 0xa24f, // (10,  591) 133
      (short) 0xcb1f, // (12, 2847) 134
      (short) 0x80e7, // ( 8,  231) 135
      (short) 0xc71f, // (12, 1823) 136
      (short) 0xb12f, // (11,  303) 137
      (short) 0xcf1f, // (12, 3871) 138
      (short) 0x9077, // ( 9,  119) 139
      (short) 0xc09f, // (12,  159) 140
      (short) 0xb52f, // (11, 1327) 141
      (short) 0xc89f, // (12, 2207) 142
      (short) 0xa14f, // (10,  335) 143
      (short) 0xc49f, // (12, 1183) 144
      (short) 0xcc9f, // (12, 3231) 145
      (short) 0xc29f, // (12,  671) 146
      (short) 0xb32f, // (11,  815) 147
      (short) 0xca9f, // (12, 2719) 148
      (short) 0xc69f, // (12, 1695) 149
      (short) 0xce9f, // (12, 3743) 150
      (short) 0xb72f, // (11, 1839) 151
      (short) 0xc19f, // (12,  415) 152
      (short) 0xc99f, // (12, 2463) 153
      (short) 0xc59f, // (12, 1439) 154
      (short) 0xcd9f, // (12, 3487) 155
      (short) 0xc39f, // (12,  927) 156
      (short) 0xcb9f, // (12, 2975) 157
      (short) 0xc79f, // (12, 1951) 158
      (short) 0xcf9f, // (12, 3999) 159
      (short) 0xc05f, // (12,   95) 160
      (short) 0xc85f, // (12, 2143) 161
      (short) 0xc45f, // (12, 1119) 162
      (short) 0xcc5f, // (12, 3167) 163
      (short) 0xc25f, // (12,  607) 164
      (short) 0xca5f, // (12, 2655) 165
      (short) 0xc65f, // (12, 1631) 166
      (short) 0xce5f, // (12, 3679) 167
      (short) 0xc15f, // (12,  351) 168
      (short) 0xc95f, // (12, 2399) 169
      (short) 0xc55f, // (12, 1375) 170
      (short) 0xcd5f, // (12, 3423) 171
      (short) 0xc35f, // (12,  863) 172
      (short) 0xcb5f, // (12, 2911) 173
      (short) 0xc75f, // (12, 1887) 174
      (short) 0xcf5f, // (12, 3935) 175
      (short) 0xc0df, // (12,  223) 176
      (short) 0xc8df, // (12, 2271) 177
      (short) 0xc4df, // (12, 1247) 178
      (short) 0xccdf, // (12, 3295) 179
      (short) 0xc2df, // (12,  735) 180
      (short) 0xcadf, // (12, 2783) 181
      (short) 0xc6df, // (12, 1759) 182
      (short) 0xcedf, // (12, 3807) 183
      (short) 0xc1df, // (12,  479) 184
      (short) 0xc9df, // (12, 2527) 185
      (short) 0xc5df, // (12, 1503) 186
      (short) 0xcddf, // (12, 3551) 187
      (short) 0xc3df, // (12,  991) 188
      (short) 0xcbdf, // (12, 3039) 189
      (short) 0xc7df, // (12, 2015) 190
      (short) 0xcfdf, // (12, 4063) 191
      (short) 0xc03f, // (12,   63) 192
      (short) 0xc83f, // (12, 2111) 193
      (short) 0xc43f, // (12, 1087) 194
      (short) 0xcc3f, // (12, 3135) 195
      (short) 0xc23f, // (12,  575) 196
      (short) 0xca3f, // (12, 2623) 197
      (short) 0xc63f, // (12, 1599) 198
      (short) 0xce3f, // (12, 3647) 199
      (short) 0xc13f, // (12,  319) 200
      (short) 0xc93f, // (12, 2367) 201
      (short) 0xc53f, // (12, 1343) 202
      (short) 0xcd3f, // (12, 3391) 203
      (short) 0xc33f, // (12,  831) 204
      (short) 0xcb3f, // (12, 2879) 205
      (short) 0xc73f, // (12, 1855) 206
      (short) 0xcf3f, // (12, 3903) 207
      (short) 0xc0bf, // (12,  191) 208
      (short) 0xc8bf, // (12, 2239) 209
      (short) 0xc4bf, // (12, 1215) 210
      (short) 0xccbf, // (12, 3263) 211
      (short) 0xc2bf, // (12,  703) 212
      (short) 0xcabf, // (12, 2751) 213
      (short) 0xc6bf, // (12, 1727) 214
      (short) 0xcebf, // (12, 3775) 215
      (short) 0xc1bf, // (12,  447) 216
      (short) 0xc9bf, // (12, 2495) 217
      (short) 0xc5bf, // (12, 1471) 218
      (short) 0xcdbf, // (12, 3519) 219
      (short) 0xc3bf, // (12,  959) 220
      (short) 0xcbbf, // (12, 3007) 221
      (short) 0xc7bf, // (12, 1983) 222
      (short) 0xcfbf, // (12, 4031) 223
      (short) 0xc07f, // (12,  127) 224
      (short) 0xc87f, // (12, 2175) 225
      (short) 0xc47f, // (12, 1151) 226
      (short) 0xcc7f, // (12, 3199) 227
      (short) 0xc27f, // (12,  639) 228
      (short) 0xca7f, // (12, 2687) 229
      (short) 0xc67f, // (12, 1663) 230
      (short) 0xce7f, // (12, 3711) 231
      (short) 0xc17f, // (12,  383) 232
      (short) 0xc97f, // (12, 2431) 233
      (short) 0xc57f, // (12, 1407) 234
      (short) 0xcd7f, // (12, 3455) 235
      (short) 0xc37f, // (12,  895) 236
      (short) 0xcb7f, // (12, 2943) 237
      (short) 0xc77f, // (12, 1919) 238
      (short) 0xcf7f, // (12, 3967) 239
      (short) 0xc0ff, // (12,  255) 240
      (short) 0xc8ff, // (12, 2303) 241
      (short) 0xc4ff, // (12, 1279) 242
      (short) 0xccff, // (12, 3327) 243
      (short) 0xc2ff, // (12,  767) 244
      (short) 0xcaff, // (12, 2815) 245
      (short) 0xc6ff, // (12, 1791) 246
      (short) 0xceff, // (12, 3839) 247
      (short) 0xc1ff, // (12,  511) 248
      (short) 0xc9ff, // (12, 2559) 249
      (short) 0xc5ff, // (12, 1535) 250
      (short) 0xcdff, // (12, 3583) 251
      (short) 0xc3ff, // (12, 1023) 252
      (short) 0xcbff, // (12, 3071) 253
      (short) 0xc7ff, // (12, 2047) 254
      (short) 0xcfff  // (12, 4095) 255
    },

    // (table 12 of 22) (steady 12 of 16) (phase = 0.781250000 = 25.0 / 32.0)
    // entropy:    4.4680486273043946710
    // avg_length: 4.5521643785256946657; max_length = 12; num_symbols = 256
    {
      //table, // (4 bits,  12 bits) symbol
      //entry, // (length, codeword) [byte]
      (short) 0x807b, // ( 8,  123)   0
      (short) 0x4006, // ( 4,    6)   1
      (short) 0x601d, // ( 6,   29)   2
      (short) 0x2000, // ( 2,    0)   3
      (short) 0x80fb, // ( 8,  251)   4
      (short) 0x400e, // ( 4,   14)   5
      (short) 0x700b, // ( 7,   11)   6
      (short) 0x3002, // ( 3,    2)   7
      (short) 0x9097, // ( 9,  151)   8
      (short) 0x5005, // ( 5,    5)   9
      (short) 0x8007, // ( 8,    7)  10
      (short) 0x4001, // ( 4,    1)  11
      (short) 0x9197, // ( 9,  407)  12
      (short) 0x603d, // ( 6,   61)  13
      (short) 0x8087, // ( 8,  135)  14
      (short) 0x4009, // ( 4,    9)  15
      (short) 0xa177, // (10,  375)  16
      (short) 0x704b, // ( 7,   75)  17
      (short) 0x9057, // ( 9,   87)  18
      (short) 0x5015, // ( 5,   21)  19
      (short) 0xb34f, // (11,  847)  20
      (short) 0x702b, // ( 7,   43)  21
      (short) 0x9157, // ( 9,  343)  22
      (short) 0x500d, // ( 5,   13)  23
      (short) 0xc72f, // (12, 1839)  24
      (short) 0x8047, // ( 8,   71)  25
      (short) 0xa377, // (10,  887)  26
      (short) 0x6003, // ( 6,    3)  27
      (short) 0xcf2f, // (12, 3887)  28
      (short) 0x80c7, // ( 8,  199)  29
      (short) 0xa0f7, // (10,  247)  30
      (short) 0x6023, // ( 6,   35)  31
      (short) 0xc0af, // (12,  175)  32
      (short) 0x8027, // ( 8,   39)  33
      (short) 0xa2f7, // (10,  759)  34
      (short) 0x6013, // ( 6,   19)  35
      (short) 0xc8af, // (12, 2223)  36
      (short) 0x80a7, // ( 8,  167)  37
      (short) 0xa1f7, // (10,  503)  38
      (short) 0x6033, // ( 6,   51)  39
      (short) 0xc4af, // (12, 1199)  40
      (short) 0x90d7, // ( 9,  215)  41
      (short) 0xb74f, // (11, 1871)  42
      (short) 0x706b, // ( 7,  107)  43
      (short) 0xccaf, // (12, 3247)  44
      (short) 0x91d7, // ( 9,  471)  45
      (short) 0xb0cf, // (11,  207)  46
      (short) 0x701b, // ( 7,   27)  47
      (short) 0xc2af, // (12,  687)  48
      (short) 0xa3f7, // (10, 1015)  49
      (short) 0xcaaf, // (12, 2735)  50
      (short) 0x9037, // ( 9,   55)  51
      (short) 0xc6af, // (12, 1711)  52
      (short) 0xa00f, // (10,   15)  53
      (short) 0xceaf, // (12, 3759)  54
      (short) 0x9137, // ( 9,  311)  55
      (short) 0xc1af, // (12,  431)  56
      (short) 0xb4cf, // (11, 1231)  57
      (short) 0xc9af, // (12, 2479)  58
      (short) 0xa20f, // (10,  527)  59
      (short) 0xc5af, // (12, 1455)  60
      (short) 0xb2cf, // (11,  719)  61
      (short) 0xcdaf, // (12, 3503)  62
      (short) 0xa10f, // (10,  271)  63
      (short) 0xc3af, // (12,  943)  64
      (short) 0x90b7, // ( 9,  183)  65
      (short) 0xb6cf, // (11, 1743)  66
      (short) 0x705b, // ( 7,   91)  67
      (short) 0xcbaf, // (12, 2991)  68
      (short) 0x91b7, // ( 9,  439)  69
      (short) 0xb1cf, // (11,  463)  70
      (short) 0x703b, // ( 7,   59)  71
      (short) 0xc7af, // (12, 1967)  72
      (short) 0xa30f, // (10,  783)  73
      (short) 0xcfaf, // (12, 4015)  74
      (short) 0x8067, // ( 8,  103)  75
      (short) 0xc06f, // (12,  111)  76
      (short) 0xa08f, // (10,  143)  77
      (short) 0xc86f, // (12, 2159)  78
      (short) 0x9077, // ( 9,  119)  79
      (short) 0xc46f, // (12, 1135)  80
      (short) 0xb5cf, // (11, 1487)  81
      (short) 0xcc6f, // (12, 3183)  82
      (short) 0xa28f, // (10,  655)  83
      (short) 0xc26f, // (12,  623)  84
      (short) 0xb3cf, // (11,  975)  85
      (short) 0xca6f, // (12, 2671)  86
      (short) 0xa18f, // (10,  399)  87
      (short) 0xc66f, // (12, 1647)  88
      (short) 0xce6f, // (12, 3695)  89
      (short) 0xc16f, // (12,  367)  90
      (short) 0xb7cf, // (11, 1999)  91
      (short) 0xc96f, // (12, 2415)  92
      (short) 0xc56f, // (12, 1391)  93
      (short) 0xcd6f, // (12, 3439)  94
      (short) 0xb02f, // (11,   47)  95
      (short) 0xc36f, // (12,  879)  96
      (short) 0xcb6f, // (12, 2927)  97
      (short) 0xc76f, // (12, 1903)  98
      (short) 0xb42f, // (11, 1071)  99
      (short) 0xcf6f, // (12, 3951) 100
      (short) 0xc0ef, // (12,  239) 101
      (short) 0xc8ef, // (12, 2287) 102
      (short) 0xb22f, // (11,  559) 103
      (short) 0xc4ef, // (12, 1263) 104
      (short) 0xccef, // (12, 3311) 105
      (short) 0xc2ef, // (12,  751) 106
      (short) 0xcaef, // (12, 2799) 107
      (short) 0xc6ef, // (12, 1775) 108
      (short) 0xceef, // (12, 3823) 109
      (short) 0xc1ef, // (12,  495) 110
      (short) 0xc9ef, // (12, 2543) 111
      (short) 0xc5ef, // (12, 1519) 112
      (short) 0xcdef, // (12, 3567) 113
      (short) 0xc3ef, // (12, 1007) 114
      (short) 0xcbef, // (12, 3055) 115
      (short) 0xc7ef, // (12, 2031) 116
      (short) 0xcfef, // (12, 4079) 117
      (short) 0xc01f, // (12,   31) 118
      (short) 0xc81f, // (12, 2079) 119
      (short) 0xc41f, // (12, 1055) 120
      (short) 0xcc1f, // (12, 3103) 121
      (short) 0xc21f, // (12,  543) 122
      (short) 0xca1f, // (12, 2591) 123
      (short) 0xc61f, // (12, 1567) 124
      (short) 0xce1f, // (12, 3615) 125
      (short) 0xc11f, // (12,  287) 126
      (short) 0xc91f, // (12, 2335) 127
      (short) 0xc51f, // (12, 1311) 128
      (short) 0xa38f, // (10,  911) 129
      (short) 0xcd1f, // (12, 3359) 130
      (short) 0x80e7, // ( 8,  231) 131
      (short) 0xc31f, // (12,  799) 132
      (short) 0xa04f, // (10,   79) 133
      (short) 0xcb1f, // (12, 2847) 134
      (short) 0x8017, // ( 8,   23) 135
      (short) 0xc71f, // (12, 1823) 136
      (short) 0xb62f, // (11, 1583) 137
      (short) 0xcf1f, // (12, 3871) 138
      (short) 0xa24f, // (10,  591) 139
      (short) 0xc09f, // (12,  159) 140
      (short) 0xb12f, // (11,  303) 141
      (short) 0xc89f, // (12, 2207) 142
      (short) 0xa14f, // (10,  335) 143
      (short) 0xc49f, // (12, 1183) 144
      (short) 0xcc9f, // (12, 3231) 145
      (short) 0xc29f, // (12,  671) 146
      (short) 0xb52f, // (11, 1327) 147
      (short) 0xca9f, // (12, 2719) 148
      (short) 0xc69f, // (12, 1695) 149
      (short) 0xce9f, // (12, 3743) 150
      (short) 0xb32f, // (11,  815) 151
      (short) 0xc19f, // (12,  415) 152
      (short) 0xc99f, // (12, 2463) 153
      (short) 0xc59f, // (12, 1439) 154
      (short) 0xcd9f, // (12, 3487) 155
      (short) 0xc39f, // (12,  927) 156
      (short) 0xcb9f, // (12, 2975) 157
      (short) 0xc79f, // (12, 1951) 158
      (short) 0xcf9f, // (12, 3999) 159
      (short) 0xc05f, // (12,   95) 160
      (short) 0xc85f, // (12, 2143) 161
      (short) 0xc45f, // (12, 1119) 162
      (short) 0xcc5f, // (12, 3167) 163
      (short) 0xc25f, // (12,  607) 164
      (short) 0xca5f, // (12, 2655) 165
      (short) 0xc65f, // (12, 1631) 166
      (short) 0xce5f, // (12, 3679) 167
      (short) 0xc15f, // (12,  351) 168
      (short) 0xc95f, // (12, 2399) 169
      (short) 0xc55f, // (12, 1375) 170
      (short) 0xcd5f, // (12, 3423) 171
      (short) 0xc35f, // (12,  863) 172
      (short) 0xcb5f, // (12, 2911) 173
      (short) 0xc75f, // (12, 1887) 174
      (short) 0xcf5f, // (12, 3935) 175
      (short) 0xc0df, // (12,  223) 176
      (short) 0xc8df, // (12, 2271) 177
      (short) 0xc4df, // (12, 1247) 178
      (short) 0xccdf, // (12, 3295) 179
      (short) 0xc2df, // (12,  735) 180
      (short) 0xcadf, // (12, 2783) 181
      (short) 0xc6df, // (12, 1759) 182
      (short) 0xcedf, // (12, 3807) 183
      (short) 0xc1df, // (12,  479) 184
      (short) 0xc9df, // (12, 2527) 185
      (short) 0xc5df, // (12, 1503) 186
      (short) 0xcddf, // (12, 3551) 187
      (short) 0xc3df, // (12,  991) 188
      (short) 0xcbdf, // (12, 3039) 189
      (short) 0xc7df, // (12, 2015) 190
      (short) 0xcfdf, // (12, 4063) 191
      (short) 0xc03f, // (12,   63) 192
      (short) 0xc83f, // (12, 2111) 193
      (short) 0xc43f, // (12, 1087) 194
      (short) 0xcc3f, // (12, 3135) 195
      (short) 0xc23f, // (12,  575) 196
      (short) 0xca3f, // (12, 2623) 197
      (short) 0xc63f, // (12, 1599) 198
      (short) 0xce3f, // (12, 3647) 199
      (short) 0xc13f, // (12,  319) 200
      (short) 0xc93f, // (12, 2367) 201
      (short) 0xc53f, // (12, 1343) 202
      (short) 0xcd3f, // (12, 3391) 203
      (short) 0xc33f, // (12,  831) 204
      (short) 0xcb3f, // (12, 2879) 205
      (short) 0xc73f, // (12, 1855) 206
      (short) 0xcf3f, // (12, 3903) 207
      (short) 0xc0bf, // (12,  191) 208
      (short) 0xc8bf, // (12, 2239) 209
      (short) 0xc4bf, // (12, 1215) 210
      (short) 0xccbf, // (12, 3263) 211
      (short) 0xc2bf, // (12,  703) 212
      (short) 0xcabf, // (12, 2751) 213
      (short) 0xc6bf, // (12, 1727) 214
      (short) 0xcebf, // (12, 3775) 215
      (short) 0xc1bf, // (12,  447) 216
      (short) 0xc9bf, // (12, 2495) 217
      (short) 0xc5bf, // (12, 1471) 218
      (short) 0xcdbf, // (12, 3519) 219
      (short) 0xc3bf, // (12,  959) 220
      (short) 0xcbbf, // (12, 3007) 221
      (short) 0xc7bf, // (12, 1983) 222
      (short) 0xcfbf, // (12, 4031) 223
      (short) 0xc07f, // (12,  127) 224
      (short) 0xc87f, // (12, 2175) 225
      (short) 0xc47f, // (12, 1151) 226
      (short) 0xcc7f, // (12, 3199) 227
      (short) 0xc27f, // (12,  639) 228
      (short) 0xca7f, // (12, 2687) 229
      (short) 0xc67f, // (12, 1663) 230
      (short) 0xce7f, // (12, 3711) 231
      (short) 0xc17f, // (12,  383) 232
      (short) 0xc97f, // (12, 2431) 233
      (short) 0xc57f, // (12, 1407) 234
      (short) 0xcd7f, // (12, 3455) 235
      (short) 0xc37f, // (12,  895) 236
      (short) 0xcb7f, // (12, 2943) 237
      (short) 0xc77f, // (12, 1919) 238
      (short) 0xcf7f, // (12, 3967) 239
      (short) 0xc0ff, // (12,  255) 240
      (short) 0xc8ff, // (12, 2303) 241
      (short) 0xc4ff, // (12, 1279) 242
      (short) 0xccff, // (12, 3327) 243
      (short) 0xc2ff, // (12,  767) 244
      (short) 0xcaff, // (12, 2815) 245
      (short) 0xc6ff, // (12, 1791) 246
      (short) 0xceff, // (12, 3839) 247
      (short) 0xc1ff, // (12,  511) 248
      (short) 0xc9ff, // (12, 2559) 249
      (short) 0xc5ff, // (12, 1535) 250
      (short) 0xcdff, // (12, 3583) 251
      (short) 0xc3ff, // (12, 1023) 252
      (short) 0xcbff, // (12, 3071) 253
      (short) 0xc7ff, // (12, 2047) 254
      (short) 0xcfff  // (12, 4095) 255
    },

    // (table 13 of 22) (steady 13 of 16) (phase = 0.843750000 = 27.0 / 32.0)
    // entropy:    4.4684687952964843305
    // avg_length: 4.5509169030369793774; max_length = 12; num_symbols = 256
    {
      //table, // (4 bits,  12 bits) symbol
      //entry, // (length, codeword) [byte]
      (short) 0x803b, // ( 8,   59)   0
      (short) 0x4006, // ( 4,    6)   1
      (short) 0x7033, // ( 7,   51)   2
      (short) 0x3002, // ( 3,    2)   3
      (short) 0x80bb, // ( 8,  187)   4
      (short) 0x400e, // ( 4,   14)   5
      (short) 0x7073, // ( 7,  115)   6
      (short) 0x2000, // ( 2,    0)   7
      (short) 0xa0f7, // (10,  247)   8
      (short) 0x601d, // ( 6,   29)   9
      (short) 0x807b, // ( 8,  123)  10
      (short) 0x4001, // ( 4,    1)  11
      (short) 0xa2f7, // (10,  759)  12
      (short) 0x5005, // ( 5,    5)  13
      (short) 0x80fb, // ( 8,  251)  14
      (short) 0x4009, // ( 4,    9)  15
      (short) 0xb34f, // (11,  847)  16
      (short) 0x700b, // ( 7,   11)  17
      (short) 0x9057, // ( 9,   87)  18
      (short) 0x5015, // ( 5,   21)  19
      (short) 0xb74f, // (11, 1871)  20
      (short) 0x704b, // ( 7,   75)  21
      (short) 0x9157, // ( 9,  343)  22
      (short) 0x500d, // ( 5,   13)  23
      (short) 0xc72f, // (12, 1839)  24
      (short) 0x8007, // ( 8,    7)  25
      (short) 0xa1f7, // (10,  503)  26
      (short) 0x603d, // ( 6,   61)  27
      (short) 0xcf2f, // (12, 3887)  28
      (short) 0x8087, // ( 8,  135)  29
      (short) 0xa3f7, // (10, 1015)  30
      (short) 0x6003, // ( 6,    3)  31
      (short) 0xc0af, // (12,  175)  32
      (short) 0x8047, // ( 8,   71)  33
      (short) 0xa00f, // (10,   15)  34
      (short) 0x6023, // ( 6,   35)  35
      (short) 0xc8af, // (12, 2223)  36
      (short) 0x80c7, // ( 8,  199)  37
      (short) 0xa20f, // (10,  527)  38
      (short) 0x6013, // ( 6,   19)  39
      (short) 0xc4af, // (12, 1199)  40
      (short) 0x90d7, // ( 9,  215)  41
      (short) 0xb0cf, // (11,  207)  42
      (short) 0x702b, // ( 7,   43)  43
      (short) 0xccaf, // (12, 3247)  44
      (short) 0x91d7, // ( 9,  471)  45
      (short) 0xb4cf, // (11, 1231)  46
      (short) 0x706b, // ( 7,  107)  47
      (short) 0xc2af, // (12,  687)  48
      (short) 0xa10f, // (10,  271)  49
      (short) 0xcaaf, // (12, 2735)  50
      (short) 0x8027, // ( 8,   39)  51
      (short) 0xc6af, // (12, 1711)  52
      (short) 0xa30f, // (10,  783)  53
      (short) 0xceaf, // (12, 3759)  54
      (short) 0x80a7, // ( 8,  167)  55
      (short) 0xc1af, // (12,  431)  56
      (short) 0xb2cf, // (11,  719)  57
      (short) 0xc9af, // (12, 2479)  58
      (short) 0xa08f, // (10,  143)  59
      (short) 0xc5af, // (12, 1455)  60
      (short) 0xb6cf, // (11, 1743)  61
      (short) 0xcdaf, // (12, 3503)  62
      (short) 0xa28f, // (10,  655)  63
      (short) 0xc3af, // (12,  943)  64
      (short) 0x9037, // ( 9,   55)  65
      (short) 0xb1cf, // (11,  463)  66
      (short) 0x701b, // ( 7,   27)  67
      (short) 0xcbaf, // (12, 2991)  68
      (short) 0x9137, // ( 9,  311)  69
      (short) 0xb5cf, // (11, 1487)  70
      (short) 0x705b, // ( 7,   91)  71
      (short) 0xc7af, // (12, 1967)  72
      (short) 0xa18f, // (10,  399)  73
      (short) 0xcfaf, // (12, 4015)  74
      (short) 0x8067, // ( 8,  103)  75
      (short) 0xc06f, // (12,  111)  76
      (short) 0xa38f, // (10,  911)  77
      (short) 0xc86f, // (12, 2159)  78
      (short) 0x80e7, // ( 8,  231)  79
      (short) 0xc46f, // (12, 1135)  80
      (short) 0xb3cf, // (11,  975)  81
      (short) 0xcc6f, // (12, 3183)  82
      (short) 0x90b7, // ( 9,  183)  83
      (short) 0xc26f, // (12,  623)  84
      (short) 0xb7cf, // (11, 1999)  85
      (short) 0xca6f, // (12, 2671)  86
      (short) 0x91b7, // ( 9,  439)  87
      (short) 0xc66f, // (12, 1647)  88
      (short) 0xce6f, // (12, 3695)  89
      (short) 0xc16f, // (12,  367)  90
      (short) 0xb02f, // (11,   47)  91
      (short) 0xc96f, // (12, 2415)  92
      (short) 0xc56f, // (12, 1391)  93
      (short) 0xcd6f, // (12, 3439)  94
      (short) 0xb42f, // (11, 1071)  95
      (short) 0xc36f, // (12,  879)  96
      (short) 0xcb6f, // (12, 2927)  97
      (short) 0xc76f, // (12, 1903)  98
      (short) 0xb22f, // (11,  559)  99
      (short) 0xcf6f, // (12, 3951) 100
      (short) 0xc0ef, // (12,  239) 101
      (short) 0xc8ef, // (12, 2287) 102
      (short) 0xb62f, // (11, 1583) 103
      (short) 0xc4ef, // (12, 1263) 104
      (short) 0xccef, // (12, 3311) 105
      (short) 0xc2ef, // (12,  751) 106
      (short) 0xcaef, // (12, 2799) 107
      (short) 0xc6ef, // (12, 1775) 108
      (short) 0xceef, // (12, 3823) 109
      (short) 0xc1ef, // (12,  495) 110
      (short) 0xc9ef, // (12, 2543) 111
      (short) 0xc5ef, // (12, 1519) 112
      (short) 0xcdef, // (12, 3567) 113
      (short) 0xc3ef, // (12, 1007) 114
      (short) 0xcbef, // (12, 3055) 115
      (short) 0xc7ef, // (12, 2031) 116
      (short) 0xcfef, // (12, 4079) 117
      (short) 0xc01f, // (12,   31) 118
      (short) 0xc81f, // (12, 2079) 119
      (short) 0xc41f, // (12, 1055) 120
      (short) 0xcc1f, // (12, 3103) 121
      (short) 0xc21f, // (12,  543) 122
      (short) 0xca1f, // (12, 2591) 123
      (short) 0xc61f, // (12, 1567) 124
      (short) 0xce1f, // (12, 3615) 125
      (short) 0xc11f, // (12,  287) 126
      (short) 0xc91f, // (12, 2335) 127
      (short) 0xc51f, // (12, 1311) 128
      (short) 0xa04f, // (10,   79) 129
      (short) 0xcd1f, // (12, 3359) 130
      (short) 0x8017, // ( 8,   23) 131
      (short) 0xc31f, // (12,  799) 132
      (short) 0xa24f, // (10,  591) 133
      (short) 0xcb1f, // (12, 2847) 134
      (short) 0x8097, // ( 8,  151) 135
      (short) 0xc71f, // (12, 1823) 136
      (short) 0xb12f, // (11,  303) 137
      (short) 0xcf1f, // (12, 3871) 138
      (short) 0x9077, // ( 9,  119) 139
      (short) 0xc09f, // (12,  159) 140
      (short) 0xb52f, // (11, 1327) 141
      (short) 0xc89f, // (12, 2207) 142
      (short) 0x9177, // ( 9,  375) 143
      (short) 0xc49f, // (12, 1183) 144
      (short) 0xcc9f, // (12, 3231) 145
      (short) 0xc29f, // (12,  671) 146
      (short) 0xb32f, // (11,  815) 147
      (short) 0xca9f, // (12, 2719) 148
      (short) 0xc69f, // (12, 1695) 149
      (short) 0xce9f, // (12, 3743) 150
      (short) 0xa14f, // (10,  335) 151
      (short) 0xc19f, // (12,  415) 152
      (short) 0xc99f, // (12, 2463) 153
      (short) 0xc59f, // (12, 1439) 154
      (short) 0xcd9f, // (12, 3487) 155
      (short) 0xc39f, // (12,  927) 156
      (short) 0xcb9f, // (12, 2975) 157
      (short) 0xc79f, // (12, 1951) 158
      (short) 0xcf9f, // (12, 3999) 159
      (short) 0xc05f, // (12,   95) 160
      (short) 0xc85f, // (12, 2143) 161
      (short) 0xc45f, // (12, 1119) 162
      (short) 0xcc5f, // (12, 3167) 163
      (short) 0xc25f, // (12,  607) 164
      (short) 0xca5f, // (12, 2655) 165
      (short) 0xc65f, // (12, 1631) 166
      (short) 0xce5f, // (12, 3679) 167
      (short) 0xc15f, // (12,  351) 168
      (short) 0xc95f, // (12, 2399) 169
      (short) 0xc55f, // (12, 1375) 170
      (short) 0xcd5f, // (12, 3423) 171
      (short) 0xc35f, // (12,  863) 172
      (short) 0xcb5f, // (12, 2911) 173
      (short) 0xc75f, // (12, 1887) 174
      (short) 0xcf5f, // (12, 3935) 175
      (short) 0xc0df, // (12,  223) 176
      (short) 0xc8df, // (12, 2271) 177
      (short) 0xc4df, // (12, 1247) 178
      (short) 0xccdf, // (12, 3295) 179
      (short) 0xc2df, // (12,  735) 180
      (short) 0xcadf, // (12, 2783) 181
      (short) 0xc6df, // (12, 1759) 182
      (short) 0xcedf, // (12, 3807) 183
      (short) 0xc1df, // (12,  479) 184
      (short) 0xc9df, // (12, 2527) 185
      (short) 0xc5df, // (12, 1503) 186
      (short) 0xcddf, // (12, 3551) 187
      (short) 0xc3df, // (12,  991) 188
      (short) 0xcbdf, // (12, 3039) 189
      (short) 0xc7df, // (12, 2015) 190
      (short) 0xcfdf, // (12, 4063) 191
      (short) 0xc03f, // (12,   63) 192
      (short) 0xc83f, // (12, 2111) 193
      (short) 0xc43f, // (12, 1087) 194
      (short) 0xcc3f, // (12, 3135) 195
      (short) 0xc23f, // (12,  575) 196
      (short) 0xca3f, // (12, 2623) 197
      (short) 0xc63f, // (12, 1599) 198
      (short) 0xce3f, // (12, 3647) 199
      (short) 0xc13f, // (12,  319) 200
      (short) 0xc93f, // (12, 2367) 201
      (short) 0xc53f, // (12, 1343) 202
      (short) 0xcd3f, // (12, 3391) 203
      (short) 0xc33f, // (12,  831) 204
      (short) 0xcb3f, // (12, 2879) 205
      (short) 0xc73f, // (12, 1855) 206
      (short) 0xcf3f, // (12, 3903) 207
      (short) 0xc0bf, // (12,  191) 208
      (short) 0xc8bf, // (12, 2239) 209
      (short) 0xc4bf, // (12, 1215) 210
      (short) 0xccbf, // (12, 3263) 211
      (short) 0xc2bf, // (12,  703) 212
      (short) 0xcabf, // (12, 2751) 213
      (short) 0xc6bf, // (12, 1727) 214
      (short) 0xcebf, // (12, 3775) 215
      (short) 0xc1bf, // (12,  447) 216
      (short) 0xc9bf, // (12, 2495) 217
      (short) 0xc5bf, // (12, 1471) 218
      (short) 0xcdbf, // (12, 3519) 219
      (short) 0xc3bf, // (12,  959) 220
      (short) 0xcbbf, // (12, 3007) 221
      (short) 0xc7bf, // (12, 1983) 222
      (short) 0xcfbf, // (12, 4031) 223
      (short) 0xc07f, // (12,  127) 224
      (short) 0xc87f, // (12, 2175) 225
      (short) 0xc47f, // (12, 1151) 226
      (short) 0xcc7f, // (12, 3199) 227
      (short) 0xc27f, // (12,  639) 228
      (short) 0xca7f, // (12, 2687) 229
      (short) 0xc67f, // (12, 1663) 230
      (short) 0xce7f, // (12, 3711) 231
      (short) 0xc17f, // (12,  383) 232
      (short) 0xc97f, // (12, 2431) 233
      (short) 0xc57f, // (12, 1407) 234
      (short) 0xcd7f, // (12, 3455) 235
      (short) 0xc37f, // (12,  895) 236
      (short) 0xcb7f, // (12, 2943) 237
      (short) 0xc77f, // (12, 1919) 238
      (short) 0xcf7f, // (12, 3967) 239
      (short) 0xc0ff, // (12,  255) 240
      (short) 0xc8ff, // (12, 2303) 241
      (short) 0xc4ff, // (12, 1279) 242
      (short) 0xccff, // (12, 3327) 243
      (short) 0xc2ff, // (12,  767) 244
      (short) 0xcaff, // (12, 2815) 245
      (short) 0xc6ff, // (12, 1791) 246
      (short) 0xceff, // (12, 3839) 247
      (short) 0xc1ff, // (12,  511) 248
      (short) 0xc9ff, // (12, 2559) 249
      (short) 0xc5ff, // (12, 1535) 250
      (short) 0xcdff, // (12, 3583) 251
      (short) 0xc3ff, // (12, 1023) 252
      (short) 0xcbff, // (12, 3071) 253
      (short) 0xc7ff, // (12, 2047) 254
      (short) 0xcfff  // (12, 4095) 255
    },

    // (table 14 of 22) (steady 14 of 16) (phase = 0.906250000 = 29.0 / 32.0)
    // entropy:    4.4675179140944036860
    // avg_length: 4.5477235350841240802; max_length = 12; num_symbols = 256
    {
      //table, // (4 bits,  12 bits) symbol
      //entry, // (length, codeword) [byte]
      (short) 0x9017, // ( 9,   23)   0
      (short) 0x4006, // ( 4,    6)   1
      (short) 0x7033, // ( 7,   51)   2
      (short) 0x3002, // ( 3,    2)   3
      (short) 0x9117, // ( 9,  279)   4
      (short) 0x400e, // ( 4,   14)   5
      (short) 0x7073, // ( 7,  115)   6
      (short) 0x2000, // ( 2,    0)   7
      (short) 0xa177, // (10,  375)   8
      (short) 0x601d, // ( 6,   29)   9
      (short) 0x803b, // ( 8,   59)  10
      (short) 0x4001, // ( 4,    1)  11
      (short) 0xa377, // (10,  887)  12
      (short) 0x5005, // ( 5,    5)  13
      (short) 0x80bb, // ( 8,  187)  14
      (short) 0x4009, // ( 4,    9)  15
      (short) 0xb0cf, // (11,  207)  16
      (short) 0x700b, // ( 7,   11)  17
      (short) 0x9097, // ( 9,  151)  18
      (short) 0x5015, // ( 5,   21)  19
      (short) 0xb4cf, // (11, 1231)  20
      (short) 0x704b, // ( 7,   75)  21
      (short) 0x9197, // ( 9,  407)  22
      (short) 0x500d, // ( 5,   13)  23
      (short) 0xc4af, // (12, 1199)  24
      (short) 0x807b, // ( 8,  123)  25
      (short) 0xa0f7, // (10,  247)  26
      (short) 0x603d, // ( 6,   61)  27
      (short) 0xccaf, // (12, 3247)  28
      (short) 0x80fb, // ( 8,  251)  29
      (short) 0xa2f7, // (10,  759)  30
      (short) 0x6003, // ( 6,    3)  31
      (short) 0xc2af, // (12,  687)  32
      (short) 0x8007, // ( 8,    7)  33
      (short) 0xa1f7, // (10,  503)  34
      (short) 0x6023, // ( 6,   35)  35
      (short) 0xcaaf, // (12, 2735)  36
      (short) 0x8087, // ( 8,  135)  37
      (short) 0xa3f7, // (10, 1015)  38
      (short) 0x6013, // ( 6,   19)  39
      (short) 0xc6af, // (12, 1711)  40
      (short) 0x9057, // ( 9,   87)  41
      (short) 0xb2cf, // (11,  719)  42
      (short) 0x702b, // ( 7,   43)  43
      (short) 0xceaf, // (12, 3759)  44
      (short) 0x9157, // ( 9,  343)  45
      (short) 0xb6cf, // (11, 1743)  46
      (short) 0x706b, // ( 7,  107)  47
      (short) 0xc1af, // (12,  431)  48
      (short) 0xa00f, // (10,   15)  49
      (short) 0xc9af, // (12, 2479)  50
      (short) 0x8047, // ( 8,   71)  51
      (short) 0xc5af, // (12, 1455)  52
      (short) 0xa20f, // (10,  527)  53
      (short) 0xcdaf, // (12, 3503)  54
      (short) 0x80c7, // ( 8,  199)  55
      (short) 0xc3af, // (12,  943)  56
      (short) 0xb1cf, // (11,  463)  57
      (short) 0xcbaf, // (12, 2991)  58
      (short) 0xa10f, // (10,  271)  59
      (short) 0xc7af, // (12, 1967)  60
      (short) 0xb5cf, // (11, 1487)  61
      (short) 0xcfaf, // (12, 4015)  62
      (short) 0x90d7, // ( 9,  215)  63
      (short) 0xc06f, // (12,  111)  64
      (short) 0x91d7, // ( 9,  471)  65
      (short) 0xb3cf, // (11,  975)  66
      (short) 0x701b, // ( 7,   27)  67
      (short) 0xc86f, // (12, 2159)  68
      (short) 0x9037, // ( 9,   55)  69
      (short) 0xb7cf, // (11, 1999)  70
      (short) 0x705b, // ( 7,   91)  71
      (short) 0xc46f, // (12, 1135)  72
      (short) 0xa30f, // (10,  783)  73
      (short) 0xcc6f, // (12, 3183)  74
      (short) 0x8027, // ( 8,   39)  75
      (short) 0xc26f, // (12,  623)  76
      (short) 0xa08f, // (10,  143)  77
      (short) 0xca6f, // (12, 2671)  78
      (short) 0x80a7, // ( 8,  167)  79
      (short) 0xc66f, // (12, 1647)  80
      (short) 0xb02f, // (11,   47)  81
      (short) 0xce6f, // (12, 3695)  82
      (short) 0x9137, // ( 9,  311)  83
      (short) 0xc16f, // (12,  367)  84
      (short) 0xb42f, // (11, 1071)  85
      (short) 0xc96f, // (12, 2415)  86
      (short) 0x90b7, // ( 9,  183)  87
      (short) 0xc56f, // (12, 1391)  88
      (short) 0xcd6f, // (12, 3439)  89
      (short) 0xc36f, // (12,  879)  90
      (short) 0xb22f, // (11,  559)  91
      (short) 0xcb6f, // (12, 2927)  92
      (short) 0xc76f, // (12, 1903)  93
      (short) 0xcf6f, // (12, 3951)  94
      (short) 0xa28f, // (10,  655)  95
      (short) 0xc0ef, // (12,  239)  96
      (short) 0xc8ef, // (12, 2287)  97
      (short) 0xc4ef, // (12, 1263)  98
      (short) 0xa18f, // (10,  399)  99
      (short) 0xccef, // (12, 3311) 100
      (short) 0xc2ef, // (12,  751) 101
      (short) 0xcaef, // (12, 2799) 102
      (short) 0xa38f, // (10,  911) 103
      (short) 0xc6ef, // (12, 1775) 104
      (short) 0xceef, // (12, 3823) 105
      (short) 0xc1ef, // (12,  495) 106
      (short) 0xc9ef, // (12, 2543) 107
      (short) 0xc5ef, // (12, 1519) 108
      (short) 0xcdef, // (12, 3567) 109
      (short) 0xc3ef, // (12, 1007) 110
      (short) 0xb62f, // (11, 1583) 111
      (short) 0xcbef, // (12, 3055) 112
      (short) 0xc7ef, // (12, 2031) 113
      (short) 0xcfef, // (12, 4079) 114
      (short) 0xc01f, // (12,   31) 115
      (short) 0xc81f, // (12, 2079) 116
      (short) 0xc41f, // (12, 1055) 117
      (short) 0xcc1f, // (12, 3103) 118
      (short) 0xc21f, // (12,  543) 119
      (short) 0xca1f, // (12, 2591) 120
      (short) 0xc61f, // (12, 1567) 121
      (short) 0xce1f, // (12, 3615) 122
      (short) 0xc11f, // (12,  287) 123
      (short) 0xc91f, // (12, 2335) 124
      (short) 0xc51f, // (12, 1311) 125
      (short) 0xcd1f, // (12, 3359) 126
      (short) 0xc31f, // (12,  799) 127
      (short) 0xcb1f, // (12, 2847) 128
      (short) 0xa04f, // (10,   79) 129
      (short) 0xc71f, // (12, 1823) 130
      (short) 0x8067, // ( 8,  103) 131
      (short) 0xcf1f, // (12, 3871) 132
      (short) 0xa24f, // (10,  591) 133
      (short) 0xc09f, // (12,  159) 134
      (short) 0x80e7, // ( 8,  231) 135
      (short) 0xc89f, // (12, 2207) 136
      (short) 0xb12f, // (11,  303) 137
      (short) 0xc49f, // (12, 1183) 138
      (short) 0x91b7, // ( 9,  439) 139
      (short) 0xcc9f, // (12, 3231) 140
      (short) 0xb52f, // (11, 1327) 141
      (short) 0xc29f, // (12,  671) 142
      (short) 0x9077, // ( 9,  119) 143
      (short) 0xca9f, // (12, 2719) 144
      (short) 0xc69f, // (12, 1695) 145
      (short) 0xce9f, // (12, 3743) 146
      (short) 0xa14f, // (10,  335) 147
      (short) 0xc19f, // (12,  415) 148
      (short) 0xc99f, // (12, 2463) 149
      (short) 0xc59f, // (12, 1439) 150
      (short) 0xa34f, // (10,  847) 151
      (short) 0xcd9f, // (12, 3487) 152
      (short) 0xc39f, // (12,  927) 153
      (short) 0xcb9f, // (12, 2975) 154
      (short) 0xc79f, // (12, 1951) 155
      (short) 0xcf9f, // (12, 3999) 156
      (short) 0xc05f, // (12,   95) 157
      (short) 0xc85f, // (12, 2143) 158
      (short) 0xb32f, // (11,  815) 159
      (short) 0xc45f, // (12, 1119) 160
      (short) 0xcc5f, // (12, 3167) 161
      (short) 0xc25f, // (12,  607) 162
      (short) 0xb72f, // (11, 1839) 163
      (short) 0xca5f, // (12, 2655) 164
      (short) 0xc65f, // (12, 1631) 165
      (short) 0xce5f, // (12, 3679) 166
      (short) 0xb0af, // (11,  175) 167
      (short) 0xc15f, // (12,  351) 168
      (short) 0xc95f, // (12, 2399) 169
      (short) 0xc55f, // (12, 1375) 170
      (short) 0xcd5f, // (12, 3423) 171
      (short) 0xc35f, // (12,  863) 172
      (short) 0xcb5f, // (12, 2911) 173
      (short) 0xc75f, // (12, 1887) 174
      (short) 0xcf5f, // (12, 3935) 175
      (short) 0xc0df, // (12,  223) 176
      (short) 0xc8df, // (12, 2271) 177
      (short) 0xc4df, // (12, 1247) 178
      (short) 0xccdf, // (12, 3295) 179
      (short) 0xc2df, // (12,  735) 180
      (short) 0xcadf, // (12, 2783) 181
      (short) 0xc6df, // (12, 1759) 182
      (short) 0xcedf, // (12, 3807) 183
      (short) 0xc1df, // (12,  479) 184
      (short) 0xc9df, // (12, 2527) 185
      (short) 0xc5df, // (12, 1503) 186
      (short) 0xcddf, // (12, 3551) 187
      (short) 0xc3df, // (12,  991) 188
      (short) 0xcbdf, // (12, 3039) 189
      (short) 0xc7df, // (12, 2015) 190
      (short) 0xcfdf, // (12, 4063) 191
      (short) 0xc03f, // (12,   63) 192
      (short) 0xc83f, // (12, 2111) 193
      (short) 0xc43f, // (12, 1087) 194
      (short) 0xcc3f, // (12, 3135) 195
      (short) 0xc23f, // (12,  575) 196
      (short) 0xca3f, // (12, 2623) 197
      (short) 0xc63f, // (12, 1599) 198
      (short) 0xce3f, // (12, 3647) 199
      (short) 0xc13f, // (12,  319) 200
      (short) 0xc93f, // (12, 2367) 201
      (short) 0xc53f, // (12, 1343) 202
      (short) 0xcd3f, // (12, 3391) 203
      (short) 0xc33f, // (12,  831) 204
      (short) 0xcb3f, // (12, 2879) 205
      (short) 0xc73f, // (12, 1855) 206
      (short) 0xcf3f, // (12, 3903) 207
      (short) 0xc0bf, // (12,  191) 208
      (short) 0xc8bf, // (12, 2239) 209
      (short) 0xc4bf, // (12, 1215) 210
      (short) 0xccbf, // (12, 3263) 211
      (short) 0xc2bf, // (12,  703) 212
      (short) 0xcabf, // (12, 2751) 213
      (short) 0xc6bf, // (12, 1727) 214
      (short) 0xcebf, // (12, 3775) 215
      (short) 0xc1bf, // (12,  447) 216
      (short) 0xc9bf, // (12, 2495) 217
      (short) 0xc5bf, // (12, 1471) 218
      (short) 0xcdbf, // (12, 3519) 219
      (short) 0xc3bf, // (12,  959) 220
      (short) 0xcbbf, // (12, 3007) 221
      (short) 0xc7bf, // (12, 1983) 222
      (short) 0xcfbf, // (12, 4031) 223
      (short) 0xc07f, // (12,  127) 224
      (short) 0xc87f, // (12, 2175) 225
      (short) 0xc47f, // (12, 1151) 226
      (short) 0xcc7f, // (12, 3199) 227
      (short) 0xc27f, // (12,  639) 228
      (short) 0xca7f, // (12, 2687) 229
      (short) 0xc67f, // (12, 1663) 230
      (short) 0xce7f, // (12, 3711) 231
      (short) 0xc17f, // (12,  383) 232
      (short) 0xc97f, // (12, 2431) 233
      (short) 0xc57f, // (12, 1407) 234
      (short) 0xcd7f, // (12, 3455) 235
      (short) 0xc37f, // (12,  895) 236
      (short) 0xcb7f, // (12, 2943) 237
      (short) 0xc77f, // (12, 1919) 238
      (short) 0xcf7f, // (12, 3967) 239
      (short) 0xc0ff, // (12,  255) 240
      (short) 0xc8ff, // (12, 2303) 241
      (short) 0xc4ff, // (12, 1279) 242
      (short) 0xccff, // (12, 3327) 243
      (short) 0xc2ff, // (12,  767) 244
      (short) 0xcaff, // (12, 2815) 245
      (short) 0xc6ff, // (12, 1791) 246
      (short) 0xceff, // (12, 3839) 247
      (short) 0xc1ff, // (12,  511) 248
      (short) 0xc9ff, // (12, 2559) 249
      (short) 0xc5ff, // (12, 1535) 250
      (short) 0xcdff, // (12, 3583) 251
      (short) 0xc3ff, // (12, 1023) 252
      (short) 0xcbff, // (12, 3071) 253
      (short) 0xc7ff, // (12, 2047) 254
      (short) 0xcfff  // (12, 4095) 255
    },

    // (table 15 of 22) (steady 15 of 16) (phase = 0.968750000 = 31.0 / 32.0)
    // entropy:    4.4653007097343397902
    // avg_length: 4.5480722016259509388; max_length = 12; num_symbols = 256
    {
      //table, // (4 bits,  12 bits) symbol
      //entry, // (length, codeword) [byte]
      (short) 0x9017, // ( 9,   23)   0
      (short) 0x4006, // ( 4,    6)   1
      (short) 0x7033, // ( 7,   51)   2
      (short) 0x3002, // ( 3,    2)   3
      (short) 0x9117, // ( 9,  279)   4
      (short) 0x400e, // ( 4,   14)   5
      (short) 0x7073, // ( 7,  115)   6
      (short) 0x2000, // ( 2,    0)   7
      (short) 0xa0f7, // (10,  247)   8
      (short) 0x601d, // ( 6,   29)   9
      (short) 0x803b, // ( 8,   59)  10
      (short) 0x4001, // ( 4,    1)  11
      (short) 0xa2f7, // (10,  759)  12
      (short) 0x5005, // ( 5,    5)  13
      (short) 0x80bb, // ( 8,  187)  14
      (short) 0x4009, // ( 4,    9)  15
      (short) 0xb0cf, // (11,  207)  16
      (short) 0x700b, // ( 7,   11)  17
      (short) 0x9097, // ( 9,  151)  18
      (short) 0x5015, // ( 5,   21)  19
      (short) 0xb4cf, // (11, 1231)  20
      (short) 0x704b, // ( 7,   75)  21
      (short) 0x9197, // ( 9,  407)  22
      (short) 0x500d, // ( 5,   13)  23
      (short) 0xc0af, // (12,  175)  24
      (short) 0x807b, // ( 8,  123)  25
      (short) 0xb2cf, // (11,  719)  26
      (short) 0x603d, // ( 6,   61)  27
      (short) 0xc8af, // (12, 2223)  28
      (short) 0x80fb, // ( 8,  251)  29
      (short) 0xa1f7, // (10,  503)  30
      (short) 0x6003, // ( 6,    3)  31
      (short) 0xc4af, // (12, 1199)  32
      (short) 0x8007, // ( 8,    7)  33
      (short) 0xb6cf, // (11, 1743)  34
      (short) 0x6023, // ( 6,   35)  35
      (short) 0xccaf, // (12, 3247)  36
      (short) 0x8087, // ( 8,  135)  37
      (short) 0xa3f7, // (10, 1015)  38
      (short) 0x6013, // ( 6,   19)  39
      (short) 0xc2af, // (12,  687)  40
      (short) 0x9057, // ( 9,   87)  41
      (short) 0xcaaf, // (12, 2735)  42
      (short) 0x702b, // ( 7,   43)  43
      (short) 0xc6af, // (12, 1711)  44
      (short) 0x9157, // ( 9,  343)  45
      (short) 0xb1cf, // (11,  463)  46
      (short) 0x706b, // ( 7,  107)  47
      (short) 0xceaf, // (12, 3759)  48
      (short) 0xa00f, // (10,   15)  49
      (short) 0xc1af, // (12,  431)  50
      (short) 0x8047, // ( 8,   71)  51
      (short) 0xc9af, // (12, 2479)  52
      (short) 0xa20f, // (10,  527)  53
      (short) 0xc5af, // (12, 1455)  54
      (short) 0x80c7, // ( 8,  199)  55
      (short) 0xcdaf, // (12, 3503)  56
      (short) 0xb5cf, // (11, 1487)  57
      (short) 0xc3af, // (12,  943)  58
      (short) 0x90d7, // ( 9,  215)  59
      (short) 0xcbaf, // (12, 2991)  60
      (short) 0xb3cf, // (11,  975)  61
      (short) 0xc7af, // (12, 1967)  62
      (short) 0x91d7, // ( 9,  471)  63
      (short) 0xcfaf, // (12, 4015)  64
      (short) 0x9037, // ( 9,   55)  65
      (short) 0xc06f, // (12,  111)  66
      (short) 0x701b, // ( 7,   27)  67
      (short) 0xc86f, // (12, 2159)  68
      (short) 0x9137, // ( 9,  311)  69
      (short) 0xb7cf, // (11, 1999)  70
      (short) 0x705b, // ( 7,   91)  71
      (short) 0xc46f, // (12, 1135)  72
      (short) 0xa10f, // (10,  271)  73
      (short) 0xcc6f, // (12, 3183)  74
      (short) 0x8027, // ( 8,   39)  75
      (short) 0xc26f, // (12,  623)  76
      (short) 0xa30f, // (10,  783)  77
      (short) 0xca6f, // (12, 2671)  78
      (short) 0x80a7, // ( 8,  167)  79
      (short) 0xc66f, // (12, 1647)  80
      (short) 0xb02f, // (11,   47)  81
      (short) 0xce6f, // (12, 3695)  82
      (short) 0x90b7, // ( 9,  183)  83
      (short) 0xc16f, // (12,  367)  84
      (short) 0xb42f, // (11, 1071)  85
      (short) 0xc96f, // (12, 2415)  86
      (short) 0x91b7, // ( 9,  439)  87
      (short) 0xc56f, // (12, 1391)  88
      (short) 0xcd6f, // (12, 3439)  89
      (short) 0xc36f, // (12,  879)  90
      (short) 0xa08f, // (10,  143)  91
      (short) 0xcb6f, // (12, 2927)  92
      (short) 0xc76f, // (12, 1903)  93
      (short) 0xcf6f, // (12, 3951)  94
      (short) 0xa28f, // (10,  655)  95
      (short) 0xc0ef, // (12,  239)  96
      (short) 0xc8ef, // (12, 2287)  97
      (short) 0xc4ef, // (12, 1263)  98
      (short) 0xa18f, // (10,  399)  99
      (short) 0xccef, // (12, 3311) 100
      (short) 0xc2ef, // (12,  751) 101
      (short) 0xcaef, // (12, 2799) 102
      (short) 0xa38f, // (10,  911) 103
      (short) 0xc6ef, // (12, 1775) 104
      (short) 0xceef, // (12, 3823) 105
      (short) 0xc1ef, // (12,  495) 106
      (short) 0xc9ef, // (12, 2543) 107
      (short) 0xc5ef, // (12, 1519) 108
      (short) 0xcdef, // (12, 3567) 109
      (short) 0xc3ef, // (12, 1007) 110
      (short) 0xb22f, // (11,  559) 111
      (short) 0xcbef, // (12, 3055) 112
      (short) 0xc7ef, // (12, 2031) 113
      (short) 0xcfef, // (12, 4079) 114
      (short) 0xc01f, // (12,   31) 115
      (short) 0xc81f, // (12, 2079) 116
      (short) 0xc41f, // (12, 1055) 117
      (short) 0xcc1f, // (12, 3103) 118
      (short) 0xc21f, // (12,  543) 119
      (short) 0xca1f, // (12, 2591) 120
      (short) 0xc61f, // (12, 1567) 121
      (short) 0xce1f, // (12, 3615) 122
      (short) 0xc11f, // (12,  287) 123
      (short) 0xc91f, // (12, 2335) 124
      (short) 0xc51f, // (12, 1311) 125
      (short) 0xcd1f, // (12, 3359) 126
      (short) 0xc31f, // (12,  799) 127
      (short) 0xcb1f, // (12, 2847) 128
      (short) 0xa04f, // (10,   79) 129
      (short) 0xc71f, // (12, 1823) 130
      (short) 0x8067, // ( 8,  103) 131
      (short) 0xcf1f, // (12, 3871) 132
      (short) 0xa24f, // (10,  591) 133
      (short) 0xc09f, // (12,  159) 134
      (short) 0x80e7, // ( 8,  231) 135
      (short) 0xc89f, // (12, 2207) 136
      (short) 0xb62f, // (11, 1583) 137
      (short) 0xc49f, // (12, 1183) 138
      (short) 0x9077, // ( 9,  119) 139
      (short) 0xcc9f, // (12, 3231) 140
      (short) 0xb12f, // (11,  303) 141
      (short) 0xc29f, // (12,  671) 142
      (short) 0x9177, // ( 9,  375) 143
      (short) 0xca9f, // (12, 2719) 144
      (short) 0xc69f, // (12, 1695) 145
      (short) 0xce9f, // (12, 3743) 146
      (short) 0xa14f, // (10,  335) 147
      (short) 0xc19f, // (12,  415) 148
      (short) 0xc99f, // (12, 2463) 149
      (short) 0xc59f, // (12, 1439) 150
      (short) 0xa34f, // (10,  847) 151
      (short) 0xcd9f, // (12, 3487) 152
      (short) 0xc39f, // (12,  927) 153
      (short) 0xcb9f, // (12, 2975) 154
      (short) 0xc79f, // (12, 1951) 155
      (short) 0xcf9f, // (12, 3999) 156
      (short) 0xc05f, // (12,   95) 157
      (short) 0xc85f, // (12, 2143) 158
      (short) 0xb52f, // (11, 1327) 159
      (short) 0xc45f, // (12, 1119) 160
      (short) 0xcc5f, // (12, 3167) 161
      (short) 0xc25f, // (12,  607) 162
      (short) 0xb32f, // (11,  815) 163
      (short) 0xca5f, // (12, 2655) 164
      (short) 0xc65f, // (12, 1631) 165
      (short) 0xce5f, // (12, 3679) 166
      (short) 0xb72f, // (11, 1839) 167
      (short) 0xc15f, // (12,  351) 168
      (short) 0xc95f, // (12, 2399) 169
      (short) 0xc55f, // (12, 1375) 170
      (short) 0xcd5f, // (12, 3423) 171
      (short) 0xc35f, // (12,  863) 172
      (short) 0xcb5f, // (12, 2911) 173
      (short) 0xc75f, // (12, 1887) 174
      (short) 0xcf5f, // (12, 3935) 175
      (short) 0xc0df, // (12,  223) 176
      (short) 0xc8df, // (12, 2271) 177
      (short) 0xc4df, // (12, 1247) 178
      (short) 0xccdf, // (12, 3295) 179
      (short) 0xc2df, // (12,  735) 180
      (short) 0xcadf, // (12, 2783) 181
      (short) 0xc6df, // (12, 1759) 182
      (short) 0xcedf, // (12, 3807) 183
      (short) 0xc1df, // (12,  479) 184
      (short) 0xc9df, // (12, 2527) 185
      (short) 0xc5df, // (12, 1503) 186
      (short) 0xcddf, // (12, 3551) 187
      (short) 0xc3df, // (12,  991) 188
      (short) 0xcbdf, // (12, 3039) 189
      (short) 0xc7df, // (12, 2015) 190
      (short) 0xcfdf, // (12, 4063) 191
      (short) 0xc03f, // (12,   63) 192
      (short) 0xc83f, // (12, 2111) 193
      (short) 0xc43f, // (12, 1087) 194
      (short) 0xcc3f, // (12, 3135) 195
      (short) 0xc23f, // (12,  575) 196
      (short) 0xca3f, // (12, 2623) 197
      (short) 0xc63f, // (12, 1599) 198
      (short) 0xce3f, // (12, 3647) 199
      (short) 0xc13f, // (12,  319) 200
      (short) 0xc93f, // (12, 2367) 201
      (short) 0xc53f, // (12, 1343) 202
      (short) 0xcd3f, // (12, 3391) 203
      (short) 0xc33f, // (12,  831) 204
      (short) 0xcb3f, // (12, 2879) 205
      (short) 0xc73f, // (12, 1855) 206
      (short) 0xcf3f, // (12, 3903) 207
      (short) 0xc0bf, // (12,  191) 208
      (short) 0xc8bf, // (12, 2239) 209
      (short) 0xc4bf, // (12, 1215) 210
      (short) 0xccbf, // (12, 3263) 211
      (short) 0xc2bf, // (12,  703) 212
      (short) 0xcabf, // (12, 2751) 213
      (short) 0xc6bf, // (12, 1727) 214
      (short) 0xcebf, // (12, 3775) 215
      (short) 0xc1bf, // (12,  447) 216
      (short) 0xc9bf, // (12, 2495) 217
      (short) 0xc5bf, // (12, 1471) 218
      (short) 0xcdbf, // (12, 3519) 219
      (short) 0xc3bf, // (12,  959) 220
      (short) 0xcbbf, // (12, 3007) 221
      (short) 0xc7bf, // (12, 1983) 222
      (short) 0xcfbf, // (12, 4031) 223
      (short) 0xc07f, // (12,  127) 224
      (short) 0xc87f, // (12, 2175) 225
      (short) 0xc47f, // (12, 1151) 226
      (short) 0xcc7f, // (12, 3199) 227
      (short) 0xc27f, // (12,  639) 228
      (short) 0xca7f, // (12, 2687) 229
      (short) 0xc67f, // (12, 1663) 230
      (short) 0xce7f, // (12, 3711) 231
      (short) 0xc17f, // (12,  383) 232
      (short) 0xc97f, // (12, 2431) 233
      (short) 0xc57f, // (12, 1407) 234
      (short) 0xcd7f, // (12, 3455) 235
      (short) 0xc37f, // (12,  895) 236
      (short) 0xcb7f, // (12, 2943) 237
      (short) 0xc77f, // (12, 1919) 238
      (short) 0xcf7f, // (12, 3967) 239
      (short) 0xc0ff, // (12,  255) 240
      (short) 0xc8ff, // (12, 2303) 241
      (short) 0xc4ff, // (12, 1279) 242
      (short) 0xccff, // (12, 3327) 243
      (short) 0xc2ff, // (12,  767) 244
      (short) 0xcaff, // (12, 2815) 245
      (short) 0xc6ff, // (12, 1791) 246
      (short) 0xceff, // (12, 3839) 247
      (short) 0xc1ff, // (12,  511) 248
      (short) 0xc9ff, // (12, 2559) 249
      (short) 0xc5ff, // (12, 1535) 250
      (short) 0xcdff, // (12, 3583) 251
      (short) 0xc3ff, // (12, 1023) 252
      (short) 0xcbff, // (12, 3071) 253
      (short) 0xc7ff, // (12, 2047) 254
      (short) 0xcfff  // (12, 4095) 255
    },

    // Six Encoding Tables for the Midrange.

    // (table 16 of 22) (midrange 0 of 6) (c/k = 0.500000000 = 3.0 / 6.0)
    // entropy:    2.1627885076675394949
    // avg_length: 2.2704182849800043087; max_length = 12; num_symbols = 256
    {
      //table, // (4 bits,  12 bits) symbol
      //entry, // (length, codeword) [byte]
      (short) 0x1000, // ( 1,    0)   0
      (short) 0x2001, // ( 2,    1)   1
      (short) 0x4003, // ( 4,    3)   2
      (short) 0x500b, // ( 5,   11)   3
      (short) 0x501b, // ( 5,   27)   4
      (short) 0x6007, // ( 6,    7)   5
      (short) 0x8057, // ( 8,   87)   6
      (short) 0x9077, // ( 9,  119)   7
      (short) 0x6027, // ( 6,   39)   8
      (short) 0x80d7, // ( 8,  215)   9
      (short) 0x9177, // ( 9,  375)  10
      (short) 0xa1f7, // (10,  503)  11
      (short) 0xa3f7, // (10, 1015)  12
      (short) 0xb08f, // (11,  143)  13
      (short) 0xc58f, // (12, 1423)  14
      (short) 0xcd8f, // (12, 3471)  15
      (short) 0x7017, // ( 7,   23)  16
      (short) 0x8037, // ( 8,   55)  17
      (short) 0xa00f, // (10,   15)  18
      (short) 0xb48f, // (11, 1167)  19
      (short) 0xb28f, // (11,  655)  20
      (short) 0xc38f, // (12,  911)  21
      (short) 0xcb8f, // (12, 2959)  22
      (short) 0xc78f, // (12, 1935)  23
      (short) 0xcf8f, // (12, 3983)  24
      (short) 0xc04f, // (12,   79)  25
      (short) 0xc84f, // (12, 2127)  26
      (short) 0xc44f, // (12, 1103)  27
      (short) 0xcc4f, // (12, 3151)  28
      (short) 0xc24f, // (12,  591)  29
      (short) 0xca4f, // (12, 2639)  30
      (short) 0xc64f, // (12, 1615)  31
      (short) 0x80b7, // ( 8,  183)  32
      (short) 0xa20f, // (10,  527)  33
      (short) 0xb68f, // (11, 1679)  34
      (short) 0xce4f, // (12, 3663)  35
      (short) 0xc14f, // (12,  335)  36
      (short) 0xc94f, // (12, 2383)  37
      (short) 0xc54f, // (12, 1359)  38
      (short) 0xcd4f, // (12, 3407)  39
      (short) 0xc34f, // (12,  847)  40
      (short) 0xcb4f, // (12, 2895)  41
      (short) 0xc74f, // (12, 1871)  42
      (short) 0xcf4f, // (12, 3919)  43
      (short) 0xc0cf, // (12,  207)  44
      (short) 0xc8cf, // (12, 2255)  45
      (short) 0xc4cf, // (12, 1231)  46
      (short) 0xcccf, // (12, 3279)  47
      (short) 0xc2cf, // (12,  719)  48
      (short) 0xcacf, // (12, 2767)  49
      (short) 0xc6cf, // (12, 1743)  50
      (short) 0xcecf, // (12, 3791)  51
      (short) 0xc1cf, // (12,  463)  52
      (short) 0xc9cf, // (12, 2511)  53
      (short) 0xc5cf, // (12, 1487)  54
      (short) 0xcdcf, // (12, 3535)  55
      (short) 0xc3cf, // (12,  975)  56
      (short) 0xcbcf, // (12, 3023)  57
      (short) 0xc7cf, // (12, 1999)  58
      (short) 0xcfcf, // (12, 4047)  59
      (short) 0xc02f, // (12,   47)  60
      (short) 0xc82f, // (12, 2095)  61
      (short) 0xc42f, // (12, 1071)  62
      (short) 0xcc2f, // (12, 3119)  63
      (short) 0x90f7, // ( 9,  247)  64
      (short) 0xa10f, // (10,  271)  65
      (short) 0xc22f, // (12,  559)  66
      (short) 0xca2f, // (12, 2607)  67
      (short) 0xc62f, // (12, 1583)  68
      (short) 0xce2f, // (12, 3631)  69
      (short) 0xc12f, // (12,  303)  70
      (short) 0xc92f, // (12, 2351)  71
      (short) 0xc52f, // (12, 1327)  72
      (short) 0xcd2f, // (12, 3375)  73
      (short) 0xc32f, // (12,  815)  74
      (short) 0xcb2f, // (12, 2863)  75
      (short) 0xc72f, // (12, 1839)  76
      (short) 0xcf2f, // (12, 3887)  77
      (short) 0xc0af, // (12,  175)  78
      (short) 0xc8af, // (12, 2223)  79
      (short) 0xc4af, // (12, 1199)  80
      (short) 0xccaf, // (12, 3247)  81
      (short) 0xc2af, // (12,  687)  82
      (short) 0xcaaf, // (12, 2735)  83
      (short) 0xc6af, // (12, 1711)  84
      (short) 0xceaf, // (12, 3759)  85
      (short) 0xc1af, // (12,  431)  86
      (short) 0xc9af, // (12, 2479)  87
      (short) 0xc5af, // (12, 1455)  88
      (short) 0xcdaf, // (12, 3503)  89
      (short) 0xc3af, // (12,  943)  90
      (short) 0xcbaf, // (12, 2991)  91
      (short) 0xc7af, // (12, 1967)  92
      (short) 0xcfaf, // (12, 4015)  93
      (short) 0xc06f, // (12,  111)  94
      (short) 0xc86f, // (12, 2159)  95
      (short) 0xc46f, // (12, 1135)  96
      (short) 0xcc6f, // (12, 3183)  97
      (short) 0xc26f, // (12,  623)  98
      (short) 0xca6f, // (12, 2671)  99
      (short) 0xc66f, // (12, 1647) 100
      (short) 0xce6f, // (12, 3695) 101
      (short) 0xc16f, // (12,  367) 102
      (short) 0xc96f, // (12, 2415) 103
      (short) 0xc56f, // (12, 1391) 104
      (short) 0xcd6f, // (12, 3439) 105
      (short) 0xc36f, // (12,  879) 106
      (short) 0xcb6f, // (12, 2927) 107
      (short) 0xc76f, // (12, 1903) 108
      (short) 0xcf6f, // (12, 3951) 109
      (short) 0xc0ef, // (12,  239) 110
      (short) 0xc8ef, // (12, 2287) 111
      (short) 0xc4ef, // (12, 1263) 112
      (short) 0xccef, // (12, 3311) 113
      (short) 0xc2ef, // (12,  751) 114
      (short) 0xcaef, // (12, 2799) 115
      (short) 0xc6ef, // (12, 1775) 116
      (short) 0xceef, // (12, 3823) 117
      (short) 0xc1ef, // (12,  495) 118
      (short) 0xc9ef, // (12, 2543) 119
      (short) 0xc5ef, // (12, 1519) 120
      (short) 0xcdef, // (12, 3567) 121
      (short) 0xc3ef, // (12, 1007) 122
      (short) 0xcbef, // (12, 3055) 123
      (short) 0xc7ef, // (12, 2031) 124
      (short) 0xcfef, // (12, 4079) 125
      (short) 0xc01f, // (12,   31) 126
      (short) 0xc81f, // (12, 2079) 127
      (short) 0xa30f, // (10,  783) 128
      (short) 0xb18f, // (11,  399) 129
      (short) 0xc41f, // (12, 1055) 130
      (short) 0xcc1f, // (12, 3103) 131
      (short) 0xc21f, // (12,  543) 132
      (short) 0xca1f, // (12, 2591) 133
      (short) 0xc61f, // (12, 1567) 134
      (short) 0xce1f, // (12, 3615) 135
      (short) 0xc11f, // (12,  287) 136
      (short) 0xc91f, // (12, 2335) 137
      (short) 0xc51f, // (12, 1311) 138
      (short) 0xcd1f, // (12, 3359) 139
      (short) 0xc31f, // (12,  799) 140
      (short) 0xcb1f, // (12, 2847) 141
      (short) 0xc71f, // (12, 1823) 142
      (short) 0xcf1f, // (12, 3871) 143
      (short) 0xc09f, // (12,  159) 144
      (short) 0xc89f, // (12, 2207) 145
      (short) 0xc49f, // (12, 1183) 146
      (short) 0xcc9f, // (12, 3231) 147
      (short) 0xc29f, // (12,  671) 148
      (short) 0xca9f, // (12, 2719) 149
      (short) 0xc69f, // (12, 1695) 150
      (short) 0xce9f, // (12, 3743) 151
      (short) 0xc19f, // (12,  415) 152
      (short) 0xc99f, // (12, 2463) 153
      (short) 0xc59f, // (12, 1439) 154
      (short) 0xcd9f, // (12, 3487) 155
      (short) 0xc39f, // (12,  927) 156
      (short) 0xcb9f, // (12, 2975) 157
      (short) 0xc79f, // (12, 1951) 158
      (short) 0xcf9f, // (12, 3999) 159
      (short) 0xc05f, // (12,   95) 160
      (short) 0xc85f, // (12, 2143) 161
      (short) 0xc45f, // (12, 1119) 162
      (short) 0xcc5f, // (12, 3167) 163
      (short) 0xc25f, // (12,  607) 164
      (short) 0xca5f, // (12, 2655) 165
      (short) 0xc65f, // (12, 1631) 166
      (short) 0xce5f, // (12, 3679) 167
      (short) 0xc15f, // (12,  351) 168
      (short) 0xc95f, // (12, 2399) 169
      (short) 0xc55f, // (12, 1375) 170
      (short) 0xcd5f, // (12, 3423) 171
      (short) 0xc35f, // (12,  863) 172
      (short) 0xcb5f, // (12, 2911) 173
      (short) 0xc75f, // (12, 1887) 174
      (short) 0xcf5f, // (12, 3935) 175
      (short) 0xc0df, // (12,  223) 176
      (short) 0xc8df, // (12, 2271) 177
      (short) 0xc4df, // (12, 1247) 178
      (short) 0xccdf, // (12, 3295) 179
      (short) 0xc2df, // (12,  735) 180
      (short) 0xcadf, // (12, 2783) 181
      (short) 0xc6df, // (12, 1759) 182
      (short) 0xcedf, // (12, 3807) 183
      (short) 0xc1df, // (12,  479) 184
      (short) 0xc9df, // (12, 2527) 185
      (short) 0xc5df, // (12, 1503) 186
      (short) 0xcddf, // (12, 3551) 187
      (short) 0xc3df, // (12,  991) 188
      (short) 0xcbdf, // (12, 3039) 189
      (short) 0xc7df, // (12, 2015) 190
      (short) 0xcfdf, // (12, 4063) 191
      (short) 0xc03f, // (12,   63) 192
      (short) 0xc83f, // (12, 2111) 193
      (short) 0xc43f, // (12, 1087) 194
      (short) 0xcc3f, // (12, 3135) 195
      (short) 0xc23f, // (12,  575) 196
      (short) 0xca3f, // (12, 2623) 197
      (short) 0xc63f, // (12, 1599) 198
      (short) 0xce3f, // (12, 3647) 199
      (short) 0xc13f, // (12,  319) 200
      (short) 0xc93f, // (12, 2367) 201
      (short) 0xc53f, // (12, 1343) 202
      (short) 0xcd3f, // (12, 3391) 203
      (short) 0xc33f, // (12,  831) 204
      (short) 0xcb3f, // (12, 2879) 205
      (short) 0xc73f, // (12, 1855) 206
      (short) 0xcf3f, // (12, 3903) 207
      (short) 0xc0bf, // (12,  191) 208
      (short) 0xc8bf, // (12, 2239) 209
      (short) 0xc4bf, // (12, 1215) 210
      (short) 0xccbf, // (12, 3263) 211
      (short) 0xc2bf, // (12,  703) 212
      (short) 0xcabf, // (12, 2751) 213
      (short) 0xc6bf, // (12, 1727) 214
      (short) 0xcebf, // (12, 3775) 215
      (short) 0xc1bf, // (12,  447) 216
      (short) 0xc9bf, // (12, 2495) 217
      (short) 0xc5bf, // (12, 1471) 218
      (short) 0xcdbf, // (12, 3519) 219
      (short) 0xc3bf, // (12,  959) 220
      (short) 0xcbbf, // (12, 3007) 221
      (short) 0xc7bf, // (12, 1983) 222
      (short) 0xcfbf, // (12, 4031) 223
      (short) 0xc07f, // (12,  127) 224
      (short) 0xc87f, // (12, 2175) 225
      (short) 0xc47f, // (12, 1151) 226
      (short) 0xcc7f, // (12, 3199) 227
      (short) 0xc27f, // (12,  639) 228
      (short) 0xca7f, // (12, 2687) 229
      (short) 0xc67f, // (12, 1663) 230
      (short) 0xce7f, // (12, 3711) 231
      (short) 0xc17f, // (12,  383) 232
      (short) 0xc97f, // (12, 2431) 233
      (short) 0xc57f, // (12, 1407) 234
      (short) 0xcd7f, // (12, 3455) 235
      (short) 0xc37f, // (12,  895) 236
      (short) 0xcb7f, // (12, 2943) 237
      (short) 0xc77f, // (12, 1919) 238
      (short) 0xcf7f, // (12, 3967) 239
      (short) 0xc0ff, // (12,  255) 240
      (short) 0xc8ff, // (12, 2303) 241
      (short) 0xc4ff, // (12, 1279) 242
      (short) 0xccff, // (12, 3327) 243
      (short) 0xc2ff, // (12,  767) 244
      (short) 0xcaff, // (12, 2815) 245
      (short) 0xc6ff, // (12, 1791) 246
      (short) 0xceff, // (12, 3839) 247
      (short) 0xc1ff, // (12,  511) 248
      (short) 0xc9ff, // (12, 2559) 249
      (short) 0xc5ff, // (12, 1535) 250
      (short) 0xcdff, // (12, 3583) 251
      (short) 0xc3ff, // (12, 1023) 252
      (short) 0xcbff, // (12, 3071) 253
      (short) 0xc7ff, // (12, 2047) 254
      (short) 0xcfff  // (12, 4095) 255
    },

    // (table 17 of 22) (midrange 1 of 6) (c/k = 0.833333333 = 5.0 / 6.0)
    // entropy:    2.9553294756640680063
    // avg_length: 3.0766035704232641557; max_length = 12; num_symbols = 256
    {
      //table, // (4 bits,  12 bits) symbol
      //entry, // (length, codeword) [byte]
      (short) 0x2000, // ( 2,    0)   0
      (short) 0x2002, // ( 2,    2)   1
      (short) 0x3001, // ( 3,    1)   2
      (short) 0x4005, // ( 4,    5)   3
      (short) 0x400d, // ( 4,   13)   4
      (short) 0x5003, // ( 5,    3)   5
      (short) 0x600b, // ( 6,   11)   6
      (short) 0x602b, // ( 6,   43)   7
      (short) 0x5013, // ( 5,   19)   8
      (short) 0x601b, // ( 6,   27)   9
      (short) 0x7007, // ( 7,    7)  10
      (short) 0x7047, // ( 7,   71)  11
      (short) 0x8017, // ( 8,   23)  12
      (short) 0x90b7, // ( 9,  183)  13
      (short) 0xa1f7, // (10,  503)  14
      (short) 0xa3f7, // (10, 1015)  15
      (short) 0x603b, // ( 6,   59)  16
      (short) 0x7027, // ( 7,   39)  17
      (short) 0x8097, // ( 8,  151)  18
      (short) 0x8057, // ( 8,   87)  19
      (short) 0x91b7, // ( 9,  439)  20
      (short) 0xa00f, // (10,   15)  21
      (short) 0xb18f, // (11,  399)  22
      (short) 0xb58f, // (11, 1423)  23
      (short) 0xa20f, // (10,  527)  24
      (short) 0xb38f, // (11,  911)  25
      (short) 0xc54f, // (12, 1359)  26
      (short) 0xcd4f, // (12, 3407)  27
      (short) 0xc34f, // (12,  847)  28
      (short) 0xcb4f, // (12, 2895)  29
      (short) 0xc74f, // (12, 1871)  30
      (short) 0xcf4f, // (12, 3919)  31
      (short) 0x7067, // ( 7,  103)  32
      (short) 0x80d7, // ( 8,  215)  33
      (short) 0x9077, // ( 9,  119)  34
      (short) 0xa10f, // (10,  271)  35
      (short) 0xa30f, // (10,  783)  36
      (short) 0xb78f, // (11, 1935)  37
      (short) 0xc0cf, // (12,  207)  38
      (short) 0xc8cf, // (12, 2255)  39
      (short) 0xb04f, // (11,   79)  40
      (short) 0xc4cf, // (12, 1231)  41
      (short) 0xcccf, // (12, 3279)  42
      (short) 0xc2cf, // (12,  719)  43
      (short) 0xcacf, // (12, 2767)  44
      (short) 0xc6cf, // (12, 1743)  45
      (short) 0xcecf, // (12, 3791)  46
      (short) 0xc1cf, // (12,  463)  47
      (short) 0xc9cf, // (12, 2511)  48
      (short) 0xc5cf, // (12, 1487)  49
      (short) 0xcdcf, // (12, 3535)  50
      (short) 0xc3cf, // (12,  975)  51
      (short) 0xcbcf, // (12, 3023)  52
      (short) 0xc7cf, // (12, 1999)  53
      (short) 0xcfcf, // (12, 4047)  54
      (short) 0xc02f, // (12,   47)  55
      (short) 0xc82f, // (12, 2095)  56
      (short) 0xc42f, // (12, 1071)  57
      (short) 0xcc2f, // (12, 3119)  58
      (short) 0xc22f, // (12,  559)  59
      (short) 0xca2f, // (12, 2607)  60
      (short) 0xc62f, // (12, 1583)  61
      (short) 0xce2f, // (12, 3631)  62
      (short) 0xc12f, // (12,  303)  63
      (short) 0x8037, // ( 8,   55)  64
      (short) 0x9177, // ( 9,  375)  65
      (short) 0xa08f, // (10,  143)  66
      (short) 0xb44f, // (11, 1103)  67
      (short) 0xb24f, // (11,  591)  68
      (short) 0xc92f, // (12, 2351)  69
      (short) 0xc52f, // (12, 1327)  70
      (short) 0xcd2f, // (12, 3375)  71
      (short) 0xc32f, // (12,  815)  72
      (short) 0xcb2f, // (12, 2863)  73
      (short) 0xc72f, // (12, 1839)  74
      (short) 0xcf2f, // (12, 3887)  75
      (short) 0xc0af, // (12,  175)  76
      (short) 0xc8af, // (12, 2223)  77
      (short) 0xc4af, // (12, 1199)  78
      (short) 0xccaf, // (12, 3247)  79
      (short) 0xc2af, // (12,  687)  80
      (short) 0xcaaf, // (12, 2735)  81
      (short) 0xc6af, // (12, 1711)  82
      (short) 0xceaf, // (12, 3759)  83
      (short) 0xc1af, // (12,  431)  84
      (short) 0xc9af, // (12, 2479)  85
      (short) 0xc5af, // (12, 1455)  86
      (short) 0xcdaf, // (12, 3503)  87
      (short) 0xc3af, // (12,  943)  88
      (short) 0xcbaf, // (12, 2991)  89
      (short) 0xc7af, // (12, 1967)  90
      (short) 0xcfaf, // (12, 4015)  91
      (short) 0xc06f, // (12,  111)  92
      (short) 0xc86f, // (12, 2159)  93
      (short) 0xc46f, // (12, 1135)  94
      (short) 0xcc6f, // (12, 3183)  95
      (short) 0xc26f, // (12,  623)  96
      (short) 0xca6f, // (12, 2671)  97
      (short) 0xc66f, // (12, 1647)  98
      (short) 0xce6f, // (12, 3695)  99
      (short) 0xc16f, // (12,  367) 100
      (short) 0xc96f, // (12, 2415) 101
      (short) 0xc56f, // (12, 1391) 102
      (short) 0xcd6f, // (12, 3439) 103
      (short) 0xc36f, // (12,  879) 104
      (short) 0xcb6f, // (12, 2927) 105
      (short) 0xc76f, // (12, 1903) 106
      (short) 0xcf6f, // (12, 3951) 107
      (short) 0xc0ef, // (12,  239) 108
      (short) 0xc8ef, // (12, 2287) 109
      (short) 0xc4ef, // (12, 1263) 110
      (short) 0xccef, // (12, 3311) 111
      (short) 0xc2ef, // (12,  751) 112
      (short) 0xcaef, // (12, 2799) 113
      (short) 0xc6ef, // (12, 1775) 114
      (short) 0xceef, // (12, 3823) 115
      (short) 0xc1ef, // (12,  495) 116
      (short) 0xc9ef, // (12, 2543) 117
      (short) 0xc5ef, // (12, 1519) 118
      (short) 0xcdef, // (12, 3567) 119
      (short) 0xc3ef, // (12, 1007) 120
      (short) 0xcbef, // (12, 3055) 121
      (short) 0xc7ef, // (12, 2031) 122
      (short) 0xcfef, // (12, 4079) 123
      (short) 0xc01f, // (12,   31) 124
      (short) 0xc81f, // (12, 2079) 125
      (short) 0xc41f, // (12, 1055) 126
      (short) 0xcc1f, // (12, 3103) 127
      (short) 0x90f7, // ( 9,  247) 128
      (short) 0xa28f, // (10,  655) 129
      (short) 0xb64f, // (11, 1615) 130
      (short) 0xb14f, // (11,  335) 131
      (short) 0xc21f, // (12,  543) 132
      (short) 0xca1f, // (12, 2591) 133
      (short) 0xc61f, // (12, 1567) 134
      (short) 0xce1f, // (12, 3615) 135
      (short) 0xc11f, // (12,  287) 136
      (short) 0xc91f, // (12, 2335) 137
      (short) 0xc51f, // (12, 1311) 138
      (short) 0xcd1f, // (12, 3359) 139
      (short) 0xc31f, // (12,  799) 140
      (short) 0xcb1f, // (12, 2847) 141
      (short) 0xc71f, // (12, 1823) 142
      (short) 0xcf1f, // (12, 3871) 143
      (short) 0xc09f, // (12,  159) 144
      (short) 0xc89f, // (12, 2207) 145
      (short) 0xc49f, // (12, 1183) 146
      (short) 0xcc9f, // (12, 3231) 147
      (short) 0xc29f, // (12,  671) 148
      (short) 0xca9f, // (12, 2719) 149
      (short) 0xc69f, // (12, 1695) 150
      (short) 0xce9f, // (12, 3743) 151
      (short) 0xc19f, // (12,  415) 152
      (short) 0xc99f, // (12, 2463) 153
      (short) 0xc59f, // (12, 1439) 154
      (short) 0xcd9f, // (12, 3487) 155
      (short) 0xc39f, // (12,  927) 156
      (short) 0xcb9f, // (12, 2975) 157
      (short) 0xc79f, // (12, 1951) 158
      (short) 0xcf9f, // (12, 3999) 159
      (short) 0xc05f, // (12,   95) 160
      (short) 0xc85f, // (12, 2143) 161
      (short) 0xc45f, // (12, 1119) 162
      (short) 0xcc5f, // (12, 3167) 163
      (short) 0xc25f, // (12,  607) 164
      (short) 0xca5f, // (12, 2655) 165
      (short) 0xc65f, // (12, 1631) 166
      (short) 0xce5f, // (12, 3679) 167
      (short) 0xc15f, // (12,  351) 168
      (short) 0xc95f, // (12, 2399) 169
      (short) 0xc55f, // (12, 1375) 170
      (short) 0xcd5f, // (12, 3423) 171
      (short) 0xc35f, // (12,  863) 172
      (short) 0xcb5f, // (12, 2911) 173
      (short) 0xc75f, // (12, 1887) 174
      (short) 0xcf5f, // (12, 3935) 175
      (short) 0xc0df, // (12,  223) 176
      (short) 0xc8df, // (12, 2271) 177
      (short) 0xc4df, // (12, 1247) 178
      (short) 0xccdf, // (12, 3295) 179
      (short) 0xc2df, // (12,  735) 180
      (short) 0xcadf, // (12, 2783) 181
      (short) 0xc6df, // (12, 1759) 182
      (short) 0xcedf, // (12, 3807) 183
      (short) 0xc1df, // (12,  479) 184
      (short) 0xc9df, // (12, 2527) 185
      (short) 0xc5df, // (12, 1503) 186
      (short) 0xcddf, // (12, 3551) 187
      (short) 0xc3df, // (12,  991) 188
      (short) 0xcbdf, // (12, 3039) 189
      (short) 0xc7df, // (12, 2015) 190
      (short) 0xcfdf, // (12, 4063) 191
      (short) 0xc03f, // (12,   63) 192
      (short) 0xc83f, // (12, 2111) 193
      (short) 0xc43f, // (12, 1087) 194
      (short) 0xcc3f, // (12, 3135) 195
      (short) 0xc23f, // (12,  575) 196
      (short) 0xca3f, // (12, 2623) 197
      (short) 0xc63f, // (12, 1599) 198
      (short) 0xce3f, // (12, 3647) 199
      (short) 0xc13f, // (12,  319) 200
      (short) 0xc93f, // (12, 2367) 201
      (short) 0xc53f, // (12, 1343) 202
      (short) 0xcd3f, // (12, 3391) 203
      (short) 0xc33f, // (12,  831) 204
      (short) 0xcb3f, // (12, 2879) 205
      (short) 0xc73f, // (12, 1855) 206
      (short) 0xcf3f, // (12, 3903) 207
      (short) 0xc0bf, // (12,  191) 208
      (short) 0xc8bf, // (12, 2239) 209
      (short) 0xc4bf, // (12, 1215) 210
      (short) 0xccbf, // (12, 3263) 211
      (short) 0xc2bf, // (12,  703) 212
      (short) 0xcabf, // (12, 2751) 213
      (short) 0xc6bf, // (12, 1727) 214
      (short) 0xcebf, // (12, 3775) 215
      (short) 0xc1bf, // (12,  447) 216
      (short) 0xc9bf, // (12, 2495) 217
      (short) 0xc5bf, // (12, 1471) 218
      (short) 0xcdbf, // (12, 3519) 219
      (short) 0xc3bf, // (12,  959) 220
      (short) 0xcbbf, // (12, 3007) 221
      (short) 0xc7bf, // (12, 1983) 222
      (short) 0xcfbf, // (12, 4031) 223
      (short) 0xc07f, // (12,  127) 224
      (short) 0xc87f, // (12, 2175) 225
      (short) 0xc47f, // (12, 1151) 226
      (short) 0xcc7f, // (12, 3199) 227
      (short) 0xc27f, // (12,  639) 228
      (short) 0xca7f, // (12, 2687) 229
      (short) 0xc67f, // (12, 1663) 230
      (short) 0xce7f, // (12, 3711) 231
      (short) 0xc17f, // (12,  383) 232
      (short) 0xc97f, // (12, 2431) 233
      (short) 0xc57f, // (12, 1407) 234
      (short) 0xcd7f, // (12, 3455) 235
      (short) 0xc37f, // (12,  895) 236
      (short) 0xcb7f, // (12, 2943) 237
      (short) 0xc77f, // (12, 1919) 238
      (short) 0xcf7f, // (12, 3967) 239
      (short) 0xc0ff, // (12,  255) 240
      (short) 0xc8ff, // (12, 2303) 241
      (short) 0xc4ff, // (12, 1279) 242
      (short) 0xccff, // (12, 3327) 243
      (short) 0xc2ff, // (12,  767) 244
      (short) 0xcaff, // (12, 2815) 245
      (short) 0xc6ff, // (12, 1791) 246
      (short) 0xceff, // (12, 3839) 247
      (short) 0xc1ff, // (12,  511) 248
      (short) 0xc9ff, // (12, 2559) 249
      (short) 0xc5ff, // (12, 1535) 250
      (short) 0xcdff, // (12, 3583) 251
      (short) 0xc3ff, // (12, 1023) 252
      (short) 0xcbff, // (12, 3071) 253
      (short) 0xc7ff, // (12, 2047) 254
      (short) 0xcfff  // (12, 4095) 255
    },

    // (table 18 of 22) (midrange 2 of 6) (c/k = 1.166666667 = 7.0 / 6.0)
    // entropy:    3.5218672531711128215
    // avg_length: 3.6153551492375441967; max_length = 12; num_symbols = 256
    {
      //table, // (4 bits,  12 bits) symbol
      //entry, // (length, codeword) [byte]
      (short) 0x2000, // ( 2,    0)   0
      (short) 0x2002, // ( 2,    2)   1
      (short) 0x4005, // ( 4,    5)   2
      (short) 0x3001, // ( 3,    1)   3
      (short) 0x5003, // ( 5,    3)   4
      (short) 0x400d, // ( 4,   13)   5
      (short) 0x600b, // ( 6,   11)   6
      (short) 0x602b, // ( 6,   43)   7
      (short) 0x601b, // ( 6,   27)   8
      (short) 0x5013, // ( 5,   19)   9
      (short) 0x703b, // ( 7,   59)  10
      (short) 0x707b, // ( 7,  123)  11
      (short) 0x8067, // ( 8,  103)  12
      (short) 0x80e7, // ( 8,  231)  13
      (short) 0x90d7, // ( 9,  215)  14
      (short) 0x91d7, // ( 9,  471)  15
      (short) 0x7007, // ( 7,    7)  16
      (short) 0x7047, // ( 7,   71)  17
      (short) 0x8017, // ( 8,   23)  18
      (short) 0x8097, // ( 8,  151)  19
      (short) 0x9037, // ( 9,   55)  20
      (short) 0x9137, // ( 9,  311)  21
      (short) 0xa1f7, // (10,  503)  22
      (short) 0xa3f7, // (10, 1015)  23
      (short) 0xa00f, // (10,   15)  24
      (short) 0xa20f, // (10,  527)  25
      (short) 0xb38f, // (11,  911)  26
      (short) 0xb78f, // (11, 1935)  27
      (short) 0xc0cf, // (12,  207)  28
      (short) 0xc8cf, // (12, 2255)  29
      (short) 0xc4cf, // (12, 1231)  30
      (short) 0xcccf, // (12, 3279)  31
      (short) 0x8057, // ( 8,   87)  32
      (short) 0x7027, // ( 7,   39)  33
      (short) 0x90b7, // ( 9,  183)  34
      (short) 0x91b7, // ( 9,  439)  35
      (short) 0xa10f, // (10,  271)  36
      (short) 0xa30f, // (10,  783)  37
      (short) 0xb04f, // (11,   79)  38
      (short) 0xb44f, // (11, 1103)  39
      (short) 0xb24f, // (11,  591)  40
      (short) 0xb64f, // (11, 1615)  41
      (short) 0xc2cf, // (12,  719)  42
      (short) 0xcacf, // (12, 2767)  43
      (short) 0xc6cf, // (12, 1743)  44
      (short) 0xcecf, // (12, 3791)  45
      (short) 0xc1cf, // (12,  463)  46
      (short) 0xc9cf, // (12, 2511)  47
      (short) 0xc5cf, // (12, 1487)  48
      (short) 0xcdcf, // (12, 3535)  49
      (short) 0xc3cf, // (12,  975)  50
      (short) 0xcbcf, // (12, 3023)  51
      (short) 0xc7cf, // (12, 1999)  52
      (short) 0xcfcf, // (12, 4047)  53
      (short) 0xc02f, // (12,   47)  54
      (short) 0xc82f, // (12, 2095)  55
      (short) 0xc42f, // (12, 1071)  56
      (short) 0xcc2f, // (12, 3119)  57
      (short) 0xc22f, // (12,  559)  58
      (short) 0xca2f, // (12, 2607)  59
      (short) 0xc62f, // (12, 1583)  60
      (short) 0xce2f, // (12, 3631)  61
      (short) 0xc12f, // (12,  303)  62
      (short) 0xc92f, // (12, 2351)  63
      (short) 0x9077, // ( 9,  119)  64
      (short) 0x9177, // ( 9,  375)  65
      (short) 0xa08f, // (10,  143)  66
      (short) 0xa28f, // (10,  655)  67
      (short) 0xb14f, // (11,  335)  68
      (short) 0xb54f, // (11, 1359)  69
      (short) 0xc52f, // (12, 1327)  70
      (short) 0xcd2f, // (12, 3375)  71
      (short) 0xc32f, // (12,  815)  72
      (short) 0xcb2f, // (12, 2863)  73
      (short) 0xc72f, // (12, 1839)  74
      (short) 0xcf2f, // (12, 3887)  75
      (short) 0xc0af, // (12,  175)  76
      (short) 0xc8af, // (12, 2223)  77
      (short) 0xc4af, // (12, 1199)  78
      (short) 0xccaf, // (12, 3247)  79
      (short) 0xc2af, // (12,  687)  80
      (short) 0xcaaf, // (12, 2735)  81
      (short) 0xc6af, // (12, 1711)  82
      (short) 0xceaf, // (12, 3759)  83
      (short) 0xc1af, // (12,  431)  84
      (short) 0xc9af, // (12, 2479)  85
      (short) 0xc5af, // (12, 1455)  86
      (short) 0xcdaf, // (12, 3503)  87
      (short) 0xc3af, // (12,  943)  88
      (short) 0xcbaf, // (12, 2991)  89
      (short) 0xc7af, // (12, 1967)  90
      (short) 0xcfaf, // (12, 4015)  91
      (short) 0xc06f, // (12,  111)  92
      (short) 0xc86f, // (12, 2159)  93
      (short) 0xc46f, // (12, 1135)  94
      (short) 0xcc6f, // (12, 3183)  95
      (short) 0xc26f, // (12,  623)  96
      (short) 0xca6f, // (12, 2671)  97
      (short) 0xc66f, // (12, 1647)  98
      (short) 0xce6f, // (12, 3695)  99
      (short) 0xc16f, // (12,  367) 100
      (short) 0xc96f, // (12, 2415) 101
      (short) 0xc56f, // (12, 1391) 102
      (short) 0xcd6f, // (12, 3439) 103
      (short) 0xc36f, // (12,  879) 104
      (short) 0xcb6f, // (12, 2927) 105
      (short) 0xc76f, // (12, 1903) 106
      (short) 0xcf6f, // (12, 3951) 107
      (short) 0xc0ef, // (12,  239) 108
      (short) 0xc8ef, // (12, 2287) 109
      (short) 0xc4ef, // (12, 1263) 110
      (short) 0xccef, // (12, 3311) 111
      (short) 0xc2ef, // (12,  751) 112
      (short) 0xcaef, // (12, 2799) 113
      (short) 0xc6ef, // (12, 1775) 114
      (short) 0xceef, // (12, 3823) 115
      (short) 0xc1ef, // (12,  495) 116
      (short) 0xc9ef, // (12, 2543) 117
      (short) 0xc5ef, // (12, 1519) 118
      (short) 0xcdef, // (12, 3567) 119
      (short) 0xc3ef, // (12, 1007) 120
      (short) 0xcbef, // (12, 3055) 121
      (short) 0xc7ef, // (12, 2031) 122
      (short) 0xcfef, // (12, 4079) 123
      (short) 0xc01f, // (12,   31) 124
      (short) 0xc81f, // (12, 2079) 125
      (short) 0xc41f, // (12, 1055) 126
      (short) 0xcc1f, // (12, 3103) 127
      (short) 0xa18f, // (10,  399) 128
      (short) 0x90f7, // ( 9,  247) 129
      (short) 0xb34f, // (11,  847) 130
      (short) 0xb74f, // (11, 1871) 131
      (short) 0xc21f, // (12,  543) 132
      (short) 0xca1f, // (12, 2591) 133
      (short) 0xc61f, // (12, 1567) 134
      (short) 0xce1f, // (12, 3615) 135
      (short) 0xc11f, // (12,  287) 136
      (short) 0xc91f, // (12, 2335) 137
      (short) 0xc51f, // (12, 1311) 138
      (short) 0xcd1f, // (12, 3359) 139
      (short) 0xc31f, // (12,  799) 140
      (short) 0xcb1f, // (12, 2847) 141
      (short) 0xc71f, // (12, 1823) 142
      (short) 0xcf1f, // (12, 3871) 143
      (short) 0xc09f, // (12,  159) 144
      (short) 0xc89f, // (12, 2207) 145
      (short) 0xc49f, // (12, 1183) 146
      (short) 0xcc9f, // (12, 3231) 147
      (short) 0xc29f, // (12,  671) 148
      (short) 0xca9f, // (12, 2719) 149
      (short) 0xc69f, // (12, 1695) 150
      (short) 0xce9f, // (12, 3743) 151
      (short) 0xc19f, // (12,  415) 152
      (short) 0xc99f, // (12, 2463) 153
      (short) 0xc59f, // (12, 1439) 154
      (short) 0xcd9f, // (12, 3487) 155
      (short) 0xc39f, // (12,  927) 156
      (short) 0xcb9f, // (12, 2975) 157
      (short) 0xc79f, // (12, 1951) 158
      (short) 0xcf9f, // (12, 3999) 159
      (short) 0xc05f, // (12,   95) 160
      (short) 0xc85f, // (12, 2143) 161
      (short) 0xc45f, // (12, 1119) 162
      (short) 0xcc5f, // (12, 3167) 163
      (short) 0xc25f, // (12,  607) 164
      (short) 0xca5f, // (12, 2655) 165
      (short) 0xc65f, // (12, 1631) 166
      (short) 0xce5f, // (12, 3679) 167
      (short) 0xc15f, // (12,  351) 168
      (short) 0xc95f, // (12, 2399) 169
      (short) 0xc55f, // (12, 1375) 170
      (short) 0xcd5f, // (12, 3423) 171
      (short) 0xc35f, // (12,  863) 172
      (short) 0xcb5f, // (12, 2911) 173
      (short) 0xc75f, // (12, 1887) 174
      (short) 0xcf5f, // (12, 3935) 175
      (short) 0xc0df, // (12,  223) 176
      (short) 0xc8df, // (12, 2271) 177
      (short) 0xc4df, // (12, 1247) 178
      (short) 0xccdf, // (12, 3295) 179
      (short) 0xc2df, // (12,  735) 180
      (short) 0xcadf, // (12, 2783) 181
      (short) 0xc6df, // (12, 1759) 182
      (short) 0xcedf, // (12, 3807) 183
      (short) 0xc1df, // (12,  479) 184
      (short) 0xc9df, // (12, 2527) 185
      (short) 0xc5df, // (12, 1503) 186
      (short) 0xcddf, // (12, 3551) 187
      (short) 0xc3df, // (12,  991) 188
      (short) 0xcbdf, // (12, 3039) 189
      (short) 0xc7df, // (12, 2015) 190
      (short) 0xcfdf, // (12, 4063) 191
      (short) 0xc03f, // (12,   63) 192
      (short) 0xc83f, // (12, 2111) 193
      (short) 0xc43f, // (12, 1087) 194
      (short) 0xcc3f, // (12, 3135) 195
      (short) 0xc23f, // (12,  575) 196
      (short) 0xca3f, // (12, 2623) 197
      (short) 0xc63f, // (12, 1599) 198
      (short) 0xce3f, // (12, 3647) 199
      (short) 0xc13f, // (12,  319) 200
      (short) 0xc93f, // (12, 2367) 201
      (short) 0xc53f, // (12, 1343) 202
      (short) 0xcd3f, // (12, 3391) 203
      (short) 0xc33f, // (12,  831) 204
      (short) 0xcb3f, // (12, 2879) 205
      (short) 0xc73f, // (12, 1855) 206
      (short) 0xcf3f, // (12, 3903) 207
      (short) 0xc0bf, // (12,  191) 208
      (short) 0xc8bf, // (12, 2239) 209
      (short) 0xc4bf, // (12, 1215) 210
      (short) 0xccbf, // (12, 3263) 211
      (short) 0xc2bf, // (12,  703) 212
      (short) 0xcabf, // (12, 2751) 213
      (short) 0xc6bf, // (12, 1727) 214
      (short) 0xcebf, // (12, 3775) 215
      (short) 0xc1bf, // (12,  447) 216
      (short) 0xc9bf, // (12, 2495) 217
      (short) 0xc5bf, // (12, 1471) 218
      (short) 0xcdbf, // (12, 3519) 219
      (short) 0xc3bf, // (12,  959) 220
      (short) 0xcbbf, // (12, 3007) 221
      (short) 0xc7bf, // (12, 1983) 222
      (short) 0xcfbf, // (12, 4031) 223
      (short) 0xc07f, // (12,  127) 224
      (short) 0xc87f, // (12, 2175) 225
      (short) 0xc47f, // (12, 1151) 226
      (short) 0xcc7f, // (12, 3199) 227
      (short) 0xc27f, // (12,  639) 228
      (short) 0xca7f, // (12, 2687) 229
      (short) 0xc67f, // (12, 1663) 230
      (short) 0xce7f, // (12, 3711) 231
      (short) 0xc17f, // (12,  383) 232
      (short) 0xc97f, // (12, 2431) 233
      (short) 0xc57f, // (12, 1407) 234
      (short) 0xcd7f, // (12, 3455) 235
      (short) 0xc37f, // (12,  895) 236
      (short) 0xcb7f, // (12, 2943) 237
      (short) 0xc77f, // (12, 1919) 238
      (short) 0xcf7f, // (12, 3967) 239
      (short) 0xc0ff, // (12,  255) 240
      (short) 0xc8ff, // (12, 2303) 241
      (short) 0xc4ff, // (12, 1279) 242
      (short) 0xccff, // (12, 3327) 243
      (short) 0xc2ff, // (12,  767) 244
      (short) 0xcaff, // (12, 2815) 245
      (short) 0xc6ff, // (12, 1791) 246
      (short) 0xceff, // (12, 3839) 247
      (short) 0xc1ff, // (12,  511) 248
      (short) 0xc9ff, // (12, 2559) 249
      (short) 0xc5ff, // (12, 1535) 250
      (short) 0xcdff, // (12, 3583) 251
      (short) 0xc3ff, // (12, 1023) 252
      (short) 0xcbff, // (12, 3071) 253
      (short) 0xc7ff, // (12, 2047) 254
      (short) 0xcfff  // (12, 4095) 255
    },

    // (table 19 of 22) (midrange 3 of 6) (c/k = 1.500000000 = 9.0 / 6.0)
    // entropy:    3.9228873257934386842
    // avg_length: 3.9989687586992346269; max_length = 12; num_symbols = 256
    {
      //table, // (4 bits,  12 bits) symbol
      //entry, // (length, codeword) [byte]
      (short) 0x3002, // ( 3,    2)   0
      (short) 0x2000, // ( 2,    0)   1
      (short) 0x4001, // ( 4,    1)   2
      (short) 0x3006, // ( 3,    6)   3
      (short) 0x500d, // ( 5,   13)   4
      (short) 0x4009, // ( 4,    9)   5
      (short) 0x501d, // ( 5,   29)   6
      (short) 0x4005, // ( 4,    5)   7
      (short) 0x6013, // ( 6,   19)   8
      (short) 0x5003, // ( 5,    3)   9
      (short) 0x6033, // ( 6,   51)  10
      (short) 0x600b, // ( 6,   11)  11
      (short) 0x8027, // ( 8,   39)  12
      (short) 0x701b, // ( 7,   27)  13
      (short) 0x80a7, // ( 8,  167)  14
      (short) 0x705b, // ( 7,   91)  15
      (short) 0x703b, // ( 7,   59)  16
      (short) 0x602b, // ( 6,   43)  17
      (short) 0x707b, // ( 7,  123)  18
      (short) 0x7007, // ( 7,    7)  19
      (short) 0x90d7, // ( 9,  215)  20
      (short) 0x8067, // ( 8,  103)  21
      (short) 0x91d7, // ( 9,  471)  22
      (short) 0x80e7, // ( 8,  231)  23
      (short) 0xa1f7, // (10,  503)  24
      (short) 0x9037, // ( 9,   55)  25
      (short) 0xa3f7, // (10, 1015)  26
      (short) 0xa00f, // (10,   15)  27
      (short) 0xc5cf, // (12, 1487)  28
      (short) 0xb04f, // (11,   79)  29
      (short) 0xcdcf, // (12, 3535)  30
      (short) 0xb44f, // (11, 1103)  31
      (short) 0x8017, // ( 8,   23)  32
      (short) 0x7047, // ( 7,   71)  33
      (short) 0x9137, // ( 9,  311)  34
      (short) 0x8097, // ( 8,  151)  35
      (short) 0xa20f, // (10,  527)  36
      (short) 0x90b7, // ( 9,  183)  37
      (short) 0xa10f, // (10,  271)  38
      (short) 0x91b7, // ( 9,  439)  39
      (short) 0xb24f, // (11,  591)  40
      (short) 0xa30f, // (10,  783)  41
      (short) 0xb64f, // (11, 1615)  42
      (short) 0xb14f, // (11,  335)  43
      (short) 0xc3cf, // (12,  975)  44
      (short) 0xcbcf, // (12, 3023)  45
      (short) 0xc7cf, // (12, 1999)  46
      (short) 0xcfcf, // (12, 4047)  47
      (short) 0xc02f, // (12,   47)  48
      (short) 0xb54f, // (11, 1359)  49
      (short) 0xc82f, // (12, 2095)  50
      (short) 0xc42f, // (12, 1071)  51
      (short) 0xcc2f, // (12, 3119)  52
      (short) 0xc22f, // (12,  559)  53
      (short) 0xca2f, // (12, 2607)  54
      (short) 0xc62f, // (12, 1583)  55
      (short) 0xce2f, // (12, 3631)  56
      (short) 0xc12f, // (12,  303)  57
      (short) 0xc92f, // (12, 2351)  58
      (short) 0xc52f, // (12, 1327)  59
      (short) 0xcd2f, // (12, 3375)  60
      (short) 0xc32f, // (12,  815)  61
      (short) 0xcb2f, // (12, 2863)  62
      (short) 0xc72f, // (12, 1839)  63
      (short) 0x9077, // ( 9,  119)  64
      (short) 0x8057, // ( 8,   87)  65
      (short) 0xa08f, // (10,  143)  66
      (short) 0x9177, // ( 9,  375)  67
      (short) 0xb34f, // (11,  847)  68
      (short) 0xa28f, // (10,  655)  69
      (short) 0xb74f, // (11, 1871)  70
      (short) 0xb0cf, // (11,  207)  71
      (short) 0xcf2f, // (12, 3887)  72
      (short) 0xb4cf, // (11, 1231)  73
      (short) 0xc0af, // (12,  175)  74
      (short) 0xc8af, // (12, 2223)  75
      (short) 0xc4af, // (12, 1199)  76
      (short) 0xccaf, // (12, 3247)  77
      (short) 0xc2af, // (12,  687)  78
      (short) 0xcaaf, // (12, 2735)  79
      (short) 0xc6af, // (12, 1711)  80
      (short) 0xceaf, // (12, 3759)  81
      (short) 0xc1af, // (12,  431)  82
      (short) 0xc9af, // (12, 2479)  83
      (short) 0xc5af, // (12, 1455)  84
      (short) 0xcdaf, // (12, 3503)  85
      (short) 0xc3af, // (12,  943)  86
      (short) 0xcbaf, // (12, 2991)  87
      (short) 0xc7af, // (12, 1967)  88
      (short) 0xcfaf, // (12, 4015)  89
      (short) 0xc06f, // (12,  111)  90
      (short) 0xc86f, // (12, 2159)  91
      (short) 0xc46f, // (12, 1135)  92
      (short) 0xcc6f, // (12, 3183)  93
      (short) 0xc26f, // (12,  623)  94
      (short) 0xca6f, // (12, 2671)  95
      (short) 0xc66f, // (12, 1647)  96
      (short) 0xce6f, // (12, 3695)  97
      (short) 0xc16f, // (12,  367)  98
      (short) 0xc96f, // (12, 2415)  99
      (short) 0xc56f, // (12, 1391) 100
      (short) 0xcd6f, // (12, 3439) 101
      (short) 0xc36f, // (12,  879) 102
      (short) 0xcb6f, // (12, 2927) 103
      (short) 0xc76f, // (12, 1903) 104
      (short) 0xcf6f, // (12, 3951) 105
      (short) 0xc0ef, // (12,  239) 106
      (short) 0xc8ef, // (12, 2287) 107
      (short) 0xc4ef, // (12, 1263) 108
      (short) 0xccef, // (12, 3311) 109
      (short) 0xc2ef, // (12,  751) 110
      (short) 0xcaef, // (12, 2799) 111
      (short) 0xc6ef, // (12, 1775) 112
      (short) 0xceef, // (12, 3823) 113
      (short) 0xc1ef, // (12,  495) 114
      (short) 0xc9ef, // (12, 2543) 115
      (short) 0xc5ef, // (12, 1519) 116
      (short) 0xcdef, // (12, 3567) 117
      (short) 0xc3ef, // (12, 1007) 118
      (short) 0xcbef, // (12, 3055) 119
      (short) 0xc7ef, // (12, 2031) 120
      (short) 0xcfef, // (12, 4079) 121
      (short) 0xc01f, // (12,   31) 122
      (short) 0xc81f, // (12, 2079) 123
      (short) 0xc41f, // (12, 1055) 124
      (short) 0xcc1f, // (12, 3103) 125
      (short) 0xc21f, // (12,  543) 126
      (short) 0xca1f, // (12, 2591) 127
      (short) 0xa18f, // (10,  399) 128
      (short) 0x90f7, // ( 9,  247) 129
      (short) 0xb2cf, // (11,  719) 130
      (short) 0xa38f, // (10,  911) 131
      (short) 0xc61f, // (12, 1567) 132
      (short) 0xb6cf, // (11, 1743) 133
      (short) 0xce1f, // (12, 3615) 134
      (short) 0xb1cf, // (11,  463) 135
      (short) 0xc11f, // (12,  287) 136
      (short) 0xc91f, // (12, 2335) 137
      (short) 0xc51f, // (12, 1311) 138
      (short) 0xcd1f, // (12, 3359) 139
      (short) 0xc31f, // (12,  799) 140
      (short) 0xcb1f, // (12, 2847) 141
      (short) 0xc71f, // (12, 1823) 142
      (short) 0xcf1f, // (12, 3871) 143
      (short) 0xc09f, // (12,  159) 144
      (short) 0xc89f, // (12, 2207) 145
      (short) 0xc49f, // (12, 1183) 146
      (short) 0xcc9f, // (12, 3231) 147
      (short) 0xc29f, // (12,  671) 148
      (short) 0xca9f, // (12, 2719) 149
      (short) 0xc69f, // (12, 1695) 150
      (short) 0xce9f, // (12, 3743) 151
      (short) 0xc19f, // (12,  415) 152
      (short) 0xc99f, // (12, 2463) 153
      (short) 0xc59f, // (12, 1439) 154
      (short) 0xcd9f, // (12, 3487) 155
      (short) 0xc39f, // (12,  927) 156
      (short) 0xcb9f, // (12, 2975) 157
      (short) 0xc79f, // (12, 1951) 158
      (short) 0xcf9f, // (12, 3999) 159
      (short) 0xc05f, // (12,   95) 160
      (short) 0xc85f, // (12, 2143) 161
      (short) 0xc45f, // (12, 1119) 162
      (short) 0xcc5f, // (12, 3167) 163
      (short) 0xc25f, // (12,  607) 164
      (short) 0xca5f, // (12, 2655) 165
      (short) 0xc65f, // (12, 1631) 166
      (short) 0xce5f, // (12, 3679) 167
      (short) 0xc15f, // (12,  351) 168
      (short) 0xc95f, // (12, 2399) 169
      (short) 0xc55f, // (12, 1375) 170
      (short) 0xcd5f, // (12, 3423) 171
      (short) 0xc35f, // (12,  863) 172
      (short) 0xcb5f, // (12, 2911) 173
      (short) 0xc75f, // (12, 1887) 174
      (short) 0xcf5f, // (12, 3935) 175
      (short) 0xc0df, // (12,  223) 176
      (short) 0xc8df, // (12, 2271) 177
      (short) 0xc4df, // (12, 1247) 178
      (short) 0xccdf, // (12, 3295) 179
      (short) 0xc2df, // (12,  735) 180
      (short) 0xcadf, // (12, 2783) 181
      (short) 0xc6df, // (12, 1759) 182
      (short) 0xcedf, // (12, 3807) 183
      (short) 0xc1df, // (12,  479) 184
      (short) 0xc9df, // (12, 2527) 185
      (short) 0xc5df, // (12, 1503) 186
      (short) 0xcddf, // (12, 3551) 187
      (short) 0xc3df, // (12,  991) 188
      (short) 0xcbdf, // (12, 3039) 189
      (short) 0xc7df, // (12, 2015) 190
      (short) 0xcfdf, // (12, 4063) 191
      (short) 0xc03f, // (12,   63) 192
      (short) 0xc83f, // (12, 2111) 193
      (short) 0xc43f, // (12, 1087) 194
      (short) 0xcc3f, // (12, 3135) 195
      (short) 0xc23f, // (12,  575) 196
      (short) 0xca3f, // (12, 2623) 197
      (short) 0xc63f, // (12, 1599) 198
      (short) 0xce3f, // (12, 3647) 199
      (short) 0xc13f, // (12,  319) 200
      (short) 0xc93f, // (12, 2367) 201
      (short) 0xc53f, // (12, 1343) 202
      (short) 0xcd3f, // (12, 3391) 203
      (short) 0xc33f, // (12,  831) 204
      (short) 0xcb3f, // (12, 2879) 205
      (short) 0xc73f, // (12, 1855) 206
      (short) 0xcf3f, // (12, 3903) 207
      (short) 0xc0bf, // (12,  191) 208
      (short) 0xc8bf, // (12, 2239) 209
      (short) 0xc4bf, // (12, 1215) 210
      (short) 0xccbf, // (12, 3263) 211
      (short) 0xc2bf, // (12,  703) 212
      (short) 0xcabf, // (12, 2751) 213
      (short) 0xc6bf, // (12, 1727) 214
      (short) 0xcebf, // (12, 3775) 215
      (short) 0xc1bf, // (12,  447) 216
      (short) 0xc9bf, // (12, 2495) 217
      (short) 0xc5bf, // (12, 1471) 218
      (short) 0xcdbf, // (12, 3519) 219
      (short) 0xc3bf, // (12,  959) 220
      (short) 0xcbbf, // (12, 3007) 221
      (short) 0xc7bf, // (12, 1983) 222
      (short) 0xcfbf, // (12, 4031) 223
      (short) 0xc07f, // (12,  127) 224
      (short) 0xc87f, // (12, 2175) 225
      (short) 0xc47f, // (12, 1151) 226
      (short) 0xcc7f, // (12, 3199) 227
      (short) 0xc27f, // (12,  639) 228
      (short) 0xca7f, // (12, 2687) 229
      (short) 0xc67f, // (12, 1663) 230
      (short) 0xce7f, // (12, 3711) 231
      (short) 0xc17f, // (12,  383) 232
      (short) 0xc97f, // (12, 2431) 233
      (short) 0xc57f, // (12, 1407) 234
      (short) 0xcd7f, // (12, 3455) 235
      (short) 0xc37f, // (12,  895) 236
      (short) 0xcb7f, // (12, 2943) 237
      (short) 0xc77f, // (12, 1919) 238
      (short) 0xcf7f, // (12, 3967) 239
      (short) 0xc0ff, // (12,  255) 240
      (short) 0xc8ff, // (12, 2303) 241
      (short) 0xc4ff, // (12, 1279) 242
      (short) 0xccff, // (12, 3327) 243
      (short) 0xc2ff, // (12,  767) 244
      (short) 0xcaff, // (12, 2815) 245
      (short) 0xc6ff, // (12, 1791) 246
      (short) 0xceff, // (12, 3839) 247
      (short) 0xc1ff, // (12,  511) 248
      (short) 0xc9ff, // (12, 2559) 249
      (short) 0xc5ff, // (12, 1535) 250
      (short) 0xcdff, // (12, 3583) 251
      (short) 0xc3ff, // (12, 1023) 252
      (short) 0xcbff, // (12, 3071) 253
      (short) 0xc7ff, // (12, 2047) 254
      (short) 0xcfff  // (12, 4095) 255
    },

    // (table 20 of 22) (midrange 4 of 6) (c/k = 1.833333333 = 11.0 / 6.0)
    // entropy:    4.1937026483207340277
    // avg_length: 4.2809622975207295426; max_length = 12; num_symbols = 256
    {
      //table, // (4 bits,  12 bits) symbol
      //entry, // (length, codeword) [byte]
      (short) 0x4006, // ( 4,    6)   0
      (short) 0x2000, // ( 2,    0)   1
      (short) 0x400e, // ( 4,   14)   2
      (short) 0x3002, // ( 3,    2)   3
      (short) 0x5005, // ( 5,    5)   4
      (short) 0x4001, // ( 4,    1)   5
      (short) 0x5015, // ( 5,   21)   6
      (short) 0x4009, // ( 4,    9)   7
      (short) 0x6003, // ( 6,    3)   8
      (short) 0x500d, // ( 5,   13)   9
      (short) 0x6023, // ( 6,   35)  10
      (short) 0x501d, // ( 5,   29)  11
      (short) 0x8047, // ( 8,   71)  12
      (short) 0x6013, // ( 6,   19)  13
      (short) 0x80c7, // ( 8,  199)  14
      (short) 0x6033, // ( 6,   51)  15
      (short) 0x701b, // ( 7,   27)  16
      (short) 0x600b, // ( 6,   11)  17
      (short) 0x8027, // ( 8,   39)  18
      (short) 0x602b, // ( 6,   43)  19
      (short) 0x90d7, // ( 9,  215)  20
      (short) 0x705b, // ( 7,   91)  21
      (short) 0x91d7, // ( 9,  471)  22
      (short) 0x703b, // ( 7,   59)  23
      (short) 0xa1f7, // (10,  503)  24
      (short) 0x80a7, // ( 8,  167)  25
      (short) 0xa3f7, // (10, 1015)  26
      (short) 0x8067, // ( 8,  103)  27
      (short) 0xb24f, // (11,  591)  28
      (short) 0xa00f, // (10,   15)  29
      (short) 0xb64f, // (11, 1615)  30
      (short) 0xa20f, // (10,  527)  31
      (short) 0x9037, // ( 9,   55)  32
      (short) 0x707b, // ( 7,  123)  33
      (short) 0x9137, // ( 9,  311)  34
      (short) 0x7007, // ( 7,    7)  35
      (short) 0xa10f, // (10,  271)  36
      (short) 0x80e7, // ( 8,  231)  37
      (short) 0xa30f, // (10,  783)  38
      (short) 0x8017, // ( 8,   23)  39
      (short) 0xb14f, // (11,  335)  40
      (short) 0x90b7, // ( 9,  183)  41
      (short) 0xb54f, // (11, 1359)  42
      (short) 0xa08f, // (10,  143)  43
      (short) 0xc02f, // (12,   47)  44
      (short) 0xb34f, // (11,  847)  45
      (short) 0xc82f, // (12, 2095)  46
      (short) 0xb74f, // (11, 1871)  47
      (short) 0xc42f, // (12, 1071)  48
      (short) 0xb0cf, // (11,  207)  49
      (short) 0xcc2f, // (12, 3119)  50
      (short) 0xb4cf, // (11, 1231)  51
      (short) 0xc22f, // (12,  559)  52
      (short) 0xca2f, // (12, 2607)  53
      (short) 0xc62f, // (12, 1583)  54
      (short) 0xce2f, // (12, 3631)  55
      (short) 0xc12f, // (12,  303)  56
      (short) 0xc92f, // (12, 2351)  57
      (short) 0xc52f, // (12, 1327)  58
      (short) 0xcd2f, // (12, 3375)  59
      (short) 0xc32f, // (12,  815)  60
      (short) 0xcb2f, // (12, 2863)  61
      (short) 0xc72f, // (12, 1839)  62
      (short) 0xcf2f, // (12, 3887)  63
      (short) 0xa28f, // (10,  655)  64
      (short) 0x8097, // ( 8,  151)  65
      (short) 0xa18f, // (10,  399)  66
      (short) 0x8057, // ( 8,   87)  67
      (short) 0xb2cf, // (11,  719)  68
      (short) 0x91b7, // ( 9,  439)  69
      (short) 0xb6cf, // (11, 1743)  70
      (short) 0x9077, // ( 9,  119)  71
      (short) 0xc0af, // (12,  175)  72
      (short) 0xb1cf, // (11,  463)  73
      (short) 0xc8af, // (12, 2223)  74
      (short) 0xb5cf, // (11, 1487)  75
      (short) 0xc4af, // (12, 1199)  76
      (short) 0xccaf, // (12, 3247)  77
      (short) 0xc2af, // (12,  687)  78
      (short) 0xcaaf, // (12, 2735)  79
      (short) 0xc6af, // (12, 1711)  80
      (short) 0xceaf, // (12, 3759)  81
      (short) 0xc1af, // (12,  431)  82
      (short) 0xc9af, // (12, 2479)  83
      (short) 0xc5af, // (12, 1455)  84
      (short) 0xcdaf, // (12, 3503)  85
      (short) 0xc3af, // (12,  943)  86
      (short) 0xcbaf, // (12, 2991)  87
      (short) 0xc7af, // (12, 1967)  88
      (short) 0xcfaf, // (12, 4015)  89
      (short) 0xc06f, // (12,  111)  90
      (short) 0xc86f, // (12, 2159)  91
      (short) 0xc46f, // (12, 1135)  92
      (short) 0xcc6f, // (12, 3183)  93
      (short) 0xc26f, // (12,  623)  94
      (short) 0xca6f, // (12, 2671)  95
      (short) 0xc66f, // (12, 1647)  96
      (short) 0xce6f, // (12, 3695)  97
      (short) 0xc16f, // (12,  367)  98
      (short) 0xc96f, // (12, 2415)  99
      (short) 0xc56f, // (12, 1391) 100
      (short) 0xcd6f, // (12, 3439) 101
      (short) 0xc36f, // (12,  879) 102
      (short) 0xcb6f, // (12, 2927) 103
      (short) 0xc76f, // (12, 1903) 104
      (short) 0xcf6f, // (12, 3951) 105
      (short) 0xc0ef, // (12,  239) 106
      (short) 0xc8ef, // (12, 2287) 107
      (short) 0xc4ef, // (12, 1263) 108
      (short) 0xccef, // (12, 3311) 109
      (short) 0xc2ef, // (12,  751) 110
      (short) 0xcaef, // (12, 2799) 111
      (short) 0xc6ef, // (12, 1775) 112
      (short) 0xceef, // (12, 3823) 113
      (short) 0xc1ef, // (12,  495) 114
      (short) 0xc9ef, // (12, 2543) 115
      (short) 0xc5ef, // (12, 1519) 116
      (short) 0xcdef, // (12, 3567) 117
      (short) 0xc3ef, // (12, 1007) 118
      (short) 0xcbef, // (12, 3055) 119
      (short) 0xc7ef, // (12, 2031) 120
      (short) 0xcfef, // (12, 4079) 121
      (short) 0xc01f, // (12,   31) 122
      (short) 0xc81f, // (12, 2079) 123
      (short) 0xc41f, // (12, 1055) 124
      (short) 0xcc1f, // (12, 3103) 125
      (short) 0xc21f, // (12,  543) 126
      (short) 0xca1f, // (12, 2591) 127
      (short) 0xb3cf, // (11,  975) 128
      (short) 0x9177, // ( 9,  375) 129
      (short) 0xb7cf, // (11, 1999) 130
      (short) 0x90f7, // ( 9,  247) 131
      (short) 0xc61f, // (12, 1567) 132
      (short) 0xa38f, // (10,  911) 133
      (short) 0xce1f, // (12, 3615) 134
      (short) 0xa04f, // (10,   79) 135
      (short) 0xc11f, // (12,  287) 136
      (short) 0xc91f, // (12, 2335) 137
      (short) 0xc51f, // (12, 1311) 138
      (short) 0xcd1f, // (12, 3359) 139
      (short) 0xc31f, // (12,  799) 140
      (short) 0xcb1f, // (12, 2847) 141
      (short) 0xc71f, // (12, 1823) 142
      (short) 0xcf1f, // (12, 3871) 143
      (short) 0xc09f, // (12,  159) 144
      (short) 0xc89f, // (12, 2207) 145
      (short) 0xc49f, // (12, 1183) 146
      (short) 0xcc9f, // (12, 3231) 147
      (short) 0xc29f, // (12,  671) 148
      (short) 0xca9f, // (12, 2719) 149
      (short) 0xc69f, // (12, 1695) 150
      (short) 0xce9f, // (12, 3743) 151
      (short) 0xc19f, // (12,  415) 152
      (short) 0xc99f, // (12, 2463) 153
      (short) 0xc59f, // (12, 1439) 154
      (short) 0xcd9f, // (12, 3487) 155
      (short) 0xc39f, // (12,  927) 156
      (short) 0xcb9f, // (12, 2975) 157
      (short) 0xc79f, // (12, 1951) 158
      (short) 0xcf9f, // (12, 3999) 159
      (short) 0xc05f, // (12,   95) 160
      (short) 0xc85f, // (12, 2143) 161
      (short) 0xc45f, // (12, 1119) 162
      (short) 0xcc5f, // (12, 3167) 163
      (short) 0xc25f, // (12,  607) 164
      (short) 0xca5f, // (12, 2655) 165
      (short) 0xc65f, // (12, 1631) 166
      (short) 0xce5f, // (12, 3679) 167
      (short) 0xc15f, // (12,  351) 168
      (short) 0xc95f, // (12, 2399) 169
      (short) 0xc55f, // (12, 1375) 170
      (short) 0xcd5f, // (12, 3423) 171
      (short) 0xc35f, // (12,  863) 172
      (short) 0xcb5f, // (12, 2911) 173
      (short) 0xc75f, // (12, 1887) 174
      (short) 0xcf5f, // (12, 3935) 175
      (short) 0xc0df, // (12,  223) 176
      (short) 0xc8df, // (12, 2271) 177
      (short) 0xc4df, // (12, 1247) 178
      (short) 0xccdf, // (12, 3295) 179
      (short) 0xc2df, // (12,  735) 180
      (short) 0xcadf, // (12, 2783) 181
      (short) 0xc6df, // (12, 1759) 182
      (short) 0xcedf, // (12, 3807) 183
      (short) 0xc1df, // (12,  479) 184
      (short) 0xc9df, // (12, 2527) 185
      (short) 0xc5df, // (12, 1503) 186
      (short) 0xcddf, // (12, 3551) 187
      (short) 0xc3df, // (12,  991) 188
      (short) 0xcbdf, // (12, 3039) 189
      (short) 0xc7df, // (12, 2015) 190
      (short) 0xcfdf, // (12, 4063) 191
      (short) 0xc03f, // (12,   63) 192
      (short) 0xc83f, // (12, 2111) 193
      (short) 0xc43f, // (12, 1087) 194
      (short) 0xcc3f, // (12, 3135) 195
      (short) 0xc23f, // (12,  575) 196
      (short) 0xca3f, // (12, 2623) 197
      (short) 0xc63f, // (12, 1599) 198
      (short) 0xce3f, // (12, 3647) 199
      (short) 0xc13f, // (12,  319) 200
      (short) 0xc93f, // (12, 2367) 201
      (short) 0xc53f, // (12, 1343) 202
      (short) 0xcd3f, // (12, 3391) 203
      (short) 0xc33f, // (12,  831) 204
      (short) 0xcb3f, // (12, 2879) 205
      (short) 0xc73f, // (12, 1855) 206
      (short) 0xcf3f, // (12, 3903) 207
      (short) 0xc0bf, // (12,  191) 208
      (short) 0xc8bf, // (12, 2239) 209
      (short) 0xc4bf, // (12, 1215) 210
      (short) 0xccbf, // (12, 3263) 211
      (short) 0xc2bf, // (12,  703) 212
      (short) 0xcabf, // (12, 2751) 213
      (short) 0xc6bf, // (12, 1727) 214
      (short) 0xcebf, // (12, 3775) 215
      (short) 0xc1bf, // (12,  447) 216
      (short) 0xc9bf, // (12, 2495) 217
      (short) 0xc5bf, // (12, 1471) 218
      (short) 0xcdbf, // (12, 3519) 219
      (short) 0xc3bf, // (12,  959) 220
      (short) 0xcbbf, // (12, 3007) 221
      (short) 0xc7bf, // (12, 1983) 222
      (short) 0xcfbf, // (12, 4031) 223
      (short) 0xc07f, // (12,  127) 224
      (short) 0xc87f, // (12, 2175) 225
      (short) 0xc47f, // (12, 1151) 226
      (short) 0xcc7f, // (12, 3199) 227
      (short) 0xc27f, // (12,  639) 228
      (short) 0xca7f, // (12, 2687) 229
      (short) 0xc67f, // (12, 1663) 230
      (short) 0xce7f, // (12, 3711) 231
      (short) 0xc17f, // (12,  383) 232
      (short) 0xc97f, // (12, 2431) 233
      (short) 0xc57f, // (12, 1407) 234
      (short) 0xcd7f, // (12, 3455) 235
      (short) 0xc37f, // (12,  895) 236
      (short) 0xcb7f, // (12, 2943) 237
      (short) 0xc77f, // (12, 1919) 238
      (short) 0xcf7f, // (12, 3967) 239
      (short) 0xc0ff, // (12,  255) 240
      (short) 0xc8ff, // (12, 2303) 241
      (short) 0xc4ff, // (12, 1279) 242
      (short) 0xccff, // (12, 3327) 243
      (short) 0xc2ff, // (12,  767) 244
      (short) 0xcaff, // (12, 2815) 245
      (short) 0xc6ff, // (12, 1791) 246
      (short) 0xceff, // (12, 3839) 247
      (short) 0xc1ff, // (12,  511) 248
      (short) 0xc9ff, // (12, 2559) 249
      (short) 0xc5ff, // (12, 1535) 250
      (short) 0xcdff, // (12, 3583) 251
      (short) 0xc3ff, // (12, 1023) 252
      (short) 0xcbff, // (12, 3071) 253
      (short) 0xc7ff, // (12, 2047) 254
      (short) 0xcfff  // (12, 4095) 255
    },

    // (table 21 of 22) (midrange 5 of 6) (c/k = 2.166666667 = 13.0 / 6.0)
    // entropy:    4.3601926041863263706
    // avg_length: 4.4384101723259572481; max_length = 12; num_symbols = 256
    {
      //table, // (4 bits,  12 bits) symbol
      //entry, // (length, codeword) [byte]
      (short) 0x5009, // ( 5,    9)   0
      (short) 0x3002, // ( 3,    2)   1
      (short) 0x5019, // ( 5,   25)   2
      (short) 0x2000, // ( 2,    0)   3
      (short) 0x6003, // ( 6,    3)   4
      (short) 0x4001, // ( 4,    1)   5
      (short) 0x5005, // ( 5,    5)   6
      (short) 0x3006, // ( 3,    6)   7
      (short) 0x702b, // ( 7,   43)   8
      (short) 0x5015, // ( 5,   21)   9
      (short) 0x706b, // ( 7,  107)  10
      (short) 0x500d, // ( 5,   13)  11
      (short) 0x8007, // ( 8,    7)  12
      (short) 0x6023, // ( 6,   35)  13
      (short) 0x8087, // ( 8,  135)  14
      (short) 0x501d, // ( 5,   29)  15
      (short) 0x8047, // ( 8,   71)  16
      (short) 0x6013, // ( 6,   19)  17
      (short) 0x80c7, // ( 8,  199)  18
      (short) 0x6033, // ( 6,   51)  19
      (short) 0x9097, // ( 9,  151)  20
      (short) 0x701b, // ( 7,   27)  21
      (short) 0x9197, // ( 9,  407)  22
      (short) 0x600b, // ( 6,   11)  23
      (short) 0xa0f7, // (10,  247)  24
      (short) 0x8027, // ( 8,   39)  25
      (short) 0xa2f7, // (10,  759)  26
      (short) 0x80a7, // ( 8,  167)  27
      (short) 0xb14f, // (11,  335)  28
      (short) 0x9057, // ( 9,   87)  29
      (short) 0xb54f, // (11, 1359)  30
      (short) 0x9157, // ( 9,  343)  31
      (short) 0x90d7, // ( 9,  215)  32
      (short) 0x705b, // ( 7,   91)  33
      (short) 0x91d7, // ( 9,  471)  34
      (short) 0x703b, // ( 7,   59)  35
      (short) 0xa1f7, // (10,  503)  36
      (short) 0x8067, // ( 8,  103)  37
      (short) 0xa3f7, // (10, 1015)  38
      (short) 0x707b, // ( 7,  123)  39
      (short) 0xb34f, // (11,  847)  40
      (short) 0x9037, // ( 9,   55)  41
      (short) 0xb74f, // (11, 1871)  42
      (short) 0x9137, // ( 9,  311)  43
      (short) 0xc12f, // (12,  303)  44
      (short) 0xa00f, // (10,   15)  45
      (short) 0xc92f, // (12, 2351)  46
      (short) 0xa20f, // (10,  527)  47
      (short) 0xc52f, // (12, 1327)  48
      (short) 0xa10f, // (10,  271)  49
      (short) 0xcd2f, // (12, 3375)  50
      (short) 0xa30f, // (10,  783)  51
      (short) 0xc32f, // (12,  815)  52
      (short) 0xb0cf, // (11,  207)  53
      (short) 0xcb2f, // (12, 2863)  54
      (short) 0xb4cf, // (11, 1231)  55
      (short) 0xc72f, // (12, 1839)  56
      (short) 0xcf2f, // (12, 3887)  57
      (short) 0xc0af, // (12,  175)  58
      (short) 0xc8af, // (12, 2223)  59
      (short) 0xc4af, // (12, 1199)  60
      (short) 0xccaf, // (12, 3247)  61
      (short) 0xc2af, // (12,  687)  62
      (short) 0xcaaf, // (12, 2735)  63
      (short) 0xa08f, // (10,  143)  64
      (short) 0x80e7, // ( 8,  231)  65
      (short) 0xa28f, // (10,  655)  66
      (short) 0x8017, // ( 8,   23)  67
      (short) 0xb2cf, // (11,  719)  68
      (short) 0x90b7, // ( 9,  183)  69
      (short) 0xb6cf, // (11, 1743)  70
      (short) 0x91b7, // ( 9,  439)  71
      (short) 0xc6af, // (12, 1711)  72
      (short) 0xa18f, // (10,  399)  73
      (short) 0xceaf, // (12, 3759)  74
      (short) 0xa38f, // (10,  911)  75
      (short) 0xc1af, // (12,  431)  76
      (short) 0xb1cf, // (11,  463)  77
      (short) 0xc9af, // (12, 2479)  78
      (short) 0xb5cf, // (11, 1487)  79
      (short) 0xc5af, // (12, 1455)  80
      (short) 0xb3cf, // (11,  975)  81
      (short) 0xcdaf, // (12, 3503)  82
      (short) 0xb7cf, // (11, 1999)  83
      (short) 0xc3af, // (12,  943)  84
      (short) 0xcbaf, // (12, 2991)  85
      (short) 0xc7af, // (12, 1967)  86
      (short) 0xcfaf, // (12, 4015)  87
      (short) 0xc06f, // (12,  111)  88
      (short) 0xc86f, // (12, 2159)  89
      (short) 0xc46f, // (12, 1135)  90
      (short) 0xcc6f, // (12, 3183)  91
      (short) 0xc26f, // (12,  623)  92
      (short) 0xca6f, // (12, 2671)  93
      (short) 0xc66f, // (12, 1647)  94
      (short) 0xce6f, // (12, 3695)  95
      (short) 0xc16f, // (12,  367)  96
      (short) 0xc96f, // (12, 2415)  97
      (short) 0xc56f, // (12, 1391)  98
      (short) 0xcd6f, // (12, 3439)  99
      (short) 0xc36f, // (12,  879) 100
      (short) 0xcb6f, // (12, 2927) 101
      (short) 0xc76f, // (12, 1903) 102
      (short) 0xcf6f, // (12, 3951) 103
      (short) 0xc0ef, // (12,  239) 104
      (short) 0xc8ef, // (12, 2287) 105
      (short) 0xc4ef, // (12, 1263) 106
      (short) 0xccef, // (12, 3311) 107
      (short) 0xc2ef, // (12,  751) 108
      (short) 0xcaef, // (12, 2799) 109
      (short) 0xc6ef, // (12, 1775) 110
      (short) 0xceef, // (12, 3823) 111
      (short) 0xc1ef, // (12,  495) 112
      (short) 0xc9ef, // (12, 2543) 113
      (short) 0xc5ef, // (12, 1519) 114
      (short) 0xcdef, // (12, 3567) 115
      (short) 0xc3ef, // (12, 1007) 116
      (short) 0xcbef, // (12, 3055) 117
      (short) 0xc7ef, // (12, 2031) 118
      (short) 0xcfef, // (12, 4079) 119
      (short) 0xc01f, // (12,   31) 120
      (short) 0xc81f, // (12, 2079) 121
      (short) 0xc41f, // (12, 1055) 122
      (short) 0xcc1f, // (12, 3103) 123
      (short) 0xc21f, // (12,  543) 124
      (short) 0xca1f, // (12, 2591) 125
      (short) 0xc61f, // (12, 1567) 126
      (short) 0xce1f, // (12, 3615) 127
      (short) 0xb02f, // (11,   47) 128
      (short) 0x9077, // ( 9,  119) 129
      (short) 0xb42f, // (11, 1071) 130
      (short) 0x9177, // ( 9,  375) 131
      (short) 0xc11f, // (12,  287) 132
      (short) 0xa04f, // (10,   79) 133
      (short) 0xc91f, // (12, 2335) 134
      (short) 0xa24f, // (10,  591) 135
      (short) 0xc51f, // (12, 1311) 136
      (short) 0xb22f, // (11,  559) 137
      (short) 0xcd1f, // (12, 3359) 138
      (short) 0xb62f, // (11, 1583) 139
      (short) 0xc31f, // (12,  799) 140
      (short) 0xcb1f, // (12, 2847) 141
      (short) 0xc71f, // (12, 1823) 142
      (short) 0xcf1f, // (12, 3871) 143
      (short) 0xc09f, // (12,  159) 144
      (short) 0xc89f, // (12, 2207) 145
      (short) 0xc49f, // (12, 1183) 146
      (short) 0xcc9f, // (12, 3231) 147
      (short) 0xc29f, // (12,  671) 148
      (short) 0xca9f, // (12, 2719) 149
      (short) 0xc69f, // (12, 1695) 150
      (short) 0xce9f, // (12, 3743) 151
      (short) 0xc19f, // (12,  415) 152
      (short) 0xc99f, // (12, 2463) 153
      (short) 0xc59f, // (12, 1439) 154
      (short) 0xcd9f, // (12, 3487) 155
      (short) 0xc39f, // (12,  927) 156
      (short) 0xcb9f, // (12, 2975) 157
      (short) 0xc79f, // (12, 1951) 158
      (short) 0xcf9f, // (12, 3999) 159
      (short) 0xc05f, // (12,   95) 160
      (short) 0xc85f, // (12, 2143) 161
      (short) 0xc45f, // (12, 1119) 162
      (short) 0xcc5f, // (12, 3167) 163
      (short) 0xc25f, // (12,  607) 164
      (short) 0xca5f, // (12, 2655) 165
      (short) 0xc65f, // (12, 1631) 166
      (short) 0xce5f, // (12, 3679) 167
      (short) 0xc15f, // (12,  351) 168
      (short) 0xc95f, // (12, 2399) 169
      (short) 0xc55f, // (12, 1375) 170
      (short) 0xcd5f, // (12, 3423) 171
      (short) 0xc35f, // (12,  863) 172
      (short) 0xcb5f, // (12, 2911) 173
      (short) 0xc75f, // (12, 1887) 174
      (short) 0xcf5f, // (12, 3935) 175
      (short) 0xc0df, // (12,  223) 176
      (short) 0xc8df, // (12, 2271) 177
      (short) 0xc4df, // (12, 1247) 178
      (short) 0xccdf, // (12, 3295) 179
      (short) 0xc2df, // (12,  735) 180
      (short) 0xcadf, // (12, 2783) 181
      (short) 0xc6df, // (12, 1759) 182
      (short) 0xcedf, // (12, 3807) 183
      (short) 0xc1df, // (12,  479) 184
      (short) 0xc9df, // (12, 2527) 185
      (short) 0xc5df, // (12, 1503) 186
      (short) 0xcddf, // (12, 3551) 187
      (short) 0xc3df, // (12,  991) 188
      (short) 0xcbdf, // (12, 3039) 189
      (short) 0xc7df, // (12, 2015) 190
      (short) 0xcfdf, // (12, 4063) 191
      (short) 0xc03f, // (12,   63) 192
      (short) 0xc83f, // (12, 2111) 193
      (short) 0xc43f, // (12, 1087) 194
      (short) 0xcc3f, // (12, 3135) 195
      (short) 0xc23f, // (12,  575) 196
      (short) 0xca3f, // (12, 2623) 197
      (short) 0xc63f, // (12, 1599) 198
      (short) 0xce3f, // (12, 3647) 199
      (short) 0xc13f, // (12,  319) 200
      (short) 0xc93f, // (12, 2367) 201
      (short) 0xc53f, // (12, 1343) 202
      (short) 0xcd3f, // (12, 3391) 203
      (short) 0xc33f, // (12,  831) 204
      (short) 0xcb3f, // (12, 2879) 205
      (short) 0xc73f, // (12, 1855) 206
      (short) 0xcf3f, // (12, 3903) 207
      (short) 0xc0bf, // (12,  191) 208
      (short) 0xc8bf, // (12, 2239) 209
      (short) 0xc4bf, // (12, 1215) 210
      (short) 0xccbf, // (12, 3263) 211
      (short) 0xc2bf, // (12,  703) 212
      (short) 0xcabf, // (12, 2751) 213
      (short) 0xc6bf, // (12, 1727) 214
      (short) 0xcebf, // (12, 3775) 215
      (short) 0xc1bf, // (12,  447) 216
      (short) 0xc9bf, // (12, 2495) 217
      (short) 0xc5bf, // (12, 1471) 218
      (short) 0xcdbf, // (12, 3519) 219
      (short) 0xc3bf, // (12,  959) 220
      (short) 0xcbbf, // (12, 3007) 221
      (short) 0xc7bf, // (12, 1983) 222
      (short) 0xcfbf, // (12, 4031) 223
      (short) 0xc07f, // (12,  127) 224
      (short) 0xc87f, // (12, 2175) 225
      (short) 0xc47f, // (12, 1151) 226
      (short) 0xcc7f, // (12, 3199) 227
      (short) 0xc27f, // (12,  639) 228
      (short) 0xca7f, // (12, 2687) 229
      (short) 0xc67f, // (12, 1663) 230
      (short) 0xce7f, // (12, 3711) 231
      (short) 0xc17f, // (12,  383) 232
      (short) 0xc97f, // (12, 2431) 233
      (short) 0xc57f, // (12, 1407) 234
      (short) 0xcd7f, // (12, 3455) 235
      (short) 0xc37f, // (12,  895) 236
      (short) 0xcb7f, // (12, 2943) 237
      (short) 0xc77f, // (12, 1919) 238
      (short) 0xcf7f, // (12, 3967) 239
      (short) 0xc0ff, // (12,  255) 240
      (short) 0xc8ff, // (12, 2303) 241
      (short) 0xc4ff, // (12, 1279) 242
      (short) 0xccff, // (12, 3327) 243
      (short) 0xc2ff, // (12,  767) 244
      (short) 0xcaff, // (12, 2815) 245
      (short) 0xc6ff, // (12, 1791) 246
      (short) 0xceff, // (12, 3839) 247
      (short) 0xc1ff, // (12,  511) 248
      (short) 0xc9ff, // (12, 2559) 249
      (short) 0xc5ff, // (12, 1535) 250
      (short) 0xcdff, // (12, 3583) 251
      (short) 0xc3ff, // (12, 1023) 252
      (short) 0xcbff, // (12, 3071) 253
      (short) 0xc7ff, // (12, 2047) 254
      (short) 0xcfff  // (12, 4095) 255
    }
  };

  /**
   * Notice that there are only 65 symbols here, which is different from our
   * usual 8 to 12 coding scheme which handles 256 symbols.
   */
  static short[] lengthLimitedUnaryDecodingTable65 = null;

  static short[] lengthLimitedUnaryEncodingTable65 = new short[] //[65]
  {
    // Length-limited "unary" code with 65 symbols.
    // entropy:    2.0
    // avg_length: 2.0249023437500000000; max_length = 12; num_symbols = 65

    //table, (4 bits,  12 bits) symbol
    //entry, (length, codeword) [byte]
    (short) 0x1000, // ( 1,    0)   0
    (short) 0x2001, // ( 2,    1)   1
    (short) 0x3003, // ( 3,    3)   2
    (short) 0x4007, // ( 4,    7)   3
    (short) 0x500f, // ( 5,   15)   4
    (short) 0x701f, // ( 7,   31)   5
    (short) 0x805f, // ( 8,   95)   6
    (short) 0x80df, // ( 8,  223)   7
    (short) 0xa03f, // (10,   63)   8
    (short) 0xa23f, // (10,  575)   9
    (short) 0xb13f, // (11,  319)  10
    (short) 0xc53f, // (12, 1343)  11
    (short) 0xcd3f, // (12, 3391)  12
    (short) 0xc33f, // (12,  831)  13
    (short) 0xcb3f, // (12, 2879)  14
    (short) 0xc73f, // (12, 1855)  15
    (short) 0xcf3f, // (12, 3903)  16
    (short) 0xc0bf, // (12,  191)  17
    (short) 0xc8bf, // (12, 2239)  18
    (short) 0xc4bf, // (12, 1215)  19
    (short) 0xccbf, // (12, 3263)  20
    (short) 0xc2bf, // (12,  703)  21
    (short) 0xcabf, // (12, 2751)  22
    (short) 0xc6bf, // (12, 1727)  23
    (short) 0xcebf, // (12, 3775)  24
    (short) 0xc1bf, // (12,  447)  25
    (short) 0xc9bf, // (12, 2495)  26
    (short) 0xc5bf, // (12, 1471)  27
    (short) 0xcdbf, // (12, 3519)  28
    (short) 0xc3bf, // (12,  959)  29
    (short) 0xcbbf, // (12, 3007)  30
    (short) 0xc7bf, // (12, 1983)  31
    (short) 0xcfbf, // (12, 4031)  32
    (short) 0xc07f, // (12,  127)  33
    (short) 0xc87f, // (12, 2175)  34
    (short) 0xc47f, // (12, 1151)  35
    (short) 0xcc7f, // (12, 3199)  36
    (short) 0xc27f, // (12,  639)  37
    (short) 0xca7f, // (12, 2687)  38
    (short) 0xc67f, // (12, 1663)  39
    (short) 0xce7f, // (12, 3711)  40
    (short) 0xc17f, // (12,  383)  41
    (short) 0xc97f, // (12, 2431)  42
    (short) 0xc57f, // (12, 1407)  43
    (short) 0xcd7f, // (12, 3455)  44
    (short) 0xc37f, // (12,  895)  45
    (short) 0xcb7f, // (12, 2943)  46
    (short) 0xc77f, // (12, 1919)  47
    (short) 0xcf7f, // (12, 3967)  48
    (short) 0xc0ff, // (12,  255)  49
    (short) 0xc8ff, // (12, 2303)  50
    (short) 0xc4ff, // (12, 1279)  51
    (short) 0xccff, // (12, 3327)  52
    (short) 0xc2ff, // (12,  767)  53
    (short) 0xcaff, // (12, 2815)  54
    (short) 0xc6ff, // (12, 1791)  55
    (short) 0xceff, // (12, 3839)  56
    (short) 0xc1ff, // (12,  511)  57
    (short) 0xc9ff, // (12, 2559)  58
    (short) 0xc5ff, // (12, 1535)  59
    (short) 0xcdff, // (12, 3583)  60
    (short) 0xc3ff, // (12, 1023)  61
    (short) 0xcbff, // (12, 3071)  62
    (short) 0xc7ff, // (12, 2047)  63
    (short) 0xcfff  // (12, 4095)  64
  };

  /**
   * Note: these column permutations are part of the encoding scheme for sketches where
   * C &ge; 3.375 * K.
   * In each row, we identify the (0-based) column indices of all surprising bits
   * outside of the high-entropy byte.
   *
   * <p>These indices are "rotated right" via the formula
   * new = (old - (8+shift_by) + 64) mod 64 = (old + 56 - shift_by) mod 64.
   * resulting in canonicalized indices between 0 and 55 inclusive.
   *
   * <p>These are then mapped through the forwards permutation specified below (and selected
   * by the phase of C / K). Finally, the remapped indices are encoding with a unary code
   * (with delta encoding for rows containing more than one surprising bit).
   */
  static byte[][] columnPermutationsForDecoding = new byte[16][]; //[16][56]

  /**
   * These permutations were created by
   * the ocaml program "generatePermutationsForSLIDING.ml".
   */
  static final byte[][] columnPermutationsForEncoding = new byte[][] //[16] [56]
  {
    // for phase = 1 / 32
    {0, 1, 2, 3, 5, 6, 7, 8, 9, 10, 11, 12, 13, 15, 16, 17, 18, 19, 20, 21,
     22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 35, 36, 37, 38, 39, 40,
     41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 34, 14, 4},
    // for phase = 3 / 32
    {0, 1, 2, 3, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 16, 17, 18, 19, 20, 21,
     22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 36, 37, 38, 39, 40,
     41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 35, 15, 4},
    // for phase = 5 / 32
    {0, 1, 2, 3, 4, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 17, 18, 19, 20, 21,
     22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 38, 39, 40,
     41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 37, 16, 5},
    // for phase = 7 / 32
    {0, 1, 2, 3, 4, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 18, 19, 20, 21,
     22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 40,
     41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 39, 17, 5},
    // for phase = 9 / 32
    {0, 1, 2, 3, 4, 5, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 19, 20, 21,
     22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39,
     40, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 41, 18, 6},
    // for phase = 11 / 32
    {0, 1, 2, 3, 4, 5, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 20, 21,
     22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39,
     40, 41, 42, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 43, 19, 6},
    // for phase = 13 / 32
    {1, 2, 3, 4, 5, 6, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 21, 22,
     23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
     41, 42, 43, 44, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 45, 20, 7, 0},
    // for phase = 15 / 32
    {1, 2, 3, 4, 5, 6, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 22,
     23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
     41, 42, 43, 44, 45, 46, 48, 49, 50, 51, 52, 53, 54, 55, 47, 21, 7, 0},
    // for phase = 17 / 32
    {1, 2, 3, 4, 5, 6, 7, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
     23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
     41, 42, 43, 44, 45, 46, 47, 48, 49, 51, 52, 53, 54, 55, 50, 22, 8, 0},
    // for phase = 19 / 32
    {0, 2, 3, 4, 5, 6, 7, 8, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
     22, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
     41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 53, 54, 55, 52, 23, 9, 1},
    // for phase = 21 / 32
    {0, 2, 3, 4, 5, 6, 7, 8, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
     22, 23, 24, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
     41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 25, 9, 1},
    // for phase = 23 / 32
    {0, 2, 3, 4, 5, 6, 7, 8, 9, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
     22, 23, 24, 25, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
     41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 26, 10, 1},
    // for phase = 25 / 32
    {0, 1, 3, 4, 5, 6, 7, 8, 9, 10, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
     22, 23, 24, 25, 26, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
     41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 27, 11, 2},
    // for phase = 27 / 32
    {0, 1, 3, 4, 5, 6, 7, 8, 9, 10, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
     22, 23, 24, 25, 26, 27, 28, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
     41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 29, 11, 2},
    // for phase = 29 / 32
    {0, 1, 2, 4, 5, 6, 7, 8, 9, 10, 11, 13, 14, 15, 16, 17, 18, 19, 20, 21,
     22, 23, 24, 25, 26, 27, 28, 29, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
     41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 30, 12, 3},
    // for phase = 31 / 32
    {0, 1, 2, 4, 5, 6, 7, 8, 9, 10, 11, 12, 14, 15, 16, 17, 18, 19, 20, 21,
     22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 33, 34, 35, 36, 37, 38, 39, 40,
     41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 32, 13, 3}
  };

  //Initialize this class
  static {
    makeTheDecodingTables();
  }

}
