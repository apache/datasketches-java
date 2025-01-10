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

package org.apache.datasketches.theta;

import org.apache.datasketches.common.SketchesArgumentException;

/**
 * Used as part of Theta compression.
 */
public class BitPacking {

  /**
   * The bit packing operation
   * @param value the value to pack
   * @param bits number of bits to pack
   * @param buffer the output byte array buffer
   * @param bufOffset the byte offset in the buffer
   * @param bitOffset the bit offset
   */
  public static void packBits(final long value, int bits, final byte[] buffer, int bufOffset, final int bitOffset) {
    if (bitOffset > 0) {
      final int chunkBits = 8 - bitOffset;
      final int mask = (1 << chunkBits) - 1;
      if (bits < chunkBits) {
        buffer[bufOffset] |= (value << (chunkBits - bits)) & mask;
        return;
      }
      buffer[bufOffset++] |= (value >>> (bits - chunkBits)) & mask;
      bits -= chunkBits;
    }
    while (bits >= 8) {
      buffer[bufOffset++] = (byte)(value >>> (bits - 8));
      bits -= 8;
    }
    if (bits > 0) {
      buffer[bufOffset] = (byte)(value << (8 - bits));
    }
  }

  /**
   * The unpacking operation
   * @param value the output array
   * @param index index of the value array
   * @param bits the number of bits to unpack
   * @param buffer the input packed buffer
   * @param bufOffset the buffer offset
   * @param bitOffset the bit offset
   */
  public static void unpackBits(final long[] value, final int index, int bits, final byte[] buffer,
      int bufOffset,final int bitOffset) {
    final int availBits = 8 - bitOffset;
    final int chunkBits = availBits <= bits ? availBits : bits;
    final int mask = (1 << chunkBits) - 1;
    value[index] = (buffer[bufOffset] >>> (availBits - chunkBits)) & mask;
    bufOffset += availBits == chunkBits ? 1 : 0;
    bits -= chunkBits;
    while (bits >= 8) {
      value[index] <<= 8;
      value[index] |= (Byte.toUnsignedLong(buffer[bufOffset++]));
      bits -= 8;
    }
    if (bits > 0) {
      value[index] <<= bits;
      value[index] |= Byte.toUnsignedLong(buffer[bufOffset]) >>> (8 - bits);
    }
  }

  // pack given number of bits from a block of 8 64-bit values into bytes
  // we don't need 0 and 64 bits
  // we assume that higher bits (which we are not packing) are zeros
  // this assumption allows to avoid masking operations

  static void packBitsBlock8(final long[] values, final int i, final byte[] buf, final int off, final int bits) {
    switch (bits) {
      case 1: packBits1(values, i, buf, off); break;
      case 2: packBits2(values, i, buf, off); break;
      case 3: packBits3(values, i, buf, off); break;
      case 4: packBits4(values, i, buf, off); break;
      case 5: packBits5(values, i, buf, off); break;
      case 6: packBits6(values, i, buf, off); break;
      case 7: packBits7(values, i, buf, off); break;
      case 8: packBits8(values, i, buf, off); break;
      case 9: packBits9(values, i, buf, off); break;
      case 10: packBits10(values, i, buf, off); break;
      case 11: packBits11(values, i, buf, off); break;
      case 12: packBits12(values, i, buf, off); break;
      case 13: packBits13(values, i, buf, off); break;
      case 14: packBits14(values, i, buf, off); break;
      case 15: packBits15(values, i, buf, off); break;
      case 16: packBits16(values, i, buf, off); break;
      case 17: packBits17(values, i, buf, off); break;
      case 18: packBits18(values, i, buf, off); break;
      case 19: packBits19(values, i, buf, off); break;
      case 20: packBits20(values, i, buf, off); break;
      case 21: packBits21(values, i, buf, off); break;
      case 22: packBits22(values, i, buf, off); break;
      case 23: packBits23(values, i, buf, off); break;
      case 24: packBits24(values, i, buf, off); break;
      case 25: packBits25(values, i, buf, off); break;
      case 26: packBits26(values, i, buf, off); break;
      case 27: packBits27(values, i, buf, off); break;
      case 28: packBits28(values, i, buf, off); break;
      case 29: packBits29(values, i, buf, off); break;
      case 30: packBits30(values, i, buf, off); break;
      case 31: packBits31(values, i, buf, off); break;
      case 32: packBits32(values, i, buf, off); break;
      case 33: packBits33(values, i, buf, off); break;
      case 34: packBits34(values, i, buf, off); break;
      case 35: packBits35(values, i, buf, off); break;
      case 36: packBits36(values, i, buf, off); break;
      case 37: packBits37(values, i, buf, off); break;
      case 38: packBits38(values, i, buf, off); break;
      case 39: packBits39(values, i, buf, off); break;
      case 40: packBits40(values, i, buf, off); break;
      case 41: packBits41(values, i, buf, off); break;
      case 42: packBits42(values, i, buf, off); break;
      case 43: packBits43(values, i, buf, off); break;
      case 44: packBits44(values, i, buf, off); break;
      case 45: packBits45(values, i, buf, off); break;
      case 46: packBits46(values, i, buf, off); break;
      case 47: packBits47(values, i, buf, off); break;
      case 48: packBits48(values, i, buf, off); break;
      case 49: packBits49(values, i, buf, off); break;
      case 50: packBits50(values, i, buf, off); break;
      case 51: packBits51(values, i, buf, off); break;
      case 52: packBits52(values, i, buf, off); break;
      case 53: packBits53(values, i, buf, off); break;
      case 54: packBits54(values, i, buf, off); break;
      case 55: packBits55(values, i, buf, off); break;
      case 56: packBits56(values, i, buf, off); break;
      case 57: packBits57(values, i, buf, off); break;
      case 58: packBits58(values, i, buf, off); break;
      case 59: packBits59(values, i, buf, off); break;
      case 60: packBits60(values, i, buf, off); break;
      case 61: packBits61(values, i, buf, off); break;
      case 62: packBits62(values, i, buf, off); break;
      case 63: packBits63(values, i, buf, off); break;
      default: throw new SketchesArgumentException("wrong number of bits in packBitsBlock8: " + bits);
    }
  }

  static void unpackBitsBlock8(final long[] values, final int i, final byte[] buf, final int off, final int bits) {
    switch (bits) {
      case 1: unpackBits1(values, i, buf, off); break;
      case 2: unpackBits2(values, i, buf, off); break;
      case 3: unpackBits3(values, i, buf, off); break;
      case 4: unpackBits4(values, i, buf, off); break;
      case 5: unpackBits5(values, i, buf, off); break;
      case 6: unpackBits6(values, i, buf, off); break;
      case 7: unpackBits7(values, i, buf, off); break;
      case 8: unpackBits8(values, i, buf, off); break;
      case 9: unpackBits9(values, i, buf, off); break;
      case 10: unpackBits10(values, i, buf, off); break;
      case 11: unpackBits11(values, i, buf, off); break;
      case 12: unpackBits12(values, i, buf, off); break;
      case 13: unpackBits13(values, i, buf, off); break;
      case 14: unpackBits14(values, i, buf, off); break;
      case 15: unpackBits15(values, i, buf, off); break;
      case 16: unpackBits16(values, i, buf, off); break;
      case 17: unpackBits17(values, i, buf, off); break;
      case 18: unpackBits18(values, i, buf, off); break;
      case 19: unpackBits19(values, i, buf, off); break;
      case 20: unpackBits20(values, i, buf, off); break;
      case 21: unpackBits21(values, i, buf, off); break;
      case 22: unpackBits22(values, i, buf, off); break;
      case 23: unpackBits23(values, i, buf, off); break;
      case 24: unpackBits24(values, i, buf, off); break;
      case 25: unpackBits25(values, i, buf, off); break;
      case 26: unpackBits26(values, i, buf, off); break;
      case 27: unpackBits27(values, i, buf, off); break;
      case 28: unpackBits28(values, i, buf, off); break;
      case 29: unpackBits29(values, i, buf, off); break;
      case 30: unpackBits30(values, i, buf, off); break;
      case 31: unpackBits31(values, i, buf, off); break;
      case 32: unpackBits32(values, i, buf, off); break;
      case 33: unpackBits33(values, i, buf, off); break;
      case 34: unpackBits34(values, i, buf, off); break;
      case 35: unpackBits35(values, i, buf, off); break;
      case 36: unpackBits36(values, i, buf, off); break;
      case 37: unpackBits37(values, i, buf, off); break;
      case 38: unpackBits38(values, i, buf, off); break;
      case 39: unpackBits39(values, i, buf, off); break;
      case 40: unpackBits40(values, i, buf, off); break;
      case 41: unpackBits41(values, i, buf, off); break;
      case 42: unpackBits42(values, i, buf, off); break;
      case 43: unpackBits43(values, i, buf, off); break;
      case 44: unpackBits44(values, i, buf, off); break;
      case 45: unpackBits45(values, i, buf, off); break;
      case 46: unpackBits46(values, i, buf, off); break;
      case 47: unpackBits47(values, i, buf, off); break;
      case 48: unpackBits48(values, i, buf, off); break;
      case 49: unpackBits49(values, i, buf, off); break;
      case 50: unpackBits50(values, i, buf, off); break;
      case 51: unpackBits51(values, i, buf, off); break;
      case 52: unpackBits52(values, i, buf, off); break;
      case 53: unpackBits53(values, i, buf, off); break;
      case 54: unpackBits54(values, i, buf, off); break;
      case 55: unpackBits55(values, i, buf, off); break;
      case 56: unpackBits56(values, i, buf, off); break;
      case 57: unpackBits57(values, i, buf, off); break;
      case 58: unpackBits58(values, i, buf, off); break;
      case 59: unpackBits59(values, i, buf, off); break;
      case 60: unpackBits60(values, i, buf, off); break;
      case 61: unpackBits61(values, i, buf, off); break;
      case 62: unpackBits62(values, i, buf, off); break;
      case 63: unpackBits63(values, i, buf, off); break;
      default: throw new SketchesArgumentException("wrong number of bits unpackBitsBlock8: " + bits);
    }
  }

  static void packBits1(final long[] values, final int i, final byte[] buf, final int off) {
    buf[off] = (byte) (values[i + 0] << 7);
    buf[off] |= values[i + 1] << 6;
    buf[off] |= values[i + 2] << 5;
    buf[off] |= values[i + 3] << 4;
    buf[off] |= values[i + 4] << 3;
    buf[off] |= values[i + 5] << 2;
    buf[off] |= values[i + 6] << 1;
    buf[off] |= values[i + 7];
  }

  static void packBits2(final long[] values, final int i, final byte[] buf, int off) {
    buf[off] = (byte) (values[i + 0] << 6);
    buf[off] |= values[i + 1] << 4;
    buf[off] |= values[i + 2] << 2;
    buf[off++] |= values[i + 3];

    buf[off] = (byte) (values[i + 4] << 6);
    buf[off] |= values[i + 5] << 4;
    buf[off] |= values[i + 6] << 2;
    buf[off] |= values[i + 7];
  }

  static void packBits3(final long[] values, final int i, final byte[] buf, int off) {
    buf[off] = (byte) (values[i + 0] << 5);
    buf[off] |= values[i + 1] << 2;
    buf[off++] |= values[i + 2] >>> 1;

    buf[off] = (byte) (values[i + 2] << 7);
    buf[off] |= values[i + 3] << 4;
    buf[off] |= values[i + 4] << 1;
    buf[off++] |= values[i + 5] >>> 2;

    buf[off] = (byte) (values[i + 5] << 6);
    buf[off] |= values[i + 6] << 3;
    buf[off] |= values[i + 7];
  }

  static void packBits4(final long[] values, final int i, final byte[] buf, int off) {
    buf[off] = (byte) (values[i + 0] << 4);
    buf[off++] |= values[i + 1];

    buf[off] = (byte) (values[i + 2] << 4);
    buf[off++] |= values[i + 3];

    buf[off] = (byte) (values[i + 4] << 4);
    buf[off++] |= values[i + 5];

    buf[off] = (byte) (values[i + 6] << 4);
    buf[off] |= values[i + 7];
  }

  static void packBits5(final long[] values, final int i, final byte[] buf, int off) {
    buf[off] = (byte) (values[i + 0] << 3);
    buf[off++] |= values[i + 1] >>> 2;

    buf[off] = (byte) (values[i + 1] << 6);
    buf[off] |= values[i + 2] << 1;
    buf[off++] |= values[i + 3] >>> 4;

    buf[off] = (byte) (values[i + 3] << 4);
    buf[off++] |= values[i + 4] >>> 1;

    buf[off] = (byte) (values[i + 4] << 7);
    buf[off] |= values[i + 5] << 2;
    buf[off++] |= values[i + 6] >>> 3;

    buf[off] = (byte) (values[i + 6] << 5);
    buf[off] |= values[i + 7];
  }

  static void packBits6(final long[] values, final int i, final byte[] buf, int off) {
    buf[off] = (byte) (values[i + 0] << 2);
    buf[off++] |= values[i + 1] >>> 4;

    buf[off] = (byte) (values[i + 1] << 4);
    buf[off++] |= values[i + 2] >>> 2;

    buf[off] = (byte) (values[i + 2] << 6);
    buf[off++] |= values[i + 3];

    buf[off] = (byte) (values[i + 4] << 2);
    buf[off++] |= values[i + 5] >>> 4;

    buf[off] = (byte) (values[i + 5] << 4);
    buf[off++] |= values[i + 6] >>> 2;

    buf[off] = (byte) (values[i + 6] << 6);
    buf[off] |= values[i + 7];
  }

  static void packBits7(final long[] values, final int i, final byte[] buf, int off) {
    buf[off] = (byte) (values[i + 0] << 1);
    buf[off++] |= values[i + 1] >>> 6;

    buf[off] = (byte) (values[i + 1] << 2);
    buf[off++] |= values[i + 2] >>> 5;

    buf[off] = (byte) (values[i + 2] << 3);
    buf[off++] |= values[i + 3] >>> 4;

    buf[off] = (byte) (values[i + 3] << 4);
    buf[off++] |= values[i + 4] >>> 3;

    buf[off] = (byte) (values[i + 4] << 5);
    buf[off++] |= values[i + 5] >>> 2;

    buf[off] = (byte) (values[i + 5] << 6);
    buf[off++] |= values[i + 6] >>> 1;

    buf[off] = (byte) (values[i + 6] << 7);
    buf[off] |= values[i + 7];
  }

  static void packBits8(final long[] values, final int i, final byte[] buf, int off) {
    buf[off++] = (byte) (values[i + 0]);
    buf[off++] = (byte) (values[i + 1]);
    buf[off++] = (byte) (values[i + 2]);
    buf[off++] = (byte) (values[i + 3]);
    buf[off++] = (byte) (values[i + 4]);
    buf[off++] = (byte) (values[i + 5]);
    buf[off++] = (byte) (values[i + 6]);
    buf[off] = (byte) (values[i + 7]);
  }

  static void packBits9(final long[] values, final int i, final byte[] buf, int off) {
    buf[off++] = (byte) (values[i + 0] >>> 1);

    buf[off] = (byte) (values[i + 0] << 7);
    buf[off++] |= values[i + 1] >>> 2;

    buf[off] = (byte) (values[i + 1] << 6);
    buf[off++] |= values[i + 2] >>> 3;

    buf[off] = (byte) (values[i + 2] << 5);
    buf[off++] |= values[i + 3] >>> 4;

    buf[off] = (byte) (values[i + 3] << 4);
    buf[off++] |= values[i + 4] >>> 5;

    buf[off] = (byte) (values[i + 4] << 3);
    buf[off++] |= values[i + 5] >>> 6;

    buf[off] = (byte) (values[i + 5] << 2);
    buf[off++] |= values[i + 6] >>> 7;

    buf[off] = (byte) (values[i + 6] << 1);
    buf[off++] |= values[i + 7] >>> 8;

    buf[off] = (byte) (values[i + 7]);
  }

  static void packBits10(final long[] values, final int i, final byte[] buf, int off) {
    buf[off++] = (byte) (values[i + 0] >>> 2);

    buf[off] = (byte) (values[i + 0] << 6);
    buf[off++] |= values[i + 1] >>> 4;

    buf[off] = (byte) (values[i + 1] << 4);
    buf[off++] |= values[i + 2] >>> 6;

    buf[off] = (byte) (values[i + 2] << 2);
    buf[off++] |= values[i + 3] >>> 8;

    buf[off++] = (byte) (values[i + 3]);

    buf[off++] = (byte) (values[i + 4] >>> 2);

    buf[off] = (byte) (values[i + 4] << 6);
    buf[off++] |= values[i + 5] >>> 4;

    buf[off] = (byte) (values[i + 5] << 4);
    buf[off++] |= values[i + 6] >>> 6;

    buf[off] = (byte) (values[i + 6] << 2);
    buf[off++] |= values[i + 7] >>> 8;

    buf[off] = (byte) (values[i + 7]);
  }

  static void packBits11(final long[] values, final int i, final byte[] buf, int off) {
    buf[off++] = (byte) (values[i + 0] >>> 3);

    buf[off] = (byte) (values[i + 0] << 5);
    buf[off++] |= values[i + 1] >>> 6;

    buf[off] = (byte) (values[i + 1] << 2);
    buf[off++] |= values[i + 2] >>> 9;

    buf[off++] = (byte) (values[i + 2] >>> 1);

    buf[off] = (byte) (values[i + 2] << 7);
    buf[off++] |= values[i + 3] >>> 4;

    buf[off] = (byte) (values[i + 3] << 4);
    buf[off++] |= values[i + 4] >>> 7;

    buf[off] = (byte) (values[i + 4] << 1);
    buf[off++] |= values[i + 5] >>> 10;

    buf[off++] = (byte) (values[i + 5] >>> 2);

    buf[off] = (byte) (values[i + 5] << 6);
    buf[off++] |= values[i + 6] >>> 5;

    buf[off] = (byte) (values[i + 6] << 3);
    buf[off++] |= values[i + 7] >>> 8;

    buf[off] = (byte) (values[i + 7]);
  }

  static void packBits12(final long[] values, final int i, final byte[] buf, int off) {
    buf[off++] = (byte) (values[i + 0] >>> 4);

    buf[off] = (byte) (values[i + 0] << 4);
    buf[off++] |= values[i + 1] >>> 8;

    buf[off++] = (byte) (values[i + 1]);

    buf[off++] = (byte) (values[i + 2] >>> 4);

    buf[off] = (byte) (values[i + 2] << 4);
    buf[off++] |= values[i + 3] >>> 8;

    buf[off++] = (byte) (values[i + 3]);

    buf[off++] = (byte) (values[i + 4] >>> 4);

    buf[off] = (byte) (values[i + 4] << 4);
    buf[off++] |= values[i + 5] >>> 8;

    buf[off++] = (byte) (values[i + 5]);

    buf[off++] = (byte) (values[i + 6] >>> 4);

    buf[off] = (byte) (values[i + 6] << 4);
    buf[off++] |= values[i + 7] >>> 8;

    buf[off] = (byte) (values[i + 7]);
  }

  static void packBits13(final long[] values, final int i, final byte[] buf, int off) {
    buf[off++] = (byte) (values[i + 0] >>> 5);

    buf[off] = (byte) (values[i + 0] << 3);
    buf[off++] |= values[i + 1] >>> 10;

    buf[off++] = (byte) (values[i + 1] >>> 2);

    buf[off] = (byte) (values[i + 1] << 6);
    buf[off++] |= values[i + 2] >>> 7;

    buf[off] = (byte) (values[i + 2] << 1);
    buf[off++] |= values[i + 3] >>> 12;

    buf[off++] = (byte) (values[i + 3] >>> 4);

    buf[off] = (byte) (values[i + 3] << 4);
    buf[off++] |= values[i + 4] >>> 9;

    buf[off++] = (byte) (values[i + 4] >>> 1);

    buf[off] = (byte) (values[i + 4] << 7);
    buf[off++] |= values[i + 5] >>> 6;

    buf[off] = (byte) (values[i + 5] << 2);
    buf[off++] |= values[i + 6] >>> 11;

    buf[off++] = (byte) (values[i + 6] >>> 3);

    buf[off] = (byte) (values[i + 6] << 5);
    buf[off++] |= values[i + 7] >>> 8;

    buf[off] = (byte) (values[i + 7]);
  }

  static void packBits14(final long[] values, final int i, final byte[] buf, int off) {
    buf[off++] = (byte) (values[i + 0] >>> 6);

    buf[off] = (byte) (values[i + 0] << 2);
    buf[off++] |= values[i + 1] >>> 12;

    buf[off++] = (byte) (values[i + 1] >>> 4);

    buf[off] = (byte) (values[i + 1] << 4);
    buf[off++] |= values[i + 2] >>> 10;

    buf[off++] = (byte) (values[i + 2] >>> 2);

    buf[off] = (byte) (values[i + 2] << 6);
    buf[off++] |= values[i + 3] >>> 8;

    buf[off++] = (byte) (values[i + 3]);

    buf[off++] = (byte) (values[i + 4] >>> 6);

    buf[off] = (byte) (values[i + 4] << 2);
    buf[off++] |= values[i + 5] >>> 12;

    buf[off++] = (byte) (values[i + 5] >>> 4);

    buf[off] = (byte) (values[i + 5] << 4);
    buf[off++] |= values[i + 6] >>> 10;

    buf[off++] = (byte) (values[i + 6] >>> 2);

    buf[off] = (byte) (values[i + 6] << 6);
    buf[off++] |= values[i + 7] >>> 8;

    buf[off] = (byte) (values[i + 7]);
  }

  static void packBits15(final long[] values, final int i, final byte[] buf, int off) {
    buf[off++] = (byte) (values[i + 0] >>> 7);

    buf[off] = (byte) (values[i + 0] << 1);
    buf[off++] |= values[i + 1] >>> 14;

    buf[off++] = (byte) (values[i + 1] >>> 6);

    buf[off] = (byte) (values[i + 1] << 2);
    buf[off++] |= values[i + 2] >>> 13;

    buf[off++] = (byte) (values[i + 2] >>> 5);

    buf[off] = (byte) (values[i + 2] << 3);
    buf[off++] |= values[i + 3] >>> 12;

    buf[off++] = (byte) (values[i + 3] >>> 4);

    buf[off] = (byte) (values[i + 3] << 4);
    buf[off++] |= values[i + 4] >>> 11;

    buf[off++] = (byte) (values[i + 4] >>> 3);

    buf[off] = (byte) (values[i + 4] << 5);
    buf[off++] |= values[i + 5] >>> 10;

    buf[off++] = (byte) (values[i + 5] >>> 2);

    buf[off] = (byte) (values[i + 5] << 6);
    buf[off++] |= values[i + 6] >>> 9;

    buf[off++] = (byte) (values[i + 6] >>> 1);

    buf[off] = (byte) (values[i + 6] << 7);
    buf[off++] |= values[i + 7] >>> 8;

    buf[off] = (byte) (values[i + 7]);
  }

  static void packBits16(final long[] values, final int i, final byte[] buf, int off) {
    buf[off++] = (byte) (values[i + 0] >>> 8);
    buf[off++] = (byte) (values[i + 0]);

    buf[off++] = (byte) (values[i + 1] >>> 8);
    buf[off++] = (byte) (values[i + 1]);

    buf[off++] = (byte) (values[i + 2] >>> 8);
    buf[off++] = (byte) (values[i + 2]);

    buf[off++] = (byte) (values[i + 3] >>> 8);
    buf[off++] = (byte) (values[i + 3]);

    buf[off++] = (byte) (values[i + 4] >>> 8);
    buf[off++] = (byte) (values[i + 4]);

    buf[off++] = (byte) (values[i + 5] >>> 8);
    buf[off++] = (byte) (values[i + 5]);

    buf[off++] = (byte) (values[i + 6] >>> 8);
    buf[off++] = (byte) (values[i + 6]);

    buf[off++] = (byte) (values[i + 7] >>> 8);
    buf[off] = (byte) (values[i + 7]);
  }

  static void packBits17(final long[] values, final int i, final byte[] buf, int off) {
    buf[off++] = (byte) (values[i + 0] >>> 9);

    buf[off++] = (byte) (values[i + 0] >>> 1);

    buf[off] = (byte) (values[i + 0] << 7);
    buf[off++] |= values[i + 1] >>> 10;

    buf[off++] = (byte) (values[i + 1] >>> 2);

    buf[off] = (byte) (values[i + 1] << 6);
    buf[off++] |= values[i + 2] >>> 11;

    buf[off++] = (byte) (values[i + 2] >>> 3);

    buf[off] = (byte) (values[i + 2] << 5);
    buf[off++] |= values[i + 3] >>> 12;

    buf[off++] = (byte) (values[i + 3] >>> 4);

    buf[off] = (byte) (values[i + 3] << 4);
    buf[off++] |= values[i + 4] >>> 13;

    buf[off++] = (byte) (values[i + 4] >>> 5);

    buf[off] = (byte) (values[i + 4] << 3);
    buf[off++] |= values[i + 5] >>> 14;

    buf[off++] = (byte) (values[i + 5] >>> 6);

    buf[off] = (byte) (values[i + 5] << 2);
    buf[off++] |= values[i + 6] >>> 15;

    buf[off++] = (byte) (values[i + 6] >>> 7);

    buf[off] = (byte) (values[i + 6] << 1);
    buf[off++] |= values[i + 7] >>> 16;

    buf[off++] = (byte) (values[i + 7] >>> 8);

    buf[off] = (byte) (values[i + 7]);
  }

  static void packBits18(final long[] values, final int i, final byte[] buf, int off) {
    buf[off++] = (byte) (values[i + 0] >>> 10);

    buf[off++] = (byte) (values[i + 0] >>> 2);

    buf[off] = (byte) (values[i + 0] << 6);
    buf[off++] |= values[i + 1] >>> 12;

    buf[off++] = (byte) (values[i + 1] >>> 4);

    buf[off] = (byte) (values[i + 1] << 4);
    buf[off++] |= values[i + 2] >>> 14;

    buf[off++] = (byte) (values[i + 2] >>> 6);

    buf[off] = (byte) (values[i + 2] << 2);
    buf[off++] |= values[i + 3] >>> 16;

    buf[off++] = (byte) (values[i + 3] >>> 8);

    buf[off++] = (byte) (values[i + 3]);

    buf[off++] = (byte) (values[i + 4] >>> 10);

    buf[off++] = (byte) (values[i + 4] >>> 2);

    buf[off] = (byte) (values[i + 4] << 6);
    buf[off++] |= values[i + 5] >>> 12;

    buf[off++] = (byte) (values[i + 5] >>> 4);

    buf[off] = (byte) (values[i + 5] << 4);
    buf[off++] |= values[i + 6] >>> 14;

    buf[off++] = (byte) (values[i + 6] >>> 6);

    buf[off] = (byte) (values[i + 6] << 2);
    buf[off++] |= values[i + 7] >>> 16;

    buf[off++] = (byte) (values[i + 7] >>> 8);

    buf[off] = (byte) (values[i + 7]);
  }

  static void packBits19(final long[] values, final int i, final byte[] buf, int off) {
    buf[off++] = (byte) (values[i + 0] >>> 11);

    buf[off++] = (byte) (values[i + 0] >>> 3);

    buf[off] = (byte) (values[i + 0] << 5);
    buf[off++] |= values[i + 1] >>> 14;

    buf[off++] = (byte) (values[i + 1] >>> 6);

    buf[off] = (byte) (values[i + 1] << 2);
    buf[off++] |= values[i + 2] >>> 17;

    buf[off++] = (byte) (values[i + 2] >>> 9);

    buf[off++] = (byte) (values[i + 2] >>> 1);

    buf[off] = (byte) (values[i + 2] << 7);
    buf[off++] |= values[i + 3] >>> 12;

    buf[off++] = (byte) (values[i + 3] >>> 4);

    buf[off] = (byte) (values[i + 3] << 4);
    buf[off++] |= values[i + 4] >>> 15;

    buf[off++] |= values[i + 4] >>> 7;

    buf[off] = (byte) (values[i + 4] << 1);
    buf[off++] |= values[i + 5] >>> 18;

    buf[off++] = (byte) (values[i + 5] >>> 10);

    buf[off++] = (byte) (values[i + 5] >>> 2);

    buf[off] = (byte) (values[i + 5] << 6);
    buf[off++] |= values[i + 6] >>> 13;

    buf[off++] = (byte) (values[i + 6] >>> 5);

    buf[off] = (byte) (values[i + 6] << 3);
    buf[off++] |= values[i + 7] >>> 16;

    buf[off++] = (byte) (values[i + 7] >>> 8);

    buf[off] = (byte) (values[i + 7]);
  }

  static void packBits20(final long[] values, final int i, final byte[] buf, int off) {
    buf[off++] = (byte) (values[i + 0] >>> 12);

    buf[off++] = (byte) (values[i + 0] >>> 4);

    buf[off] = (byte) (values[i + 0] << 4);
    buf[off++] |= values[i + 1] >>> 16;

    buf[off++] = (byte) (values[i + 1] >>> 8);

    buf[off++] = (byte) (values[i + 1]);

    buf[off++] = (byte) (values[i + 2] >>> 12);

    buf[off++] = (byte) (values[i + 2] >>> 4);

    buf[off] = (byte) (values[i + 2] << 4);
    buf[off++] |= values[i + 3] >>> 16;

    buf[off++] = (byte) (values[i + 3] >>> 8);

    buf[off++] = (byte) (values[i + 3]);

    buf[off++] = (byte) (values[i + 4] >>> 12);

    buf[off++] = (byte) (values[i + 4] >>> 4);

    buf[off] = (byte) (values[i + 4] << 4);
    buf[off++] |= values[i + 5] >>> 16;

    buf[off++] = (byte) (values[i + 5] >>> 8);

    buf[off++] = (byte) (values[i + 5]);

    buf[off++] = (byte) (values[i + 6] >>> 12);

    buf[off++] = (byte) (values[i + 6] >>> 4);

    buf[off] = (byte) (values[i + 6] << 4);
    buf[off++] |= values[i + 7] >>> 16;

    buf[off++] = (byte) (values[i + 7] >>> 8);

    buf[off] = (byte) (values[i + 7]);
  }

  static void packBits21(final long[] values, final int i, final byte[] buf, int off) {
    buf[off++] = (byte) (values[i + 0] >>> 13);

    buf[off++] = (byte) (values[i + 0] >>> 5);

    buf[off] = (byte) (values[i + 0] << 3);
    buf[off++] |= values[i + 1] >>> 18;

    buf[off++] = (byte) (values[i + 1] >>> 10);

    buf[off++] = (byte) (values[i + 1] >>> 2);

    buf[off] = (byte) (values[i + 1] << 6);
    buf[off++] |= values[i + 2] >>> 15;

    buf[off++] = (byte) (values[i + 2] >>> 7);

    buf[off] = (byte) (values[i + 2] << 1);
    buf[off++] |= values[i + 3] >>> 20;

    buf[off++] = (byte) (values[i + 3] >>> 12);

    buf[off++] = (byte) (values[i + 3] >>> 4);

    buf[off] = (byte) (values[i + 3] << 4);
    buf[off++] |= values[i + 4] >>> 17;

    buf[off++] = (byte) (values[i + 4] >>> 9);

    buf[off++] = (byte) (values[i + 4] >>> 1);

    buf[off] = (byte) (values[i + 4] << 7);
    buf[off++] |= values[i + 5] >>> 14;

    buf[off++] = (byte) (values[i + 5] >>> 6);

    buf[off] = (byte) (values[i + 5] << 2);
    buf[off++] |= values[i + 6] >>> 19;

    buf[off++] = (byte) (values[i + 6] >>> 11);

    buf[off++] = (byte) (values[i + 6] >>> 3);

    buf[off] = (byte) (values[i + 6] << 5);
    buf[off++] |= values[i + 7] >>> 16;

    buf[off++] = (byte) (values[i + 7] >>> 8);

    buf[off] = (byte) (values[i + 7]);
  }

  static void packBits22(final long[] values, final int i, final byte[] buf, int off) {
    buf[off++] = (byte) (values[i + 0] >>> 14);

    buf[off++] = (byte) (values[i + 0] >>> 6);

    buf[off] = (byte) (values[i + 0] << 2);
    buf[off++] |= values[i + 1] >>> 20;

    buf[off++] = (byte) (values[i + 1] >>> 12);

    buf[off++] = (byte) (values[i + 1] >>> 4);

    buf[off] = (byte) (values[i + 1] << 4);
    buf[off++] |= values[i + 2] >>> 18;

    buf[off++] = (byte) (values[i + 2] >>> 10);

    buf[off++] = (byte) (values[i + 2] >>> 2);

    buf[off] = (byte) (values[i + 2] << 6);
    buf[off++] |= values[i + 3] >>> 16;

    buf[off++] = (byte) (values[i + 3] >>> 8);

    buf[off++] = (byte) (values[i + 3]);

    buf[off++] = (byte) (values[i + 4] >>> 14);

    buf[off++] = (byte) (values[i + 4] >>> 6);

    buf[off] = (byte) (values[i + 4] << 2);
    buf[off++] |= values[i + 5] >>> 20;

    buf[off++] = (byte) (values[i + 5] >>> 12);

    buf[off++] = (byte) (values[i + 5] >>> 4);

    buf[off] = (byte) (values[i + 5] << 4);
    buf[off++] |= values[i + 6] >>> 18;

    buf[off++] = (byte) (values[i + 6] >>> 10);

    buf[off++] = (byte) (values[i + 6] >>> 2);

    buf[off] = (byte) (values[i + 6] << 6);
    buf[off++] |= values[i + 7] >>> 16;

    buf[off++] = (byte) (values[i + 7] >>> 8);

    buf[off] = (byte) (values[i + 7]);
  }

  static void packBits23(final long[] values, final int i, final byte[] buf, int off) {
    buf[off++] = (byte) (values[i + 0] >>> 15);

    buf[off++] = (byte) (values[i + 0] >>> 7);

    buf[off] = (byte) (values[i + 0] << 1);
    buf[off++] |= values[i + 1] >>> 22;

    buf[off++] = (byte) (values[i + 1] >>> 14);

    buf[off++] = (byte) (values[i + 1] >>> 6);

    buf[off] = (byte) (values[i + 1] << 2);
    buf[off++] |= values[i + 2] >>> 21;

    buf[off++] = (byte) (values[i + 2] >>> 13);

    buf[off++] = (byte) (values[i + 2] >>> 5);

    buf[off] = (byte) (values[i + 2] << 3);
    buf[off++] |= values[i + 3] >>> 20;

    buf[off++] = (byte) (values[i + 3] >>> 12);

    buf[off++] = (byte) (values[i + 3] >>> 4);

    buf[off] = (byte) (values[i + 3] << 4);
    buf[off++] |= values[i + 4] >>> 19;

    buf[off++] = (byte) (values[i + 4] >>> 11);

    buf[off++] = (byte) (values[i + 4] >>> 3);

    buf[off] = (byte) (values[i + 4] << 5);
    buf[off++] |= values[i + 5] >>> 18;

    buf[off++] = (byte) (values[i + 5] >>> 10);

    buf[off++] = (byte) (values[i + 5] >>> 2);

    buf[off] = (byte) (values[i + 5] << 6);
    buf[off++] |= values[i + 6] >>> 17;

    buf[off++] = (byte) (values[i + 6] >>> 9);

    buf[off++] = (byte) (values[i + 6] >>> 1);

    buf[off] = (byte) (values[i + 6] << 7);
    buf[off++] |= values[i + 7] >>> 16;

    buf[off++] = (byte) (values[i + 7] >>> 8);

    buf[off] = (byte) (values[i + 7]);
  }

  static void packBits24(final long[] values, final int i, final byte[] buf, int off) {
    buf[off++] = (byte) (values[i + 0] >>> 16);
    buf[off++] = (byte) (values[i + 0] >>> 8);
    buf[off++] = (byte) (values[i + 0]);

    buf[off++] = (byte) (values[i + 1] >>> 16);
    buf[off++] = (byte) (values[i + 1] >>> 8);
    buf[off++] = (byte) (values[i + 1]);

    buf[off++] = (byte) (values[i + 2] >>> 16);
    buf[off++] = (byte) (values[i + 2] >>> 8);
    buf[off++] = (byte) (values[i + 2]);

    buf[off++] = (byte) (values[i + 3] >>> 16);
    buf[off++] = (byte) (values[i + 3] >>> 8);
    buf[off++] = (byte) (values[i + 3]);

    buf[off++] = (byte) (values[i + 4] >>> 16);
    buf[off++] = (byte) (values[i + 4] >>> 8);
    buf[off++] = (byte) (values[i + 4]);

    buf[off++] = (byte) (values[i + 5] >>> 16);
    buf[off++] = (byte) (values[i + 5] >>> 8);
    buf[off++] = (byte) (values[i + 5]);

    buf[off++] = (byte) (values[i + 6] >>> 16);
    buf[off++] = (byte) (values[i + 6] >>> 8);
    buf[off++] = (byte) (values[i + 6]);

    buf[off++] = (byte) (values[i + 7] >>> 16);
    buf[off++] = (byte) (values[i + 7] >>> 8);
    buf[off] = (byte) (values[i + 7]);
  }

  static void packBits25(final long[] values, final int i, final byte[] buf, int off) {
    buf[off++] = (byte) (values[i + 0] >>> 17);

    buf[off++] = (byte) (values[i + 0] >>> 9);

    buf[off++] = (byte) (values[i + 0] >>> 1);

    buf[off] = (byte) (values[i + 0] << 7);
    buf[off++] |= values[i + 1] >>> 18;

    buf[off++] = (byte) (values[i + 1] >>> 10);

    buf[off++] = (byte) (values[i + 1] >>> 2);

    buf[off] = (byte) (values[i + 1] << 6);
    buf[off++] |= values[i + 2] >>> 19;

    buf[off++] = (byte) (values[i + 2] >>> 11);

    buf[off++] = (byte) (values[i + 2] >>> 3);

    buf[off] = (byte) (values[i + 2] << 5);
    buf[off++] |= values[i + 3] >>> 20;

    buf[off++] = (byte) (values[i + 3] >>> 12);

    buf[off++] = (byte) (values[i + 3] >>> 4);

    buf[off] = (byte) (values[i + 3] << 4);
    buf[off++] |= values[i + 4] >>> 21;

    buf[off++] = (byte) (values[i + 4] >>> 13);

    buf[off++] = (byte) (values[i + 4] >>> 5);

    buf[off] = (byte) (values[i + 4] << 3);
    buf[off++] |= values[i + 5] >>> 22;

    buf[off++] = (byte) (values[i + 5] >>> 14);

    buf[off++] = (byte) (values[i + 5] >>> 6);

    buf[off] = (byte) (values[i + 5] << 2);
    buf[off++] |= values[i + 6] >>> 23;

    buf[off++] = (byte) (values[i + 6] >>> 15);

    buf[off++] = (byte) (values[i + 6] >>> 7);

    buf[off] = (byte) (values[i + 6] << 1);
    buf[off++] |= values[i + 7] >>> 24;

    buf[off++] = (byte) (values[i + 7] >>> 16);

    buf[off++] = (byte) (values[i + 7] >>> 8);

    buf[off] = (byte) (values[i + 7]);
  }

  static void packBits26(final long[] values, final int i, final byte[] buf, int off) {
    buf[off++] = (byte) (values[i + 0] >>> 18);

    buf[off++] = (byte) (values[i + 0] >>> 10);

    buf[off++] = (byte) (values[i + 0] >>> 2);

    buf[off] = (byte) (values[i + 0] << 6);
    buf[off++] |= values[i + 1] >>> 20;

    buf[off++] = (byte) (values[i + 1] >>> 12);

    buf[off++] = (byte) (values[i + 1] >>> 4);

    buf[off] = (byte) (values[i + 1] << 4);
    buf[off++] |= values[i + 2] >>> 22;

    buf[off++] = (byte) (values[i + 2] >>> 14);

    buf[off++] = (byte) (values[i + 2] >>> 6);

    buf[off] = (byte) (values[i + 2] << 2);
    buf[off++] |= values[i + 3] >>> 24;

    buf[off++] = (byte) (values[i + 3] >>> 16);

    buf[off++] = (byte) (values[i + 3] >>> 8);

    buf[off++] = (byte) (values[i + 3]);

    buf[off++] = (byte) (values[i + 4] >>> 18);

    buf[off++] = (byte) (values[i + 4] >>> 10);

    buf[off++] = (byte) (values[i + 4] >>> 2);

    buf[off] = (byte) (values[i + 4] << 6);
    buf[off++] |= values[i + 5] >>> 20;

    buf[off++] = (byte) (values[i + 5] >>> 12);

    buf[off++] = (byte) (values[i + 5] >>> 4);

    buf[off] = (byte) (values[i + 5] << 4);
    buf[off++] |= values[i + 6] >>> 22;

    buf[off++] = (byte) (values[i + 6] >>> 14);

    buf[off++] = (byte) (values[i + 6] >>> 6);

    buf[off] = (byte) (values[i + 6] << 2);
    buf[off++] |= values[i + 7] >>> 24;

    buf[off++] = (byte) (values[i + 7] >>> 16);

    buf[off++] = (byte) (values[i + 7] >>> 8);

    buf[off] = (byte) (values[i + 7]);
  }

  static void packBits27(final long[] values, final int i, final byte[] buf, int off) {
    buf[off++] = (byte) (values[i + 0] >>> 19);

    buf[off++] = (byte) (values[i + 0] >>> 11);

    buf[off++] = (byte) (values[i + 0] >>> 3);

    buf[off] = (byte) (values[i + 0] << 5);
    buf[off++] |= values[i + 1] >>> 22;

    buf[off++] = (byte) (values[i + 1] >>> 14);

    buf[off++] = (byte) (values[i + 1] >>> 6);

    buf[off] = (byte) (values[i + 1] << 2);
    buf[off++] |= values[i + 2] >>> 25;

    buf[off++] = (byte) (values[i + 2] >>> 17);

    buf[off++] = (byte) (values[i + 2] >>> 9);

    buf[off++] = (byte) (values[i + 2] >>> 1);

    buf[off] = (byte) (values[i + 2] << 7);
    buf[off++] |= values[i + 3] >>> 20;

    buf[off++] = (byte) (values[i + 3] >>> 12);

    buf[off++] = (byte) (values[i + 3] >>> 4);

    buf[off] = (byte) (values[i + 3] << 4);
    buf[off++] |= values[i + 4] >>> 23;

    buf[off++] = (byte) (values[i + 4] >>> 15);

    buf[off++] = (byte) (values[i + 4] >>> 7);

    buf[off] = (byte) (values[i + 4] << 1);
    buf[off++] |= values[i + 5] >>> 26;

    buf[off++] = (byte) (values[i + 5] >>> 18);

    buf[off++] = (byte) (values[i + 5] >>> 10);

    buf[off++] = (byte) (values[i + 5] >>> 2);

    buf[off] = (byte) (values[i + 5] << 6);
    buf[off++] |= values[i + 6] >>> 21;

    buf[off++] = (byte) (values[i + 6] >>> 13);

    buf[off++] = (byte) (values[i + 6] >>> 5);

    buf[off] = (byte) (values[i + 6] << 3);
    buf[off++] |= values[i + 7] >>> 24;

    buf[off++] = (byte) (values[i + 7] >>> 16);

    buf[off++] = (byte) (values[i + 7] >>> 8);

    buf[off] = (byte) (values[i + 7]);
  }

  static void packBits28(final long[] values, final int i, final byte[] buf, int off) {
    buf[off++] = (byte) (values[i + 0] >>> 20);
    buf[off++] = (byte) (values[i + 0] >>> 12);
    buf[off++] = (byte) (values[i + 0] >>> 4);
    buf[off] = (byte) (values[i + 0] << 4);
    buf[off++] |= values[i + 1] >>> 24;
    buf[off++] = (byte) (values[i + 1] >>> 16);
    buf[off++] = (byte) (values[i + 1] >>> 8);
    buf[off++] = (byte) (values[i + 1]);
    buf[off++] = (byte) (values[i + 2] >>> 20);
    buf[off++] = (byte) (values[i + 2] >>> 12);
    buf[off++] = (byte) (values[i + 2] >>> 4);
    buf[off] = (byte) (values[i + 2] << 4);
    buf[off++] |= values[i + 3] >>> 24;
    buf[off++] = (byte) (values[i + 3] >>> 16);
    buf[off++] = (byte) (values[i + 3] >>> 8);
    buf[off++] = (byte) (values[i + 3]);
    buf[off++] = (byte) (values[i + 4] >>> 20);
    buf[off++] = (byte) (values[i + 4] >>> 12);
    buf[off++] = (byte) (values[i + 4] >>> 4);
    buf[off] = (byte) (values[i + 4] << 4);
    buf[off++] |= values[i + 5] >>> 24;
    buf[off++] = (byte) (values[i + 5] >>> 16);
    buf[off++] = (byte) (values[i + 5] >>> 8);
    buf[off++] = (byte) (values[i + 5]);
    buf[off++] = (byte) (values[i + 6] >>> 20);
    buf[off++] = (byte) (values[i + 6] >>> 12);
    buf[off++] = (byte) (values[i + 6] >>> 4);
    buf[off] = (byte) (values[i + 6] << 4);
    buf[off++] |= values[i + 7] >>> 24;
    buf[off++] = (byte) (values[i + 7] >>> 16);
    buf[off++] = (byte) (values[i + 7] >>> 8);
    buf[off] = (byte) (values[i + 7]);
  }

  static void packBits29(final long[] values, final int i, final byte[] buf, int off) {
    buf[off++] = (byte) (values[i + 0] >>> 21);

    buf[off++] = (byte) (values[i + 0] >>> 13);

    buf[off++] = (byte) (values[i + 0] >>> 5);

    buf[off] = (byte) (values[i + 0] << 3);
    buf[off++] |= values[i + 1] >>> 26;

    buf[off++] = (byte) (values[i + 1] >>> 18);

    buf[off++] = (byte) (values[i + 1] >>> 10);

    buf[off++] = (byte) (values[i + 1] >>> 2);

    buf[off] = (byte) (values[i + 1] << 6);
    buf[off++] |= values[i + 2] >>> 23;

    buf[off++] = (byte) (values[i + 2] >>> 15);

    buf[off++] = (byte) (values[i + 2] >>> 7);

    buf[off] = (byte) (values[i + 2] << 1);
    buf[off++] |= values[i + 3] >>> 28;

    buf[off++] = (byte) (values[i + 3] >>> 20);

    buf[off++] = (byte) (values[i + 3] >>> 12);

    buf[off++] = (byte) (values[i + 3] >>> 4);

    buf[off] = (byte) (values[i + 3] << 4);
    buf[off++] |= values[i + 4] >>> 25;

    buf[off++] = (byte) (values[i + 4] >>> 17);

    buf[off++] = (byte) (values[i + 4] >>> 9);

    buf[off++] = (byte) (values[i + 4] >>> 1);

    buf[off] = (byte) (values[i + 4] << 7);
    buf[off++] |= values[i + 5] >>> 22;

    buf[off++] = (byte) (values[i + 5] >>> 14);

    buf[off++] = (byte) (values[i + 5] >>> 6);

    buf[off] = (byte) (values[i + 5] << 2);
    buf[off++] |= values[i + 6] >>> 27;

    buf[off++] = (byte) (values[i + 6] >>> 19);

    buf[off++] = (byte) (values[i + 6] >>> 11);

    buf[off++] = (byte) (values[i + 6] >>> 3);

    buf[off] = (byte) (values[i + 6] << 5);
    buf[off++] |= values[i + 7] >>> 24;

    buf[off++] = (byte) (values[i + 7] >>> 16);

    buf[off++] = (byte) (values[i + 7] >>> 8);

    buf[off] = (byte) (values[i + 7]);
  }

  static void packBits30(final long[] values, final int i, final byte[] buf, int off) {
    buf[off++] = (byte) (values[i + 0] >>> 22);
    buf[off++] = (byte) (values[i + 0] >>> 14);
    buf[off++] = (byte) (values[i + 0] >>> 6);

    buf[off] = (byte) (values[i + 0] << 2);
    buf[off++] |= values[i + 1] >>> 28;
    buf[off++] = (byte) (values[i + 1] >>> 20);
    buf[off++] = (byte) (values[i + 1] >>> 12);
    buf[off++] = (byte) (values[i + 1] >>> 4);

    buf[off] = (byte) (values[i + 1] << 4);
    buf[off++] |= values[i + 2] >>> 26;
    buf[off++] = (byte) (values[i + 2] >>> 18);
    buf[off++] = (byte) (values[i + 2] >>> 10);
    buf[off++] = (byte) (values[i + 2] >>> 2);

    buf[off] = (byte) (values[i + 2] << 6);
    buf[off++] |= values[i + 3] >>> 24;
    buf[off++] = (byte) (values[i + 3] >>> 16);
    buf[off++] = (byte) (values[i + 3] >>> 8);
    buf[off++] = (byte) (values[i + 3]);

    buf[off++] = (byte) (values[i + 4] >>> 22);
    buf[off++] = (byte) (values[i + 4] >>> 14);
    buf[off++] = (byte) (values[i + 4] >>> 6);

    buf[off] = (byte) (values[i + 4] << 2);
    buf[off++] |= values[i + 5] >>> 28;
    buf[off++] = (byte) (values[i + 5] >>> 20);
    buf[off++] = (byte) (values[i + 5] >>> 12);
    buf[off++] = (byte) (values[i + 5] >>> 4);

    buf[off] = (byte) (values[i + 5] << 4);
    buf[off++] |= values[i + 6] >>> 26;
    buf[off++] = (byte) (values[i + 6] >>> 18);
    buf[off++] = (byte) (values[i + 6] >>> 10);
    buf[off++] = (byte) (values[i + 6] >>> 2);

    buf[off] = (byte) (values[i + 6] << 6);
    buf[off++] |= values[i + 7] >>> 24;
    buf[off++] = (byte) (values[i + 7] >>> 16);
    buf[off++] = (byte) (values[i + 7] >>> 8);
    buf[off] = (byte) (values[i + 7]);
  }

  static void packBits31(final long[] values, final int i, final byte[] buf, int off) {
    buf[off++] = (byte) (values[i + 0] >>> 23);
    buf[off++] = (byte) (values[i + 0] >>> 15);
    buf[off++] = (byte) (values[i + 0] >>> 7);

    buf[off] = (byte) (values[i + 0] << 1);
    buf[off++] |= values[i + 1] >>> 30;
    buf[off++] = (byte) (values[i + 1] >>> 22);
    buf[off++] = (byte) (values[i + 1] >>> 14);
    buf[off++] = (byte) (values[i + 1] >>> 6);

    buf[off] = (byte) (values[i + 1] << 2);
    buf[off++] |= values[i + 2] >>> 29;
    buf[off++] = (byte) (values[i + 2] >>> 21);
    buf[off++] = (byte) (values[i + 2] >>> 13);
    buf[off++] = (byte) (values[i + 2] >>> 5);

    buf[off] = (byte) (values[i + 2] << 3);
    buf[off++] |= values[i + 3] >>> 28;
    buf[off++] = (byte) (values[i + 3] >>> 20);
    buf[off++] = (byte) (values[i + 3] >>> 12);
    buf[off++] = (byte) (values[i + 3] >>> 4);

    buf[off] = (byte) (values[i + 3] << 4);
    buf[off++] |= values[i + 4] >>> 27;
    buf[off++] = (byte) (values[i + 4] >>> 19);
    buf[off++] = (byte) (values[i + 4] >>> 11);
    buf[off++] = (byte) (values[i + 4] >>> 3);

    buf[off] = (byte) (values[i + 4] << 5);
    buf[off++] |= values[i + 5] >>> 26;
    buf[off++] = (byte) (values[i + 5] >>> 18);
    buf[off++] = (byte) (values[i + 5] >>> 10);
    buf[off++] = (byte) (values[i + 5] >>> 2);

    buf[off] = (byte) (values[i + 5] << 6);
    buf[off++] |= values[i + 6] >>> 25;
    buf[off++] = (byte) (values[i + 6] >>> 17);
    buf[off++] = (byte) (values[i + 6] >>> 9);
    buf[off++] = (byte) (values[i + 6] >>> 1);

    buf[off] = (byte) (values[i + 6] << 7);
    buf[off++] |= values[i + 7] >>> 24;
    buf[off++] = (byte) (values[i + 7] >>> 16);
    buf[off++] = (byte) (values[i + 7] >>> 8);
    buf[off] = (byte) (values[i + 7]);
  }

  static void packBits32(final long[] values, final int i, final byte[] buf, int off) {
    buf[off++] = (byte) (values[i + 0] >>> 24);
    buf[off++] = (byte) (values[i + 0] >>> 16);
    buf[off++] = (byte) (values[i + 0] >>> 8);
    buf[off++] = (byte) (values[i + 0]);

    buf[off++] = (byte) (values[i + 1] >>> 24);
    buf[off++] = (byte) (values[i + 1] >>> 16);
    buf[off++] = (byte) (values[i + 1] >>> 8);
    buf[off++] = (byte) (values[i + 1]);

    buf[off++] = (byte) (values[i + 2] >>> 24);
    buf[off++] = (byte) (values[i + 2] >>> 16);
    buf[off++] = (byte) (values[i + 2] >>> 8);
    buf[off++] = (byte) (values[i + 2]);

    buf[off++] = (byte) (values[i + 3] >>> 24);
    buf[off++] = (byte) (values[i + 3] >>> 16);
    buf[off++] = (byte) (values[i + 3] >>> 8);
    buf[off++] = (byte) (values[i + 3]);

    buf[off++] = (byte) (values[i + 4] >>> 24);
    buf[off++] = (byte) (values[i + 4] >>> 16);
    buf[off++] = (byte) (values[i + 4] >>> 8);
    buf[off++] = (byte) (values[i + 4]);

    buf[off++] = (byte) (values[i + 5] >>> 24);
    buf[off++] = (byte) (values[i + 5] >>> 16);
    buf[off++] = (byte) (values[i + 5] >>> 8);
    buf[off++] = (byte) (values[i + 5]);

    buf[off++] = (byte) (values[i + 6] >>> 24);
    buf[off++] = (byte) (values[i + 6] >>> 16);
    buf[off++] = (byte) (values[i + 6] >>> 8);
    buf[off++] = (byte) (values[i + 6]);

    buf[off++] = (byte) (values[i + 7] >>> 24);
    buf[off++] = (byte) (values[i + 7] >>> 16);
    buf[off++] = (byte) (values[i + 7] >>> 8);
    buf[off] = (byte) (values[i + 7]);
  }

  static void packBits33(final long[] values, final int i, final byte[] buf, int off) {
    buf[off++] = (byte) (values[i + 0] >>> 25);
    buf[off++] = (byte) (values[i + 0] >>> 17);
    buf[off++] = (byte) (values[i + 0] >>> 9);
    buf[off++] = (byte) (values[i + 0] >>> 1);

    buf[off] = (byte) (values[i + 0] << 7);
    buf[off++] |= values[i + 1] >>> 26;
    buf[off++] = (byte) (values[i + 1] >>> 18);
    buf[off++] = (byte) (values[i + 1] >>> 10);
    buf[off++] = (byte) (values[i + 1] >>> 2);

    buf[off] = (byte) (values[i + 1] << 6);
    buf[off++] |= values[i + 2] >>> 27;
    buf[off++] = (byte) (values[i + 2] >>> 19);
    buf[off++] = (byte) (values[i + 2] >>> 11);
    buf[off++] = (byte) (values[i + 2] >>> 3);

    buf[off] = (byte) (values[i + 2] << 5);
    buf[off++] |= values[i + 3] >>> 28;
    buf[off++] = (byte) (values[i + 3] >>> 20);
    buf[off++] = (byte) (values[i + 3] >>> 12);
    buf[off++] = (byte) (values[i + 3] >>> 4);

    buf[off] = (byte) (values[i + 3] << 4);
    buf[off++] |= values[i + 4] >>> 29;
    buf[off++] = (byte) (values[i + 4] >>> 21);
    buf[off++] = (byte) (values[i + 4] >>> 13);
    buf[off++] = (byte) (values[i + 4] >>> 5);

    buf[off] = (byte) (values[i + 4] << 3);
    buf[off++] |= values[i + 5] >>> 30;
    buf[off++] = (byte) (values[i + 5] >>> 22);
    buf[off++] = (byte) (values[i + 5] >>> 14);
    buf[off++] = (byte) (values[i + 5] >>> 6);

    buf[off] = (byte) (values[i + 5] << 2);
    buf[off++] |= values[i + 6] >>> 31;
    buf[off++] = (byte) (values[i + 6] >>> 23);
    buf[off++] = (byte) (values[i + 6] >>> 15);
    buf[off++] = (byte) (values[i + 6] >>> 7);

    buf[off] = (byte) (values[i + 6] << 1);
    buf[off++] |= values[i + 7] >>> 32;
    buf[off++] = (byte) (values[i + 7] >>> 24);
    buf[off++] = (byte) (values[i + 7] >>> 16);
    buf[off++] = (byte) (values[i + 7] >>> 8);
    buf[off] = (byte) (values[i + 7]);
  }

  static void packBits34(final long[] values, final int i, final byte[] buf, int off) {
    buf[off++] = (byte) (values[i + 0] >>> 26);
    buf[off++] = (byte) (values[i + 0] >>> 18);
    buf[off++] = (byte) (values[i + 0] >>> 10);
    buf[off++] = (byte) (values[i + 0] >>> 2);

    buf[off] = (byte) (values[i + 0] << 6);
    buf[off++] |= values[i + 1] >>> 28;
    buf[off++] = (byte) (values[i + 1] >>> 20);
    buf[off++] = (byte) (values[i + 1] >>> 12);
    buf[off++] = (byte) (values[i + 1] >>> 4);

    buf[off] = (byte) (values[i + 1] << 4);
    buf[off++] |= values[i + 2] >>> 30;
    buf[off++] = (byte) (values[i + 2] >>> 22);
    buf[off++] = (byte) (values[i + 2] >>> 14);
    buf[off++] = (byte) (values[i + 2] >>> 6);

    buf[off] = (byte) (values[i + 2] << 2);
    buf[off++] |= values[i + 3] >>> 32;
    buf[off++] = (byte) (values[i + 3] >>> 24);
    buf[off++] = (byte) (values[i + 3] >>> 16);
    buf[off++] = (byte) (values[i + 3] >>> 8);
    buf[off++] = (byte) (values[i + 3]);

    buf[off++] = (byte) (values[i + 4] >>> 26);
    buf[off++] = (byte) (values[i + 4] >>> 18);
    buf[off++] = (byte) (values[i + 4] >>> 10);
    buf[off++] = (byte) (values[i + 4] >>> 2);

    buf[off] = (byte) (values[i + 4] << 6);
    buf[off++] |= values[i + 5] >>> 28;
    buf[off++] = (byte) (values[i + 5] >>> 20);
    buf[off++] = (byte) (values[i + 5] >>> 12);
    buf[off++] = (byte) (values[i + 5] >>> 4);

    buf[off] = (byte) (values[i + 5] << 4);
    buf[off++] |= values[i + 6] >>> 30;
    buf[off++] = (byte) (values[i + 6] >>> 22);
    buf[off++] = (byte) (values[i + 6] >>> 14);
    buf[off++] = (byte) (values[i + 6] >>> 6);

    buf[off] = (byte) (values[i + 6] << 2);
    buf[off++] |= values[i + 7] >>> 32;
    buf[off++] = (byte) (values[i + 7] >>> 24);
    buf[off++] = (byte) (values[i + 7] >>> 16);
    buf[off++] = (byte) (values[i + 7] >>> 8);
    buf[off] = (byte) (values[i + 7]);
  }

  static void packBits35(final long[] values, final int i, final byte[] buf, int off) {
    buf[off++] = (byte) (values[i + 0] >>> 27);
    buf[off++] = (byte) (values[i + 0] >>> 19);
    buf[off++] = (byte) (values[i + 0] >>> 11);
    buf[off++] = (byte) (values[i + 0] >>> 3);

    buf[off] = (byte) (values[i + 0] << 5);
    buf[off++] |= values[i + 1] >>> 30;
    buf[off++] = (byte) (values[i + 1] >>> 22);
    buf[off++] = (byte) (values[i + 1] >>> 14);
    buf[off++] = (byte) (values[i + 1] >>> 6);

    buf[off] = (byte) (values[i + 1] << 2);
    buf[off++] |= values[i + 2] >>> 33;
    buf[off++] = (byte) (values[i + 2] >>> 25);
    buf[off++] = (byte) (values[i + 2] >>> 17);
    buf[off++] = (byte) (values[i + 2] >>> 9);
    buf[off++] = (byte) (values[i + 2] >>> 1);

    buf[off] = (byte) (values[i + 2] << 7);
    buf[off++] |= values[i + 3] >>> 28;
    buf[off++] = (byte) (values[i + 3] >>> 20);
    buf[off++] = (byte) (values[i + 3] >>> 12);
    buf[off++] = (byte) (values[i + 3] >>> 4);

    buf[off] = (byte) (values[i + 3] << 4);
    buf[off++] |= values[i + 4] >>> 31;
    buf[off++] = (byte) (values[i + 4] >>> 23);
    buf[off++] = (byte) (values[i + 4] >>> 15);
    buf[off++] = (byte) (values[i + 4] >>> 7);

    buf[off] = (byte) (values[i + 4] << 1);
    buf[off++] |= values[i + 5] >>> 34;
    buf[off++] = (byte) (values[i + 5] >>> 26);
    buf[off++] = (byte) (values[i + 5] >>> 18);
    buf[off++] = (byte) (values[i + 5] >>> 10);
    buf[off++] = (byte) (values[i + 5] >>> 2);

    buf[off] = (byte) (values[i + 5] << 6);
    buf[off++] |= values[i + 6] >>> 29;
    buf[off++] = (byte) (values[i + 6] >>> 21);
    buf[off++] = (byte) (values[i + 6] >>> 13);
    buf[off++] = (byte) (values[i + 6] >>> 5);

    buf[off] = (byte) (values[i + 6] << 3);
    buf[off++] |= values[i + 7] >>> 32;
    buf[off++] = (byte) (values[i + 7] >>> 24);
    buf[off++] = (byte) (values[i + 7] >>> 16);
    buf[off++] = (byte) (values[i + 7] >>> 8);
    buf[off] = (byte) (values[i + 7]);
  }

  static void packBits36(final long[] values, final int i, final byte[] buf, int off) {
    buf[off++] = (byte) (values[i + 0] >>> 28);
    buf[off++] = (byte) (values[i + 0] >>> 20);
    buf[off++] = (byte) (values[i + 0] >>> 12);
    buf[off++] = (byte) (values[i + 0] >>> 4);

    buf[off] = (byte) (values[i + 0] << 4);
    buf[off++] |= values[i + 1] >>> 32;
    buf[off++] = (byte) (values[i + 1] >>> 24);
    buf[off++] = (byte) (values[i + 1] >>> 16);
    buf[off++] = (byte) (values[i + 1] >>> 8);
    buf[off++] = (byte) (values[i + 1]);

    buf[off++] = (byte) (values[i + 2] >>> 28);
    buf[off++] = (byte) (values[i + 2] >>> 20);
    buf[off++] = (byte) (values[i + 2] >>> 12);
    buf[off++] = (byte) (values[i + 2] >>> 4);

    buf[off] = (byte) (values[i + 2] << 4);
    buf[off++] |= values[i + 3] >>> 32;
    buf[off++] = (byte) (values[i + 3] >>> 24);
    buf[off++] = (byte) (values[i + 3] >>> 16);
    buf[off++] = (byte) (values[i + 3] >>> 8);
    buf[off++] = (byte) (values[i + 3]);

    buf[off++] = (byte) (values[i + 4] >>> 28);
    buf[off++] = (byte) (values[i + 4] >>> 20);
    buf[off++] = (byte) (values[i + 4] >>> 12);
    buf[off++] = (byte) (values[i + 4] >>> 4);

    buf[off] = (byte) (values[i + 4] << 4);
    buf[off++] |= values[i + 5] >>> 32;
    buf[off++] = (byte) (values[i + 5] >>> 24);
    buf[off++] = (byte) (values[i + 5] >>> 16);
    buf[off++] = (byte) (values[i + 5] >>> 8);
    buf[off++] = (byte) (values[i + 5]);

    buf[off++] = (byte) (values[i + 6] >>> 28);
    buf[off++] = (byte) (values[i + 6] >>> 20);
    buf[off++] = (byte) (values[i + 6] >>> 12);
    buf[off++] = (byte) (values[i + 6] >>> 4);

    buf[off] = (byte) (values[i + 6] << 4);
    buf[off++] |= values[i + 7] >>> 32;
    buf[off++] = (byte) (values[i + 7] >>> 24);
    buf[off++] = (byte) (values[i + 7] >>> 16);
    buf[off++] = (byte) (values[i + 7] >>> 8);
    buf[off] = (byte) (values[i + 7]);
  }

  static void packBits37(final long[] values, final int i, final byte[] buf, int off) {
    buf[off++] = (byte) (values[i + 0] >>> 29);
    buf[off++] = (byte) (values[i + 0] >>> 21);
    buf[off++] = (byte) (values[i + 0] >>> 13);
    buf[off++] = (byte) (values[i + 0] >>> 5);

    buf[off] = (byte) (values[i + 0] << 3);
    buf[off++] |= values[i + 1] >>> 34;
    buf[off++] = (byte) (values[i + 1] >>> 26);
    buf[off++] = (byte) (values[i + 1] >>> 18);
    buf[off++] = (byte) (values[i + 1] >>> 10);
    buf[off++] = (byte) (values[i + 1] >>> 2);

    buf[off] = (byte) (values[i + 1] << 6);
    buf[off++] |= values[i + 2] >>> 31;
    buf[off++] = (byte) (values[i + 2] >>> 23);
    buf[off++] = (byte) (values[i + 2] >>> 15);
    buf[off++] = (byte) (values[i + 2] >>> 7);

    buf[off] = (byte) (values[i + 2] << 1);
    buf[off++] |= values[i + 3] >>> 36;
    buf[off++] = (byte) (values[i + 3] >>> 28);
    buf[off++] = (byte) (values[i + 3] >>> 20);
    buf[off++] = (byte) (values[i + 3] >>> 12);
    buf[off++] = (byte) (values[i + 3] >>> 4);

    buf[off] = (byte) (values[i + 3] << 4);
    buf[off++] |= values[i + 4] >>> 33;
    buf[off++] = (byte) (values[i + 4] >>> 25);
    buf[off++] = (byte) (values[i + 4] >>> 17);
    buf[off++] = (byte) (values[i + 4] >>> 9);
    buf[off++] = (byte) (values[i + 4] >>> 1);

    buf[off] = (byte) (values[i + 4] << 7);
    buf[off++] |= values[i + 5] >>> 30;
    buf[off++] = (byte) (values[i + 5] >>> 22);
    buf[off++] = (byte) (values[i + 5] >>> 14);
    buf[off++] = (byte) (values[i + 5] >>> 6);

    buf[off] = (byte) (values[i + 5] << 2);
    buf[off++] |= values[i + 6] >>> 35;
    buf[off++] = (byte) (values[i + 6] >>> 27);
    buf[off++] = (byte) (values[i + 6] >>> 19);
    buf[off++] = (byte) (values[i + 6] >>> 11);
    buf[off++] = (byte) (values[i + 6] >>> 3);

    buf[off] = (byte) (values[i + 6] << 5);
    buf[off++] |= values[i + 7] >>> 32;
    buf[off++] = (byte) (values[i + 7] >>> 24);
    buf[off++] = (byte) (values[i + 7] >>> 16);
    buf[off++] = (byte) (values[i + 7] >>> 8);
    buf[off] = (byte) (values[i + 7]);
  }

  static void packBits38(final long[] values, final int i, final byte[] buf, int off) {
    buf[off++] = (byte) (values[i + 0] >>> 30);
    buf[off++] = (byte) (values[i + 0] >>> 22);
    buf[off++] = (byte) (values[i + 0] >>> 14);
    buf[off++] = (byte) (values[i + 0] >>> 6);

    buf[off] = (byte) (values[i + 0] << 2);
    buf[off++] |= values[i + 1] >>> 36;
    buf[off++] = (byte) (values[i + 1] >>> 28);
    buf[off++] = (byte) (values[i + 1] >>> 20);
    buf[off++] = (byte) (values[i + 1] >>> 12);
    buf[off++] = (byte) (values[i + 1] >>> 4);

    buf[off] = (byte) (values[i + 1] << 4);
    buf[off++] |= values[i + 2] >>> 34;
    buf[off++] = (byte) (values[i + 2] >>> 26);
    buf[off++] = (byte) (values[i + 2] >>> 18);
    buf[off++] = (byte) (values[i + 2] >>> 10);
    buf[off++] = (byte) (values[i + 2] >>> 2);

    buf[off] = (byte) (values[i + 2] << 6);
    buf[off++] |= values[i + 3] >>> 32;
    buf[off++] = (byte) (values[i + 3] >>> 24);
    buf[off++] = (byte) (values[i + 3] >>> 16);
    buf[off++] = (byte) (values[i + 3] >>> 8);
    buf[off++] = (byte) (values[i + 3]);

    buf[off++] = (byte) (values[i + 4] >>> 30);
    buf[off++] = (byte) (values[i + 4] >>> 22);
    buf[off++] = (byte) (values[i + 4] >>> 14);
    buf[off++] = (byte) (values[i + 4] >>> 6);

    buf[off] = (byte) (values[i + 4] << 2);
    buf[off++] |= values[i + 5] >>> 36;
    buf[off++] = (byte) (values[i + 5] >>> 28);
    buf[off++] = (byte) (values[i + 5] >>> 20);
    buf[off++] = (byte) (values[i + 5] >>> 12);
    buf[off++] = (byte) (values[i + 5] >>> 4);

    buf[off] = (byte) (values[i + 5] << 4);
    buf[off++] |= values[i + 6] >>> 34;
    buf[off++] = (byte) (values[i + 6] >>> 26);
    buf[off++] = (byte) (values[i + 6] >>> 18);
    buf[off++] = (byte) (values[i + 6] >>> 10);
    buf[off++] = (byte) (values[i + 6] >>> 2);

    buf[off] = (byte) (values[i + 6] << 6);
    buf[off++] |= values[i + 7] >>> 32;
    buf[off++] = (byte) (values[i + 7] >>> 24);
    buf[off++] = (byte) (values[i + 7] >>> 16);
    buf[off++] = (byte) (values[i + 7] >>> 8);
    buf[off] = (byte) (values[i + 7]);
  }

  static void packBits39(final long[] values, final int i, final byte[] buf, int off) {
    buf[off++] = (byte) (values[i + 0] >>> 31);
    buf[off++] = (byte) (values[i + 0] >>> 23);
    buf[off++] = (byte) (values[i + 0] >>> 15);
    buf[off++] = (byte) (values[i + 0] >>> 7);

    buf[off] = (byte) (values[i + 0] << 1);
    buf[off++] |= values[i + 1] >>> 38;
    buf[off++] = (byte) (values[i + 1] >>> 30);
    buf[off++] = (byte) (values[i + 1] >>> 22);
    buf[off++] = (byte) (values[i + 1] >>> 14);
    buf[off++] = (byte) (values[i + 1] >>> 6);

    buf[off] = (byte) (values[i + 1] << 2);
    buf[off++] |= values[i + 2] >>> 37;
    buf[off++] = (byte) (values[i + 2] >>> 29);
    buf[off++] = (byte) (values[i + 2] >>> 21);
    buf[off++] = (byte) (values[i + 2] >>> 13);
    buf[off++] = (byte) (values[i + 2] >>> 5);

    buf[off] = (byte) (values[i + 2] << 3);
    buf[off++] |= values[i + 3] >>> 36;
    buf[off++] = (byte) (values[i + 3] >>> 28);
    buf[off++] = (byte) (values[i + 3] >>> 20);
    buf[off++] = (byte) (values[i + 3] >>> 12);
    buf[off++] = (byte) (values[i + 3] >>> 4);

    buf[off] = (byte) (values[i + 3] << 4);
    buf[off++] |= values[i + 4] >>> 35;
    buf[off++] = (byte) (values[i + 4] >>> 27);
    buf[off++] = (byte) (values[i + 4] >>> 19);
    buf[off++] = (byte) (values[i + 4] >>> 11);
    buf[off++] = (byte) (values[i + 4] >>> 3);

    buf[off] = (byte) (values[i + 4] << 5);
    buf[off++] |= values[i + 5] >>> 34;
    buf[off++] = (byte) (values[i + 5] >>> 26);
    buf[off++] = (byte) (values[i + 5] >>> 18);
    buf[off++] = (byte) (values[i + 5] >>> 10);
    buf[off++] = (byte) (values[i + 5] >>> 2);

    buf[off] = (byte) (values[i + 5] << 6);
    buf[off++] |= values[i + 6] >>> 33;
    buf[off++] = (byte) (values[i + 6] >>> 25);
    buf[off++] = (byte) (values[i + 6] >>> 17);
    buf[off++] = (byte) (values[i + 6] >>> 9);
    buf[off++] = (byte) (values[i + 6] >>> 1);

    buf[off] = (byte) (values[i + 6] << 7);
    buf[off++] |= values[i + 7] >>> 32;
    buf[off++] = (byte) (values[i + 7] >>> 24);
    buf[off++] = (byte) (values[i + 7] >>> 16);
    buf[off++] = (byte) (values[i + 7] >>> 8);
    buf[off] = (byte) (values[i + 7]);
  }

  static void packBits40(final long[] values, final int i, final byte[] buf, int off) {
    buf[off++] = (byte) (values[i + 0] >>> 32);
    buf[off++] = (byte) (values[i + 0] >>> 24);
    buf[off++] = (byte) (values[i + 0] >>> 16);
    buf[off++] = (byte) (values[i + 0] >>> 8);
    buf[off++] = (byte) (values[i + 0]);

    buf[off++] = (byte) (values[i + 1] >>> 32);
    buf[off++] = (byte) (values[i + 1] >>> 24);
    buf[off++] = (byte) (values[i + 1] >>> 16);
    buf[off++] = (byte) (values[i + 1] >>> 8);
    buf[off++] = (byte) (values[i + 1]);

    buf[off++] = (byte) (values[i + 2] >>> 32);
    buf[off++] = (byte) (values[i + 2] >>> 24);
    buf[off++] = (byte) (values[i + 2] >>> 16);
    buf[off++] = (byte) (values[i + 2] >>> 8);
    buf[off++] = (byte) (values[i + 2]);

    buf[off++] = (byte) (values[i + 3] >>> 32);
    buf[off++] = (byte) (values[i + 3] >>> 24);
    buf[off++] = (byte) (values[i + 3] >>> 16);
    buf[off++] = (byte) (values[i + 3] >>> 8);
    buf[off++] = (byte) (values[i + 3]);

    buf[off++] = (byte) (values[i + 4] >>> 32);
    buf[off++] = (byte) (values[i + 4] >>> 24);
    buf[off++] = (byte) (values[i + 4] >>> 16);
    buf[off++] = (byte) (values[i + 4] >>> 8);
    buf[off++] = (byte) (values[i + 4]);

    buf[off++] = (byte) (values[i + 5] >>> 32);
    buf[off++] = (byte) (values[i + 5] >>> 24);
    buf[off++] = (byte) (values[i + 5] >>> 16);
    buf[off++] = (byte) (values[i + 5] >>> 8);
    buf[off++] = (byte) (values[i + 5]);

    buf[off++] = (byte) (values[i + 6] >>> 32);
    buf[off++] = (byte) (values[i + 6] >>> 24);
    buf[off++] = (byte) (values[i + 6] >>> 16);
    buf[off++] = (byte) (values[i + 6] >>> 8);
    buf[off++] = (byte) (values[i + 6]);

    buf[off++] = (byte) (values[i + 7] >>> 32);
    buf[off++] = (byte) (values[i + 7] >>> 24);
    buf[off++] = (byte) (values[i + 7] >>> 16);
    buf[off++] = (byte) (values[i + 7] >>> 8);
    buf[off] = (byte) (values[i + 7]);
  }

  static void packBits41(final long[] values, final int i, final byte[] buf, int off) {
    buf[off++] = (byte) (values[i + 0] >>> 33);
    buf[off++] = (byte) (values[i + 0] >>> 25);
    buf[off++] = (byte) (values[i + 0] >>> 17);
    buf[off++] = (byte) (values[i + 0] >>> 9);
    buf[off++] = (byte) (values[i + 0] >>> 1);

    buf[off] = (byte) (values[i + 0] << 7);
    buf[off++] |= values[i + 1] >>> 34;
    buf[off++] = (byte) (values[i + 1] >>> 26);
    buf[off++] = (byte) (values[i + 1] >>> 18);
    buf[off++] = (byte) (values[i + 1] >>> 10);
    buf[off++] = (byte) (values[i + 1] >>> 2);

    buf[off] = (byte) (values[i + 1] << 6);
    buf[off++] |= values[i + 2] >>> 35;
    buf[off++] = (byte) (values[i + 2] >>> 27);
    buf[off++] = (byte) (values[i + 2] >>> 19);
    buf[off++] = (byte) (values[i + 2] >>> 11);
    buf[off++] = (byte) (values[i + 2] >>> 3);

    buf[off] = (byte) (values[i + 2] << 5);
    buf[off++] |= values[i + 3] >>> 36;
    buf[off++] = (byte) (values[i + 3] >>> 28);
    buf[off++] = (byte) (values[i + 3] >>> 20);
    buf[off++] = (byte) (values[i + 3] >>> 12);
    buf[off++] = (byte) (values[i + 3] >>> 4);

    buf[off] = (byte) (values[i + 3] << 4);
    buf[off++] |= values[i + 4] >>> 37;
    buf[off++] = (byte) (values[i + 4] >>> 29);
    buf[off++] = (byte) (values[i + 4] >>> 21);
    buf[off++] = (byte) (values[i + 4] >>> 13);
    buf[off++] = (byte) (values[i + 4] >>> 5);

    buf[off] = (byte) (values[i + 4] << 3);
    buf[off++] |= values[i + 5] >>> 38;
    buf[off++] = (byte) (values[i + 5] >>> 30);
    buf[off++] = (byte) (values[i + 5] >>> 22);
    buf[off++] = (byte) (values[i + 5] >>> 14);
    buf[off++] = (byte) (values[i + 5] >>> 6);

    buf[off] = (byte) (values[i + 5] << 2);
    buf[off++] |= values[i + 6] >>> 39;
    buf[off++] = (byte) (values[i + 6] >>> 31);
    buf[off++] = (byte) (values[i + 6] >>> 23);
    buf[off++] = (byte) (values[i + 6] >>> 15);
    buf[off++] = (byte) (values[i + 6] >>> 7);

    buf[off] = (byte) (values[i + 6] << 1);
    buf[off++] |= values[i + 7] >>> 40;
    buf[off++] = (byte) (values[i + 7] >>> 32);
    buf[off++] = (byte) (values[i + 7] >>> 24);
    buf[off++] = (byte) (values[i + 7] >>> 16);
    buf[off++] = (byte) (values[i + 7] >>> 8);
    buf[off] = (byte) (values[i + 7]);
  }

  static void packBits42(final long[] values, final int i, final byte[] buf, int off) {
    buf[off++] = (byte) (values[i + 0] >>> 34);
    buf[off++] = (byte) (values[i + 0] >>> 26);
    buf[off++] = (byte) (values[i + 0] >>> 18);
    buf[off++] = (byte) (values[i + 0] >>> 10);
    buf[off++] = (byte) (values[i + 0] >>> 2);

    buf[off] = (byte) (values[i + 0] << 6);
    buf[off++] |= values[i + 1] >>> 36;
    buf[off++] = (byte) (values[i + 1] >>> 28);
    buf[off++] = (byte) (values[i + 1] >>> 20);
    buf[off++] = (byte) (values[i + 1] >>> 12);
    buf[off++] = (byte) (values[i + 1] >>> 4);

    buf[off] = (byte) (values[i + 1] << 4);
    buf[off++] |= values[i + 2] >>> 38;
    buf[off++] = (byte) (values[i + 2] >>> 30);
    buf[off++] = (byte) (values[i + 2] >>> 22);
    buf[off++] = (byte) (values[i + 2] >>> 14);
    buf[off++] = (byte) (values[i + 2] >>> 6);

    buf[off] = (byte) (values[i + 2] << 2);
    buf[off++] |= values[i + 3] >>> 40;
    buf[off++] = (byte) (values[i + 3] >>> 32);
    buf[off++] = (byte) (values[i + 3] >>> 24);
    buf[off++] = (byte) (values[i + 3] >>> 16);
    buf[off++] = (byte) (values[i + 3] >>> 8);
    buf[off++] = (byte) (values[i + 3]);

    buf[off++] = (byte) (values[i + 4] >>> 34);
    buf[off++] = (byte) (values[i + 4] >>> 26);
    buf[off++] = (byte) (values[i + 4] >>> 18);
    buf[off++] = (byte) (values[i + 4] >>> 10);
    buf[off++] = (byte) (values[i + 4] >>> 2);

    buf[off] = (byte) (values[i + 4] << 6);
    buf[off++] |= values[i + 5] >>> 36;
    buf[off++] = (byte) (values[i + 5] >>> 28);
    buf[off++] = (byte) (values[i + 5] >>> 20);
    buf[off++] = (byte) (values[i + 5] >>> 12);
    buf[off++] = (byte) (values[i + 5] >>> 4);

    buf[off] = (byte) (values[i + 5] << 4);
    buf[off++] |= values[i + 6] >>> 38;
    buf[off++] = (byte) (values[i + 6] >>> 30);
    buf[off++] = (byte) (values[i + 6] >>> 22);
    buf[off++] = (byte) (values[i + 6] >>> 14);
    buf[off++] = (byte) (values[i + 6] >>> 6);

    buf[off] = (byte) (values[i + 6] << 2);
    buf[off++] |= values[i + 7] >>> 40;
    buf[off++] = (byte) (values[i + 7] >>> 32);
    buf[off++] = (byte) (values[i + 7] >>> 24);
    buf[off++] = (byte) (values[i + 7] >>> 16);
    buf[off++] = (byte) (values[i + 7] >>> 8);
    buf[off] = (byte) (values[i + 7]);
  }

  static void packBits43(final long[] values, final int i, final byte[] buf, int off) {
    buf[off++] = (byte) (values[i + 0] >>> 35);
    buf[off++] = (byte) (values[i + 0] >>> 27);
    buf[off++] = (byte) (values[i + 0] >>> 19);
    buf[off++] = (byte) (values[i + 0] >>> 11);
    buf[off++] = (byte) (values[i + 0] >>> 3);

    buf[off] = (byte) (values[i + 0] << 5);
    buf[off++] |= values[i + 1] >>> 38;
    buf[off++] = (byte) (values[i + 1] >>> 30);
    buf[off++] = (byte) (values[i + 1] >>> 22);
    buf[off++] = (byte) (values[i + 1] >>> 14);
    buf[off++] = (byte) (values[i + 1] >>> 6);

    buf[off] = (byte) (values[i + 1] << 2);
    buf[off++] |= values[i + 2] >>> 41;
    buf[off++] = (byte) (values[i + 2] >>> 33);
    buf[off++] = (byte) (values[i + 2] >>> 25);
    buf[off++] = (byte) (values[i + 2] >>> 17);
    buf[off++] = (byte) (values[i + 2] >>> 9);
    buf[off++] = (byte) (values[i + 2] >>> 1);

    buf[off] = (byte) (values[i + 2] << 7);
    buf[off++] |= values[i + 3] >>> 36;
    buf[off++] = (byte) (values[i + 3] >>> 28);
    buf[off++] = (byte) (values[i + 3] >>> 20);
    buf[off++] = (byte) (values[i + 3] >>> 12);
    buf[off++] = (byte) (values[i + 3] >>> 4);

    buf[off] = (byte) (values[i + 3] << 4);
    buf[off++] |= values[i + 4] >>> 39;
    buf[off++] = (byte) (values[i + 4] >>> 31);
    buf[off++] = (byte) (values[i + 4] >>> 23);
    buf[off++] = (byte) (values[i + 4] >>> 15);
    buf[off++] = (byte) (values[i + 4] >>> 7);

    buf[off] = (byte) (values[i + 4] << 1);
    buf[off++] |= values[i + 5] >>> 42;
    buf[off++] = (byte) (values[i + 5] >>> 34);
    buf[off++] = (byte) (values[i + 5] >>> 26);
    buf[off++] = (byte) (values[i + 5] >>> 18);
    buf[off++] = (byte) (values[i + 5] >>> 10);
    buf[off++] = (byte) (values[i + 5] >>> 2);

    buf[off] = (byte) (values[i + 5] << 6);
    buf[off++] |= values[i + 6] >>> 37;
    buf[off++] = (byte) (values[i + 6] >>> 29);
    buf[off++] = (byte) (values[i + 6] >>> 21);
    buf[off++] = (byte) (values[i + 6] >>> 13);
    buf[off++] = (byte) (values[i + 6] >>> 5);

    buf[off] = (byte) (values[i + 6] << 3);
    buf[off++] |= values[i + 7] >>> 40;
    buf[off++] = (byte) (values[i + 7] >>> 32);
    buf[off++] = (byte) (values[i + 7] >>> 24);
    buf[off++] = (byte) (values[i + 7] >>> 16);
    buf[off++] = (byte) (values[i + 7] >>> 8);
    buf[off] = (byte) (values[i + 7]);
  }

  static void packBits44(final long[] values, final int i, final byte[] buf, int off) {
    buf[off++] = (byte) (values[i + 0] >>> 36);
    buf[off++] = (byte) (values[i + 0] >>> 28);
    buf[off++] = (byte) (values[i + 0] >>> 20);
    buf[off++] = (byte) (values[i + 0] >>> 12);
    buf[off++] = (byte) (values[i + 0] >>> 4);

    buf[off] = (byte) (values[i + 0] << 4);
    buf[off++] |= values[i + 1] >>> 40;
    buf[off++] = (byte) (values[i + 1] >>> 32);
    buf[off++] = (byte) (values[i + 1] >>> 24);
    buf[off++] = (byte) (values[i + 1] >>> 16);
    buf[off++] = (byte) (values[i + 1] >>> 8);
    buf[off++] = (byte) (values[i + 1]);

    buf[off++] = (byte) (values[i + 2] >>> 36);
    buf[off++] = (byte) (values[i + 2] >>> 28);
    buf[off++] = (byte) (values[i + 2] >>> 20);
    buf[off++] = (byte) (values[i + 2] >>> 12);
    buf[off++] = (byte) (values[i + 2] >>> 4);

    buf[off] = (byte) (values[i + 2] << 4);
    buf[off++] |= values[i + 3] >>> 40;
    buf[off++] = (byte) (values[i + 3] >>> 32);
    buf[off++] = (byte) (values[i + 3] >>> 24);
    buf[off++] = (byte) (values[i + 3] >>> 16);
    buf[off++] = (byte) (values[i + 3] >>> 8);
    buf[off++] = (byte) (values[i + 3]);

    buf[off++] = (byte) (values[i + 4] >>> 36);
    buf[off++] = (byte) (values[i + 4] >>> 28);
    buf[off++] = (byte) (values[i + 4] >>> 20);
    buf[off++] = (byte) (values[i + 4] >>> 12);
    buf[off++] = (byte) (values[i + 4] >>> 4);

    buf[off] = (byte) (values[i + 4] << 4);
    buf[off++] |= values[i + 5] >>> 40;
    buf[off++] = (byte) (values[i + 5] >>> 32);
    buf[off++] = (byte) (values[i + 5] >>> 24);
    buf[off++] = (byte) (values[i + 5] >>> 16);
    buf[off++] = (byte) (values[i + 5] >>> 8);
    buf[off++] = (byte) (values[i + 5]);

    buf[off++] = (byte) (values[i + 6] >>> 36);
    buf[off++] = (byte) (values[i + 6] >>> 28);
    buf[off++] = (byte) (values[i + 6] >>> 20);
    buf[off++] = (byte) (values[i + 6] >>> 12);
    buf[off++] = (byte) (values[i + 6] >>> 4);

    buf[off] = (byte) (values[i + 6] << 4);
    buf[off++] |= values[i + 7] >>> 40;
    buf[off++] = (byte) (values[i + 7] >>> 32);
    buf[off++] = (byte) (values[i + 7] >>> 24);
    buf[off++] = (byte) (values[i + 7] >>> 16);
    buf[off++] = (byte) (values[i + 7] >>> 8);
    buf[off] = (byte) (values[i + 7]);
  }

  static void packBits45(final long[] values, final int i, final byte[] buf, int off) {
    buf[off++] = (byte) (values[i + 0] >>> 37);
    buf[off++] = (byte) (values[i + 0] >>> 29);
    buf[off++] = (byte) (values[i + 0] >>> 21);
    buf[off++] = (byte) (values[i + 0] >>> 13);
    buf[off++] = (byte) (values[i + 0] >>> 5);

    buf[off] = (byte) (values[i + 0] << 3);
    buf[off++] |= values[i + 1] >>> 42;
    buf[off++] = (byte) (values[i + 1] >>> 34);
    buf[off++] = (byte) (values[i + 1] >>> 26);
    buf[off++] = (byte) (values[i + 1] >>> 18);
    buf[off++] = (byte) (values[i + 1] >>> 10);
    buf[off++] = (byte) (values[i + 1] >>> 2);

    buf[off] = (byte) (values[i + 1] << 6);
    buf[off++] |= values[i + 2] >>> 39;
    buf[off++] = (byte) (values[i + 2] >>> 31);
    buf[off++] = (byte) (values[i + 2] >>> 23);
    buf[off++] = (byte) (values[i + 2] >>> 15);
    buf[off++] = (byte) (values[i + 2] >>> 7);

    buf[off] = (byte) (values[i + 2] << 1);
    buf[off++] |= values[i + 3] >>> 44;
    buf[off++] = (byte) (values[i + 3] >>> 36);
    buf[off++] = (byte) (values[i + 3] >>> 28);
    buf[off++] = (byte) (values[i + 3] >>> 20);
    buf[off++] = (byte) (values[i + 3] >>> 12);
    buf[off++] = (byte) (values[i + 3] >>> 4);

    buf[off] = (byte) (values[i + 3] << 4);
    buf[off++] |= values[i + 4] >>> 41;
    buf[off++] = (byte) (values[i + 4] >>> 33);
    buf[off++] = (byte) (values[i + 4] >>> 25);
    buf[off++] = (byte) (values[i + 4] >>> 17);
    buf[off++] = (byte) (values[i + 4] >>> 9);
    buf[off++] = (byte) (values[i + 4] >>> 1);

    buf[off] = (byte) (values[i + 4] << 7);
    buf[off++] |= values[i + 5] >>> 38;
    buf[off++] = (byte) (values[i + 5] >>> 30);
    buf[off++] = (byte) (values[i + 5] >>> 22);
    buf[off++] = (byte) (values[i + 5] >>> 14);
    buf[off++] = (byte) (values[i + 5] >>> 6);

    buf[off] = (byte) (values[i + 5] << 2);
    buf[off++] |= values[i + 6] >>> 43;
    buf[off++] = (byte) (values[i + 6] >>> 35);
    buf[off++] = (byte) (values[i + 6] >>> 27);
    buf[off++] = (byte) (values[i + 6] >>> 19);
    buf[off++] = (byte) (values[i + 6] >>> 11);
    buf[off++] = (byte) (values[i + 6] >>> 3);

    buf[off] = (byte) (values[i + 6] << 5);
    buf[off++] |= values[i + 7] >>> 40;
    buf[off++] = (byte) (values[i + 7] >>> 32);
    buf[off++] = (byte) (values[i + 7] >>> 24);
    buf[off++] = (byte) (values[i + 7] >>> 16);
    buf[off++] = (byte) (values[i + 7] >>> 8);
    buf[off] = (byte) (values[i + 7]);
  }

  static void packBits46(final long[] values, final int i, final byte[] buf, int off) {
    buf[off++] = (byte) (values[i + 0] >>> 38);
    buf[off++] = (byte) (values[i + 0] >>> 30);
    buf[off++] = (byte) (values[i + 0] >>> 22);
    buf[off++] = (byte) (values[i + 0] >>> 14);
    buf[off++] = (byte) (values[i + 0] >>> 6);

    buf[off] = (byte) (values[i + 0] << 2);
    buf[off++] |= values[i + 1] >>> 44;
    buf[off++] = (byte) (values[i + 1] >>> 36);
    buf[off++] = (byte) (values[i + 1] >>> 28);
    buf[off++] = (byte) (values[i + 1] >>> 20);
    buf[off++] = (byte) (values[i + 1] >>> 12);
    buf[off++] = (byte) (values[i + 1] >>> 4);

    buf[off] = (byte) (values[i + 1] << 4);
    buf[off++] |= values[i + 2] >>> 42;
    buf[off++] = (byte) (values[i + 2] >>> 34);
    buf[off++] = (byte) (values[i + 2] >>> 26);
    buf[off++] = (byte) (values[i + 2] >>> 18);
    buf[off++] = (byte) (values[i + 2] >>> 10);
    buf[off++] = (byte) (values[i + 2] >>> 2);

    buf[off] = (byte) (values[i + 2] << 6);
    buf[off++] |= values[i + 3] >>> 40;
    buf[off++] = (byte) (values[i + 3] >>> 32);
    buf[off++] = (byte) (values[i + 3] >>> 24);
    buf[off++] = (byte) (values[i + 3] >>> 16);
    buf[off++] = (byte) (values[i + 3] >>> 8);
    buf[off++] = (byte) (values[i + 3]);

    buf[off++] = (byte) (values[i + 4] >>> 38);
    buf[off++] = (byte) (values[i + 4] >>> 30);
    buf[off++] = (byte) (values[i + 4] >>> 22);
    buf[off++] = (byte) (values[i + 4] >>> 14);
    buf[off++] = (byte) (values[i + 4] >>> 6);

    buf[off] = (byte) (values[i + 4] << 2);
    buf[off++] |= values[i + 5] >>> 44;
    buf[off++] = (byte) (values[i + 5] >>> 36);
    buf[off++] = (byte) (values[i + 5] >>> 28);
    buf[off++] = (byte) (values[i + 5] >>> 20);
    buf[off++] = (byte) (values[i + 5] >>> 12);
    buf[off++] = (byte) (values[i + 5] >>> 4);

    buf[off] = (byte) (values[i + 5] << 4);
    buf[off++] |= values[i + 6] >>> 42;
    buf[off++] = (byte) (values[i + 6] >>> 34);
    buf[off++] = (byte) (values[i + 6] >>> 26);
    buf[off++] = (byte) (values[i + 6] >>> 18);
    buf[off++] = (byte) (values[i + 6] >>> 10);
    buf[off++] = (byte) (values[i + 6] >>> 2);

    buf[off] = (byte) (values[i + 6] << 6);
    buf[off++] |= values[i + 7] >>> 40;
    buf[off++] = (byte) (values[i + 7] >>> 32);
    buf[off++] = (byte) (values[i + 7] >>> 24);
    buf[off++] = (byte) (values[i + 7] >>> 16);
    buf[off++] = (byte) (values[i + 7] >>> 8);
    buf[off] = (byte) (values[i + 7]);
  }

  static void packBits47(final long[] values, final int i, final byte[] buf, int off) {
    buf[off++] = (byte) (values[i + 0] >>> 39);
    buf[off++] = (byte) (values[i + 0] >>> 31);
    buf[off++] = (byte) (values[i + 0] >>> 23);
    buf[off++] = (byte) (values[i + 0] >>> 15);
    buf[off++] = (byte) (values[i + 0] >>> 7);

    buf[off] = (byte) (values[i + 0] << 1);
    buf[off++] |= values[i + 1] >>> 46;
    buf[off++] = (byte) (values[i + 1] >>> 38);
    buf[off++] = (byte) (values[i + 1] >>> 30);
    buf[off++] = (byte) (values[i + 1] >>> 22);
    buf[off++] = (byte) (values[i + 1] >>> 14);
    buf[off++] = (byte) (values[i + 1] >>> 6);

    buf[off] = (byte) (values[i + 1] << 2);
    buf[off++] |= values[i + 2] >>> 45;
    buf[off++] = (byte) (values[i + 2] >>> 37);
    buf[off++] = (byte) (values[i + 2] >>> 29);
    buf[off++] = (byte) (values[i + 2] >>> 21);
    buf[off++] = (byte) (values[i + 2] >>> 13);
    buf[off++] = (byte) (values[i + 2] >>> 5);

    buf[off] = (byte) (values[i + 2] << 3);
    buf[off++] |= values[i + 3] >>> 44;
    buf[off++] = (byte) (values[i + 3] >>> 36);
    buf[off++] = (byte) (values[i + 3] >>> 28);
    buf[off++] = (byte) (values[i + 3] >>> 20);
    buf[off++] = (byte) (values[i + 3] >>> 12);
    buf[off++] = (byte) (values[i + 3] >>> 4);

    buf[off] = (byte) (values[i + 3] << 4);
    buf[off++] |= values[i + 4] >>> 43;
    buf[off++] = (byte) (values[i + 4] >>> 35);
    buf[off++] = (byte) (values[i + 4] >>> 27);
    buf[off++] = (byte) (values[i + 4] >>> 19);
    buf[off++] = (byte) (values[i + 4] >>> 11);
    buf[off++] = (byte) (values[i + 4] >>> 3);

    buf[off] = (byte) (values[i + 4] << 5);
    buf[off++] |= values[i + 5] >>> 42;
    buf[off++] = (byte) (values[i + 5] >>> 34);
    buf[off++] = (byte) (values[i + 5] >>> 26);
    buf[off++] = (byte) (values[i + 5] >>> 18);
    buf[off++] = (byte) (values[i + 5] >>> 10);
    buf[off++] = (byte) (values[i + 5] >>> 2);

    buf[off] = (byte) (values[i + 5] << 6);
    buf[off++] |= values[i + 6] >>> 41;
    buf[off++] = (byte) (values[i + 6] >>> 33);
    buf[off++] = (byte) (values[i + 6] >>> 25);
    buf[off++] = (byte) (values[i + 6] >>> 17);
    buf[off++] = (byte) (values[i + 6] >>> 9);
    buf[off++] = (byte) (values[i + 6] >>> 1);

    buf[off] = (byte) (values[i + 6] << 7);
    buf[off++] |= values[i + 7] >>> 40;
    buf[off++] = (byte) (values[i + 7] >>> 32);
    buf[off++] = (byte) (values[i + 7] >>> 24);
    buf[off++] = (byte) (values[i + 7] >>> 16);
    buf[off++] = (byte) (values[i + 7] >>> 8);
    buf[off] = (byte) (values[i + 7]);
  }

  static void packBits48(final long[] values, final int i, final byte[] buf, int off) {
    buf[off++] = (byte) (values[i + 0] >>> 40);
    buf[off++] = (byte) (values[i + 0] >>> 32);
    buf[off++] = (byte) (values[i + 0] >>> 24);
    buf[off++] = (byte) (values[i + 0] >>> 16);
    buf[off++] = (byte) (values[i + 0] >>> 8);
    buf[off++] = (byte) (values[i + 0]);

    buf[off++] = (byte) (values[i + 1] >>> 40);
    buf[off++] = (byte) (values[i + 1] >>> 32);
    buf[off++] = (byte) (values[i + 1] >>> 24);
    buf[off++] = (byte) (values[i + 1] >>> 16);
    buf[off++] = (byte) (values[i + 1] >>> 8);
    buf[off++] = (byte) (values[i + 1]);

    buf[off++] = (byte) (values[i + 2] >>> 40);
    buf[off++] = (byte) (values[i + 2] >>> 32);
    buf[off++] = (byte) (values[i + 2] >>> 24);
    buf[off++] = (byte) (values[i + 2] >>> 16);
    buf[off++] = (byte) (values[i + 2] >>> 8);
    buf[off++] = (byte) (values[i + 2]);

    buf[off++] = (byte) (values[i + 3] >>> 40);
    buf[off++] = (byte) (values[i + 3] >>> 32);
    buf[off++] = (byte) (values[i + 3] >>> 24);
    buf[off++] = (byte) (values[i + 3] >>> 16);
    buf[off++] = (byte) (values[i + 3] >>> 8);
    buf[off++] = (byte) (values[i + 3]);

    buf[off++] = (byte) (values[i + 4] >>> 40);
    buf[off++] = (byte) (values[i + 4] >>> 32);
    buf[off++] = (byte) (values[i + 4] >>> 24);
    buf[off++] = (byte) (values[i + 4] >>> 16);
    buf[off++] = (byte) (values[i + 4] >>> 8);
    buf[off++] = (byte) (values[i + 4]);

    buf[off++] = (byte) (values[i + 5] >>> 40);
    buf[off++] = (byte) (values[i + 5] >>> 32);
    buf[off++] = (byte) (values[i + 5] >>> 24);
    buf[off++] = (byte) (values[i + 5] >>> 16);
    buf[off++] = (byte) (values[i + 5] >>> 8);
    buf[off++] = (byte) (values[i + 5]);

    buf[off++] = (byte) (values[i + 6] >>> 40);
    buf[off++] = (byte) (values[i + 6] >>> 32);
    buf[off++] = (byte) (values[i + 6] >>> 24);
    buf[off++] = (byte) (values[i + 6] >>> 16);
    buf[off++] = (byte) (values[i + 6] >>> 8);
    buf[off++] = (byte) (values[i + 6]);

    buf[off++] = (byte) (values[i + 7] >>> 40);
    buf[off++] = (byte) (values[i + 7] >>> 32);
    buf[off++] = (byte) (values[i + 7] >>> 24);
    buf[off++] = (byte) (values[i + 7] >>> 16);
    buf[off++] = (byte) (values[i + 7] >>> 8);
    buf[off] = (byte) (values[i + 7]);
  }

  static void packBits49(final long[] values, final int i, final byte[] buf, int off) {
    buf[off++] = (byte) (values[i + 0] >>> 41);
    buf[off++] = (byte) (values[i + 0] >>> 33);
    buf[off++] = (byte) (values[i + 0] >>> 25);
    buf[off++] = (byte) (values[i + 0] >>> 17);
    buf[off++] = (byte) (values[i + 0] >>> 9);
    buf[off++] = (byte) (values[i + 0] >>> 1);

    buf[off] = (byte) (values[i + 0] << 7);
    buf[off++] |= values[i + 1] >>> 42;
    buf[off++] = (byte) (values[i + 1] >>> 34);
    buf[off++] = (byte) (values[i + 1] >>> 26);
    buf[off++] = (byte) (values[i + 1] >>> 18);
    buf[off++] = (byte) (values[i + 1] >>> 10);
    buf[off++] = (byte) (values[i + 1] >>> 2);

    buf[off] = (byte) (values[i + 1] << 6);
    buf[off++] |= values[i + 2] >>> 43;
    buf[off++] = (byte) (values[i + 2] >>> 35);
    buf[off++] = (byte) (values[i + 2] >>> 27);
    buf[off++] = (byte) (values[i + 2] >>> 19);
    buf[off++] = (byte) (values[i + 2] >>> 11);
    buf[off++] = (byte) (values[i + 2] >>> 3);

    buf[off] = (byte) (values[i + 2] << 5);
    buf[off++] |= values[i + 3] >>> 44;
    buf[off++] = (byte) (values[i + 3] >>> 36);
    buf[off++] = (byte) (values[i + 3] >>> 28);
    buf[off++] = (byte) (values[i + 3] >>> 20);
    buf[off++] = (byte) (values[i + 3] >>> 12);
    buf[off++] = (byte) (values[i + 3] >>> 4);

    buf[off] = (byte) (values[i + 3] << 4);
    buf[off++] |= values[i + 4] >>> 45;
    buf[off++] = (byte) (values[i + 4] >>> 37);
    buf[off++] = (byte) (values[i + 4] >>> 29);
    buf[off++] = (byte) (values[i + 4] >>> 21);
    buf[off++] = (byte) (values[i + 4] >>> 13);
    buf[off++] = (byte) (values[i + 4] >>> 5);

    buf[off] = (byte) (values[i + 4] << 3);
    buf[off++] |= values[i + 5] >>> 46;
    buf[off++] = (byte) (values[i + 5] >>> 38);
    buf[off++] = (byte) (values[i + 5] >>> 30);
    buf[off++] = (byte) (values[i + 5] >>> 22);
    buf[off++] = (byte) (values[i + 5] >>> 14);
    buf[off++] = (byte) (values[i + 5] >>> 6);

    buf[off] = (byte) (values[i + 5] << 2);
    buf[off++] |= values[i + 6] >>> 47;
    buf[off++] = (byte) (values[i + 6] >>> 39);
    buf[off++] = (byte) (values[i + 6] >>> 31);
    buf[off++] = (byte) (values[i + 6] >>> 23);
    buf[off++] = (byte) (values[i + 6] >>> 15);
    buf[off++] = (byte) (values[i + 6] >>> 7);

    buf[off] = (byte) (values[i + 6] << 1);
    buf[off++] |= values[i + 7] >>> 48;
    buf[off++] = (byte) (values[i + 7] >>> 40);
    buf[off++] = (byte) (values[i + 7] >>> 32);
    buf[off++] = (byte) (values[i + 7] >>> 24);
    buf[off++] = (byte) (values[i + 7] >>> 16);
    buf[off++] = (byte) (values[i + 7] >>> 8);
    buf[off] = (byte) (values[i + 7]);
  }

  static void packBits50(final long[] values, final int i, final byte[] buf, int off) {
    buf[off++] = (byte) (values[i + 0] >>> 42);
    buf[off++] = (byte) (values[i + 0] >>> 34);
    buf[off++] = (byte) (values[i + 0] >>> 26);
    buf[off++] = (byte) (values[i + 0] >>> 18);
    buf[off++] = (byte) (values[i + 0] >>> 10);
    buf[off++] = (byte) (values[i + 0] >>> 2);

    buf[off] = (byte) (values[i + 0] << 6);
    buf[off++] |= values[i + 1] >>> 44;
    buf[off++] = (byte) (values[i + 1] >>> 36);
    buf[off++] = (byte) (values[i + 1] >>> 28);
    buf[off++] = (byte) (values[i + 1] >>> 20);
    buf[off++] = (byte) (values[i + 1] >>> 12);
    buf[off++] = (byte) (values[i + 1] >>> 4);

    buf[off] = (byte) (values[i + 1] << 4);
    buf[off++] |= values[i + 2] >>> 46;
    buf[off++] = (byte) (values[i + 2] >>> 38);
    buf[off++] = (byte) (values[i + 2] >>> 30);
    buf[off++] = (byte) (values[i + 2] >>> 22);
    buf[off++] = (byte) (values[i + 2] >>> 14);
    buf[off++] = (byte) (values[i + 2] >>> 6);

    buf[off] = (byte) (values[i + 2] << 2);
    buf[off++] |= values[i + 3] >>> 48;
    buf[off++] = (byte) (values[i + 3] >>> 40);
    buf[off++] = (byte) (values[i + 3] >>> 32);
    buf[off++] = (byte) (values[i + 3] >>> 24);
    buf[off++] = (byte) (values[i + 3] >>> 16);
    buf[off++] = (byte) (values[i + 3] >>> 8);
    buf[off++] = (byte) (values[i + 3]);

    buf[off++] = (byte) (values[i + 4] >>> 42);
    buf[off++] = (byte) (values[i + 4] >>> 34);
    buf[off++] = (byte) (values[i + 4] >>> 26);
    buf[off++] = (byte) (values[i + 4] >>> 18);
    buf[off++] = (byte) (values[i + 4] >>> 10);
    buf[off++] = (byte) (values[i + 4] >>> 2);

    buf[off] = (byte) (values[i + 4] << 6);
    buf[off++] |= values[i + 5] >>> 44;
    buf[off++] = (byte) (values[i + 5] >>> 36);
    buf[off++] = (byte) (values[i + 5] >>> 28);
    buf[off++] = (byte) (values[i + 5] >>> 20);
    buf[off++] = (byte) (values[i + 5] >>> 12);
    buf[off++] = (byte) (values[i + 5] >>> 4);

    buf[off] = (byte) (values[i + 5] << 4);
    buf[off++] |= values[i + 6] >>> 46;
    buf[off++] = (byte) (values[i + 6] >>> 38);
    buf[off++] = (byte) (values[i + 6] >>> 30);
    buf[off++] = (byte) (values[i + 6] >>> 22);
    buf[off++] = (byte) (values[i + 6] >>> 14);
    buf[off++] = (byte) (values[i + 6] >>> 6);

    buf[off] = (byte) (values[i + 6] << 2);
    buf[off++] |= values[i + 7] >>> 48;
    buf[off++] = (byte) (values[i + 7] >>> 40);
    buf[off++] = (byte) (values[i + 7] >>> 32);
    buf[off++] = (byte) (values[i + 7] >>> 24);
    buf[off++] = (byte) (values[i + 7] >>> 16);
    buf[off++] = (byte) (values[i + 7] >>> 8);
    buf[off] = (byte) (values[i + 7]);
  }

  static void packBits51(final long[] values, final int i, final byte[] buf, int off) {
    buf[off++] = (byte) (values[i + 0] >>> 43);
    buf[off++] = (byte) (values[i + 0] >>> 35);
    buf[off++] = (byte) (values[i + 0] >>> 27);
    buf[off++] = (byte) (values[i + 0] >>> 19);
    buf[off++] = (byte) (values[i + 0] >>> 11);
    buf[off++] = (byte) (values[i + 0] >>> 3);

    buf[off] = (byte) (values[i + 0] << 5);
    buf[off++] |= values[i + 1] >>> 46;
    buf[off++] = (byte) (values[i + 1] >>> 38);
    buf[off++] = (byte) (values[i + 1] >>> 30);
    buf[off++] = (byte) (values[i + 1] >>> 22);
    buf[off++] = (byte) (values[i + 1] >>> 14);
    buf[off++] = (byte) (values[i + 1] >>> 6);

    buf[off] = (byte) (values[i + 1] << 2);
    buf[off++] |= values[i + 2] >>> 49;
    buf[off++] = (byte) (values[i + 2] >>> 41);
    buf[off++] = (byte) (values[i + 2] >>> 33);
    buf[off++] = (byte) (values[i + 2] >>> 25);
    buf[off++] = (byte) (values[i + 2] >>> 17);
    buf[off++] = (byte) (values[i + 2] >>> 9);
    buf[off++] = (byte) (values[i + 2] >>> 1);

    buf[off] = (byte) (values[i + 2] << 7);
    buf[off++] |= values[i + 3] >>> 44;
    buf[off++] = (byte) (values[i + 3] >>> 36);
    buf[off++] = (byte) (values[i + 3] >>> 28);
    buf[off++] = (byte) (values[i + 3] >>> 20);
    buf[off++] = (byte) (values[i + 3] >>> 12);
    buf[off++] = (byte) (values[i + 3] >>> 4);

    buf[off] = (byte) (values[i + 3] << 4);
    buf[off++] |= values[i + 4] >>> 47;
    buf[off++] = (byte) (values[i + 4] >>> 39);
    buf[off++] = (byte) (values[i + 4] >>> 31);
    buf[off++] = (byte) (values[i + 4] >>> 23);
    buf[off++] = (byte) (values[i + 4] >>> 15);
    buf[off++] = (byte) (values[i + 4] >>> 7);

    buf[off] = (byte) (values[i + 4] << 1);
    buf[off++] |= values[i + 5] >>> 50;
    buf[off++] = (byte) (values[i + 5] >>> 42);
    buf[off++] = (byte) (values[i + 5] >>> 34);
    buf[off++] = (byte) (values[i + 5] >>> 26);
    buf[off++] = (byte) (values[i + 5] >>> 18);
    buf[off++] = (byte) (values[i + 5] >>> 10);
    buf[off++] = (byte) (values[i + 5] >>> 2);

    buf[off] = (byte) (values[i + 5] << 6);
    buf[off++] |= values[i + 6] >>> 45;
    buf[off++] = (byte) (values[i + 6] >>> 37);
    buf[off++] = (byte) (values[i + 6] >>> 29);
    buf[off++] = (byte) (values[i + 6] >>> 21);
    buf[off++] = (byte) (values[i + 6] >>> 13);
    buf[off++] = (byte) (values[i + 6] >>> 5);

    buf[off] = (byte) (values[i + 6] << 3);
    buf[off++] |= values[i + 7] >>> 48;
    buf[off++] = (byte) (values[i + 7] >>> 40);
    buf[off++] = (byte) (values[i + 7] >>> 32);
    buf[off++] = (byte) (values[i + 7] >>> 24);
    buf[off++] = (byte) (values[i + 7] >>> 16);
    buf[off++] = (byte) (values[i + 7] >>> 8);
    buf[off] = (byte) (values[i + 7]);
  }

  static void packBits52(final long[] values, final int i, final byte[] buf, int off) {
    buf[off++] = (byte) (values[i + 0] >>> 44);
    buf[off++] = (byte) (values[i + 0] >>> 36);
    buf[off++] = (byte) (values[i + 0] >>> 28);
    buf[off++] = (byte) (values[i + 0] >>> 20);
    buf[off++] = (byte) (values[i + 0] >>> 12);
    buf[off++] = (byte) (values[i + 0] >>> 4);

    buf[off] = (byte) (values[i + 0] << 4);
    buf[off++] |= values[i + 1] >>> 48;
    buf[off++] = (byte) (values[i + 1] >>> 40);
    buf[off++] = (byte) (values[i + 1] >>> 32);
    buf[off++] = (byte) (values[i + 1] >>> 24);
    buf[off++] = (byte) (values[i + 1] >>> 16);
    buf[off++] = (byte) (values[i + 1] >>> 8);
    buf[off++] = (byte) (values[i + 1]);

    buf[off++] = (byte) (values[i + 2] >>> 44);
    buf[off++] = (byte) (values[i + 2] >>> 36);
    buf[off++] = (byte) (values[i + 2] >>> 28);
    buf[off++] = (byte) (values[i + 2] >>> 20);
    buf[off++] = (byte) (values[i + 2] >>> 12);
    buf[off++] = (byte) (values[i + 2] >>> 4);

    buf[off] = (byte) (values[i + 2] << 4);
    buf[off++] |= values[i + 3] >>> 48;
    buf[off++] = (byte) (values[i + 3] >>> 40);
    buf[off++] = (byte) (values[i + 3] >>> 32);
    buf[off++] = (byte) (values[i + 3] >>> 24);
    buf[off++] = (byte) (values[i + 3] >>> 16);
    buf[off++] = (byte) (values[i + 3] >>> 8);
    buf[off++] = (byte) (values[i + 3]);

    buf[off++] = (byte) (values[i + 4] >>> 44);
    buf[off++] = (byte) (values[i + 4] >>> 36);
    buf[off++] = (byte) (values[i + 4] >>> 28);
    buf[off++] = (byte) (values[i + 4] >>> 20);
    buf[off++] = (byte) (values[i + 4] >>> 12);
    buf[off++] = (byte) (values[i + 4] >>> 4);

    buf[off] = (byte) (values[i + 4] << 4);
    buf[off++] |= values[i + 5] >>> 48;
    buf[off++] = (byte) (values[i + 5] >>> 40);
    buf[off++] = (byte) (values[i + 5] >>> 32);
    buf[off++] = (byte) (values[i + 5] >>> 24);
    buf[off++] = (byte) (values[i + 5] >>> 16);
    buf[off++] = (byte) (values[i + 5] >>> 8);
    buf[off++] = (byte) (values[i + 5]);

    buf[off++] = (byte) (values[i + 6] >>> 44);
    buf[off++] = (byte) (values[i + 6] >>> 36);
    buf[off++] = (byte) (values[i + 6] >>> 28);
    buf[off++] = (byte) (values[i + 6] >>> 20);
    buf[off++] = (byte) (values[i + 6] >>> 12);
    buf[off++] = (byte) (values[i + 6] >>> 4);

    buf[off] = (byte) (values[i + 6] << 4);
    buf[off++] |= values[i + 7] >>> 48;
    buf[off++] = (byte) (values[i + 7] >>> 40);
    buf[off++] = (byte) (values[i + 7] >>> 32);
    buf[off++] = (byte) (values[i + 7] >>> 24);
    buf[off++] = (byte) (values[i + 7] >>> 16);
    buf[off++] = (byte) (values[i + 7] >>> 8);
    buf[off] = (byte) (values[i + 7]);
  }

  static void packBits53(final long[] values, final int i, final byte[] buf, int off) {
    buf[off++] = (byte) (values[i + 0] >>> 45);
    buf[off++] = (byte) (values[i + 0] >>> 37);
    buf[off++] = (byte) (values[i + 0] >>> 29);
    buf[off++] = (byte) (values[i + 0] >>> 21);
    buf[off++] = (byte) (values[i + 0] >>> 13);
    buf[off++] = (byte) (values[i + 0] >>> 5);

    buf[off] = (byte) (values[i + 0] << 3);
    buf[off++] |= values[i + 1] >>> 50;
    buf[off++] = (byte) (values[i + 1] >>> 42);
    buf[off++] = (byte) (values[i + 1] >>> 34);
    buf[off++] = (byte) (values[i + 1] >>> 26);
    buf[off++] = (byte) (values[i + 1] >>> 18);
    buf[off++] = (byte) (values[i + 1] >>> 10);
    buf[off++] = (byte) (values[i + 1] >>> 2);

    buf[off] = (byte) (values[i + 1] << 6);
    buf[off++] |= values[i + 2] >>> 47;
    buf[off++] = (byte) (values[i + 2] >>> 39);
    buf[off++] = (byte) (values[i + 2] >>> 31);
    buf[off++] = (byte) (values[i + 2] >>> 23);
    buf[off++] = (byte) (values[i + 2] >>> 15);
    buf[off++] = (byte) (values[i + 2] >>> 7);

    buf[off] = (byte) (values[i + 2] << 1);
    buf[off++] |= values[i + 3] >>> 52;
    buf[off++] = (byte) (values[i + 3] >>> 44);
    buf[off++] = (byte) (values[i + 3] >>> 36);
    buf[off++] = (byte) (values[i + 3] >>> 28);
    buf[off++] = (byte) (values[i + 3] >>> 20);
    buf[off++] = (byte) (values[i + 3] >>> 12);
    buf[off++] = (byte) (values[i + 3] >>> 4);

    buf[off] = (byte) (values[i + 3] << 4);
    buf[off++] |= values[i + 4] >>> 49;
    buf[off++] = (byte) (values[i + 4] >>> 41);
    buf[off++] = (byte) (values[i + 4] >>> 33);
    buf[off++] = (byte) (values[i + 4] >>> 25);
    buf[off++] = (byte) (values[i + 4] >>> 17);
    buf[off++] = (byte) (values[i + 4] >>> 9);
    buf[off++] = (byte) (values[i + 4] >>> 1);

    buf[off] = (byte) (values[i + 4] << 7);
    buf[off++] |= values[i + 5] >>> 46;
    buf[off++] = (byte) (values[i + 5] >>> 38);
    buf[off++] = (byte) (values[i + 5] >>> 30);
    buf[off++] = (byte) (values[i + 5] >>> 22);
    buf[off++] = (byte) (values[i + 5] >>> 14);
    buf[off++] = (byte) (values[i + 5] >>> 6);

    buf[off] = (byte) (values[i + 5] << 2);
    buf[off++] |= values[i + 6] >>> 51;
    buf[off++] = (byte) (values[i + 6] >>> 43);
    buf[off++] = (byte) (values[i + 6] >>> 35);
    buf[off++] = (byte) (values[i + 6] >>> 27);
    buf[off++] = (byte) (values[i + 6] >>> 19);
    buf[off++] = (byte) (values[i + 6] >>> 11);
    buf[off++] = (byte) (values[i + 6] >>> 3);

    buf[off] = (byte) (values[i + 6] << 5);
    buf[off++] |= values[i + 7] >>> 48;
    buf[off++] = (byte) (values[i + 7] >>> 40);
    buf[off++] = (byte) (values[i + 7] >>> 32);
    buf[off++] = (byte) (values[i + 7] >>> 24);
    buf[off++] = (byte) (values[i + 7] >>> 16);
    buf[off++] = (byte) (values[i + 7] >>> 8);
    buf[off] = (byte) (values[i + 7]);
  }

  static void packBits54(final long[] values, final int i, final byte[] buf, int off) {
    buf[off++] = (byte) (values[i + 0] >>> 46);
    buf[off++] = (byte) (values[i + 0] >>> 38);
    buf[off++] = (byte) (values[i + 0] >>> 30);
    buf[off++] = (byte) (values[i + 0] >>> 22);
    buf[off++] = (byte) (values[i + 0] >>> 14);
    buf[off++] = (byte) (values[i + 0] >>> 6);

    buf[off] = (byte) (values[i + 0] << 2);
    buf[off++] |= values[i + 1] >>> 52;
    buf[off++] = (byte) (values[i + 1] >>> 44);
    buf[off++] = (byte) (values[i + 1] >>> 36);
    buf[off++] = (byte) (values[i + 1] >>> 28);
    buf[off++] = (byte) (values[i + 1] >>> 20);
    buf[off++] = (byte) (values[i + 1] >>> 12);
    buf[off++] = (byte) (values[i + 1] >>> 4);

    buf[off] = (byte) (values[i + 1] << 4);
    buf[off++] |= values[i + 2] >>> 50;
    buf[off++] = (byte) (values[i + 2] >>> 42);
    buf[off++] = (byte) (values[i + 2] >>> 34);
    buf[off++] = (byte) (values[i + 2] >>> 26);
    buf[off++] = (byte) (values[i + 2] >>> 18);
    buf[off++] = (byte) (values[i + 2] >>> 10);
    buf[off++] = (byte) (values[i + 2] >>> 2);

    buf[off] = (byte) (values[i + 2] << 6);
    buf[off++] |= values[i + 3] >>> 48;
    buf[off++] = (byte) (values[i + 3] >>> 40);
    buf[off++] = (byte) (values[i + 3] >>> 32);
    buf[off++] = (byte) (values[i + 3] >>> 24);
    buf[off++] = (byte) (values[i + 3] >>> 16);
    buf[off++] = (byte) (values[i + 3] >>> 8);
    buf[off++] = (byte) (values[i + 3]);

    buf[off++] = (byte) (values[i + 4] >>> 46);
    buf[off++] = (byte) (values[i + 4] >>> 38);
    buf[off++] = (byte) (values[i + 4] >>> 30);
    buf[off++] = (byte) (values[i + 4] >>> 22);
    buf[off++] = (byte) (values[i + 4] >>> 14);
    buf[off++] = (byte) (values[i + 4] >>> 6);

    buf[off] = (byte) (values[i + 4] << 2);
    buf[off++] |= values[i + 5] >>> 52;
    buf[off++] = (byte) (values[i + 5] >>> 44);
    buf[off++] = (byte) (values[i + 5] >>> 36);
    buf[off++] = (byte) (values[i + 5] >>> 28);
    buf[off++] = (byte) (values[i + 5] >>> 20);
    buf[off++] = (byte) (values[i + 5] >>> 12);
    buf[off++] = (byte) (values[i + 5] >>> 4);

    buf[off] = (byte) (values[i + 5] << 4);
    buf[off++] |= values[i + 6] >>> 50;
    buf[off++] = (byte) (values[i + 6] >>> 42);
    buf[off++] = (byte) (values[i + 6] >>> 34);
    buf[off++] = (byte) (values[i + 6] >>> 26);
    buf[off++] = (byte) (values[i + 6] >>> 18);
    buf[off++] = (byte) (values[i + 6] >>> 10);
    buf[off++] = (byte) (values[i + 6] >>> 2);

    buf[off] = (byte) (values[i + 6] << 6);
    buf[off++] |= values[i + 7] >>> 48;
    buf[off++] = (byte) (values[i + 7] >>> 40);
    buf[off++] = (byte) (values[i + 7] >>> 32);
    buf[off++] = (byte) (values[i + 7] >>> 24);
    buf[off++] = (byte) (values[i + 7] >>> 16);
    buf[off++] = (byte) (values[i + 7] >>> 8);
    buf[off] = (byte) (values[i + 7]);
  }

  static void packBits55(final long[] values, final int i, final byte[] buf, int off) {
    buf[off++] = (byte) (values[i + 0] >>> 47);
    buf[off++] = (byte) (values[i + 0] >>> 39);
    buf[off++] = (byte) (values[i + 0] >>> 31);
    buf[off++] = (byte) (values[i + 0] >>> 23);
    buf[off++] = (byte) (values[i + 0] >>> 15);
    buf[off++] = (byte) (values[i + 0] >>> 7);

    buf[off] = (byte) (values[i + 0] << 1);
    buf[off++] |= values[i + 1] >>> 54;
    buf[off++] = (byte) (values[i + 1] >>> 46);
    buf[off++] = (byte) (values[i + 1] >>> 38);
    buf[off++] = (byte) (values[i + 1] >>> 30);
    buf[off++] = (byte) (values[i + 1] >>> 22);
    buf[off++] = (byte) (values[i + 1] >>> 14);
    buf[off++] = (byte) (values[i + 1] >>> 6);

    buf[off] = (byte) (values[i + 1] << 2);
    buf[off++] |= values[i + 2] >>> 53;
    buf[off++] = (byte) (values[i + 2] >>> 45);
    buf[off++] = (byte) (values[i + 2] >>> 37);
    buf[off++] = (byte) (values[i + 2] >>> 29);
    buf[off++] = (byte) (values[i + 2] >>> 21);
    buf[off++] = (byte) (values[i + 2] >>> 13);
    buf[off++] = (byte) (values[i + 2] >>> 5);

    buf[off] = (byte) (values[i + 2] << 3);
    buf[off++] |= values[i + 3] >>> 52;
    buf[off++] = (byte) (values[i + 3] >>> 44);
    buf[off++] = (byte) (values[i + 3] >>> 36);
    buf[off++] = (byte) (values[i + 3] >>> 28);
    buf[off++] = (byte) (values[i + 3] >>> 20);
    buf[off++] = (byte) (values[i + 3] >>> 12);
    buf[off++] = (byte) (values[i + 3] >>> 4);

    buf[off] = (byte) (values[i + 3] << 4);
    buf[off++] |= values[i + 4] >>> 51;
    buf[off++] = (byte) (values[i + 4] >>> 43);
    buf[off++] = (byte) (values[i + 4] >>> 35);
    buf[off++] = (byte) (values[i + 4] >>> 27);
    buf[off++] = (byte) (values[i + 4] >>> 19);
    buf[off++] = (byte) (values[i + 4] >>> 11);
    buf[off++] = (byte) (values[i + 4] >>> 3);

    buf[off] = (byte) (values[i + 4] << 5);
    buf[off++] |= values[i + 5] >>> 50;
    buf[off++] = (byte) (values[i + 5] >>> 42);
    buf[off++] = (byte) (values[i + 5] >>> 34);
    buf[off++] = (byte) (values[i + 5] >>> 26);
    buf[off++] = (byte) (values[i + 5] >>> 18);
    buf[off++] = (byte) (values[i + 5] >>> 10);
    buf[off++] = (byte) (values[i + 5] >>> 2);

    buf[off] = (byte) (values[i + 5] << 6);
    buf[off++] |= values[i + 6] >>> 49;
    buf[off++] = (byte) (values[i + 6] >>> 41);
    buf[off++] = (byte) (values[i + 6] >>> 33);
    buf[off++] = (byte) (values[i + 6] >>> 25);
    buf[off++] = (byte) (values[i + 6] >>> 17);
    buf[off++] = (byte) (values[i + 6] >>> 9);
    buf[off++] = (byte) (values[i + 6] >>> 1);

    buf[off] = (byte) (values[i + 6] << 7);
    buf[off++] |= values[i + 7] >>> 48;
    buf[off++] = (byte) (values[i + 7] >>> 40);
    buf[off++] = (byte) (values[i + 7] >>> 32);
    buf[off++] = (byte) (values[i + 7] >>> 24);
    buf[off++] = (byte) (values[i + 7] >>> 16);
    buf[off++] = (byte) (values[i + 7] >>> 8);
    buf[off] = (byte) (values[i + 7]);
  }

  static void packBits56(final long[] values, final int i, final byte[] buf, int off) {
    buf[off++] = (byte) (values[i + 0] >>> 48);
    buf[off++] = (byte) (values[i + 0] >>> 40);
    buf[off++] = (byte) (values[i + 0] >>> 32);
    buf[off++] = (byte) (values[i + 0] >>> 24);
    buf[off++] = (byte) (values[i + 0] >>> 16);
    buf[off++] = (byte) (values[i + 0] >>> 8);
    buf[off++] = (byte) (values[i + 0]);

    buf[off++] = (byte) (values[i + 1] >>> 48);
    buf[off++] = (byte) (values[i + 1] >>> 40);
    buf[off++] = (byte) (values[i + 1] >>> 32);
    buf[off++] = (byte) (values[i + 1] >>> 24);
    buf[off++] = (byte) (values[i + 1] >>> 16);
    buf[off++] = (byte) (values[i + 1] >>> 8);
    buf[off++] = (byte) (values[i + 1]);

    buf[off++] = (byte) (values[i + 2] >>> 48);
    buf[off++] = (byte) (values[i + 2] >>> 40);
    buf[off++] = (byte) (values[i + 2] >>> 32);
    buf[off++] = (byte) (values[i + 2] >>> 24);
    buf[off++] = (byte) (values[i + 2] >>> 16);
    buf[off++] = (byte) (values[i + 2] >>> 8);
    buf[off++] = (byte) (values[i + 2]);

    buf[off++] = (byte) (values[i + 3] >>> 48);
    buf[off++] = (byte) (values[i + 3] >>> 40);
    buf[off++] = (byte) (values[i + 3] >>> 32);
    buf[off++] = (byte) (values[i + 3] >>> 24);
    buf[off++] = (byte) (values[i + 3] >>> 16);
    buf[off++] = (byte) (values[i + 3] >>> 8);
    buf[off++] = (byte) (values[i + 3]);

    buf[off++] = (byte) (values[i + 4] >>> 48);
    buf[off++] = (byte) (values[i + 4] >>> 40);
    buf[off++] = (byte) (values[i + 4] >>> 32);
    buf[off++] = (byte) (values[i + 4] >>> 24);
    buf[off++] = (byte) (values[i + 4] >>> 16);
    buf[off++] = (byte) (values[i + 4] >>> 8);
    buf[off++] = (byte) (values[i + 4]);

    buf[off++] = (byte) (values[i + 5] >>> 48);
    buf[off++] = (byte) (values[i + 5] >>> 40);
    buf[off++] = (byte) (values[i + 5] >>> 32);
    buf[off++] = (byte) (values[i + 5] >>> 24);
    buf[off++] = (byte) (values[i + 5] >>> 16);
    buf[off++] = (byte) (values[i + 5] >>> 8);
    buf[off++] = (byte) (values[i + 5]);

    buf[off++] = (byte) (values[i + 6] >>> 48);
    buf[off++] = (byte) (values[i + 6] >>> 40);
    buf[off++] = (byte) (values[i + 6] >>> 32);
    buf[off++] = (byte) (values[i + 6] >>> 24);
    buf[off++] = (byte) (values[i + 6] >>> 16);
    buf[off++] = (byte) (values[i + 6] >>> 8);
    buf[off++] = (byte) (values[i + 6]);

    buf[off++] = (byte) (values[i + 7] >>> 48);
    buf[off++] = (byte) (values[i + 7] >>> 40);
    buf[off++] = (byte) (values[i + 7] >>> 32);
    buf[off++] = (byte) (values[i + 7] >>> 24);
    buf[off++] = (byte) (values[i + 7] >>> 16);
    buf[off++] = (byte) (values[i + 7] >>> 8);
    buf[off] = (byte) (values[i + 7]);
  }

  static void packBits57(final long[] values, final int i, final byte[] buf, int off) {
    buf[off++] = (byte) (values[i + 0] >>> 49);
    buf[off++] = (byte) (values[i + 0] >>> 41);
    buf[off++] = (byte) (values[i + 0] >>> 33);
    buf[off++] = (byte) (values[i + 0] >>> 25);
    buf[off++] = (byte) (values[i + 0] >>> 17);
    buf[off++] = (byte) (values[i + 0] >>> 9);
    buf[off++] = (byte) (values[i + 0] >>> 1);

    buf[off] = (byte) (values[i + 0] << 7);
    buf[off++] |= values[i + 1] >>> 50;
    buf[off++] = (byte) (values[i + 1] >>> 42);
    buf[off++] = (byte) (values[i + 1] >>> 34);
    buf[off++] = (byte) (values[i + 1] >>> 26);
    buf[off++] = (byte) (values[i + 1] >>> 18);
    buf[off++] = (byte) (values[i + 1] >>> 10);
    buf[off++] = (byte) (values[i + 1] >>> 2);

    buf[off] = (byte) (values[i + 1] << 6);
    buf[off++] |= values[i + 2] >>> 51;
    buf[off++] = (byte) (values[i + 2] >>> 43);
    buf[off++] = (byte) (values[i + 2] >>> 35);
    buf[off++] = (byte) (values[i + 2] >>> 27);
    buf[off++] = (byte) (values[i + 2] >>> 19);
    buf[off++] = (byte) (values[i + 2] >>> 11);
    buf[off++] = (byte) (values[i + 2] >>> 3);

    buf[off] = (byte) (values[i + 2] << 5);
    buf[off++] |= values[i + 3] >>> 52;
    buf[off++] = (byte) (values[i + 3] >>> 44);
    buf[off++] = (byte) (values[i + 3] >>> 36);
    buf[off++] = (byte) (values[i + 3] >>> 28);
    buf[off++] = (byte) (values[i + 3] >>> 20);
    buf[off++] = (byte) (values[i + 3] >>> 12);
    buf[off++] = (byte) (values[i + 3] >>> 4);

    buf[off] = (byte) (values[i + 3] << 4);
    buf[off++] |= values[i + 4] >>> 53;
    buf[off++] = (byte) (values[i + 4] >>> 45);
    buf[off++] = (byte) (values[i + 4] >>> 37);
    buf[off++] = (byte) (values[i + 4] >>> 29);
    buf[off++] = (byte) (values[i + 4] >>> 21);
    buf[off++] = (byte) (values[i + 4] >>> 13);
    buf[off++] = (byte) (values[i + 4] >>> 5);

    buf[off] = (byte) (values[i + 4] << 3);
    buf[off++] |= values[i + 5] >>> 54;
    buf[off++] = (byte) (values[i + 5] >>> 46);
    buf[off++] = (byte) (values[i + 5] >>> 38);
    buf[off++] = (byte) (values[i + 5] >>> 30);
    buf[off++] = (byte) (values[i + 5] >>> 22);
    buf[off++] = (byte) (values[i + 5] >>> 14);
    buf[off++] = (byte) (values[i + 5] >>> 6);

    buf[off] = (byte) (values[i + 5] << 2);
    buf[off++] |= values[i + 6] >>> 55;
    buf[off++] = (byte) (values[i + 6] >>> 47);
    buf[off++] = (byte) (values[i + 6] >>> 39);
    buf[off++] = (byte) (values[i + 6] >>> 31);
    buf[off++] = (byte) (values[i + 6] >>> 23);
    buf[off++] = (byte) (values[i + 6] >>> 15);
    buf[off++] = (byte) (values[i + 6] >>> 7);

    buf[off] = (byte) (values[i + 6] << 1);
    buf[off++] |= values[i + 7] >>> 56;
    buf[off++] = (byte) (values[i + 7] >>> 48);
    buf[off++] = (byte) (values[i + 7] >>> 40);
    buf[off++] = (byte) (values[i + 7] >>> 32);
    buf[off++] = (byte) (values[i + 7] >>> 24);
    buf[off++] = (byte) (values[i + 7] >>> 16);
    buf[off++] = (byte) (values[i + 7] >>> 8);
    buf[off] = (byte) (values[i + 7]);
  }

  static void packBits58(final long[] values, final int i, final byte[] buf, int off) {
    buf[off++] = (byte) (values[i + 0] >>> 50);
    buf[off++] = (byte) (values[i + 0] >>> 42);
    buf[off++] = (byte) (values[i + 0] >>> 34);
    buf[off++] = (byte) (values[i + 0] >>> 26);
    buf[off++] = (byte) (values[i + 0] >>> 18);
    buf[off++] = (byte) (values[i + 0] >>> 10);
    buf[off++] = (byte) (values[i + 0] >>> 2);

    buf[off] = (byte) (values[i + 0] << 6);
    buf[off++] |= values[i + 1] >>> 52;
    buf[off++] = (byte) (values[i + 1] >>> 44);
    buf[off++] = (byte) (values[i + 1] >>> 36);
    buf[off++] = (byte) (values[i + 1] >>> 28);
    buf[off++] = (byte) (values[i + 1] >>> 20);
    buf[off++] = (byte) (values[i + 1] >>> 12);
    buf[off++] = (byte) (values[i + 1] >>> 4);

    buf[off] = (byte) (values[i + 1] << 4);
    buf[off++] |= values[i + 2] >>> 54;
    buf[off++] = (byte) (values[i + 2] >>> 46);
    buf[off++] = (byte) (values[i + 2] >>> 38);
    buf[off++] = (byte) (values[i + 2] >>> 30);
    buf[off++] = (byte) (values[i + 2] >>> 22);
    buf[off++] = (byte) (values[i + 2] >>> 14);
    buf[off++] = (byte) (values[i + 2] >>> 6);

    buf[off] = (byte) (values[i + 2] << 2);
    buf[off++] |= values[i + 3] >>> 56;
    buf[off++] = (byte) (values[i + 3] >>> 48);
    buf[off++] = (byte) (values[i + 3] >>> 40);
    buf[off++] = (byte) (values[i + 3] >>> 32);
    buf[off++] = (byte) (values[i + 3] >>> 24);
    buf[off++] = (byte) (values[i + 3] >>> 16);
    buf[off++] = (byte) (values[i + 3] >>> 8);
    buf[off++] = (byte) (values[i + 3]);

    buf[off++] = (byte) (values[i + 4] >>> 50);
    buf[off++] = (byte) (values[i + 4] >>> 42);
    buf[off++] = (byte) (values[i + 4] >>> 34);
    buf[off++] = (byte) (values[i + 4] >>> 26);
    buf[off++] = (byte) (values[i + 4] >>> 18);
    buf[off++] = (byte) (values[i + 4] >>> 10);
    buf[off++] = (byte) (values[i + 4] >>> 2);

    buf[off] = (byte) (values[i + 4] << 6);
    buf[off++] |= values[i + 5] >>> 52;
    buf[off++] = (byte) (values[i + 5] >>> 44);
    buf[off++] = (byte) (values[i + 5] >>> 36);
    buf[off++] = (byte) (values[i + 5] >>> 28);
    buf[off++] = (byte) (values[i + 5] >>> 20);
    buf[off++] = (byte) (values[i + 5] >>> 12);
    buf[off++] = (byte) (values[i + 5] >>> 4);

    buf[off] = (byte) (values[i + 5] << 4);
    buf[off++] |= values[i + 6] >>> 54;
    buf[off++] = (byte) (values[i + 6] >>> 46);
    buf[off++] = (byte) (values[i + 6] >>> 38);
    buf[off++] = (byte) (values[i + 6] >>> 30);
    buf[off++] = (byte) (values[i + 6] >>> 22);
    buf[off++] = (byte) (values[i + 6] >>> 14);
    buf[off++] = (byte) (values[i + 6] >>> 6);

    buf[off] = (byte) (values[i + 6] << 2);
    buf[off++] |= values[i + 7] >>> 56;
    buf[off++] = (byte) (values[i + 7] >>> 48);
    buf[off++] = (byte) (values[i + 7] >>> 40);
    buf[off++] = (byte) (values[i + 7] >>> 32);
    buf[off++] = (byte) (values[i + 7] >>> 24);
    buf[off++] = (byte) (values[i + 7] >>> 16);
    buf[off++] = (byte) (values[i + 7] >>> 8);
    buf[off] = (byte) (values[i + 7]);
  }

  static void packBits59(final long[] values, final int i, final byte[] buf, int off) {
    buf[off++] = (byte) (values[i + 0] >>> 51);
    buf[off++] = (byte) (values[i + 0] >>> 43);
    buf[off++] = (byte) (values[i + 0] >>> 35);
    buf[off++] = (byte) (values[i + 0] >>> 27);
    buf[off++] = (byte) (values[i + 0] >>> 19);
    buf[off++] = (byte) (values[i + 0] >>> 11);
    buf[off++] = (byte) (values[i + 0] >>> 3);

    buf[off] = (byte) (values[i + 0] << 5);
    buf[off++] |= values[i + 1] >>> 54;
    buf[off++] = (byte) (values[i + 1] >>> 46);
    buf[off++] = (byte) (values[i + 1] >>> 38);
    buf[off++] = (byte) (values[i + 1] >>> 30);
    buf[off++] = (byte) (values[i + 1] >>> 22);
    buf[off++] = (byte) (values[i + 1] >>> 14);
    buf[off++] = (byte) (values[i + 1] >>> 6);

    buf[off] = (byte) (values[i + 1] << 2);
    buf[off++] |= values[i + 2] >>> 57;
    buf[off++] = (byte) (values[i + 2] >>> 49);
    buf[off++] = (byte) (values[i + 2] >>> 41);
    buf[off++] = (byte) (values[i + 2] >>> 33);
    buf[off++] = (byte) (values[i + 2] >>> 25);
    buf[off++] = (byte) (values[i + 2] >>> 17);
    buf[off++] = (byte) (values[i + 2] >>> 9);
    buf[off++] = (byte) (values[i + 2] >>> 1);

    buf[off] = (byte) (values[i + 2] << 7);
    buf[off++] |= values[i + 3] >>> 52;
    buf[off++] = (byte) (values[i + 3] >>> 44);
    buf[off++] = (byte) (values[i + 3] >>> 36);
    buf[off++] = (byte) (values[i + 3] >>> 28);
    buf[off++] = (byte) (values[i + 3] >>> 20);
    buf[off++] = (byte) (values[i + 3] >>> 12);
    buf[off++] = (byte) (values[i + 3] >>> 4);

    buf[off] = (byte) (values[i + 3] << 4);
    buf[off++] |= values[i + 4] >>> 55;
    buf[off++] = (byte) (values[i + 4] >>> 47);
    buf[off++] = (byte) (values[i + 4] >>> 39);
    buf[off++] = (byte) (values[i + 4] >>> 31);
    buf[off++] = (byte) (values[i + 4] >>> 23);
    buf[off++] = (byte) (values[i + 4] >>> 15);
    buf[off++] = (byte) (values[i + 4] >>> 7);

    buf[off] = (byte) (values[i + 4] << 1);
    buf[off++] |= values[i + 5] >>> 58;
    buf[off++] = (byte) (values[i + 5] >>> 50);
    buf[off++] = (byte) (values[i + 5] >>> 42);
    buf[off++] = (byte) (values[i + 5] >>> 34);
    buf[off++] = (byte) (values[i + 5] >>> 26);
    buf[off++] = (byte) (values[i + 5] >>> 18);
    buf[off++] = (byte) (values[i + 5] >>> 10);
    buf[off++] = (byte) (values[i + 5] >>> 2);

    buf[off] = (byte) (values[i + 5] << 6);
    buf[off++] |= values[i + 6] >>> 53;
    buf[off++] = (byte) (values[i + 6] >>> 45);
    buf[off++] = (byte) (values[i + 6] >>> 37);
    buf[off++] = (byte) (values[i + 6] >>> 29);
    buf[off++] = (byte) (values[i + 6] >>> 21);
    buf[off++] = (byte) (values[i + 6] >>> 13);
    buf[off++] = (byte) (values[i + 6] >>> 5);

    buf[off] = (byte) (values[i + 6] << 3);
    buf[off++] |= values[i + 7] >>> 56;
    buf[off++] = (byte) (values[i + 7] >>> 48);
    buf[off++] = (byte) (values[i + 7] >>> 40);
    buf[off++] = (byte) (values[i + 7] >>> 32);
    buf[off++] = (byte) (values[i + 7] >>> 24);
    buf[off++] = (byte) (values[i + 7] >>> 16);
    buf[off++] = (byte) (values[i + 7] >>> 8);
    buf[off] = (byte) (values[i + 7]);
  }

  static void packBits60(final long[] values, final int i, final byte[] buf, int off) {
    buf[off++] = (byte) (values[i + 0] >>> 52);
    buf[off++] = (byte) (values[i + 0] >>> 44);
    buf[off++] = (byte) (values[i + 0] >>> 36);
    buf[off++] = (byte) (values[i + 0] >>> 28);
    buf[off++] = (byte) (values[i + 0] >>> 20);
    buf[off++] = (byte) (values[i + 0] >>> 12);
    buf[off++] = (byte) (values[i + 0] >>> 4);

    buf[off] = (byte) (values[i + 0] << 4);
    buf[off++] |= values[i + 1] >>> 56;
    buf[off++] = (byte) (values[i + 1] >>> 48);
    buf[off++] = (byte) (values[i + 1] >>> 40);
    buf[off++] = (byte) (values[i + 1] >>> 32);
    buf[off++] = (byte) (values[i + 1] >>> 24);
    buf[off++] = (byte) (values[i + 1] >>> 16);
    buf[off++] = (byte) (values[i + 1] >>> 8);
    buf[off++] = (byte) (values[i + 1]);

    buf[off++] = (byte) (values[i + 2] >>> 52);
    buf[off++] = (byte) (values[i + 2] >>> 44);
    buf[off++] = (byte) (values[i + 2] >>> 36);
    buf[off++] = (byte) (values[i + 2] >>> 28);
    buf[off++] = (byte) (values[i + 2] >>> 20);
    buf[off++] = (byte) (values[i + 2] >>> 12);
    buf[off++] = (byte) (values[i + 2] >>> 4);

    buf[off] = (byte) (values[i + 2] << 4);
    buf[off++] |= values[i + 3] >>> 56;
    buf[off++] = (byte) (values[i + 3] >>> 48);
    buf[off++] = (byte) (values[i + 3] >>> 40);
    buf[off++] = (byte) (values[i + 3] >>> 32);
    buf[off++] = (byte) (values[i + 3] >>> 24);
    buf[off++] = (byte) (values[i + 3] >>> 16);
    buf[off++] = (byte) (values[i + 3] >>> 8);
    buf[off++] = (byte) (values[i + 3]);

    buf[off++] = (byte) (values[i + 4] >>> 52);
    buf[off++] = (byte) (values[i + 4] >>> 44);
    buf[off++] = (byte) (values[i + 4] >>> 36);
    buf[off++] = (byte) (values[i + 4] >>> 28);
    buf[off++] = (byte) (values[i + 4] >>> 20);
    buf[off++] = (byte) (values[i + 4] >>> 12);
    buf[off++] = (byte) (values[i + 4] >>> 4);

    buf[off] = (byte) (values[i + 4] << 4);
    buf[off++] |= values[i + 5] >>> 56;
    buf[off++] = (byte) (values[i + 5] >>> 48);
    buf[off++] = (byte) (values[i + 5] >>> 40);
    buf[off++] = (byte) (values[i + 5] >>> 32);
    buf[off++] = (byte) (values[i + 5] >>> 24);
    buf[off++] = (byte) (values[i + 5] >>> 16);
    buf[off++] = (byte) (values[i + 5] >>> 8);
    buf[off++] = (byte) (values[i + 5]);

    buf[off++] = (byte) (values[i + 6] >>> 52);
    buf[off++] = (byte) (values[i + 6] >>> 44);
    buf[off++] = (byte) (values[i + 6] >>> 36);
    buf[off++] = (byte) (values[i + 6] >>> 28);
    buf[off++] = (byte) (values[i + 6] >>> 20);
    buf[off++] = (byte) (values[i + 6] >>> 12);
    buf[off++] = (byte) (values[i + 6] >>> 4);

    buf[off] = (byte) (values[i + 6] << 4);
    buf[off++] |= values[i + 7] >>> 56;
    buf[off++] = (byte) (values[i + 7] >>> 48);
    buf[off++] = (byte) (values[i + 7] >>> 40);
    buf[off++] = (byte) (values[i + 7] >>> 32);
    buf[off++] = (byte) (values[i + 7] >>> 24);
    buf[off++] = (byte) (values[i + 7] >>> 16);
    buf[off++] = (byte) (values[i + 7] >>> 8);
    buf[off] = (byte) (values[i + 7]);
  }

  static void packBits61(final long[] values, final int i, final byte[] buf, int off) {
    buf[off++] = (byte) (values[i + 0] >>> 53);
    buf[off++] = (byte) (values[i + 0] >>> 45);
    buf[off++] = (byte) (values[i + 0] >>> 37);
    buf[off++] = (byte) (values[i + 0] >>> 29);
    buf[off++] = (byte) (values[i + 0] >>> 21);
    buf[off++] = (byte) (values[i + 0] >>> 13);
    buf[off++] = (byte) (values[i + 0] >>> 5);

    buf[off] = (byte) (values[i + 0] << 3);
    buf[off++] |= values[i + 1] >>> 58;
    buf[off++] = (byte) (values[i + 1] >>> 50);
    buf[off++] = (byte) (values[i + 1] >>> 42);
    buf[off++] = (byte) (values[i + 1] >>> 34);
    buf[off++] = (byte) (values[i + 1] >>> 26);
    buf[off++] = (byte) (values[i + 1] >>> 18);
    buf[off++] = (byte) (values[i + 1] >>> 10);
    buf[off++] = (byte) (values[i + 1] >>> 2);

    buf[off] = (byte) (values[i + 1] << 6);
    buf[off++] |= values[i + 2] >>> 55;
    buf[off++] = (byte) (values[i + 2] >>> 47);
    buf[off++] = (byte) (values[i + 2] >>> 39);
    buf[off++] = (byte) (values[i + 2] >>> 31);
    buf[off++] = (byte) (values[i + 2] >>> 23);
    buf[off++] = (byte) (values[i + 2] >>> 15);
    buf[off++] = (byte) (values[i + 2] >>> 7);

    buf[off] = (byte) (values[i + 2] << 1);
    buf[off++] |= values[i + 3] >>> 60;
    buf[off++] = (byte) (values[i + 3] >>> 52);
    buf[off++] = (byte) (values[i + 3] >>> 44);
    buf[off++] = (byte) (values[i + 3] >>> 36);
    buf[off++] = (byte) (values[i + 3] >>> 28);
    buf[off++] = (byte) (values[i + 3] >>> 20);
    buf[off++] = (byte) (values[i + 3] >>> 12);
    buf[off++] = (byte) (values[i + 3] >>> 4);

    buf[off] = (byte) (values[i + 3] << 4);
    buf[off++] |= values[i + 4] >>> 57;
    buf[off++] = (byte) (values[i + 4] >>> 49);
    buf[off++] = (byte) (values[i + 4] >>> 41);
    buf[off++] = (byte) (values[i + 4] >>> 33);
    buf[off++] = (byte) (values[i + 4] >>> 25);
    buf[off++] = (byte) (values[i + 4] >>> 17);
    buf[off++] = (byte) (values[i + 4] >>> 9);
    buf[off++] = (byte) (values[i + 4] >>> 1);

    buf[off] = (byte) (values[i + 4] << 7);
    buf[off++] |= values[i + 5] >>> 54;
    buf[off++] = (byte) (values[i + 5] >>> 46);
    buf[off++] = (byte) (values[i + 5] >>> 38);
    buf[off++] = (byte) (values[i + 5] >>> 30);
    buf[off++] = (byte) (values[i + 5] >>> 22);
    buf[off++] = (byte) (values[i + 5] >>> 14);
    buf[off++] = (byte) (values[i + 5] >>> 6);

    buf[off] = (byte) (values[i + 5] << 2);
    buf[off++] |= values[i + 6] >>> 59;
    buf[off++] = (byte) (values[i + 6] >>> 51);
    buf[off++] = (byte) (values[i + 6] >>> 43);
    buf[off++] = (byte) (values[i + 6] >>> 35);
    buf[off++] = (byte) (values[i + 6] >>> 27);
    buf[off++] = (byte) (values[i + 6] >>> 19);
    buf[off++] = (byte) (values[i + 6] >>> 11);
    buf[off++] = (byte) (values[i + 6] >>> 3);

    buf[off] = (byte) (values[i + 6] << 5);
    buf[off++] |= values[i + 7] >>> 56;
    buf[off++] = (byte) (values[i + 7] >>> 48);
    buf[off++] = (byte) (values[i + 7] >>> 40);
    buf[off++] = (byte) (values[i + 7] >>> 32);
    buf[off++] = (byte) (values[i + 7] >>> 24);
    buf[off++] = (byte) (values[i + 7] >>> 16);
    buf[off++] = (byte) (values[i + 7] >>> 8);
    buf[off] = (byte) (values[i + 7]);
  }

  static void packBits62(final long[] values, final int i, final byte[] buf, int off) {
    buf[off++] = (byte) (values[i + 0] >>> 54);
    buf[off++] = (byte) (values[i + 0] >>> 46);
    buf[off++] = (byte) (values[i + 0] >>> 38);
    buf[off++] = (byte) (values[i + 0] >>> 30);
    buf[off++] = (byte) (values[i + 0] >>> 22);
    buf[off++] = (byte) (values[i + 0] >>> 14);
    buf[off++] = (byte) (values[i + 0] >>> 6);

    buf[off] = (byte) (values[i + 0] << 2);
    buf[off++] |= values[i + 1] >>> 60;
    buf[off++] = (byte) (values[i + 1] >>> 52);
    buf[off++] = (byte) (values[i + 1] >>> 44);
    buf[off++] = (byte) (values[i + 1] >>> 36);
    buf[off++] = (byte) (values[i + 1] >>> 28);
    buf[off++] = (byte) (values[i + 1] >>> 20);
    buf[off++] = (byte) (values[i + 1] >>> 12);
    buf[off++] = (byte) (values[i + 1] >>> 4);

    buf[off] = (byte) (values[i + 1] << 4);
    buf[off++] |= values[i + 2] >>> 58;
    buf[off++] = (byte) (values[i + 2] >>> 50);
    buf[off++] = (byte) (values[i + 2] >>> 42);
    buf[off++] = (byte) (values[i + 2] >>> 34);
    buf[off++] = (byte) (values[i + 2] >>> 26);
    buf[off++] = (byte) (values[i + 2] >>> 18);
    buf[off++] = (byte) (values[i + 2] >>> 10);
    buf[off++] = (byte) (values[i + 2] >>> 2);

    buf[off] = (byte) (values[i + 2] << 6);
    buf[off++] |= values[i + 3] >>> 56;
    buf[off++] = (byte) (values[i + 3] >>> 48);
    buf[off++] = (byte) (values[i + 3] >>> 40);
    buf[off++] = (byte) (values[i + 3] >>> 32);
    buf[off++] = (byte) (values[i + 3] >>> 24);
    buf[off++] = (byte) (values[i + 3] >>> 16);
    buf[off++] = (byte) (values[i + 3] >>> 8);
    buf[off++] = (byte) (values[i + 3]);

    buf[off++] = (byte) (values[i + 4] >>> 54);
    buf[off++] = (byte) (values[i + 4] >>> 46);
    buf[off++] = (byte) (values[i + 4] >>> 38);
    buf[off++] = (byte) (values[i + 4] >>> 30);
    buf[off++] = (byte) (values[i + 4] >>> 22);
    buf[off++] = (byte) (values[i + 4] >>> 14);
    buf[off++] = (byte) (values[i + 4] >>> 6);

    buf[off] = (byte) (values[i + 4] << 2);
    buf[off++] |= values[i + 5] >>> 60;
    buf[off++] = (byte) (values[i + 5] >>> 52);
    buf[off++] = (byte) (values[i + 5] >>> 44);
    buf[off++] = (byte) (values[i + 5] >>> 36);
    buf[off++] = (byte) (values[i + 5] >>> 28);
    buf[off++] = (byte) (values[i + 5] >>> 20);
    buf[off++] = (byte) (values[i + 5] >>> 12);
    buf[off++] = (byte) (values[i + 5] >>> 4);

    buf[off] = (byte) (values[i + 5] << 4);
    buf[off++] |= values[i + 6] >>> 58;
    buf[off++] = (byte) (values[i + 6] >>> 50);
    buf[off++] = (byte) (values[i + 6] >>> 42);
    buf[off++] = (byte) (values[i + 6] >>> 34);
    buf[off++] = (byte) (values[i + 6] >>> 26);
    buf[off++] = (byte) (values[i + 6] >>> 18);
    buf[off++] = (byte) (values[i + 6] >>> 10);
    buf[off++] = (byte) (values[i + 6] >>> 2);

    buf[off] = (byte) (values[i + 6] << 6);
    buf[off++] |= values[i + 7] >>> 56;
    buf[off++] = (byte) (values[i + 7] >>> 48);
    buf[off++] = (byte) (values[i + 7] >>> 40);
    buf[off++] = (byte) (values[i + 7] >>> 32);
    buf[off++] = (byte) (values[i + 7] >>> 24);
    buf[off++] = (byte) (values[i + 7] >>> 16);
    buf[off++] = (byte) (values[i + 7] >>> 8);
    buf[off] = (byte) (values[i + 7]);
  }

  static void packBits63(final long[] values, final int i, final byte[] buf, int off) {
    buf[off++] = (byte) (values[i + 0] >>> 55);
    buf[off++] = (byte) (values[i + 0] >>> 47);
    buf[off++] = (byte) (values[i + 0] >>> 39);
    buf[off++] = (byte) (values[i + 0] >>> 31);
    buf[off++] = (byte) (values[i + 0] >>> 23);
    buf[off++] = (byte) (values[i + 0] >>> 15);
    buf[off++] = (byte) (values[i + 0] >>> 7);

    buf[off] = (byte) (values[i + 0] << 1);
    buf[off++] |= values[i + 1] >>> 62;
    buf[off++] = (byte) (values[i + 1] >>> 54);
    buf[off++] = (byte) (values[i + 1] >>> 46);
    buf[off++] = (byte) (values[i + 1] >>> 38);
    buf[off++] = (byte) (values[i + 1] >>> 30);
    buf[off++] = (byte) (values[i + 1] >>> 22);
    buf[off++] = (byte) (values[i + 1] >>> 14);
    buf[off++] = (byte) (values[i + 1] >>> 6);

    buf[off] = (byte) (values[i + 1] << 2);
    buf[off++] |= values[i + 2] >>> 61;
    buf[off++] = (byte) (values[i + 2] >>> 53);
    buf[off++] = (byte) (values[i + 2] >>> 45);
    buf[off++] = (byte) (values[i + 2] >>> 37);
    buf[off++] = (byte) (values[i + 2] >>> 29);
    buf[off++] = (byte) (values[i + 2] >>> 21);
    buf[off++] = (byte) (values[i + 2] >>> 13);
    buf[off++] = (byte) (values[i + 2] >>> 5);

    buf[off] = (byte) (values[i + 2] << 3);
    buf[off++] |= values[i + 3] >>> 60;
    buf[off++] = (byte) (values[i + 3] >>> 52);
    buf[off++] = (byte) (values[i + 3] >>> 44);
    buf[off++] = (byte) (values[i + 3] >>> 36);
    buf[off++] = (byte) (values[i + 3] >>> 28);
    buf[off++] = (byte) (values[i + 3] >>> 20);
    buf[off++] = (byte) (values[i + 3] >>> 12);
    buf[off++] = (byte) (values[i + 3] >>> 4);

    buf[off] = (byte) (values[i + 3] << 4);
    buf[off++] |= values[i + 4] >>> 59;
    buf[off++] = (byte) (values[i + 4] >>> 51);
    buf[off++] = (byte) (values[i + 4] >>> 43);
    buf[off++] = (byte) (values[i + 4] >>> 35);
    buf[off++] = (byte) (values[i + 4] >>> 27);
    buf[off++] = (byte) (values[i + 4] >>> 19);
    buf[off++] = (byte) (values[i + 4] >>> 11);
    buf[off++] = (byte) (values[i + 4] >>> 3);

    buf[off] = (byte) (values[i + 4] << 5);
    buf[off++] |= values[i + 5] >>> 58;
    buf[off++] = (byte) (values[i + 5] >>> 50);
    buf[off++] = (byte) (values[i + 5] >>> 42);
    buf[off++] = (byte) (values[i + 5] >>> 34);
    buf[off++] = (byte) (values[i + 5] >>> 26);
    buf[off++] = (byte) (values[i + 5] >>> 18);
    buf[off++] = (byte) (values[i + 5] >>> 10);
    buf[off++] = (byte) (values[i + 5] >>> 2);

    buf[off] = (byte) (values[i + 5] << 6);
    buf[off++] |= values[i + 6] >>> 57;
    buf[off++] = (byte) (values[i + 6] >>> 49);
    buf[off++] = (byte) (values[i + 6] >>> 41);
    buf[off++] = (byte) (values[i + 6] >>> 33);
    buf[off++] = (byte) (values[i + 6] >>> 25);
    buf[off++] = (byte) (values[i + 6] >>> 17);
    buf[off++] = (byte) (values[i + 6] >>> 9);
    buf[off++] = (byte) (values[i + 6] >>> 1);

    buf[off] = (byte) (values[i + 6] << 7);
    buf[off++] |= values[i + 7] >>> 56;
    buf[off++] = (byte) (values[i + 7] >>> 48);
    buf[off++] = (byte) (values[i + 7] >>> 40);
    buf[off++] = (byte) (values[i + 7] >>> 32);
    buf[off++] = (byte) (values[i + 7] >>> 24);
    buf[off++] = (byte) (values[i + 7] >>> 16);
    buf[off++] = (byte) (values[i + 7] >>> 8);
    buf[off] = (byte) values[i + 7];
  }

  static void unpackBits1(final long[] values, final int i, final byte[] buf, final int off) {
    values[i + 0] = (Byte.toUnsignedLong(buf[off]) >>> 7) & 1;
    values[i + 1] = (Byte.toUnsignedLong(buf[off]) >>> 6) & 1;
    values[i + 2] = (Byte.toUnsignedLong(buf[off]) >>> 5) & 1;
    values[i + 3] = (Byte.toUnsignedLong(buf[off]) >>> 4) & 1;
    values[i + 4] = (Byte.toUnsignedLong(buf[off]) >>> 3) & 1;
    values[i + 5] = (Byte.toUnsignedLong(buf[off]) >>> 2) & 1;
    values[i + 6] = (Byte.toUnsignedLong(buf[off]) >>> 1) & 1;
    values[i + 7] = Byte.toUnsignedLong(buf[off]) & 1;
  }

  static void unpackBits2(final long[] values, final int i, final byte[] buf, int off) {
    values[i + 0] = (Byte.toUnsignedLong(buf[off]) >>> 6) & 3;
    values[i + 1] = (Byte.toUnsignedLong(buf[off]) >>> 4) & 3;
    values[i + 2] = (Byte.toUnsignedLong(buf[off]) >>> 2) & 3;
    values[i + 3] = Byte.toUnsignedLong(buf[off++]) & 3;
    values[i + 4] = (Byte.toUnsignedLong(buf[off]) >>> 6) & 3;
    values[i + 5] = (Byte.toUnsignedLong(buf[off]) >>> 4) & 3;
    values[i + 6] = (Byte.toUnsignedLong(buf[off]) >>> 2) & 3;
    values[i + 7] = Byte.toUnsignedLong(buf[off]) & 3;
  }

  static void unpackBits3(final long[] values, final int i, final byte[] buf, int off) {
    values[i + 0] = Byte.toUnsignedLong(buf[off]) >>> 5;
    values[i + 1] = (Byte.toUnsignedLong(buf[off]) >>> 2) & 7;
    values[i + 2] = (Byte.toUnsignedLong(buf[off++]) & 3) << 1;
    values[i + 2] |= Byte.toUnsignedLong(buf[off]) >>> 7;
    values[i + 3] = (Byte.toUnsignedLong(buf[off]) >>> 4) & 7;
    values[i + 4] = (Byte.toUnsignedLong(buf[off]) >>> 1) & 7;
    values[i + 5] = (Byte.toUnsignedLong(buf[off++]) & 1) << 2;
    values[i + 5] |= Byte.toUnsignedLong(buf[off]) >>> 6;
    values[i + 6] = (Byte.toUnsignedLong(buf[off]) >>> 3) & 7;
    values[i + 7] = Byte.toUnsignedLong(buf[off]) & 7;
  }

  static void unpackBits4(final long[] values, final int i, final byte[] buf, int off) {
    values[i + 0] = Byte.toUnsignedLong(buf[off]) >>> 4;
    values[i + 1] = Byte.toUnsignedLong(buf[off++]) & 0xf;
    values[i + 2] = Byte.toUnsignedLong(buf[off]) >>> 4;
    values[i + 3] = Byte.toUnsignedLong(buf[off++]) & 0xf;
    values[i + 4] = Byte.toUnsignedLong(buf[off]) >>> 4;
    values[i + 5] = Byte.toUnsignedLong(buf[off++]) & 0xf;
    values[i + 6] = Byte.toUnsignedLong(buf[off]) >>> 4;
    values[i + 7] = Byte.toUnsignedLong(buf[off]) & 0xf;
  }

  static void unpackBits5(final long[] values, final int i, final byte[] buf, int off) {
    values[i + 0] = Byte.toUnsignedLong(buf[off]) >>> 3;

    values[i + 1] = (Byte.toUnsignedLong(buf[off++]) & 7) << 2;
    values[i + 1] |= Byte.toUnsignedLong(buf[off]) >>> 6;

    values[i + 2] = (Byte.toUnsignedLong(buf[off]) >>> 1) & 0x1f;

    values[i + 3] = (Byte.toUnsignedLong(buf[off++]) & 1) << 4;
    values[i + 3] |= Byte.toUnsignedLong(buf[off]) >>> 4;

    values[i + 4] = (Byte.toUnsignedLong(buf[off++]) & 0xf) << 1;
    values[i + 4] |= Byte.toUnsignedLong(buf[off]) >>> 7;

    values[i + 5] = (Byte.toUnsignedLong(buf[off]) >>> 2) & 0x1f;

    values[i + 6] = (Byte.toUnsignedLong(buf[off++]) & 3) << 3;
    values[i + 6] |= Byte.toUnsignedLong(buf[off]) >>> 5;

    values[i + 7] = Byte.toUnsignedLong(buf[off]) & 0x1f;
  }

  static void unpackBits6(final long[] values, final int i, final byte[] buf, int off) {
    values[i + 0] = Byte.toUnsignedLong(buf[off]) >>> 2;

    values[i + 1] = (Byte.toUnsignedLong(buf[off++]) & 3) << 4;
    values[i + 1] |= Byte.toUnsignedLong(buf[off]) >>> 4;

    values[i + 2] = (Byte.toUnsignedLong(buf[off++]) & 0xf) << 2;
    values[i + 2] |= Byte.toUnsignedLong(buf[off]) >>> 6;

    values[i + 3] = Byte.toUnsignedLong(buf[off++]) & 0x3f;

    values[i + 4] = Byte.toUnsignedLong(buf[off]) >>> 2;

    values[i + 5] = (Byte.toUnsignedLong(buf[off++]) & 3) << 4;
    values[i + 5] |= Byte.toUnsignedLong(buf[off]) >>> 4;

    values[i + 6] = (Byte.toUnsignedLong(buf[off++]) & 0xf) << 2;
    values[i + 6] |= Byte.toUnsignedLong(buf[off]) >>> 6;

    values[i + 7] = Byte.toUnsignedLong(buf[off]) & 0x3f;
  }

  static void unpackBits7(final long[] values, final int i, final byte[] buf, int off) {
    values[i + 0] = Byte.toUnsignedLong(buf[off]) >>> 1;

    values[i + 1] = (Byte.toUnsignedLong(buf[off++]) & 1) << 6;
    values[i + 1] |= Byte.toUnsignedLong(buf[off]) >>> 2;

    values[i + 2] = (Byte.toUnsignedLong(buf[off++]) & 3) << 5;
    values[i + 2] |= Byte.toUnsignedLong(buf[off]) >>> 3;

    values[i + 3] = (Byte.toUnsignedLong(buf[off++]) & 7) << 4;
    values[i + 3] |= Byte.toUnsignedLong(buf[off]) >>> 4;

    values[i + 4] = (Byte.toUnsignedLong(buf[off++]) & 0xf) << 3;
    values[i + 4] |= Byte.toUnsignedLong(buf[off]) >>> 5;

    values[i + 5] = (Byte.toUnsignedLong(buf[off++]) & 0x1f) << 2;
    values[i + 5] |= Byte.toUnsignedLong(buf[off]) >>> 6;

    values[i + 6] = (Byte.toUnsignedLong(buf[off++]) & 0x3f) << 1;
    values[i + 6] |= Byte.toUnsignedLong(buf[off]) >>> 7;

    values[i + 7] = Byte.toUnsignedLong(buf[off]) & 0x7f;
  }

  static void unpackBits8(final long[] values, final int i, final byte[] buf, int off) {
    values[i + 0] = Byte.toUnsignedLong(buf[off++]);
    values[i + 1] = Byte.toUnsignedLong(buf[off++]);
    values[i + 2] = Byte.toUnsignedLong(buf[off++]);
    values[i + 3] = Byte.toUnsignedLong(buf[off++]);
    values[i + 4] = Byte.toUnsignedLong(buf[off++]);
    values[i + 5] = Byte.toUnsignedLong(buf[off++]);
    values[i + 6] = Byte.toUnsignedLong(buf[off++]);
    values[i + 7] = Byte.toUnsignedLong(buf[off]);
  }

  static void unpackBits9(final long[] values, final int i, final byte[] buf, int off) {
    values[i + 0] = Byte.toUnsignedLong(buf[off++]) << 1;
    values[i + 0] |= Byte.toUnsignedLong(buf[off]) >>> 7;

    values[i + 1] = (Byte.toUnsignedLong(buf[off++]) & 0x7f) << 2;
    values[i + 1] |= Byte.toUnsignedLong(buf[off]) >>> 6;

    values[i + 2] = (Byte.toUnsignedLong(buf[off++]) & 0x3f) << 3;
    values[i + 2] |= Byte.toUnsignedLong(buf[off]) >>> 5;

    values[i + 3] = (Byte.toUnsignedLong(buf[off++]) & 0x1f) << 4;
    values[i + 3] |= Byte.toUnsignedLong(buf[off]) >>> 4;

    values[i + 4] = (Byte.toUnsignedLong(buf[off++]) & 0xf) << 5;
    values[i + 4] |= Byte.toUnsignedLong(buf[off]) >>> 3;

    values[i + 5] = (Byte.toUnsignedLong(buf[off++]) & 7) << 6;
    values[i + 5] |= Byte.toUnsignedLong(buf[off]) >>> 2;

    values[i + 6] = (Byte.toUnsignedLong(buf[off++]) & 3) << 7;
    values[i + 6] |= Byte.toUnsignedLong(buf[off]) >>> 1;

    values[i + 7] = (Byte.toUnsignedLong(buf[off++]) & 1) << 8;
    values[i + 7] |= Byte.toUnsignedLong(buf[off]);
  }

  static void unpackBits10(final long[] values, final int i, final byte[] buf, int off) {
    values[i + 0] = Byte.toUnsignedLong(buf[off++]) << 2;
    values[i + 0] |= Byte.toUnsignedLong(buf[off]) >>> 6;

    values[i + 1] = (Byte.toUnsignedLong(buf[off++]) & 0x3f) << 4;
    values[i + 1] |= Byte.toUnsignedLong(buf[off]) >>> 4;

    values[i + 2] = (Byte.toUnsignedLong(buf[off++]) & 0xf) << 6;
    values[i + 2] |= Byte.toUnsignedLong(buf[off]) >>> 2;

    values[i + 3] = (Byte.toUnsignedLong(buf[off++]) & 3) << 8;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]);

    values[i + 4] = Byte.toUnsignedLong(buf[off++]) << 2;
    values[i + 4] |= Byte.toUnsignedLong(buf[off]) >>> 6;

    values[i + 5] = (Byte.toUnsignedLong(buf[off++]) & 0x3f) << 4;
    values[i + 5] |= Byte.toUnsignedLong(buf[off]) >>> 4;

    values[i + 6] = (Byte.toUnsignedLong(buf[off++]) & 0xf) << 6;
    values[i + 6] |= Byte.toUnsignedLong(buf[off]) >>> 2;

    values[i + 7] = (Byte.toUnsignedLong(buf[off++]) & 3) << 8;
    values[i + 7] |= Byte.toUnsignedLong(buf[off]);
  }

  static void unpackBits11(final long[] values, final int i, final byte[] buf, int off) {
    values[i + 0] = Byte.toUnsignedLong(buf[off++]) << 3;
    values[i + 0] |= Byte.toUnsignedLong(buf[off]) >>> 5;

    values[i + 1] = (Byte.toUnsignedLong(buf[off++]) & 0x1f) << 6;
    values[i + 1] |= Byte.toUnsignedLong(buf[off]) >>> 2;

    values[i + 2] = (Byte.toUnsignedLong(buf[off++]) & 3) << 9;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 1;
    values[i + 2] |= Byte.toUnsignedLong(buf[off]) >>> 7;

    values[i + 3] = (Byte.toUnsignedLong(buf[off++]) & 0x7f) << 4;
    values[i + 3] |= Byte.toUnsignedLong(buf[off]) >>> 4;

    values[i + 4] = (Byte.toUnsignedLong(buf[off++]) & 0xf) << 7;
    values[i + 4] |= Byte.toUnsignedLong(buf[off]) >>> 1;

    values[i + 5] = (Byte.toUnsignedLong(buf[off++]) & 1) << 10;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 2;
    values[i + 5] |= Byte.toUnsignedLong(buf[off]) >>> 6;

    values[i + 6] = (Byte.toUnsignedLong(buf[off++]) & 0x3f) << 5;
    values[i + 6] |= Byte.toUnsignedLong(buf[off]) >>> 3;

    values[i + 7] = (Byte.toUnsignedLong(buf[off++]) & 7) << 8;
    values[i + 7] |= Byte.toUnsignedLong(buf[off]);
  }

  static void unpackBits12(final long[] values, final int i, final byte[] buf, int off) {
    values[i + 0] = Byte.toUnsignedLong(buf[off++]) << 4;
    values[i + 0] |= Byte.toUnsignedLong(buf[off]) >>> 4;

    values[i + 1] = (Byte.toUnsignedLong(buf[off++]) & 0xf) << 8;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]);

    values[i + 2] = Byte.toUnsignedLong(buf[off++]) << 4;
    values[i + 2] |= Byte.toUnsignedLong(buf[off]) >>> 4;

    values[i + 3] = (Byte.toUnsignedLong(buf[off++]) & 0xf) << 8;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]);

    values[i + 4] = Byte.toUnsignedLong(buf[off++]) << 4;
    values[i + 4] |= Byte.toUnsignedLong(buf[off]) >>> 4;

    values[i + 5] = (Byte.toUnsignedLong(buf[off++]) & 0xf) << 8;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]);

    values[i + 6] = Byte.toUnsignedLong(buf[off++]) << 4;
    values[i + 6] |= Byte.toUnsignedLong(buf[off]) >>> 4;

    values[i + 7] = (Byte.toUnsignedLong(buf[off++]) & 0xf) << 8;
    values[i + 7] |= Byte.toUnsignedLong(buf[off]);
  }

  static void unpackBits13(final long[] values, final int i, final byte[] buf, int off) {
    values[i + 0] = Byte.toUnsignedLong(buf[off++]) << 5;
    values[i + 0] |= Byte.toUnsignedLong(buf[off]) >>> 3;

    values[i + 1] = (Byte.toUnsignedLong(buf[off++]) & 7) << 10;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 2;
    values[i + 1] |= Byte.toUnsignedLong(buf[off]) >>> 6;

    values[i + 2] = (Byte.toUnsignedLong(buf[off++]) & 0x3f) << 7;
    values[i + 2] |= Byte.toUnsignedLong(buf[off]) >>> 1;

    values[i + 3] = (Byte.toUnsignedLong(buf[off++]) & 1) << 12;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 4;
    values[i + 3] |= Byte.toUnsignedLong(buf[off]) >>> 4;

    values[i + 4] = (Byte.toUnsignedLong(buf[off++]) & 0xf) << 9;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 1;
    values[i + 4] |= Byte.toUnsignedLong(buf[off]) >>> 7;

    values[i + 5] = (Byte.toUnsignedLong(buf[off++]) & 0x7f) << 6;
    values[i + 5] |= Byte.toUnsignedLong(buf[off]) >>> 2;

    values[i + 6] = (Byte.toUnsignedLong(buf[off++]) & 3) << 11;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 3;
    values[i + 6] |= Byte.toUnsignedLong(buf[off]) >>> 5;

    values[i + 7] = (Byte.toUnsignedLong(buf[off++]) & 0x1f) << 8;
    values[i + 7] |= Byte.toUnsignedLong(buf[off]);
  }

  static void unpackBits14(final long[] values, final int i, final byte[] buf, int off) {
    values[i + 0] = Byte.toUnsignedLong(buf[off++]) << 6;
    values[i + 0] |= Byte.toUnsignedLong(buf[off]) >>> 2;

    values[i + 1] = (Byte.toUnsignedLong(buf[off++]) & 3) << 12;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 4;
    values[i + 1] |= Byte.toUnsignedLong(buf[off]) >>> 4;

    values[i + 2] = (Byte.toUnsignedLong(buf[off++]) & 0xf) << 10;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 2;
    values[i + 2] |= Byte.toUnsignedLong(buf[off]) >>> 6;

    values[i + 3] = (Byte.toUnsignedLong(buf[off++]) & 0x3f) << 8;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]);

    values[i + 4] = Byte.toUnsignedLong(buf[off++]) << 6;
    values[i + 4] |= Byte.toUnsignedLong(buf[off]) >>> 2;

    values[i + 5] = (Byte.toUnsignedLong(buf[off++]) & 3) << 12;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 4;
    values[i + 5] |= Byte.toUnsignedLong(buf[off]) >>> 4;

    values[i + 6] = (Byte.toUnsignedLong(buf[off++]) & 0xf) << 10;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 2;
    values[i + 6] |= Byte.toUnsignedLong(buf[off]) >>> 6;

    values[i + 7] = (Byte.toUnsignedLong(buf[off++]) & 0x3f) << 8;
    values[i + 7] |= Byte.toUnsignedLong(buf[off]);
  }

  static void unpackBits15(final long[] values, final int i, final byte[] buf, int off) {
    values[i + 0] = Byte.toUnsignedLong(buf[off++]) << 7;
    values[i + 0] |= Byte.toUnsignedLong(buf[off]) >>> 1;

    values[i + 1] = (Byte.toUnsignedLong(buf[off++]) & 1) << 14;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 6;
    values[i + 1] |= Byte.toUnsignedLong(buf[off]) >>> 2;

    values[i + 2] = (Byte.toUnsignedLong(buf[off++]) & 3) << 13;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 5;
    values[i + 2] |= Byte.toUnsignedLong(buf[off]) >>> 3;

    values[i + 3] = (Byte.toUnsignedLong(buf[off++]) & 7) << 12;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 4;
    values[i + 3] |= Byte.toUnsignedLong(buf[off]) >>> 4;

    values[i + 4] = (Byte.toUnsignedLong(buf[off++]) & 0xf) << 11;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 3;
    values[i + 4] |= Byte.toUnsignedLong(buf[off]) >>> 5;

    values[i + 5] = (Byte.toUnsignedLong(buf[off++]) & 0x1f) << 10;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 2;
    values[i + 5] |= Byte.toUnsignedLong(buf[off]) >>> 6;

    values[i + 6] = (Byte.toUnsignedLong(buf[off++]) & 0x3f) << 9;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 1;
    values[i + 6] |= Byte.toUnsignedLong(buf[off]) >>> 7;

    values[i + 7] = (Byte.toUnsignedLong(buf[off++]) & 0x7f) << 8;
    values[i + 7] |= Byte.toUnsignedLong(buf[off]);
  }

  static void unpackBits16(final long[] values, final int i, final byte[] buf, int off) {
    values[i + 0] = Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]);
    values[i + 1] = Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]);
    values[i + 2] = Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]);
    values[i + 3] = Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]);
    values[i + 4] = Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]);
    values[i + 5] = Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]);
    values[i + 6] = Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]);
    values[i + 7] = Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 7] |= Byte.toUnsignedLong(buf[off]);
  }

  static void unpackBits17(final long[] values, final int i, final byte[] buf, int off) {
    values[i + 0] = Byte.toUnsignedLong(buf[off++]) << 9;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 1;
    values[i + 0] |= Byte.toUnsignedLong(buf[off]) >>> 7;

    values[i + 1] = (Byte.toUnsignedLong(buf[off++]) & 0x7f) << 10;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 2;
    values[i + 1] |= Byte.toUnsignedLong(buf[off]) >>> 6;

    values[i + 2] = (Byte.toUnsignedLong(buf[off++]) & 0x3f) << 11;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 3;
    values[i + 2] |= Byte.toUnsignedLong(buf[off]) >>> 5;

    values[i + 3] = (Byte.toUnsignedLong(buf[off++]) & 0x1f) << 12;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 4;
    values[i + 3] |= Byte.toUnsignedLong(buf[off]) >>> 4;

    values[i + 4] = (Byte.toUnsignedLong(buf[off++]) & 0xf) << 13;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 5;
    values[i + 4] |= Byte.toUnsignedLong(buf[off]) >>> 3;

    values[i + 5] = (Byte.toUnsignedLong(buf[off++]) & 7) << 14;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 6;
    values[i + 5] |= Byte.toUnsignedLong(buf[off]) >>> 2;

    values[i + 6] = (Byte.toUnsignedLong(buf[off++]) & 3) << 15;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 7;
    values[i + 6] |= Byte.toUnsignedLong(buf[off]) >>> 1;

    values[i + 7] = (Byte.toUnsignedLong(buf[off++]) & 1) << 16;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 7] |= Byte.toUnsignedLong(buf[off]);
  }

  static void unpackBits18(final long[] values, final int i, final byte[] buf, int off) {
    values[i + 0] = Byte.toUnsignedLong(buf[off++]) << 10;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 2;
    values[i + 0] |= Byte.toUnsignedLong(buf[off]) >>> 6;

    values[i + 1] = (Byte.toUnsignedLong(buf[off++]) & 0x3f) << 12;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 4;
    values[i + 1] |= Byte.toUnsignedLong(buf[off]) >>> 4;

    values[i + 2] = (Byte.toUnsignedLong(buf[off++]) & 0xf) << 14;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 6;
    values[i + 2] |= Byte.toUnsignedLong(buf[off]) >>> 2;

    values[i + 3] = (Byte.toUnsignedLong(buf[off++]) & 3) << 16;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]);

    values[i + 4] = Byte.toUnsignedLong(buf[off++]) << 10;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 2;
    values[i + 4] |= Byte.toUnsignedLong(buf[off]) >>> 6;

    values[i + 5] = (Byte.toUnsignedLong(buf[off++]) & 0x3f) << 12;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 4;
    values[i + 5] |= Byte.toUnsignedLong(buf[off]) >>> 4;

    values[i + 6] = (Byte.toUnsignedLong(buf[off++]) & 0xf) << 14;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 6;
    values[i + 6] |= Byte.toUnsignedLong(buf[off]) >>> 2;

    values[i + 7] = (Byte.toUnsignedLong(buf[off++]) & 3) << 16;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 7] |= Byte.toUnsignedLong(buf[off]);
  }

  static void unpackBits19(final long[] values, final int i, final byte[] buf, int off) {
    values[i + 0] = Byte.toUnsignedLong(buf[off++]) << 11;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 3;
    values[i + 0] |= Byte.toUnsignedLong(buf[off]) >>> 5;

    values[i + 1] = (Byte.toUnsignedLong(buf[off++]) & 0x1f) << 14;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 6;
    values[i + 1] |= Byte.toUnsignedLong(buf[off]) >>> 2;

    values[i + 2] = (Byte.toUnsignedLong(buf[off++]) & 3) << 17;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 9;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 1;
    values[i + 2] |= Byte.toUnsignedLong(buf[off]) >>> 7;

    values[i + 3] = (Byte.toUnsignedLong(buf[off++]) & 0x7f) << 12;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 4;
    values[i + 3] |= Byte.toUnsignedLong(buf[off]) >>> 4;

    values[i + 4] = (Byte.toUnsignedLong(buf[off++]) & 0xf) << 15;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 7;
    values[i + 4] |= Byte.toUnsignedLong(buf[off]) >>> 1;

    values[i + 5] = (Byte.toUnsignedLong(buf[off++]) & 1) << 18;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 10;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 2;
    values[i + 5] |= Byte.toUnsignedLong(buf[off]) >>> 6;

    values[i + 6] = (Byte.toUnsignedLong(buf[off++]) & 0x3f) << 13;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 5;
    values[i + 6] |= Byte.toUnsignedLong(buf[off]) >>> 3;

    values[i + 7] = (Byte.toUnsignedLong(buf[off++]) & 7) << 16;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 7] |= Byte.toUnsignedLong(buf[off]);
  }

  static void unpackBits20(final long[] values, final int i, final byte[] buf, int off) {
    values[i + 0] = Byte.toUnsignedLong(buf[off++]) << 12;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 4;
    values[i + 0] |= Byte.toUnsignedLong(buf[off]) >>> 4;

    values[i + 1] = (Byte.toUnsignedLong(buf[off++]) & 0xf) << 16;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]);

    values[i + 2] = Byte.toUnsignedLong(buf[off++]) << 12;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 4;
    values[i + 2] |= Byte.toUnsignedLong(buf[off]) >>> 4;

    values[i + 3] = (Byte.toUnsignedLong(buf[off++]) & 0xf) << 16;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]);

    values[i + 4] = Byte.toUnsignedLong(buf[off++]) << 12;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 4;
    values[i + 4] |= Byte.toUnsignedLong(buf[off]) >>> 4;

    values[i + 5] = (Byte.toUnsignedLong(buf[off++]) & 0xf) << 16;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]);

    values[i + 6] = Byte.toUnsignedLong(buf[off++]) << 12;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 4;
    values[i + 6] |= Byte.toUnsignedLong(buf[off]) >>> 4;

    values[i + 7] = (Byte.toUnsignedLong(buf[off++]) & 0xf) << 16;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 7] |= Byte.toUnsignedLong(buf[off]);
  }

  static void unpackBits21(final long[] values, final int i, final byte[] buf, int off) {
    values[i + 0] = Byte.toUnsignedLong(buf[off++]) << 13;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 5;
    values[i + 0] |= Byte.toUnsignedLong(buf[off]) >>> 3;

    values[i + 1] = (Byte.toUnsignedLong(buf[off++]) & 7) << 18;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 10;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 2;
    values[i + 1] |= Byte.toUnsignedLong(buf[off]) >>> 6;

    values[i + 2] = (Byte.toUnsignedLong(buf[off++]) & 0x3f) << 15;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 7;
    values[i + 2] |= Byte.toUnsignedLong(buf[off]) >>> 1;

    values[i + 3] = (Byte.toUnsignedLong(buf[off++]) & 1) << 20;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 12;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 4;
    values[i + 3] |= Byte.toUnsignedLong(buf[off]) >>> 4;

    values[i + 4] = (Byte.toUnsignedLong(buf[off++]) & 0xf) << 17;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 9;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 1;
    values[i + 4] |= Byte.toUnsignedLong(buf[off]) >>> 7;

    values[i + 5] = (Byte.toUnsignedLong(buf[off++]) & 0x7f) << 14;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 6;
    values[i + 5] |= Byte.toUnsignedLong(buf[off]) >>> 2;

    values[i + 6] = (Byte.toUnsignedLong(buf[off++]) & 3) << 19;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 11;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 3;
    values[i + 6] |= Byte.toUnsignedLong(buf[off]) >>> 5;

    values[i + 7] = (Byte.toUnsignedLong(buf[off++]) & 0x1f) << 16;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 7] |= Byte.toUnsignedLong(buf[off]);
  }

  static void unpackBits22(final long[] values, final int i, final byte[] buf, int off) {
    values[i + 0] = Byte.toUnsignedLong(buf[off++]) << 14;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 6;
    values[i + 0] |= Byte.toUnsignedLong(buf[off]) >>> 2;

    values[i + 1] = (Byte.toUnsignedLong(buf[off++]) & 3) << 20;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 12;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 4;
    values[i + 1] |= Byte.toUnsignedLong(buf[off]) >>> 4;

    values[i + 2] = (Byte.toUnsignedLong(buf[off++]) & 0xf) << 18;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 10;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 2;
    values[i + 2] |= Byte.toUnsignedLong(buf[off]) >>> 6;

    values[i + 3] = (Byte.toUnsignedLong(buf[off++]) & 0x3f) << 16;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]);

    values[i + 4] = Byte.toUnsignedLong(buf[off++]) << 14;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 6;
    values[i + 4] |= Byte.toUnsignedLong(buf[off]) >>> 2;

    values[i + 5] = (Byte.toUnsignedLong(buf[off++]) & 3) << 20;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 12;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 4;
    values[i + 5] |= Byte.toUnsignedLong(buf[off]) >>> 4;

    values[i + 6] = (Byte.toUnsignedLong(buf[off++]) & 0xf) << 18;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 10;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 2;
    values[i + 6] |= Byte.toUnsignedLong(buf[off]) >>> 6;

    values[i + 7] = (Byte.toUnsignedLong(buf[off++]) & 0x3f) << 16;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 7] |= Byte.toUnsignedLong(buf[off]);
  }

  static void unpackBits23(final long[] values, final int i, final byte[] buf, int off) {
    values[i + 0] = Byte.toUnsignedLong(buf[off++]) << 15;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 7;
    values[i + 0] |= Byte.toUnsignedLong(buf[off]) >>> 1;

    values[i + 1] = (Byte.toUnsignedLong(buf[off++]) & 1) << 22;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 14;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 6;
    values[i + 1] |= Byte.toUnsignedLong(buf[off]) >>> 2;

    values[i + 2] = (Byte.toUnsignedLong(buf[off++]) & 3) << 21;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 13;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 5;
    values[i + 2] |= Byte.toUnsignedLong(buf[off]) >>> 3;

    values[i + 3] = (Byte.toUnsignedLong(buf[off++]) & 7) << 20;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 12;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 4;
    values[i + 3] |= Byte.toUnsignedLong(buf[off]) >>> 4;

    values[i + 4] = (Byte.toUnsignedLong(buf[off++]) & 0xf) << 19;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 11;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 3;
    values[i + 4] |= Byte.toUnsignedLong(buf[off]) >>> 5;

    values[i + 5] = (Byte.toUnsignedLong(buf[off++]) & 0x1f) << 18;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 10;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 2;
    values[i + 5] |= Byte.toUnsignedLong(buf[off]) >>> 6;

    values[i + 6] = (Byte.toUnsignedLong(buf[off++]) & 0x3f) << 17;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 9;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 1;
    values[i + 6] |= Byte.toUnsignedLong(buf[off]) >>> 7;

    values[i + 7] = (Byte.toUnsignedLong(buf[off++]) & 0x7f) << 16;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 7] |= Byte.toUnsignedLong(buf[off]);
  }

  static void unpackBits24(final long[] values, final int i, final byte[] buf, int off) {
    values[i + 0] = Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]);
    values[i + 1] = Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]);
    values[i + 2] = Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]);
    values[i + 3] = Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]);
    values[i + 4] = Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]);
    values[i + 5] = Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]);
    values[i + 6] = Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]);
    values[i + 7] = Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 7] |= Byte.toUnsignedLong(buf[off]);
  }

  static void unpackBits25(final long[] values, final int i, final byte[] buf, int off) {
    values[i + 0] = Byte.toUnsignedLong(buf[off++]) << 17;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 9;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 1;
    values[i + 0] |= Byte.toUnsignedLong(buf[off]) >>> 7;

    values[i + 1] = (Byte.toUnsignedLong(buf[off++]) & 0x7f) << 18;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 10;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 2;
    values[i + 1] |= Byte.toUnsignedLong(buf[off]) >>> 6;

    values[i + 2] = (Byte.toUnsignedLong(buf[off++]) & 0x3f) << 19;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 11;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 3;
    values[i + 2] |= Byte.toUnsignedLong(buf[off]) >>> 5;

    values[i + 3] = (Byte.toUnsignedLong(buf[off++]) & 0x1f) << 20;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 12;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 4;
    values[i + 3] |= Byte.toUnsignedLong(buf[off]) >>> 4;

    values[i + 4] = (Byte.toUnsignedLong(buf[off++]) & 0xf) << 21;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 13;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 5;
    values[i + 4] |= Byte.toUnsignedLong(buf[off]) >>> 3;

    values[i + 5] = (Byte.toUnsignedLong(buf[off++]) & 7) << 22;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 14;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 6;
    values[i + 5] |= Byte.toUnsignedLong(buf[off]) >>> 2;

    values[i + 6] = (Byte.toUnsignedLong(buf[off++]) & 3) << 23;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 15;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 7;
    values[i + 6] |= Byte.toUnsignedLong(buf[off]) >>> 1;

    values[i + 7] = (Byte.toUnsignedLong(buf[off++]) & 1) << 24;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 7] |= Byte.toUnsignedLong(buf[off]);
  }

  static void unpackBits26(final long[] values, final int i, final byte[] buf, int off) {
    values[i + 0] = Byte.toUnsignedLong(buf[off++]) << 18;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 10;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 2;
    values[i + 0] |= Byte.toUnsignedLong(buf[off]) >>> 6;

    values[i + 1] = (Byte.toUnsignedLong(buf[off++]) & 0x3f) << 20;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 12;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 4;
    values[i + 1] |= Byte.toUnsignedLong(buf[off]) >>> 4;

    values[i + 2] = (Byte.toUnsignedLong(buf[off++]) & 0xf) << 22;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 14;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 6;
    values[i + 2] |= Byte.toUnsignedLong(buf[off]) >>> 2;

    values[i + 3] = (Byte.toUnsignedLong(buf[off++]) & 3) << 24;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]);

    values[i + 4] = Byte.toUnsignedLong(buf[off++]) << 18;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 10;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 2;
    values[i + 4] |= Byte.toUnsignedLong(buf[off]) >>> 6;

    values[i + 5] = (Byte.toUnsignedLong(buf[off++]) & 0x3f) << 20;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 12;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 4;
    values[i + 5] |= Byte.toUnsignedLong(buf[off]) >>> 4;

    values[i + 6] = (Byte.toUnsignedLong(buf[off++]) & 0xf) << 22;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 14;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 6;
    values[i + 6] |= Byte.toUnsignedLong(buf[off]) >>> 2;

    values[i + 7] = (Byte.toUnsignedLong(buf[off++]) & 3) << 24;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 7] |= Byte.toUnsignedLong(buf[off]);
  }

  static void unpackBits27(final long[] values, final int i, final byte[] buf, int off) {
    values[i + 0] = Byte.toUnsignedLong(buf[off++]) << 19;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 11;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 3;
    values[i + 0] |= Byte.toUnsignedLong(buf[off]) >>> 5;

    values[i + 1] = (Byte.toUnsignedLong(buf[off++]) & 0x1f) << 22;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 14;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 6;
    values[i + 1] |= Byte.toUnsignedLong(buf[off]) >>> 2;

    values[i + 2] = (Byte.toUnsignedLong(buf[off++]) & 3) << 25;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 17;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 9;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 1;
    values[i + 2] |= Byte.toUnsignedLong(buf[off]) >>> 7;

    values[i + 3] = (Byte.toUnsignedLong(buf[off++]) & 0x7f) << 20;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 12;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 4;
    values[i + 3] |= Byte.toUnsignedLong(buf[off]) >>> 4;

    values[i + 4] = (Byte.toUnsignedLong(buf[off++]) & 0xf) << 23;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 15;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 7;
    values[i + 4] |= Byte.toUnsignedLong(buf[off]) >>> 1;

    values[i + 5] = (Byte.toUnsignedLong(buf[off++]) & 1) << 26;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 18;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 10;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 2;
    values[i + 5] |= Byte.toUnsignedLong(buf[off]) >>> 6;

    values[i + 6] = (Byte.toUnsignedLong(buf[off++]) & 0x3f) << 21;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 13;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 5;
    values[i + 6] |= Byte.toUnsignedLong(buf[off]) >>> 3;

    values[i + 7] = (Byte.toUnsignedLong(buf[off++]) & 7) << 24;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 7] |= Byte.toUnsignedLong(buf[off]);
  }

  static void unpackBits28(final long[] values, final int i, final byte[] buf, int off) {
    values[i + 0] = Byte.toUnsignedLong(buf[off++]) << 20;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 12;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 4;
    values[i + 0] |= Byte.toUnsignedLong(buf[off]) >>> 4;

    values[i + 1] = (Byte.toUnsignedLong(buf[off++]) & 0xf) << 24;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]);

    values[i + 2] = Byte.toUnsignedLong(buf[off++]) << 20;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 12;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 4;
    values[i + 2] |= Byte.toUnsignedLong(buf[off]) >>> 4;

    values[i + 3] = (Byte.toUnsignedLong(buf[off++]) & 0xf) << 24;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]);

    values[i + 4] = Byte.toUnsignedLong(buf[off++]) << 20;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 12;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 4;
    values[i + 4] |= Byte.toUnsignedLong(buf[off]) >>> 4;

    values[i + 5] = (Byte.toUnsignedLong(buf[off++]) & 0xf) << 24;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]);

    values[i + 6] = Byte.toUnsignedLong(buf[off++]) << 20;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 12;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 4;
    values[i + 6] |= Byte.toUnsignedLong(buf[off]) >>> 4;

    values[i + 7] = (Byte.toUnsignedLong(buf[off++]) & 0xf) << 24;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 7] |= Byte.toUnsignedLong(buf[off]);
  }

  static void unpackBits29(final long[] values, final int i, final byte[] buf, int off) {
    values[i + 0] = Byte.toUnsignedLong(buf[off++]) << 21;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 13;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 5;
    values[i + 0] |= Byte.toUnsignedLong(buf[off]) >>> 3;

    values[i + 1] = (Byte.toUnsignedLong(buf[off++]) & 7) << 26;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 18;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 10;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 2;
    values[i + 1] |= Byte.toUnsignedLong(buf[off]) >>> 6;

    values[i + 2] = (Byte.toUnsignedLong(buf[off++]) & 0x3f) << 23;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 15;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 7;
    values[i + 2] |= Byte.toUnsignedLong(buf[off]) >>> 1;

    values[i + 3] = (Byte.toUnsignedLong(buf[off++]) & 1) << 28;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 20;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 12;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 4;
    values[i + 3] |= Byte.toUnsignedLong(buf[off]) >>> 4;

    values[i + 4] = (Byte.toUnsignedLong(buf[off++]) & 0xf) << 25;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 17;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 9;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 1;
    values[i + 4] |= Byte.toUnsignedLong(buf[off]) >>> 7;

    values[i + 5] = (Byte.toUnsignedLong(buf[off++]) & 0x7f) << 22;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 14;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 6;
    values[i + 5] |= Byte.toUnsignedLong(buf[off]) >>> 2;

    values[i + 6] = (Byte.toUnsignedLong(buf[off++]) & 3) << 27;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 19;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 11;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 3;
    values[i + 6] |= Byte.toUnsignedLong(buf[off]) >>> 5;

    values[i + 7] = (Byte.toUnsignedLong(buf[off++]) & 0x1f) << 24;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 7] |= Byte.toUnsignedLong(buf[off]);
  }

  static void unpackBits30(final long[] values, final int i, final byte[] buf, int off) {
    values[i + 0] = Byte.toUnsignedLong(buf[off++]) << 22;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 14;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 6;
    values[i + 0] |= Byte.toUnsignedLong(buf[off]) >>> 2;

    values[i + 1] = (Byte.toUnsignedLong(buf[off++]) & 3) << 28;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 20;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 12;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 4;
    values[i + 1] |= Byte.toUnsignedLong(buf[off]) >>> 4;

    values[i + 2] = (Byte.toUnsignedLong(buf[off++]) & 0xf) << 26;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 18;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 10;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 2;
    values[i + 2] |= Byte.toUnsignedLong(buf[off]) >>> 6;

    values[i + 3] = (Byte.toUnsignedLong(buf[off++]) & 0x3f) << 24;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]);

    values[i + 4] = Byte.toUnsignedLong(buf[off++]) << 22;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 14;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 6;
    values[i + 4] |= Byte.toUnsignedLong(buf[off]) >>> 2;

    values[i + 5] = (Byte.toUnsignedLong(buf[off++]) & 3) << 28;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 20;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 12;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 4;
    values[i + 5] |= Byte.toUnsignedLong(buf[off]) >>> 4;

    values[i + 6] = (Byte.toUnsignedLong(buf[off++]) & 0xf) << 26;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 18;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 10;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 2;
    values[i + 6] |= Byte.toUnsignedLong(buf[off]) >>> 6;

    values[i + 7] = (Byte.toUnsignedLong(buf[off++]) & 0x3f) << 24;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 7] |= Byte.toUnsignedLong(buf[off]);
  }

  static void unpackBits31(final long[] values, final int i, final byte[] buf, int off) {
    values[i + 0] = Byte.toUnsignedLong(buf[off++]) << 23;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 15;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 7;
    values[i + 0] |= Byte.toUnsignedLong(buf[off]) >>> 1;

    values[i + 1] = (Byte.toUnsignedLong(buf[off++]) & 1) << 30;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 22;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 14;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 6;
    values[i + 1] |= Byte.toUnsignedLong(buf[off]) >>> 2;

    values[i + 2] = (Byte.toUnsignedLong(buf[off++]) & 3) << 29;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 21;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 13;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 5;
    values[i + 2] |= Byte.toUnsignedLong(buf[off]) >>> 3;

    values[i + 3] = (Byte.toUnsignedLong(buf[off++]) & 7) << 28;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 20;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 12;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 4;
    values[i + 3] |= Byte.toUnsignedLong(buf[off]) >>> 4;

    values[i + 4] = (Byte.toUnsignedLong(buf[off++]) & 0xf) << 27;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 19;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 11;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 3;
    values[i + 4] |= Byte.toUnsignedLong(buf[off]) >>> 5;

    values[i + 5] = (Byte.toUnsignedLong(buf[off++]) & 0x1f) << 26;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 18;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 10;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 2;
    values[i + 5] |= Byte.toUnsignedLong(buf[off]) >>> 6;

    values[i + 6] = (Byte.toUnsignedLong(buf[off++]) & 0x3f) << 25;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 17;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 9;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 1;
    values[i + 6] |= Byte.toUnsignedLong(buf[off]) >>> 7;

    values[i + 7] = (Byte.toUnsignedLong(buf[off++]) & 0x7f) << 24;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 7] |= Byte.toUnsignedLong(buf[off]);
  }

  static void unpackBits32(final long[] values, final int i, final byte[] buf, int off) {
    values[i + 0] = (Byte.toUnsignedLong(buf[off++])) << 24;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]);
    values[i + 1] = (Byte.toUnsignedLong(buf[off++])) << 24;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]);
    values[i + 2] = (Byte.toUnsignedLong(buf[off++])) << 24;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]);
    values[i + 3] = (Byte.toUnsignedLong(buf[off++])) << 24;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]);
    values[i + 4] = (Byte.toUnsignedLong(buf[off++])) << 24;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]);
    values[i + 5] = (Byte.toUnsignedLong(buf[off++])) << 24;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]);
    values[i + 6] = (Byte.toUnsignedLong(buf[off++])) << 24;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]);
    values[i + 7] = (Byte.toUnsignedLong(buf[off++])) << 24;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 7] |= Byte.toUnsignedLong(buf[off]);
  }

  static void unpackBits33(final long[] values, final int i, final byte[] buf, int off) {
    values[i + 0] = (Byte.toUnsignedLong(buf[off++])) << 25;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 17;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 9;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 1;
    values[i + 0] |= Byte.toUnsignedLong(buf[off]) >>> 7;

    values[i + 1] = (Byte.toUnsignedLong(buf[off++]) & 0x7f) << 26;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 18;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 10;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 2;
    values[i + 1] |= Byte.toUnsignedLong(buf[off]) >>> 6;

    values[i + 2] = (Byte.toUnsignedLong(buf[off++]) & 0x3f) << 27;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 19;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 11;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 3;
    values[i + 2] |= Byte.toUnsignedLong(buf[off]) >>> 5;

    values[i + 3] = (Byte.toUnsignedLong(buf[off++]) & 0x1f) << 28;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 20;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 12;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 4;
    values[i + 3] |= Byte.toUnsignedLong(buf[off]) >>> 4;

    values[i + 4] = (Byte.toUnsignedLong(buf[off++]) & 0xf) << 29;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 21;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 13;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 5;
    values[i + 4] |= Byte.toUnsignedLong(buf[off]) >>> 3;

    values[i + 5] = (Byte.toUnsignedLong(buf[off++]) & 7) << 30;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 22;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 14;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 6;
    values[i + 5] |= Byte.toUnsignedLong(buf[off]) >>> 2;

    values[i + 6] = (Byte.toUnsignedLong(buf[off++]) & 3) << 31;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 23;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 15;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 7;
    values[i + 6] |= Byte.toUnsignedLong(buf[off]) >>> 1;

    values[i + 7] = (Byte.toUnsignedLong(buf[off++]) & 1) << 32;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]) << 24;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 7] |= Byte.toUnsignedLong(buf[off]);
  }

  static void unpackBits34(final long[] values, final int i, final byte[] buf, int off) {
    values[i + 0] = (Byte.toUnsignedLong(buf[off++])) << 26;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 18;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 10;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 2;
    values[i + 0] |= Byte.toUnsignedLong(buf[off]) >>> 6;

    values[i + 1] = (Byte.toUnsignedLong(buf[off++]) & 0x3f) << 28;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 20;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 12;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 4;
    values[i + 1] |= Byte.toUnsignedLong(buf[off]) >>> 4;

    values[i + 2] = (Byte.toUnsignedLong(buf[off++]) & 0xf) << 30;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 22;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 14;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 6;
    values[i + 2] |= Byte.toUnsignedLong(buf[off]) >>> 2;

    values[i + 3] = (Byte.toUnsignedLong(buf[off++]) & 3) << 32;
    values[i + 3] |= (Byte.toUnsignedLong(buf[off++])) << 24;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]);

    values[i + 4] = (Byte.toUnsignedLong(buf[off++])) << 26;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 18;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 10;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 2;
    values[i + 4] |= Byte.toUnsignedLong(buf[off]) >>> 6;

    values[i + 5] = (Byte.toUnsignedLong(buf[off++]) & 0x3f) << 28;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 20;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 12;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 4;
    values[i + 5] |= Byte.toUnsignedLong(buf[off]) >>> 4;

    values[i + 6] = (Byte.toUnsignedLong(buf[off++]) & 0xf) << 30;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 22;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 14;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 6;
    values[i + 6] |= Byte.toUnsignedLong(buf[off]) >>> 2;

    values[i + 7] = (Byte.toUnsignedLong(buf[off++]) & 3) << 32;
    values[i + 7] |= (Byte.toUnsignedLong(buf[off++])) << 24;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]);
  }

  static void unpackBits35(final long[] values, final int i, final byte[] buf, int off) {
    values[i + 0] = (Byte.toUnsignedLong(buf[off++])) << 27;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 19;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 11;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 3;
    values[i + 0] |= Byte.toUnsignedLong(buf[off]) >>> 5;

    values[i + 1] = (Byte.toUnsignedLong(buf[off++]) & 0x1f) << 30;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 22;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 14;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 6;
    values[i + 1] |= Byte.toUnsignedLong(buf[off]) >>> 2;

    values[i + 2] = (Byte.toUnsignedLong(buf[off++]) & 3) << 33;
    values[i + 2] |= (Byte.toUnsignedLong(buf[off++])) << 25;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 17;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 9;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 1;
    values[i + 2] |= Byte.toUnsignedLong(buf[off]) >>> 7;

    values[i + 3] = (Byte.toUnsignedLong(buf[off++]) & 0x7f) << 28;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 20;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 12;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 4;
    values[i + 3] |= Byte.toUnsignedLong(buf[off]) >>> 4;

    values[i + 4] = (Byte.toUnsignedLong(buf[off++]) & 0xf) << 31;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 23;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 15;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 7;
    values[i + 4] |= Byte.toUnsignedLong(buf[off]) >>> 1;

    values[i + 5] = (Byte.toUnsignedLong(buf[off++]) & 1) << 34;
    values[i + 5] |= (Byte.toUnsignedLong(buf[off++])) << 26;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 18;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 10;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 2;
    values[i + 5] |= Byte.toUnsignedLong(buf[off]) >>> 6;

    values[i + 6] = (Byte.toUnsignedLong(buf[off++]) & 0x3f) << 29;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 21;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 13;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 5;
    values[i + 6] |= Byte.toUnsignedLong(buf[off]) >>> 3;

    values[i + 7] = (Byte.toUnsignedLong(buf[off++]) & 7) << 32;
    values[i + 7] |= (Byte.toUnsignedLong(buf[off++])) << 24;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 7] |= Byte.toUnsignedLong(buf[off]);
  }

  static void unpackBits36(final long[] values, final int i, final byte[] buf, int off) {
    values[i + 0] = (Byte.toUnsignedLong(buf[off++])) << 28;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 20;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 12;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 4;
    values[i + 0] |= Byte.toUnsignedLong(buf[off]) >>> 4;

    values[i + 1] = (Byte.toUnsignedLong(buf[off++]) & 0xf) << 32;
    values[i + 1] |= (Byte.toUnsignedLong(buf[off++])) << 24;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]);

    values[i + 2] = (Byte.toUnsignedLong(buf[off++])) << 28;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 20;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 12;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 4;
    values[i + 2] |= Byte.toUnsignedLong(buf[off]) >>> 4;

    values[i + 3] = (Byte.toUnsignedLong(buf[off++]) & 0xf) << 32;
    values[i + 3] |= (Byte.toUnsignedLong(buf[off++])) << 24;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]);

    values[i + 4] = (Byte.toUnsignedLong(buf[off++])) << 28;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 20;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 12;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 4;
    values[i + 4] |= Byte.toUnsignedLong(buf[off]) >>> 4;

    values[i + 5] = (Byte.toUnsignedLong(buf[off++]) & 0xf) << 32;
    values[i + 5] |= (Byte.toUnsignedLong(buf[off++])) << 24;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]);

    values[i + 6] = (Byte.toUnsignedLong(buf[off++])) << 28;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 20;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 12;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 4;
    values[i + 6] |= Byte.toUnsignedLong(buf[off]) >>> 4;

    values[i + 7] = (Byte.toUnsignedLong(buf[off++]) & 0xf) << 32;
    values[i + 7] |= (Byte.toUnsignedLong(buf[off++])) << 24;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 7] |= Byte.toUnsignedLong(buf[off]);
  }

  static void unpackBits37(final long[] values, final int i, final byte[] buf, int off) {
    values[i + 0] = (Byte.toUnsignedLong(buf[off++])) << 29;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 21;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 13;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 5;
    values[i + 0] |= Byte.toUnsignedLong(buf[off]) >>> 3;

    values[i + 1] = (Byte.toUnsignedLong(buf[off++]) & 7) << 34;
    values[i + 1] |= (Byte.toUnsignedLong(buf[off++])) << 26;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 18;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 10;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 2;
    values[i + 1] |= Byte.toUnsignedLong(buf[off]) >>> 6;

    values[i + 2] = (Byte.toUnsignedLong(buf[off++]) & 0x3f) << 31;
    values[i + 2] |= (Byte.toUnsignedLong(buf[off++])) << 23;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 15;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 7;
    values[i + 2] |= Byte.toUnsignedLong(buf[off]) >>> 1;

    values[i + 3] = (Byte.toUnsignedLong(buf[off++]) & 1) << 36;
    values[i + 3] |= (Byte.toUnsignedLong(buf[off++])) << 28;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 20;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 12;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 4;
    values[i + 3] |= Byte.toUnsignedLong(buf[off]) >>> 4;

    values[i + 4] = (Byte.toUnsignedLong(buf[off++]) & 0xf) << 33;
    values[i + 4] |= (Byte.toUnsignedLong(buf[off++])) << 25;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 17;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 9;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 1;
    values[i + 4] |= Byte.toUnsignedLong(buf[off]) >>> 7;

    values[i + 5] = (Byte.toUnsignedLong(buf[off++]) & 0x7f) << 30;
    values[i + 5] |= (Byte.toUnsignedLong(buf[off++])) << 22;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 14;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 6;
    values[i + 5] |= Byte.toUnsignedLong(buf[off]) >>> 2;

    values[i + 6] = (Byte.toUnsignedLong(buf[off++]) & 3) << 35;
    values[i + 6] |= (Byte.toUnsignedLong(buf[off++])) << 27;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 19;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 11;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 3;
    values[i + 6] |= Byte.toUnsignedLong(buf[off]) >>> 5;

    values[i + 7] = (Byte.toUnsignedLong(buf[off++]) & 0x1f) << 32;
    values[i + 7] |= (Byte.toUnsignedLong(buf[off++])) << 24;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 7] |= Byte.toUnsignedLong(buf[off]);
  }

  static void unpackBits38(final long[] values, final int i, final byte[] buf, int off) {
    values[i + 0] = (Byte.toUnsignedLong(buf[off++])) << 30;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 22;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 14;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 6;
    values[i + 0] |= Byte.toUnsignedLong(buf[off]) >>> 2;

    values[i + 1] = (Byte.toUnsignedLong(buf[off++]) & 3) << 36;
    values[i + 1] |= (Byte.toUnsignedLong(buf[off++])) << 28;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 20;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 12;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 4;
    values[i + 1] |= Byte.toUnsignedLong(buf[off]) >>> 4;

    values[i + 2] = (Byte.toUnsignedLong(buf[off++]) & 0xf) << 34;
    values[i + 2] |= (Byte.toUnsignedLong(buf[off++])) << 26;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 18;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 10;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 2;
    values[i + 2] |= Byte.toUnsignedLong(buf[off]) >>> 6;

    values[i + 3] = (Byte.toUnsignedLong(buf[off++]) & 0x3f) << 32;
    values[i + 3] |= (Byte.toUnsignedLong(buf[off++])) << 24;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]);

    values[i + 4] = (Byte.toUnsignedLong(buf[off++])) << 30;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 22;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 14;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 6;
    values[i + 4] |= Byte.toUnsignedLong(buf[off]) >>> 2;

    values[i + 5] = (Byte.toUnsignedLong(buf[off++]) & 3) << 36;
    values[i + 5] |= (Byte.toUnsignedLong(buf[off++])) << 28;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 20;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 12;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 4;
    values[i + 5] |= Byte.toUnsignedLong(buf[off]) >>> 4;

    values[i + 6] = (Byte.toUnsignedLong(buf[off++]) & 0xf) << 34;
    values[i + 6] |= (Byte.toUnsignedLong(buf[off++])) << 26;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 18;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 10;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 2;
    values[i + 6] |= Byte.toUnsignedLong(buf[off]) >>> 6;

    values[i + 7] = (Byte.toUnsignedLong(buf[off++]) & 0x3f) << 32;
    values[i + 7] |= (Byte.toUnsignedLong(buf[off++])) << 24;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 7] |= Byte.toUnsignedLong(buf[off]);
  }

  static void unpackBits39(final long[] values, final int i, final byte[] buf, int off) {
    values[i + 0] = (Byte.toUnsignedLong(buf[off++])) << 31;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 23;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 15;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 7;
    values[i + 0] |= Byte.toUnsignedLong(buf[off]) >>> 1;

    values[i + 1] = (Byte.toUnsignedLong(buf[off++]) & 1) << 38;
    values[i + 1] |= (Byte.toUnsignedLong(buf[off++])) << 30;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 22;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 14;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 6;
    values[i + 1] |= Byte.toUnsignedLong(buf[off]) >>> 2;

    values[i + 2] = (Byte.toUnsignedLong(buf[off++]) & 3) << 37;
    values[i + 2] |= (Byte.toUnsignedLong(buf[off++])) << 29;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 21;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 13;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 5;
    values[i + 2] |= Byte.toUnsignedLong(buf[off]) >>> 3;

    values[i + 3] = (Byte.toUnsignedLong(buf[off++]) & 7) << 36;
    values[i + 3] |= (Byte.toUnsignedLong(buf[off++])) << 28;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 20;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 12;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 4;
    values[i + 3] |= Byte.toUnsignedLong(buf[off]) >>> 4;

    values[i + 4] = (Byte.toUnsignedLong(buf[off++]) & 0xf) << 35;
    values[i + 4] |= (Byte.toUnsignedLong(buf[off++])) << 27;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 19;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 11;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 3;
    values[i + 4] |= Byte.toUnsignedLong(buf[off]) >>> 5;

    values[i + 5] = (Byte.toUnsignedLong(buf[off++]) & 0x1f) << 34;
    values[i + 5] |= (Byte.toUnsignedLong(buf[off++])) << 26;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 18;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 10;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 2;
    values[i + 5] |= Byte.toUnsignedLong(buf[off]) >>> 6;

    values[i + 6] = (Byte.toUnsignedLong(buf[off++]) & 0x3f) << 33;
    values[i + 6] |= (Byte.toUnsignedLong(buf[off++])) << 25;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 17;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 9;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 1;
    values[i + 6] |= Byte.toUnsignedLong(buf[off]) >>> 7;

    values[i + 7] = (Byte.toUnsignedLong(buf[off++]) & 0x7f) << 32;
    values[i + 7] |= (Byte.toUnsignedLong(buf[off++])) << 24;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 7] |= Byte.toUnsignedLong(buf[off]);
  }

  static void unpackBits40(final long[] values, final int i, final byte[] buf, int off) {
    values[i + 0] = (Byte.toUnsignedLong(buf[off++])) << 32;
    values[i + 0] |= (Byte.toUnsignedLong(buf[off++])) << 24;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]);
    values[i + 1] = (Byte.toUnsignedLong(buf[off++])) << 32;
    values[i + 1] |= (Byte.toUnsignedLong(buf[off++])) << 24;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]);
    values[i + 2] = (Byte.toUnsignedLong(buf[off++])) << 32;
    values[i + 2] |= (Byte.toUnsignedLong(buf[off++])) << 24;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]);
    values[i + 3] = (Byte.toUnsignedLong(buf[off++])) << 32;
    values[i + 3] |= (Byte.toUnsignedLong(buf[off++])) << 24;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]);
    values[i + 4] = (Byte.toUnsignedLong(buf[off++])) << 32;
    values[i + 4] |= (Byte.toUnsignedLong(buf[off++])) << 24;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]);
    values[i + 5] = (Byte.toUnsignedLong(buf[off++])) << 32;
    values[i + 5] |= (Byte.toUnsignedLong(buf[off++])) << 24;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]);
    values[i + 6] = (Byte.toUnsignedLong(buf[off++])) << 32;
    values[i + 6] |= (Byte.toUnsignedLong(buf[off++])) << 24;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]);
    values[i + 7] = (Byte.toUnsignedLong(buf[off++])) << 32;
    values[i + 7] |= (Byte.toUnsignedLong(buf[off++])) << 24;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 7] |= Byte.toUnsignedLong(buf[off]);
  }

  static void unpackBits41(final long[] values, final int i, final byte[] buf, int off) {
    values[i + 0] = (Byte.toUnsignedLong(buf[off++])) << 33;
    values[i + 0] |= (Byte.toUnsignedLong(buf[off++])) << 25;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 17;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 9;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 1;
    values[i + 0] |= Byte.toUnsignedLong(buf[off]) >>> 7;

    values[i + 1] = (Byte.toUnsignedLong(buf[off++]) & 0x7f) << 34;
    values[i + 1] |= (Byte.toUnsignedLong(buf[off++])) << 26;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 18;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 10;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 2;
    values[i + 1] |= Byte.toUnsignedLong(buf[off]) >>> 6;

    values[i + 2] = (Byte.toUnsignedLong(buf[off++]) & 0x3f) << 35;
    values[i + 2] |= (Byte.toUnsignedLong(buf[off++])) << 27;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 19;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 11;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 3;
    values[i + 2] |= Byte.toUnsignedLong(buf[off]) >>> 5;

    values[i + 3] = (Byte.toUnsignedLong(buf[off++]) & 0x1f) << 36;
    values[i + 3] |= (Byte.toUnsignedLong(buf[off++])) << 28;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 20;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 12;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 4;
    values[i + 3] |= Byte.toUnsignedLong(buf[off]) >>> 4;

    values[i + 4] = (Byte.toUnsignedLong(buf[off++]) & 0xf) << 37;
    values[i + 4] |= (Byte.toUnsignedLong(buf[off++])) << 29;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 21;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 13;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 5;
    values[i + 4] |= Byte.toUnsignedLong(buf[off]) >>> 3;

    values[i + 5] = (Byte.toUnsignedLong(buf[off++]) & 7) << 38;
    values[i + 5] |= (Byte.toUnsignedLong(buf[off++])) << 30;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 22;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 14;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 6;
    values[i + 5] |= Byte.toUnsignedLong(buf[off]) >>> 2;

    values[i + 6] = (Byte.toUnsignedLong(buf[off++]) & 3) << 39;
    values[i + 6] |= (Byte.toUnsignedLong(buf[off++])) << 31;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 23;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 15;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 7;
    values[i + 6] |= Byte.toUnsignedLong(buf[off]) >>> 1;

    values[i + 7] = (Byte.toUnsignedLong(buf[off++]) & 1) << 40;
    values[i + 7] |= (Byte.toUnsignedLong(buf[off++])) << 32;
    values[i + 7] |= (Byte.toUnsignedLong(buf[off++])) << 24;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 7] |= Byte.toUnsignedLong(buf[off]);
  }

  static void unpackBits42(final long[] values, final int i, final byte[] buf, int off) {
    values[i + 0] = (Byte.toUnsignedLong(buf[off++])) << 34;
    values[i + 0] |= (Byte.toUnsignedLong(buf[off++])) << 26;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 18;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 10;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 2;
    values[i + 0] |= Byte.toUnsignedLong(buf[off]) >>> 6;

    values[i + 1] = (Byte.toUnsignedLong(buf[off++]) & 0x3f) << 36;
    values[i + 1] |= (Byte.toUnsignedLong(buf[off++])) << 28;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 20;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 12;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 4;
    values[i + 1] |= Byte.toUnsignedLong(buf[off]) >>> 4;

    values[i + 2] = (Byte.toUnsignedLong(buf[off++]) & 0xf) << 38;
    values[i + 2] |= (Byte.toUnsignedLong(buf[off++])) << 30;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 22;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 14;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 6;
    values[i + 2] |= Byte.toUnsignedLong(buf[off]) >>> 2;

    values[i + 3] = (Byte.toUnsignedLong(buf[off++]) & 3) << 40;
    values[i + 3] |= (Byte.toUnsignedLong(buf[off++])) << 32;
    values[i + 3] |= (Byte.toUnsignedLong(buf[off++])) << 24;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]);

    values[i + 4] = (Byte.toUnsignedLong(buf[off++])) << 34;
    values[i + 4] |= (Byte.toUnsignedLong(buf[off++])) << 26;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 18;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 10;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 2;
    values[i + 4] |= Byte.toUnsignedLong(buf[off]) >>> 6;

    values[i + 5] = (Byte.toUnsignedLong(buf[off++]) & 0x3f) << 36;
    values[i + 5] |= (Byte.toUnsignedLong(buf[off++])) << 28;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 20;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 12;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 4;
    values[i + 5] |= Byte.toUnsignedLong(buf[off]) >>> 4;

    values[i + 6] = (Byte.toUnsignedLong(buf[off++]) & 0xf) << 38;
    values[i + 6] |= (Byte.toUnsignedLong(buf[off++])) << 30;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 22;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 14;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 6;
    values[i + 6] |= Byte.toUnsignedLong(buf[off]) >>> 2;

    values[i + 7] = (Byte.toUnsignedLong(buf[off++]) & 3) << 40;
    values[i + 7] |= (Byte.toUnsignedLong(buf[off++])) << 32;
    values[i + 7] |= (Byte.toUnsignedLong(buf[off++])) << 24;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 7] |= Byte.toUnsignedLong(buf[off]);
  }

  static void unpackBits43(final long[] values, final int i, final byte[] buf, int off) {
    values[i + 0] = (Byte.toUnsignedLong(buf[off++])) << 35;
    values[i + 0] |= (Byte.toUnsignedLong(buf[off++])) << 27;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 19;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 11;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 3;
    values[i + 0] |= Byte.toUnsignedLong(buf[off]) >>> 5;

    values[i + 1] = (Byte.toUnsignedLong(buf[off++]) & 0x1f) << 38;
    values[i + 1] |= (Byte.toUnsignedLong(buf[off++])) << 30;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 22;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 14;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 6;
    values[i + 1] |= Byte.toUnsignedLong(buf[off]) >>> 2;

    values[i + 2] = (Byte.toUnsignedLong(buf[off++]) & 3) << 41;
    values[i + 2] |= (Byte.toUnsignedLong(buf[off++])) << 33;
    values[i + 2] |= (Byte.toUnsignedLong(buf[off++])) << 25;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 17;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 9;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 1;
    values[i + 2] |= Byte.toUnsignedLong(buf[off]) >>> 7;

    values[i + 3] = (Byte.toUnsignedLong(buf[off++]) & 0x7f) << 36;
    values[i + 3] |= (Byte.toUnsignedLong(buf[off++])) << 28;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 20;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 12;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 4;
    values[i + 3] |= Byte.toUnsignedLong(buf[off]) >>> 4;

    values[i + 4] = (Byte.toUnsignedLong(buf[off++]) & 0xf) << 39;
    values[i + 4] |= (Byte.toUnsignedLong(buf[off++])) << 31;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 23;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 15;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 7;
    values[i + 4] |= Byte.toUnsignedLong(buf[off]) >>> 1;

    values[i + 5] = (Byte.toUnsignedLong(buf[off++]) & 1) << 42;
    values[i + 5] |= (Byte.toUnsignedLong(buf[off++])) << 34;
    values[i + 5] |= (Byte.toUnsignedLong(buf[off++])) << 26;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 18;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 10;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 2;
    values[i + 5] |= Byte.toUnsignedLong(buf[off]) >>> 6;

    values[i + 6] = (Byte.toUnsignedLong(buf[off++]) & 0x3f) << 37;
    values[i + 6] |= (Byte.toUnsignedLong(buf[off++])) << 29;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 21;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 13;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 5;
    values[i + 6] |= Byte.toUnsignedLong(buf[off]) >>> 3;

    values[i + 7] = (Byte.toUnsignedLong(buf[off++]) & 7) << 40;
    values[i + 7] |= (Byte.toUnsignedLong(buf[off++])) << 32;
    values[i + 7] |= (Byte.toUnsignedLong(buf[off++])) << 24;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 7] |= Byte.toUnsignedLong(buf[off]);
  }

  static void unpackBits44(final long[] values, final int i, final byte[] buf, int off) {
    values[i + 0] = (Byte.toUnsignedLong(buf[off++])) << 36;
    values[i + 0] |= (Byte.toUnsignedLong(buf[off++])) << 28;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 20;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 12;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 4;
    values[i + 0] |= Byte.toUnsignedLong(buf[off]) >>> 4;

    values[i + 1] = (Byte.toUnsignedLong(buf[off++]) & 0xf) << 40;
    values[i + 1] |= (Byte.toUnsignedLong(buf[off++])) << 32;
    values[i + 1] |= (Byte.toUnsignedLong(buf[off++])) << 24;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]);

    values[i + 2] = (Byte.toUnsignedLong(buf[off++])) << 36;
    values[i + 2] |= (Byte.toUnsignedLong(buf[off++])) << 28;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 20;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 12;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 4;
    values[i + 2] |= Byte.toUnsignedLong(buf[off]) >>> 4;

    values[i + 3] = (Byte.toUnsignedLong(buf[off++]) & 0xf) << 40;
    values[i + 3] |= (Byte.toUnsignedLong(buf[off++])) << 32;
    values[i + 3] |= (Byte.toUnsignedLong(buf[off++])) << 24;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]);

    values[i + 4] = (Byte.toUnsignedLong(buf[off++])) << 36;
    values[i + 4] |= (Byte.toUnsignedLong(buf[off++])) << 28;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 20;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 12;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 4;
    values[i + 4] |= Byte.toUnsignedLong(buf[off]) >>> 4;

    values[i + 5] = (Byte.toUnsignedLong(buf[off++]) & 0xf) << 40;
    values[i + 5] |= (Byte.toUnsignedLong(buf[off++])) << 32;
    values[i + 5] |= (Byte.toUnsignedLong(buf[off++])) << 24;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]);

    values[i + 6] = (Byte.toUnsignedLong(buf[off++])) << 36;
    values[i + 6] |= (Byte.toUnsignedLong(buf[off++])) << 28;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 20;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 12;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 4;
    values[i + 6] |= Byte.toUnsignedLong(buf[off]) >>> 4;

    values[i + 7] = (Byte.toUnsignedLong(buf[off++]) & 0xf) << 40;
    values[i + 7] |= (Byte.toUnsignedLong(buf[off++])) << 32;
    values[i + 7] |= (Byte.toUnsignedLong(buf[off++])) << 24;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 7] |= Byte.toUnsignedLong(buf[off]);
  }

  static void unpackBits45(final long[] values, final int i, final byte[] buf, int off) {
    values[i + 0] = (Byte.toUnsignedLong(buf[off++])) << 37;
    values[i + 0] |= (Byte.toUnsignedLong(buf[off++])) << 29;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 21;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 13;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 5;
    values[i + 0] |= Byte.toUnsignedLong(buf[off]) >>> 3;

    values[i + 1] = (Byte.toUnsignedLong(buf[off++]) & 7) << 42;
    values[i + 1] |= (Byte.toUnsignedLong(buf[off++])) << 34;
    values[i + 1] |= (Byte.toUnsignedLong(buf[off++])) << 26;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 18;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 10;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 2;
    values[i + 1] |= Byte.toUnsignedLong(buf[off]) >>> 6;

    values[i + 2] = (Byte.toUnsignedLong(buf[off++]) & 0x3f) << 39;
    values[i + 2] |= (Byte.toUnsignedLong(buf[off++])) << 31;
    values[i + 2] |= (Byte.toUnsignedLong(buf[off++])) << 23;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 15;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 7;
    values[i + 2] |= Byte.toUnsignedLong(buf[off]) >>> 1;

    values[i + 3] = (Byte.toUnsignedLong(buf[off++]) & 1) << 44;
    values[i + 3] |= (Byte.toUnsignedLong(buf[off++])) << 36;
    values[i + 3] |= (Byte.toUnsignedLong(buf[off++])) << 28;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 20;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 12;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 4;
    values[i + 3] |= Byte.toUnsignedLong(buf[off]) >>> 4;

    values[i + 4] = (Byte.toUnsignedLong(buf[off++]) & 0xf) << 41;
    values[i + 4] |= (Byte.toUnsignedLong(buf[off++])) << 33;
    values[i + 4] |= (Byte.toUnsignedLong(buf[off++])) << 25;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 17;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 9;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 1;
    values[i + 4] |= Byte.toUnsignedLong(buf[off]) >>> 7;

    values[i + 5] = (Byte.toUnsignedLong(buf[off++]) & 0x7f) << 38;
    values[i + 5] |= (Byte.toUnsignedLong(buf[off++])) << 30;
    values[i + 5] |= (Byte.toUnsignedLong(buf[off++])) << 22;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 14;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 6;
    values[i + 5] |= Byte.toUnsignedLong(buf[off]) >>> 2;

    values[i + 6] = (Byte.toUnsignedLong(buf[off++]) & 3) << 43;
    values[i + 6] |= (Byte.toUnsignedLong(buf[off++])) << 35;
    values[i + 6] |= (Byte.toUnsignedLong(buf[off++])) << 27;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 19;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 11;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 3;
    values[i + 6] |= Byte.toUnsignedLong(buf[off]) >>> 5;

    values[i + 7] = (Byte.toUnsignedLong(buf[off++]) & 0x1f) << 40;
    values[i + 7] |= (Byte.toUnsignedLong(buf[off++])) << 32;
    values[i + 7] |= (Byte.toUnsignedLong(buf[off++])) << 24;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 7] |= Byte.toUnsignedLong(buf[off]);
  }

  static void unpackBits46(final long[] values, final int i, final byte[] buf, int off) {
    values[i + 0] = (Byte.toUnsignedLong(buf[off++])) << 38;
    values[i + 0] |= (Byte.toUnsignedLong(buf[off++])) << 30;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 22;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 14;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 6;
    values[i + 0] |= Byte.toUnsignedLong(buf[off]) >>> 2;

    values[i + 1] = (Byte.toUnsignedLong(buf[off++]) & 3) << 44;
    values[i + 1] |= (Byte.toUnsignedLong(buf[off++])) << 36;
    values[i + 1] |= (Byte.toUnsignedLong(buf[off++])) << 28;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 20;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 12;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 4;
    values[i + 1] |= Byte.toUnsignedLong(buf[off]) >>> 4;

    values[i + 2] = (Byte.toUnsignedLong(buf[off++]) & 0xf) << 42;
    values[i + 2] |= (Byte.toUnsignedLong(buf[off++])) << 34;
    values[i + 2] |= (Byte.toUnsignedLong(buf[off++])) << 26;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 18;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 10;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 2;
    values[i + 2] |= Byte.toUnsignedLong(buf[off]) >>> 6;

    values[i + 3] = (Byte.toUnsignedLong(buf[off++]) & 0x3f) << 40;
    values[i + 3] |= (Byte.toUnsignedLong(buf[off++])) << 32;
    values[i + 3] |= (Byte.toUnsignedLong(buf[off++])) << 24;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]);

    values[i + 4] = (Byte.toUnsignedLong(buf[off++])) << 38;
    values[i + 4] |= (Byte.toUnsignedLong(buf[off++])) << 30;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 22;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 14;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 6;
    values[i + 4] |= Byte.toUnsignedLong(buf[off]) >>> 2;

    values[i + 5] = (Byte.toUnsignedLong(buf[off++]) & 3) << 44;
    values[i + 5] |= (Byte.toUnsignedLong(buf[off++])) << 36;
    values[i + 5] |= (Byte.toUnsignedLong(buf[off++])) << 28;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 20;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 12;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 4;
    values[i + 5] |= Byte.toUnsignedLong(buf[off]) >>> 4;

    values[i + 6] = (Byte.toUnsignedLong(buf[off++]) & 0xf) << 42;
    values[i + 6] |= (Byte.toUnsignedLong(buf[off++])) << 34;
    values[i + 6] |= (Byte.toUnsignedLong(buf[off++])) << 26;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 18;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 10;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 2;
    values[i + 6] |= Byte.toUnsignedLong(buf[off]) >>> 6;

    values[i + 7] = (Byte.toUnsignedLong(buf[off++]) & 0x3f) << 40;
    values[i + 7] |= (Byte.toUnsignedLong(buf[off++])) << 32;
    values[i + 7] |= (Byte.toUnsignedLong(buf[off++])) << 24;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 7] |= Byte.toUnsignedLong(buf[off]);
  }

  static void unpackBits47(final long[] values, final int i, final byte[] buf, int off) {
    values[i + 0] = (Byte.toUnsignedLong(buf[off++])) << 39;
    values[i + 0] |= (Byte.toUnsignedLong(buf[off++])) << 31;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 23;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 15;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 7;
    values[i + 0] |= Byte.toUnsignedLong(buf[off]) >>> 1;

    values[i + 1] = (Byte.toUnsignedLong(buf[off++]) & 1) << 46;
    values[i + 1] |= (Byte.toUnsignedLong(buf[off++])) << 38;
    values[i + 1] |= (Byte.toUnsignedLong(buf[off++])) << 30;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 22;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 14;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 6;
    values[i + 1] |= Byte.toUnsignedLong(buf[off]) >>> 2;

    values[i + 2] = (Byte.toUnsignedLong(buf[off++]) & 3) << 45;
    values[i + 2] |= (Byte.toUnsignedLong(buf[off++])) << 37;
    values[i + 2] |= (Byte.toUnsignedLong(buf[off++])) << 29;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 21;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 13;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 5;
    values[i + 2] |= Byte.toUnsignedLong(buf[off]) >>> 3;

    values[i + 3] = (Byte.toUnsignedLong(buf[off++]) & 7) << 44;
    values[i + 3] |= (Byte.toUnsignedLong(buf[off++])) << 36;
    values[i + 3] |= (Byte.toUnsignedLong(buf[off++])) << 28;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 20;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 12;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 4;
    values[i + 3] |= Byte.toUnsignedLong(buf[off]) >>> 4;

    values[i + 4] = (Byte.toUnsignedLong(buf[off++]) & 0xf) << 43;
    values[i + 4] |= (Byte.toUnsignedLong(buf[off++])) << 35;
    values[i + 4] |= (Byte.toUnsignedLong(buf[off++])) << 27;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 19;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 11;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 3;
    values[i + 4] |= Byte.toUnsignedLong(buf[off]) >>> 5;

    values[i + 5] = (Byte.toUnsignedLong(buf[off++]) & 0x1f) << 42;
    values[i + 5] |= (Byte.toUnsignedLong(buf[off++])) << 34;
    values[i + 5] |= (Byte.toUnsignedLong(buf[off++])) << 26;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 18;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 10;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 2;
    values[i + 5] |= Byte.toUnsignedLong(buf[off]) >>> 6;

    values[i + 6] = (Byte.toUnsignedLong(buf[off++]) & 0x3f) << 41;
    values[i + 6] |= (Byte.toUnsignedLong(buf[off++])) << 33;
    values[i + 6] |= (Byte.toUnsignedLong(buf[off++])) << 25;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 17;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 9;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 1;
    values[i + 6] |= Byte.toUnsignedLong(buf[off]) >>> 7;

    values[i + 7] = (Byte.toUnsignedLong(buf[off++]) & 0x7f) << 40;
    values[i + 7] |= (Byte.toUnsignedLong(buf[off++])) << 32;
    values[i + 7] |= (Byte.toUnsignedLong(buf[off++])) << 24;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 7] |= Byte.toUnsignedLong(buf[off]);
  }

  static void unpackBits48(final long[] values, final int i, final byte[] buf, int off) {
    values[i + 0] = (Byte.toUnsignedLong(buf[off++])) << 40;
    values[i + 0] |= (Byte.toUnsignedLong(buf[off++])) << 32;
    values[i + 0] |= (Byte.toUnsignedLong(buf[off++])) << 24;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]);
    values[i + 1] = (Byte.toUnsignedLong(buf[off++])) << 40;
    values[i + 1] |= (Byte.toUnsignedLong(buf[off++])) << 32;
    values[i + 1] |= (Byte.toUnsignedLong(buf[off++])) << 24;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]);
    values[i + 2] = (Byte.toUnsignedLong(buf[off++])) << 40;
    values[i + 2] |= (Byte.toUnsignedLong(buf[off++])) << 32;
    values[i + 2] |= (Byte.toUnsignedLong(buf[off++])) << 24;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]);
    values[i + 3] = (Byte.toUnsignedLong(buf[off++])) << 40;
    values[i + 3] |= (Byte.toUnsignedLong(buf[off++])) << 32;
    values[i + 3] |= (Byte.toUnsignedLong(buf[off++])) << 24;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]);
    values[i + 4] = (Byte.toUnsignedLong(buf[off++])) << 40;
    values[i + 4] |= (Byte.toUnsignedLong(buf[off++])) << 32;
    values[i + 4] |= (Byte.toUnsignedLong(buf[off++])) << 24;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]);
    values[i + 5] = (Byte.toUnsignedLong(buf[off++])) << 40;
    values[i + 5] |= (Byte.toUnsignedLong(buf[off++])) << 32;
    values[i + 5] |= (Byte.toUnsignedLong(buf[off++])) << 24;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]);
    values[i + 6] = (Byte.toUnsignedLong(buf[off++])) << 40;
    values[i + 6] |= (Byte.toUnsignedLong(buf[off++])) << 32;
    values[i + 6] |= (Byte.toUnsignedLong(buf[off++])) << 24;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]);
    values[i + 7] = (Byte.toUnsignedLong(buf[off++])) << 40;
    values[i + 7] |= (Byte.toUnsignedLong(buf[off++])) << 32;
    values[i + 7] |= (Byte.toUnsignedLong(buf[off++])) << 24;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 7] |= Byte.toUnsignedLong(buf[off]);
  }

  static void unpackBits49(final long[] values, final int i, final byte[] buf, int off) {
    values[i + 0] = (Byte.toUnsignedLong(buf[off++])) << 41;
    values[i + 0] |= (Byte.toUnsignedLong(buf[off++])) << 33;
    values[i + 0] |= (Byte.toUnsignedLong(buf[off++])) << 25;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 17;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 9;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 1;
    values[i + 0] |= Byte.toUnsignedLong(buf[off]) >>> 7;

    values[i + 1] = (Byte.toUnsignedLong(buf[off++]) & 0x7f) << 42;
    values[i + 1] |= (Byte.toUnsignedLong(buf[off++])) << 34;
    values[i + 1] |= (Byte.toUnsignedLong(buf[off++])) << 26;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 18;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 10;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 2;
    values[i + 1] |= Byte.toUnsignedLong(buf[off]) >>> 6;

    values[i + 2] = (Byte.toUnsignedLong(buf[off++]) & 0x3f) << 43;
    values[i + 2] |= (Byte.toUnsignedLong(buf[off++])) << 35;
    values[i + 2] |= (Byte.toUnsignedLong(buf[off++])) << 27;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 19;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 11;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 3;
    values[i + 2] |= Byte.toUnsignedLong(buf[off]) >>> 5;

    values[i + 3] = (Byte.toUnsignedLong(buf[off++]) & 0x1f) << 44;
    values[i + 3] |= (Byte.toUnsignedLong(buf[off++])) << 36;
    values[i + 3] |= (Byte.toUnsignedLong(buf[off++])) << 28;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 20;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 12;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 4;
    values[i + 3] |= Byte.toUnsignedLong(buf[off]) >>> 4;

    values[i + 4] = (Byte.toUnsignedLong(buf[off++]) & 0xf) << 45;
    values[i + 4] |= (Byte.toUnsignedLong(buf[off++])) << 37;
    values[i + 4] |= (Byte.toUnsignedLong(buf[off++])) << 29;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 21;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 13;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 5;
    values[i + 4] |= Byte.toUnsignedLong(buf[off]) >>> 3;

    values[i + 5] = (Byte.toUnsignedLong(buf[off++]) & 7) << 46;
    values[i + 5] |= (Byte.toUnsignedLong(buf[off++])) << 38;
    values[i + 5] |= (Byte.toUnsignedLong(buf[off++])) << 30;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 22;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 14;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 6;
    values[i + 5] |= Byte.toUnsignedLong(buf[off]) >>> 2;

    values[i + 6] = (Byte.toUnsignedLong(buf[off++]) & 3) << 47;
    values[i + 6] |= (Byte.toUnsignedLong(buf[off++])) << 39;
    values[i + 6] |= (Byte.toUnsignedLong(buf[off++])) << 31;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 23;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 15;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 7;
    values[i + 6] |= Byte.toUnsignedLong(buf[off]) >>> 1;

    values[i + 7] = (Byte.toUnsignedLong(buf[off++]) & 1) << 48;
    values[i + 7] |= (Byte.toUnsignedLong(buf[off++])) << 40;
    values[i + 7] |= (Byte.toUnsignedLong(buf[off++])) << 32;
    values[i + 7] |= (Byte.toUnsignedLong(buf[off++])) << 24;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 7] |= Byte.toUnsignedLong(buf[off]);
  }

  static void unpackBits50(final long[] values, final int i, final byte[] buf, int off) {
    values[i + 0] = (Byte.toUnsignedLong(buf[off++])) << 42;
    values[i + 0] |= (Byte.toUnsignedLong(buf[off++])) << 34;
    values[i + 0] |= (Byte.toUnsignedLong(buf[off++])) << 26;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 18;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 10;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 2;
    values[i + 0] |= Byte.toUnsignedLong(buf[off]) >>> 6;

    values[i + 1] = (Byte.toUnsignedLong(buf[off++]) & 0x3f) << 44;
    values[i + 1] |= (Byte.toUnsignedLong(buf[off++])) << 36;
    values[i + 1] |= (Byte.toUnsignedLong(buf[off++])) << 28;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 20;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 12;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 4;
    values[i + 1] |= Byte.toUnsignedLong(buf[off]) >>> 4;

    values[i + 2] = (Byte.toUnsignedLong(buf[off++]) & 0xf) << 46;
    values[i + 2] |= (Byte.toUnsignedLong(buf[off++])) << 38;
    values[i + 2] |= (Byte.toUnsignedLong(buf[off++])) << 30;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 22;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 14;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 6;
    values[i + 2] |= Byte.toUnsignedLong(buf[off]) >>> 2;

    values[i + 3] = (Byte.toUnsignedLong(buf[off++]) & 3) << 48;
    values[i + 3] |= (Byte.toUnsignedLong(buf[off++])) << 40;
    values[i + 3] |= (Byte.toUnsignedLong(buf[off++])) << 32;
    values[i + 3] |= (Byte.toUnsignedLong(buf[off++])) << 24;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]);

    values[i + 4] = (Byte.toUnsignedLong(buf[off++])) << 42;
    values[i + 4] |= (Byte.toUnsignedLong(buf[off++])) << 34;
    values[i + 4] |= (Byte.toUnsignedLong(buf[off++])) << 26;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 18;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 10;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 2;
    values[i + 4] |= Byte.toUnsignedLong(buf[off]) >>> 6;

    values[i + 5] = (Byte.toUnsignedLong(buf[off++]) & 0x3f) << 44;
    values[i + 5] |= (Byte.toUnsignedLong(buf[off++])) << 36;
    values[i + 5] |= (Byte.toUnsignedLong(buf[off++])) << 28;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 20;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 12;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 4;
    values[i + 5] |= Byte.toUnsignedLong(buf[off]) >>> 4;

    values[i + 6] = (Byte.toUnsignedLong(buf[off++]) & 0xf) << 46;
    values[i + 6] |= (Byte.toUnsignedLong(buf[off++])) << 38;
    values[i + 6] |= (Byte.toUnsignedLong(buf[off++])) << 30;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 22;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 14;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 6;
    values[i + 6] |= Byte.toUnsignedLong(buf[off]) >>> 2;

    values[i + 7] = (Byte.toUnsignedLong(buf[off++]) & 3) << 48;
    values[i + 7] |= (Byte.toUnsignedLong(buf[off++])) << 40;
    values[i + 7] |= (Byte.toUnsignedLong(buf[off++])) << 32;
    values[i + 7] |= (Byte.toUnsignedLong(buf[off++])) << 24;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 7] |= Byte.toUnsignedLong(buf[off]);
  }

  static void unpackBits51(final long[] values, final int i, final byte[] buf, int off) {
    values[i + 0] = (Byte.toUnsignedLong(buf[off++])) << 43;
    values[i + 0] |= (Byte.toUnsignedLong(buf[off++])) << 35;
    values[i + 0] |= (Byte.toUnsignedLong(buf[off++])) << 27;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 19;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 11;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 3;
    values[i + 0] |= Byte.toUnsignedLong(buf[off]) >>> 5;

    values[i + 1] = (Byte.toUnsignedLong(buf[off++]) & 0x1f) << 46;
    values[i + 1] |= (Byte.toUnsignedLong(buf[off++])) << 38;
    values[i + 1] |= (Byte.toUnsignedLong(buf[off++])) << 30;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 22;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 14;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 6;
    values[i + 1] |= Byte.toUnsignedLong(buf[off]) >>> 2;

    values[i + 2] = (Byte.toUnsignedLong(buf[off++]) & 3) << 49;
    values[i + 2] |= (Byte.toUnsignedLong(buf[off++])) << 41;
    values[i + 2] |= (Byte.toUnsignedLong(buf[off++])) << 33;
    values[i + 2] |= (Byte.toUnsignedLong(buf[off++])) << 25;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 17;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 9;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 1;
    values[i + 2] |= Byte.toUnsignedLong(buf[off]) >>> 7;

    values[i + 3] = (Byte.toUnsignedLong(buf[off++]) & 0x7f) << 44;
    values[i + 3] |= (Byte.toUnsignedLong(buf[off++])) << 36;
    values[i + 3] |= (Byte.toUnsignedLong(buf[off++])) << 28;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 20;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 12;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 4;
    values[i + 3] |= Byte.toUnsignedLong(buf[off]) >>> 4;

    values[i + 4] = (Byte.toUnsignedLong(buf[off++]) & 0xf) << 47;
    values[i + 4] |= (Byte.toUnsignedLong(buf[off++])) << 39;
    values[i + 4] |= (Byte.toUnsignedLong(buf[off++])) << 31;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 23;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 15;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 7;
    values[i + 4] |= Byte.toUnsignedLong(buf[off]) >>> 1;

    values[i + 5] = (Byte.toUnsignedLong(buf[off++]) & 1) << 50;
    values[i + 5] |= (Byte.toUnsignedLong(buf[off++])) << 42;
    values[i + 5] |= (Byte.toUnsignedLong(buf[off++])) << 34;
    values[i + 5] |= (Byte.toUnsignedLong(buf[off++])) << 26;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 18;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 10;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 2;
    values[i + 5] |= Byte.toUnsignedLong(buf[off]) >>> 6;

    values[i + 6] = (Byte.toUnsignedLong(buf[off++]) & 0x3f) << 45;
    values[i + 6] |= (Byte.toUnsignedLong(buf[off++])) << 37;
    values[i + 6] |= (Byte.toUnsignedLong(buf[off++])) << 29;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 21;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 13;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 5;
    values[i + 6] |= Byte.toUnsignedLong(buf[off]) >>> 3;

    values[i + 7] = (Byte.toUnsignedLong(buf[off++]) & 7) << 48;
    values[i + 7] |= (Byte.toUnsignedLong(buf[off++])) << 40;
    values[i + 7] |= (Byte.toUnsignedLong(buf[off++])) << 32;
    values[i + 7] |= (Byte.toUnsignedLong(buf[off++])) << 24;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 7] |= Byte.toUnsignedLong(buf[off]);
  }

  static void unpackBits52(final long[] values, final int i, final byte[] buf, int off) {
    values[i + 0] = (Byte.toUnsignedLong(buf[off++])) << 44;
    values[i + 0] |= (Byte.toUnsignedLong(buf[off++])) << 36;
    values[i + 0] |= (Byte.toUnsignedLong(buf[off++])) << 28;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 20;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 12;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 4;
    values[i + 0] |= Byte.toUnsignedLong(buf[off]) >>> 4;

    values[i + 1] = (Byte.toUnsignedLong(buf[off++]) & 0xf) << 48;
    values[i + 1] |= (Byte.toUnsignedLong(buf[off++])) << 40;
    values[i + 1] |= (Byte.toUnsignedLong(buf[off++])) << 32;
    values[i + 1] |= (Byte.toUnsignedLong(buf[off++])) << 24;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]);

    values[i + 2] = (Byte.toUnsignedLong(buf[off++])) << 44;
    values[i + 2] |= (Byte.toUnsignedLong(buf[off++])) << 36;
    values[i + 2] |= (Byte.toUnsignedLong(buf[off++])) << 28;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 20;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 12;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 4;
    values[i + 2] |= Byte.toUnsignedLong(buf[off]) >>> 4;

    values[i + 3] = (Byte.toUnsignedLong(buf[off++]) & 0xf) << 48;
    values[i + 3] |= (Byte.toUnsignedLong(buf[off++])) << 40;
    values[i + 3] |= (Byte.toUnsignedLong(buf[off++])) << 32;
    values[i + 3] |= (Byte.toUnsignedLong(buf[off++])) << 24;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]);

    values[i + 4] = (Byte.toUnsignedLong(buf[off++])) << 44;
    values[i + 4] |= (Byte.toUnsignedLong(buf[off++])) << 36;
    values[i + 4] |= (Byte.toUnsignedLong(buf[off++])) << 28;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 20;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 12;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 4;
    values[i + 4] |= Byte.toUnsignedLong(buf[off]) >>> 4;

    values[i + 5] = (Byte.toUnsignedLong(buf[off++]) & 0xf) << 48;
    values[i + 5] |= (Byte.toUnsignedLong(buf[off++])) << 40;
    values[i + 5] |= (Byte.toUnsignedLong(buf[off++])) << 32;
    values[i + 5] |= (Byte.toUnsignedLong(buf[off++])) << 24;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]);

    values[i + 6] = (Byte.toUnsignedLong(buf[off++])) << 44;
    values[i + 6] |= (Byte.toUnsignedLong(buf[off++])) << 36;
    values[i + 6] |= (Byte.toUnsignedLong(buf[off++])) << 28;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 20;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 12;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 4;
    values[i + 6] |= Byte.toUnsignedLong(buf[off]) >>> 4;

    values[i + 7] = (Byte.toUnsignedLong(buf[off++]) & 0xf) << 48;
    values[i + 7] |= (Byte.toUnsignedLong(buf[off++])) << 40;
    values[i + 7] |= (Byte.toUnsignedLong(buf[off++])) << 32;
    values[i + 7] |= (Byte.toUnsignedLong(buf[off++])) << 24;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 7] |= Byte.toUnsignedLong(buf[off]);
  }

  static void unpackBits53(final long[] values, final int i, final byte[] buf, int off) {
    values[i + 0] = (Byte.toUnsignedLong(buf[off++])) << 45;
    values[i + 0] |= (Byte.toUnsignedLong(buf[off++])) << 37;
    values[i + 0] |= (Byte.toUnsignedLong(buf[off++])) << 29;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 21;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 13;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 5;
    values[i + 0] |= Byte.toUnsignedLong(buf[off]) >>> 3;

    values[i + 1] = (Byte.toUnsignedLong(buf[off++]) & 7) << 50;
    values[i + 1] |= (Byte.toUnsignedLong(buf[off++])) << 42;
    values[i + 1] |= (Byte.toUnsignedLong(buf[off++])) << 34;
    values[i + 1] |= (Byte.toUnsignedLong(buf[off++])) << 26;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 18;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 10;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 2;
    values[i + 1] |= Byte.toUnsignedLong(buf[off]) >>> 6;

    values[i + 2] = (Byte.toUnsignedLong(buf[off++]) & 0x3f) << 47;
    values[i + 2] |= (Byte.toUnsignedLong(buf[off++])) << 39;
    values[i + 2] |= (Byte.toUnsignedLong(buf[off++])) << 31;
    values[i + 2] |= (Byte.toUnsignedLong(buf[off++])) << 23;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 15;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 7;
    values[i + 2] |= Byte.toUnsignedLong(buf[off]) >>> 1;

    values[i + 3] = (Byte.toUnsignedLong(buf[off++]) & 1) << 52;
    values[i + 3] |= (Byte.toUnsignedLong(buf[off++])) << 44;
    values[i + 3] |= (Byte.toUnsignedLong(buf[off++])) << 36;
    values[i + 3] |= (Byte.toUnsignedLong(buf[off++])) << 28;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 20;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 12;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 4;
    values[i + 3] |= Byte.toUnsignedLong(buf[off]) >>> 4;

    values[i + 4] = (Byte.toUnsignedLong(buf[off++]) & 0xf) << 49;
    values[i + 4] |= (Byte.toUnsignedLong(buf[off++])) << 41;
    values[i + 4] |= (Byte.toUnsignedLong(buf[off++])) << 33;
    values[i + 4] |= (Byte.toUnsignedLong(buf[off++])) << 25;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 17;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 9;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 1;
    values[i + 4] |= Byte.toUnsignedLong(buf[off]) >>> 7;

    values[i + 5] = (Byte.toUnsignedLong(buf[off++]) & 0x7f) << 46;
    values[i + 5] |= (Byte.toUnsignedLong(buf[off++])) << 38;
    values[i + 5] |= (Byte.toUnsignedLong(buf[off++])) << 30;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 22;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 14;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 6;
    values[i + 5] |= Byte.toUnsignedLong(buf[off]) >>> 2;

    values[i + 6] = (Byte.toUnsignedLong(buf[off++]) & 3) << 51;
    values[i + 6] |= (Byte.toUnsignedLong(buf[off++])) << 43;
    values[i + 6] |= (Byte.toUnsignedLong(buf[off++])) << 35;
    values[i + 6] |= (Byte.toUnsignedLong(buf[off++])) << 27;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 19;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 11;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 3;
    values[i + 6] |= Byte.toUnsignedLong(buf[off]) >>> 5;

    values[i + 7] = (Byte.toUnsignedLong(buf[off++]) & 0x1f) << 48;
    values[i + 7] |= (Byte.toUnsignedLong(buf[off++])) << 40;
    values[i + 7] |= (Byte.toUnsignedLong(buf[off++])) << 32;
    values[i + 7] |= (Byte.toUnsignedLong(buf[off++])) << 24;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 7] |= Byte.toUnsignedLong(buf[off]);
  }

  static void unpackBits54(final long[] values, final int i, final byte[] buf, int off) {
    values[i + 0] = (Byte.toUnsignedLong(buf[off++])) << 46;
    values[i + 0] |= (Byte.toUnsignedLong(buf[off++])) << 38;
    values[i + 0] |= (Byte.toUnsignedLong(buf[off++])) << 30;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 22;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 14;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 6;
    values[i + 0] |= Byte.toUnsignedLong(buf[off]) >>> 2;

    values[i + 1] = (Byte.toUnsignedLong(buf[off++]) & 3) << 52;
    values[i + 1] |= (Byte.toUnsignedLong(buf[off++])) << 44;
    values[i + 1] |= (Byte.toUnsignedLong(buf[off++])) << 36;
    values[i + 1] |= (Byte.toUnsignedLong(buf[off++])) << 28;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 20;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 12;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 4;
    values[i + 1] |= Byte.toUnsignedLong(buf[off]) >>> 4;

    values[i + 2] = (Byte.toUnsignedLong(buf[off++]) & 0xf) << 50;
    values[i + 2] |= (Byte.toUnsignedLong(buf[off++])) << 42;
    values[i + 2] |= (Byte.toUnsignedLong(buf[off++])) << 34;
    values[i + 2] |= (Byte.toUnsignedLong(buf[off++])) << 26;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 18;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 10;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 2;
    values[i + 2] |= Byte.toUnsignedLong(buf[off]) >>> 6;

    values[i + 3] = (Byte.toUnsignedLong(buf[off++]) & 0x3f) << 48;
    values[i + 3] |= (Byte.toUnsignedLong(buf[off++])) << 40;
    values[i + 3] |= (Byte.toUnsignedLong(buf[off++])) << 32;
    values[i + 3] |= (Byte.toUnsignedLong(buf[off++])) << 24;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]);

    values[i + 4] = (Byte.toUnsignedLong(buf[off++])) << 46;
    values[i + 4] |= (Byte.toUnsignedLong(buf[off++])) << 38;
    values[i + 4] |= (Byte.toUnsignedLong(buf[off++])) << 30;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 22;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 14;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 6;
    values[i + 4] |= Byte.toUnsignedLong(buf[off]) >>> 2;

    values[i + 5] = (Byte.toUnsignedLong(buf[off++]) & 3) << 52;
    values[i + 5] |= (Byte.toUnsignedLong(buf[off++])) << 44;
    values[i + 5] |= (Byte.toUnsignedLong(buf[off++])) << 36;
    values[i + 5] |= (Byte.toUnsignedLong(buf[off++])) << 28;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 20;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 12;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 4;
    values[i + 5] |= Byte.toUnsignedLong(buf[off]) >>> 4;

    values[i + 6] = (Byte.toUnsignedLong(buf[off++]) & 0xf) << 50;
    values[i + 6] |= (Byte.toUnsignedLong(buf[off++])) << 42;
    values[i + 6] |= (Byte.toUnsignedLong(buf[off++])) << 34;
    values[i + 6] |= (Byte.toUnsignedLong(buf[off++])) << 26;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 18;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 10;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 2;
    values[i + 6] |= Byte.toUnsignedLong(buf[off]) >>> 6;

    values[i + 7] = (Byte.toUnsignedLong(buf[off++]) & 0x3f) << 48;
    values[i + 7] |= (Byte.toUnsignedLong(buf[off++])) << 40;
    values[i + 7] |= (Byte.toUnsignedLong(buf[off++])) << 32;
    values[i + 7] |= (Byte.toUnsignedLong(buf[off++])) << 24;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]);
  }

  static void unpackBits55(final long[] values, final int i, final byte[] buf, int off) {
    values[i + 0] = (Byte.toUnsignedLong(buf[off++])) << 47;
    values[i + 0] |= (Byte.toUnsignedLong(buf[off++])) << 39;
    values[i + 0] |= (Byte.toUnsignedLong(buf[off++])) << 31;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 23;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 15;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 7;
    values[i + 0] |= Byte.toUnsignedLong(buf[off]) >>> 1;

    values[i + 1] = (Byte.toUnsignedLong(buf[off++]) & 1) << 54;
    values[i + 1] |= (Byte.toUnsignedLong(buf[off++])) << 46;
    values[i + 1] |= (Byte.toUnsignedLong(buf[off++])) << 38;
    values[i + 1] |= (Byte.toUnsignedLong(buf[off++])) << 30;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 22;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 14;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 6;
    values[i + 1] |= Byte.toUnsignedLong(buf[off]) >>> 2;

    values[i + 2] = (Byte.toUnsignedLong(buf[off++]) & 3) << 53;
    values[i + 2] |= (Byte.toUnsignedLong(buf[off++])) << 45;
    values[i + 2] |= (Byte.toUnsignedLong(buf[off++])) << 37;
    values[i + 2] |= (Byte.toUnsignedLong(buf[off++])) << 29;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 21;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 13;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 5;
    values[i + 2] |= Byte.toUnsignedLong(buf[off]) >>> 3;

    values[i + 3] = (Byte.toUnsignedLong(buf[off++]) & 7) << 52;
    values[i + 3] |= (Byte.toUnsignedLong(buf[off++])) << 44;
    values[i + 3] |= (Byte.toUnsignedLong(buf[off++])) << 36;
    values[i + 3] |= (Byte.toUnsignedLong(buf[off++])) << 28;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 20;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 12;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 4;
    values[i + 3] |= Byte.toUnsignedLong(buf[off]) >>> 4;

    values[i + 4] = (Byte.toUnsignedLong(buf[off++]) & 0xf) << 51;
    values[i + 4] |= (Byte.toUnsignedLong(buf[off++])) << 43;
    values[i + 4] |= (Byte.toUnsignedLong(buf[off++])) << 35;
    values[i + 4] |= (Byte.toUnsignedLong(buf[off++])) << 27;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 19;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 11;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 3;
    values[i + 4] |= Byte.toUnsignedLong(buf[off]) >>> 5;

    values[i + 5] = (Byte.toUnsignedLong(buf[off++]) & 0x1f) << 50;
    values[i + 5] |= (Byte.toUnsignedLong(buf[off++])) << 42;
    values[i + 5] |= (Byte.toUnsignedLong(buf[off++])) << 34;
    values[i + 5] |= (Byte.toUnsignedLong(buf[off++])) << 26;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 18;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 10;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 2;
    values[i + 5] |= Byte.toUnsignedLong(buf[off]) >>> 6;

    values[i + 6] = (Byte.toUnsignedLong(buf[off++]) & 0x3f) << 49;
    values[i + 6] |= (Byte.toUnsignedLong(buf[off++])) << 41;
    values[i + 6] |= (Byte.toUnsignedLong(buf[off++])) << 33;
    values[i + 6] |= (Byte.toUnsignedLong(buf[off++])) << 25;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 17;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 9;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 1;
    values[i + 6] |= Byte.toUnsignedLong(buf[off]) >>> 7;

    values[i + 7] = (Byte.toUnsignedLong(buf[off++]) & 0x7f) << 48;
    values[i + 7] |= (Byte.toUnsignedLong(buf[off++])) << 40;
    values[i + 7] |= (Byte.toUnsignedLong(buf[off++])) << 32;
    values[i + 7] |= (Byte.toUnsignedLong(buf[off++])) << 24;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 7] |= Byte.toUnsignedLong(buf[off]);
  }

  static void unpackBits56(final long[] values, final int i, final byte[] buf, int off) {
    values[i + 0] = (Byte.toUnsignedLong(buf[off++])) << 48;
    values[i + 0] |= (Byte.toUnsignedLong(buf[off++])) << 40;
    values[i + 0] |= (Byte.toUnsignedLong(buf[off++])) << 32;
    values[i + 0] |= (Byte.toUnsignedLong(buf[off++])) << 24;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]);
    values[i + 1] = (Byte.toUnsignedLong(buf[off++])) << 48;
    values[i + 1] |= (Byte.toUnsignedLong(buf[off++])) << 40;
    values[i + 1] |= (Byte.toUnsignedLong(buf[off++])) << 32;
    values[i + 1] |= (Byte.toUnsignedLong(buf[off++])) << 24;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]);
    values[i + 2] = (Byte.toUnsignedLong(buf[off++])) << 48;
    values[i + 2] |= (Byte.toUnsignedLong(buf[off++])) << 40;
    values[i + 2] |= (Byte.toUnsignedLong(buf[off++])) << 32;
    values[i + 2] |= (Byte.toUnsignedLong(buf[off++])) << 24;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]);
    values[i + 3] = (Byte.toUnsignedLong(buf[off++])) << 48;
    values[i + 3] |= (Byte.toUnsignedLong(buf[off++])) << 40;
    values[i + 3] |= (Byte.toUnsignedLong(buf[off++])) << 32;
    values[i + 3] |= (Byte.toUnsignedLong(buf[off++])) << 24;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]);
    values[i + 4] = (Byte.toUnsignedLong(buf[off++])) << 48;
    values[i + 4] |= (Byte.toUnsignedLong(buf[off++])) << 40;
    values[i + 4] |= (Byte.toUnsignedLong(buf[off++])) << 32;
    values[i + 4] |= (Byte.toUnsignedLong(buf[off++])) << 24;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]);
    values[i + 5] = (Byte.toUnsignedLong(buf[off++])) << 48;
    values[i + 5] |= (Byte.toUnsignedLong(buf[off++])) << 40;
    values[i + 5] |= (Byte.toUnsignedLong(buf[off++])) << 32;
    values[i + 5] |= (Byte.toUnsignedLong(buf[off++])) << 24;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]);
    values[i + 6] = (Byte.toUnsignedLong(buf[off++])) << 48;
    values[i + 6] |= (Byte.toUnsignedLong(buf[off++])) << 40;
    values[i + 6] |= (Byte.toUnsignedLong(buf[off++])) << 32;
    values[i + 6] |= (Byte.toUnsignedLong(buf[off++])) << 24;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]);
    values[i + 7] = (Byte.toUnsignedLong(buf[off++])) << 48;
    values[i + 7] |= (Byte.toUnsignedLong(buf[off++])) << 40;
    values[i + 7] |= (Byte.toUnsignedLong(buf[off++])) << 32;
    values[i + 7] |= (Byte.toUnsignedLong(buf[off++])) << 24;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 7] |= Byte.toUnsignedLong(buf[off]);
  }

  static void unpackBits57(final long[] values, final int i, final byte[] buf, int off) {
    values[i + 0] = (Byte.toUnsignedLong(buf[off++])) << 49;
    values[i + 0] |= (Byte.toUnsignedLong(buf[off++])) << 41;
    values[i + 0] |= (Byte.toUnsignedLong(buf[off++])) << 33;
    values[i + 0] |= (Byte.toUnsignedLong(buf[off++])) << 25;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 17;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 9;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 1;
    values[i + 0] |= Byte.toUnsignedLong(buf[off]) >>> 7;

    values[i + 1] = (Byte.toUnsignedLong(buf[off++]) & 0x7f) << 50;
    values[i + 1] |= (Byte.toUnsignedLong(buf[off++])) << 42;
    values[i + 1] |= (Byte.toUnsignedLong(buf[off++])) << 34;
    values[i + 1] |= (Byte.toUnsignedLong(buf[off++])) << 26;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 18;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 10;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 2;
    values[i + 1] |= Byte.toUnsignedLong(buf[off]) >>> 6;

    values[i + 2] = (Byte.toUnsignedLong(buf[off++]) & 0x3f) << 51;
    values[i + 2] |= (Byte.toUnsignedLong(buf[off++])) << 43;
    values[i + 2] |= (Byte.toUnsignedLong(buf[off++])) << 35;
    values[i + 2] |= (Byte.toUnsignedLong(buf[off++])) << 27;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 19;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 11;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 3;
    values[i + 2] |= Byte.toUnsignedLong(buf[off]) >>> 5;

    values[i + 3] = (Byte.toUnsignedLong(buf[off++]) & 0x1f) << 52;
    values[i + 3] |= (Byte.toUnsignedLong(buf[off++])) << 44;
    values[i + 3] |= (Byte.toUnsignedLong(buf[off++])) << 36;
    values[i + 3] |= (Byte.toUnsignedLong(buf[off++])) << 28;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 20;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 12;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 4;
    values[i + 3] |= Byte.toUnsignedLong(buf[off]) >>> 4;

    values[i + 4] = (Byte.toUnsignedLong(buf[off++]) & 0xf) << 53;
    values[i + 4] |= (Byte.toUnsignedLong(buf[off++])) << 45;
    values[i + 4] |= (Byte.toUnsignedLong(buf[off++])) << 37;
    values[i + 4] |= (Byte.toUnsignedLong(buf[off++])) << 29;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 21;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 13;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 5;
    values[i + 4] |= Byte.toUnsignedLong(buf[off]) >>> 3;

    values[i + 5] = (Byte.toUnsignedLong(buf[off++]) & 7) << 54;
    values[i + 5] |= (Byte.toUnsignedLong(buf[off++])) << 46;
    values[i + 5] |= (Byte.toUnsignedLong(buf[off++])) << 38;
    values[i + 5] |= (Byte.toUnsignedLong(buf[off++])) << 30;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 22;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 14;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 6;
    values[i + 5] |= Byte.toUnsignedLong(buf[off]) >>> 2;

    values[i + 6] = (Byte.toUnsignedLong(buf[off++]) & 3) << 55;
    values[i + 6] |= (Byte.toUnsignedLong(buf[off++])) << 47;
    values[i + 6] |= (Byte.toUnsignedLong(buf[off++])) << 39;
    values[i + 6] |= (Byte.toUnsignedLong(buf[off++])) << 31;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 23;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 15;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 7;
    values[i + 6] |= Byte.toUnsignedLong(buf[off]) >>> 1;

    values[i + 7] = (Byte.toUnsignedLong(buf[off++]) & 1) << 56;
    values[i + 7] |= (Byte.toUnsignedLong(buf[off++])) << 48;
    values[i + 7] |= (Byte.toUnsignedLong(buf[off++])) << 40;
    values[i + 7] |= (Byte.toUnsignedLong(buf[off++])) << 32;
    values[i + 7] |= (Byte.toUnsignedLong(buf[off++])) << 24;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 7] |= Byte.toUnsignedLong(buf[off]);
  }

  static void unpackBits58(final long[] values, final int i, final byte[] buf, int off) {
    values[i + 0] = (Byte.toUnsignedLong(buf[off++])) << 50;
    values[i + 0] |= (Byte.toUnsignedLong(buf[off++])) << 42;
    values[i + 0] |= (Byte.toUnsignedLong(buf[off++])) << 34;
    values[i + 0] |= (Byte.toUnsignedLong(buf[off++])) << 26;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 18;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 10;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 2;
    values[i + 0] |= Byte.toUnsignedLong(buf[off]) >>> 6;

    values[i + 1] = (Byte.toUnsignedLong(buf[off++]) & 0x3f) << 52;
    values[i + 1] |= (Byte.toUnsignedLong(buf[off++])) << 44;
    values[i + 1] |= (Byte.toUnsignedLong(buf[off++])) << 36;
    values[i + 1] |= (Byte.toUnsignedLong(buf[off++])) << 28;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 20;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 12;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 4;
    values[i + 1] |= Byte.toUnsignedLong(buf[off]) >>> 4;

    values[i + 2] = (Byte.toUnsignedLong(buf[off++]) & 0xf) << 54;
    values[i + 2] |= (Byte.toUnsignedLong(buf[off++])) << 46;
    values[i + 2] |= (Byte.toUnsignedLong(buf[off++])) << 38;
    values[i + 2] |= (Byte.toUnsignedLong(buf[off++])) << 30;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 22;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 14;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 6;
    values[i + 2] |= Byte.toUnsignedLong(buf[off]) >>> 2;

    values[i + 3] = (Byte.toUnsignedLong(buf[off++]) & 3) << 56;
    values[i + 3] |= (Byte.toUnsignedLong(buf[off++])) << 48;
    values[i + 3] |= (Byte.toUnsignedLong(buf[off++])) << 40;
    values[i + 3] |= (Byte.toUnsignedLong(buf[off++])) << 32;
    values[i + 3] |= (Byte.toUnsignedLong(buf[off++])) << 24;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]);

    values[i + 4] = (Byte.toUnsignedLong(buf[off++])) << 50;
    values[i + 4] |= (Byte.toUnsignedLong(buf[off++])) << 42;
    values[i + 4] |= (Byte.toUnsignedLong(buf[off++])) << 34;
    values[i + 4] |= (Byte.toUnsignedLong(buf[off++])) << 26;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 18;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 10;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 2;
    values[i + 4] |= Byte.toUnsignedLong(buf[off]) >>> 6;

    values[i + 5] = (Byte.toUnsignedLong(buf[off++]) & 0x3f) << 52;
    values[i + 5] |= (Byte.toUnsignedLong(buf[off++])) << 44;
    values[i + 5] |= (Byte.toUnsignedLong(buf[off++])) << 36;
    values[i + 5] |= (Byte.toUnsignedLong(buf[off++])) << 28;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 20;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 12;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 4;
    values[i + 5] |= Byte.toUnsignedLong(buf[off]) >>> 4;

    values[i + 6] = (Byte.toUnsignedLong(buf[off++]) & 0xf) << 54;
    values[i + 6] |= (Byte.toUnsignedLong(buf[off++])) << 46;
    values[i + 6] |= (Byte.toUnsignedLong(buf[off++])) << 38;
    values[i + 6] |= (Byte.toUnsignedLong(buf[off++])) << 30;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 22;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 14;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 6;
    values[i + 6] |= Byte.toUnsignedLong(buf[off]) >>> 2;

    values[i + 7] = (Byte.toUnsignedLong(buf[off++]) & 3) << 56;
    values[i + 7] |= (Byte.toUnsignedLong(buf[off++])) << 48;
    values[i + 7] |= (Byte.toUnsignedLong(buf[off++])) << 40;
    values[i + 7] |= (Byte.toUnsignedLong(buf[off++])) << 32;
    values[i + 7] |= (Byte.toUnsignedLong(buf[off++])) << 24;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]);
  }

  static void unpackBits59(final long[] values, final int i, final byte[] buf, int off) {
    values[i + 0] = (Byte.toUnsignedLong(buf[off++])) << 51;
    values[i + 0] |= (Byte.toUnsignedLong(buf[off++])) << 43;
    values[i + 0] |= (Byte.toUnsignedLong(buf[off++])) << 35;
    values[i + 0] |= (Byte.toUnsignedLong(buf[off++])) << 27;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 19;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 11;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 3;
    values[i + 0] |= Byte.toUnsignedLong(buf[off]) >>> 5;

    values[i + 1] = (Byte.toUnsignedLong(buf[off++]) & 0x1f) << 54;
    values[i + 1] |= (Byte.toUnsignedLong(buf[off++])) << 46;
    values[i + 1] |= (Byte.toUnsignedLong(buf[off++])) << 38;
    values[i + 1] |= (Byte.toUnsignedLong(buf[off++])) << 30;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 22;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 14;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 6;
    values[i + 1] |= Byte.toUnsignedLong(buf[off]) >>> 2;

    values[i + 2] = (Byte.toUnsignedLong(buf[off++]) & 3) << 57;
    values[i + 2] |= (Byte.toUnsignedLong(buf[off++])) << 49;
    values[i + 2] |= (Byte.toUnsignedLong(buf[off++])) << 41;
    values[i + 2] |= (Byte.toUnsignedLong(buf[off++])) << 33;
    values[i + 2] |= (Byte.toUnsignedLong(buf[off++])) << 25;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 17;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 9;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 1;
    values[i + 2] |= Byte.toUnsignedLong(buf[off]) >>> 7;

    values[i + 3] = (Byte.toUnsignedLong(buf[off++]) & 0x7f) << 52;
    values[i + 3] |= (Byte.toUnsignedLong(buf[off++])) << 44;
    values[i + 3] |= (Byte.toUnsignedLong(buf[off++])) << 36;
    values[i + 3] |= (Byte.toUnsignedLong(buf[off++])) << 28;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 20;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 12;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 4;
    values[i + 3] |= Byte.toUnsignedLong(buf[off]) >>> 4;

    values[i + 4] = (Byte.toUnsignedLong(buf[off++]) & 0xf) << 55;
    values[i + 4] |= (Byte.toUnsignedLong(buf[off++])) << 47;
    values[i + 4] |= (Byte.toUnsignedLong(buf[off++])) << 39;
    values[i + 4] |= (Byte.toUnsignedLong(buf[off++])) << 31;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 23;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 15;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 7;
    values[i + 4] |= Byte.toUnsignedLong(buf[off]) >>> 1;

    values[i + 5] = (Byte.toUnsignedLong(buf[off++]) & 1) << 58;
    values[i + 5] |= (Byte.toUnsignedLong(buf[off++])) << 50;
    values[i + 5] |= (Byte.toUnsignedLong(buf[off++])) << 42;
    values[i + 5] |= (Byte.toUnsignedLong(buf[off++])) << 34;
    values[i + 5] |= (Byte.toUnsignedLong(buf[off++])) << 26;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 18;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 10;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 2;
    values[i + 5] |= Byte.toUnsignedLong(buf[off]) >>> 6;

    values[i + 6] = (Byte.toUnsignedLong(buf[off++]) & 0x3f) << 53;
    values[i + 6] |= (Byte.toUnsignedLong(buf[off++])) << 45;
    values[i + 6] |= (Byte.toUnsignedLong(buf[off++])) << 37;
    values[i + 6] |= (Byte.toUnsignedLong(buf[off++])) << 29;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 21;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 13;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 5;
    values[i + 6] |= Byte.toUnsignedLong(buf[off]) >>> 3;

    values[i + 7] = (Byte.toUnsignedLong(buf[off++]) & 7) << 56;
    values[i + 7] |= (Byte.toUnsignedLong(buf[off++])) << 48;
    values[i + 7] |= (Byte.toUnsignedLong(buf[off++])) << 40;
    values[i + 7] |= (Byte.toUnsignedLong(buf[off++])) << 32;
    values[i + 7] |= (Byte.toUnsignedLong(buf[off++])) << 24;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 7] |= Byte.toUnsignedLong(buf[off]);
  }

  static void unpackBits60(final long[] values, final int i, final byte[] buf, int off) {
    values[i + 0] = (Byte.toUnsignedLong(buf[off++])) << 52;
    values[i + 0] |= (Byte.toUnsignedLong(buf[off++])) << 44;
    values[i + 0] |= (Byte.toUnsignedLong(buf[off++])) << 36;
    values[i + 0] |= (Byte.toUnsignedLong(buf[off++])) << 28;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 20;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 12;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 4;
    values[i + 0] |= Byte.toUnsignedLong(buf[off]) >>> 4;

    values[i + 1] = (Byte.toUnsignedLong(buf[off++]) & 0xf) << 56;
    values[i + 1] |= (Byte.toUnsignedLong(buf[off++])) << 48;
    values[i + 1] |= (Byte.toUnsignedLong(buf[off++])) << 40;
    values[i + 1] |= (Byte.toUnsignedLong(buf[off++])) << 32;
    values[i + 1] |= (Byte.toUnsignedLong(buf[off++])) << 24;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]);

    values[i + 2] = (Byte.toUnsignedLong(buf[off++])) << 52;
    values[i + 2] |= (Byte.toUnsignedLong(buf[off++])) << 44;
    values[i + 2] |= (Byte.toUnsignedLong(buf[off++])) << 36;
    values[i + 2] |= (Byte.toUnsignedLong(buf[off++])) << 28;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 20;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 12;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 4;
    values[i + 2] |= Byte.toUnsignedLong(buf[off]) >>> 4;

    values[i + 3] = (Byte.toUnsignedLong(buf[off++]) & 0xf) << 56;
    values[i + 3] |= (Byte.toUnsignedLong(buf[off++])) << 48;
    values[i + 3] |= (Byte.toUnsignedLong(buf[off++])) << 40;
    values[i + 3] |= (Byte.toUnsignedLong(buf[off++])) << 32;
    values[i + 3] |= (Byte.toUnsignedLong(buf[off++])) << 24;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]);

    values[i + 4] = (Byte.toUnsignedLong(buf[off++])) << 52;
    values[i + 4] |= (Byte.toUnsignedLong(buf[off++])) << 44;
    values[i + 4] |= (Byte.toUnsignedLong(buf[off++])) << 36;
    values[i + 4] |= (Byte.toUnsignedLong(buf[off++])) << 28;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 20;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 12;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 4;
    values[i + 4] |= Byte.toUnsignedLong(buf[off]) >>> 4;

    values[i + 5] = (Byte.toUnsignedLong(buf[off++]) & 0xf) << 56;
    values[i + 5] |= (Byte.toUnsignedLong(buf[off++])) << 48;
    values[i + 5] |= (Byte.toUnsignedLong(buf[off++])) << 40;
    values[i + 5] |= (Byte.toUnsignedLong(buf[off++])) << 32;
    values[i + 5] |= (Byte.toUnsignedLong(buf[off++])) << 24;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]);

    values[i + 6] = (Byte.toUnsignedLong(buf[off++])) << 52;
    values[i + 6] |= (Byte.toUnsignedLong(buf[off++])) << 44;
    values[i + 6] |= (Byte.toUnsignedLong(buf[off++])) << 36;
    values[i + 6] |= (Byte.toUnsignedLong(buf[off++])) << 28;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 20;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 12;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 4;
    values[i + 6] |= Byte.toUnsignedLong(buf[off]) >>> 4;

    values[i + 7] = (Byte.toUnsignedLong(buf[off++]) & 0xf) << 56;
    values[i + 7] |= (Byte.toUnsignedLong(buf[off++])) << 48;
    values[i + 7] |= (Byte.toUnsignedLong(buf[off++])) << 40;
    values[i + 7] |= (Byte.toUnsignedLong(buf[off++])) << 32;
    values[i + 7] |= (Byte.toUnsignedLong(buf[off++])) << 24;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 7] |= Byte.toUnsignedLong(buf[off]);
  }

  static void unpackBits61(final long[] values, final int i, final byte[] buf, int off) {
    values[i + 0] = (Byte.toUnsignedLong(buf[off++])) << 53;
    values[i + 0] |= (Byte.toUnsignedLong(buf[off++])) << 45;
    values[i + 0] |= (Byte.toUnsignedLong(buf[off++])) << 37;
    values[i + 0] |= (Byte.toUnsignedLong(buf[off++])) << 29;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 21;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 13;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 5;
    values[i + 0] |= Byte.toUnsignedLong(buf[off]) >>> 3;

    values[i + 1] = (Byte.toUnsignedLong(buf[off++]) & 7) << 58;
    values[i + 1] |= (Byte.toUnsignedLong(buf[off++])) << 50;
    values[i + 1] |= (Byte.toUnsignedLong(buf[off++])) << 42;
    values[i + 1] |= (Byte.toUnsignedLong(buf[off++])) << 34;
    values[i + 1] |= (Byte.toUnsignedLong(buf[off++])) << 26;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 18;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 10;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 2;
    values[i + 1] |= Byte.toUnsignedLong(buf[off]) >>> 6;

    values[i + 2] = (Byte.toUnsignedLong(buf[off++]) & 0x3f) << 55;
    values[i + 2] |= (Byte.toUnsignedLong(buf[off++])) << 47;
    values[i + 2] |= (Byte.toUnsignedLong(buf[off++])) << 39;
    values[i + 2] |= (Byte.toUnsignedLong(buf[off++])) << 31;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 23;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 15;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 7;
    values[i + 2] |= Byte.toUnsignedLong(buf[off]) >>> 1;

    values[i + 3] = (Byte.toUnsignedLong(buf[off++]) & 1) << 60;
    values[i + 3] |= (Byte.toUnsignedLong(buf[off++])) << 52;
    values[i + 3] |= (Byte.toUnsignedLong(buf[off++])) << 44;
    values[i + 3] |= (Byte.toUnsignedLong(buf[off++])) << 36;
    values[i + 3] |= (Byte.toUnsignedLong(buf[off++])) << 28;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 20;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 12;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 4;
    values[i + 3] |= Byte.toUnsignedLong(buf[off]) >>> 4;

    values[i + 4] = (Byte.toUnsignedLong(buf[off++]) & 0xf) << 57;
    values[i + 4] |= (Byte.toUnsignedLong(buf[off++])) << 49;
    values[i + 4] |= (Byte.toUnsignedLong(buf[off++])) << 41;
    values[i + 4] |= (Byte.toUnsignedLong(buf[off++])) << 33;
    values[i + 4] |= (Byte.toUnsignedLong(buf[off++])) << 25;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 17;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 9;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 1;
    values[i + 4] |= Byte.toUnsignedLong(buf[off]) >>> 7;

    values[i + 5] = (Byte.toUnsignedLong(buf[off++]) & 0x7f) << 54;
    values[i + 5] |= (Byte.toUnsignedLong(buf[off++])) << 46;
    values[i + 5] |= (Byte.toUnsignedLong(buf[off++])) << 38;
    values[i + 5] |= (Byte.toUnsignedLong(buf[off++])) << 30;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 22;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 14;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 6;
    values[i + 5] |= Byte.toUnsignedLong(buf[off]) >>> 2;

    values[i + 6] = (Byte.toUnsignedLong(buf[off++]) & 3) << 59;
    values[i + 6] |= (Byte.toUnsignedLong(buf[off++])) << 51;
    values[i + 6] |= (Byte.toUnsignedLong(buf[off++])) << 43;
    values[i + 6] |= (Byte.toUnsignedLong(buf[off++])) << 35;
    values[i + 6] |= (Byte.toUnsignedLong(buf[off++])) << 27;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 19;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 11;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 3;
    values[i + 6] |= Byte.toUnsignedLong(buf[off]) >>> 5;

    values[i + 7] = (Byte.toUnsignedLong(buf[off++]) & 0x1f) << 56;
    values[i + 7] |= (Byte.toUnsignedLong(buf[off++])) << 48;
    values[i + 7] |= (Byte.toUnsignedLong(buf[off++])) << 40;
    values[i + 7] |= (Byte.toUnsignedLong(buf[off++])) << 32;
    values[i + 7] |= (Byte.toUnsignedLong(buf[off++])) << 24;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 7] |= Byte.toUnsignedLong(buf[off]);
  }

  static void unpackBits62(final long[] values, final int i, final byte[] buf, int off) {
    values[i + 0] = (Byte.toUnsignedLong(buf[off++])) << 54;
    values[i + 0] |= (Byte.toUnsignedLong(buf[off++])) << 46;
    values[i + 0] |= (Byte.toUnsignedLong(buf[off++])) << 38;
    values[i + 0] |= (Byte.toUnsignedLong(buf[off++])) << 30;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 22;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 14;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 6;
    values[i + 0] |= Byte.toUnsignedLong(buf[off]) >>> 2;

    values[i + 1] = (Byte.toUnsignedLong(buf[off++]) & 3) << 60;
    values[i + 1] |= (Byte.toUnsignedLong(buf[off++])) << 52;
    values[i + 1] |= (Byte.toUnsignedLong(buf[off++])) << 44;
    values[i + 1] |= (Byte.toUnsignedLong(buf[off++])) << 36;
    values[i + 1] |= (Byte.toUnsignedLong(buf[off++])) << 28;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 20;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 12;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 4;
    values[i + 1] |= Byte.toUnsignedLong(buf[off]) >>> 4;

    values[i + 2] = (Byte.toUnsignedLong(buf[off++]) & 0xf) << 58;
    values[i + 2] |= (Byte.toUnsignedLong(buf[off++])) << 50;
    values[i + 2] |= (Byte.toUnsignedLong(buf[off++])) << 42;
    values[i + 2] |= (Byte.toUnsignedLong(buf[off++])) << 34;
    values[i + 2] |= (Byte.toUnsignedLong(buf[off++])) << 26;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 18;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 10;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 2;
    values[i + 2] |= Byte.toUnsignedLong(buf[off]) >>> 6;

    values[i + 3] = (Byte.toUnsignedLong(buf[off++]) & 0x3f) << 56;
    values[i + 3] |= (Byte.toUnsignedLong(buf[off++])) << 48;
    values[i + 3] |= (Byte.toUnsignedLong(buf[off++])) << 40;
    values[i + 3] |= (Byte.toUnsignedLong(buf[off++])) << 32;
    values[i + 3] |= (Byte.toUnsignedLong(buf[off++])) << 24;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]);

    values[i + 4] = (Byte.toUnsignedLong(buf[off++])) << 54;
    values[i + 4] |= (Byte.toUnsignedLong(buf[off++])) << 46;
    values[i + 4] |= (Byte.toUnsignedLong(buf[off++])) << 38;
    values[i + 4] |= (Byte.toUnsignedLong(buf[off++])) << 30;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 22;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 14;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 6;
    values[i + 4] |= Byte.toUnsignedLong(buf[off]) >>> 2;

    values[i + 5] = (Byte.toUnsignedLong(buf[off++]) & 3) << 60;
    values[i + 5] |= (Byte.toUnsignedLong(buf[off++])) << 52;
    values[i + 5] |= (Byte.toUnsignedLong(buf[off++])) << 44;
    values[i + 5] |= (Byte.toUnsignedLong(buf[off++])) << 36;
    values[i + 5] |= (Byte.toUnsignedLong(buf[off++])) << 28;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 20;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 12;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 4;
    values[i + 5] |= Byte.toUnsignedLong(buf[off]) >>> 4;

    values[i + 6] = (Byte.toUnsignedLong(buf[off++]) & 0xf) << 58;
    values[i + 6] |= (Byte.toUnsignedLong(buf[off++])) << 50;
    values[i + 6] |= (Byte.toUnsignedLong(buf[off++])) << 42;
    values[i + 6] |= (Byte.toUnsignedLong(buf[off++])) << 34;
    values[i + 6] |= (Byte.toUnsignedLong(buf[off++])) << 26;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 18;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 10;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 2;
    values[i + 6] |= Byte.toUnsignedLong(buf[off]) >>> 6;

    values[i + 7] = (Byte.toUnsignedLong(buf[off++]) & 0x3f) << 56;
    values[i + 7] |= (Byte.toUnsignedLong(buf[off++])) << 48;
    values[i + 7] |= (Byte.toUnsignedLong(buf[off++])) << 40;
    values[i + 7] |= (Byte.toUnsignedLong(buf[off++])) << 32;
    values[i + 7] |= (Byte.toUnsignedLong(buf[off++])) << 24;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 7] |= Byte.toUnsignedLong(buf[off]);
  }

  static void unpackBits63(final long[] values, final int i, final byte[] buf, int off) {
    values[i + 0] = (Byte.toUnsignedLong(buf[off++])) << 55;
    values[i + 0] |= (Byte.toUnsignedLong(buf[off++])) << 47;
    values[i + 0] |= (Byte.toUnsignedLong(buf[off++])) << 39;
    values[i + 0] |= (Byte.toUnsignedLong(buf[off++])) << 31;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 23;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 15;
    values[i + 0] |= Byte.toUnsignedLong(buf[off++]) << 7;
    values[i + 0] |= Byte.toUnsignedLong(buf[off]) >>> 1;

    values[i + 1] = (Byte.toUnsignedLong(buf[off++]) & 1) << 62;
    values[i + 1] |= (Byte.toUnsignedLong(buf[off++])) << 54;
    values[i + 1] |= (Byte.toUnsignedLong(buf[off++])) << 46;
    values[i + 1] |= (Byte.toUnsignedLong(buf[off++])) << 38;
    values[i + 1] |= (Byte.toUnsignedLong(buf[off++])) << 30;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 22;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 14;
    values[i + 1] |= Byte.toUnsignedLong(buf[off++]) << 6;
    values[i + 1] |= Byte.toUnsignedLong(buf[off]) >>> 2;

    values[i + 2] = (Byte.toUnsignedLong(buf[off++]) & 3) << 61;
    values[i + 2] |= (Byte.toUnsignedLong(buf[off++])) << 53;
    values[i + 2] |= (Byte.toUnsignedLong(buf[off++])) << 45;
    values[i + 2] |= (Byte.toUnsignedLong(buf[off++])) << 37;
    values[i + 2] |= (Byte.toUnsignedLong(buf[off++])) << 29;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 21;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 13;
    values[i + 2] |= Byte.toUnsignedLong(buf[off++]) << 5;
    values[i + 2] |= Byte.toUnsignedLong(buf[off]) >>> 3;

    values[i + 3] = (Byte.toUnsignedLong(buf[off++]) & 7) << 60;
    values[i + 3] |= (Byte.toUnsignedLong(buf[off++])) << 52;
    values[i + 3] |= (Byte.toUnsignedLong(buf[off++])) << 44;
    values[i + 3] |= (Byte.toUnsignedLong(buf[off++])) << 36;
    values[i + 3] |= (Byte.toUnsignedLong(buf[off++])) << 28;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 20;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 12;
    values[i + 3] |= Byte.toUnsignedLong(buf[off++]) << 4;
    values[i + 3] |= Byte.toUnsignedLong(buf[off]) >>> 4;

    values[i + 4] = (Byte.toUnsignedLong(buf[off++]) & 0xf) << 59;
    values[i + 4] |= (Byte.toUnsignedLong(buf[off++])) << 51;
    values[i + 4] |= (Byte.toUnsignedLong(buf[off++])) << 43;
    values[i + 4] |= (Byte.toUnsignedLong(buf[off++])) << 35;
    values[i + 4] |= (Byte.toUnsignedLong(buf[off++])) << 27;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 19;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 11;
    values[i + 4] |= Byte.toUnsignedLong(buf[off++]) << 3;
    values[i + 4] |= Byte.toUnsignedLong(buf[off]) >>> 5;

    values[i + 5] = (Byte.toUnsignedLong(buf[off++]) & 0x1f) << 58;
    values[i + 5] |= (Byte.toUnsignedLong(buf[off++])) << 50;
    values[i + 5] |= (Byte.toUnsignedLong(buf[off++])) << 42;
    values[i + 5] |= (Byte.toUnsignedLong(buf[off++])) << 34;
    values[i + 5] |= (Byte.toUnsignedLong(buf[off++])) << 26;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 18;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 10;
    values[i + 5] |= Byte.toUnsignedLong(buf[off++]) << 2;
    values[i + 5] |= Byte.toUnsignedLong(buf[off]) >>> 6;

    values[i + 6] = (Byte.toUnsignedLong(buf[off++]) & 0x3f) << 57;
    values[i + 6] |= (Byte.toUnsignedLong(buf[off++])) << 49;
    values[i + 6] |= (Byte.toUnsignedLong(buf[off++])) << 41;
    values[i + 6] |= (Byte.toUnsignedLong(buf[off++])) << 33;
    values[i + 6] |= (Byte.toUnsignedLong(buf[off++])) << 25;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 17;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 9;
    values[i + 6] |= Byte.toUnsignedLong(buf[off++]) << 1;
    values[i + 6] |= Byte.toUnsignedLong(buf[off]) >>> 7;

    values[i + 7] = (Byte.toUnsignedLong(buf[off++]) & 0x7f) << 56;
    values[i + 7] |= (Byte.toUnsignedLong(buf[off++])) << 48;
    values[i + 7] |= (Byte.toUnsignedLong(buf[off++])) << 40;
    values[i + 7] |= (Byte.toUnsignedLong(buf[off++])) << 32;
    values[i + 7] |= (Byte.toUnsignedLong(buf[off++])) << 24;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]) << 16;
    values[i + 7] |= Byte.toUnsignedLong(buf[off++]) << 8;
    values[i + 7] |= Byte.toUnsignedLong(buf[off]);
  }

}
