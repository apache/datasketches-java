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

import static org.testng.Assert.assertEquals;

import org.apache.datasketches.common.Util;
import org.testng.annotations.Test;

public class BitPackingTest {
  private final static boolean enablePrinting = false;
//for every number of bits from 1 to 63
//generate pseudo-random data, pack, unpack and compare

  @Test
  public void packUnpackBits() {
    long value = 0xaa55aa55aa55aa55L; // arbitrary starting value
    for (int n = 0; n < 10000; n++) {
      for (int bits = 1; bits <= 63; bits++) {
        final long mask = (1 << bits) - 1;
        long[] input = new long[8];
        for (int i = 0; i < 8; ++i) {
          input[i] = value & mask;
          value += Util.INVERSE_GOLDEN_U64;
        }

        byte[] bytes = new byte[8 * Long.BYTES];
        int bitOffset = 0;
        int bufOffset = 0;
        for (int i = 0; i < 8; ++i) {
          BitPacking.packBits(input[i], bits, bytes, bufOffset, bitOffset);
          bufOffset += (bitOffset + bits) >>> 3;
          bitOffset = (bitOffset + bits) & 7;
        }

        long[] output = new long[8];
        bitOffset = 0;
        bufOffset = 0;
        for (int i = 0; i < 8; ++i) {
          BitPacking.unpackBits(output, i, bits, bytes, bufOffset, bitOffset);
          bufOffset += (bitOffset + bits) >>> 3;
          bitOffset = (bitOffset + bits) & 7;
        }

        for (int i = 0; i < 8; ++i) {
          assertEquals(output[i], input[i]);
        }
      }
    }
  }

  @Test
  public void packUnpackBlocks() {
    long value = 0xaa55aa55aa55aa55L; // arbitrary starting value
    for (int n = 0; n < 10000; n++) {
      for (int bits = 1; bits <= 63; bits++) {
        if (enablePrinting) { System.out.println("bits " + bits); }
        final long mask = (1L << bits) - 1;
        long[] input = new long[8];
        for (int i = 0; i < 8; ++i) {
          input[i] = value & mask;
          value += Util.INVERSE_GOLDEN_U64;
        }

        byte[] bytes = new byte[8 * Long.BYTES];
        BitPacking.packBitsBlock8(input, 0, bytes, 0, bits);
        if (enablePrinting) { hexDump(bytes); }

        long[] output = new long[8];
        BitPacking.unpackBitsBlock8(output, 0, bytes, 0, bits);

        for (int i = 0; i < 8; ++i) {
          if (enablePrinting) { System.out.println("checking value " + i); }
          assertEquals(output[i], input[i]);
        }
      }
    }
  }

  @Test
  public void packBitsUnpackBlocks() {
    long value = 0; // arbitrary starting value
    for (int n = 0; n < 10000; n++) {
      for (int bits = 1; bits <= 63; bits++) {
        final long mask = (1 << bits) - 1;
        long[] input = new long[8];
        for (int i = 0; i < 8; ++i) {
          input[i] = value & mask;
          value += Util.INVERSE_GOLDEN_U64;
        }

        byte[] bytes = new byte[8 * Long.BYTES];
        int bitOffset = 0;
        int bufOffset = 0;
        for (int i = 0; i < 8; ++i) {
          BitPacking.packBits(input[i], bits, bytes, bufOffset, bitOffset);
          bufOffset += (bitOffset + bits) >>> 3;
          bitOffset = (bitOffset + bits) & 7;
        }

        long[] output = new long[8];
        BitPacking.unpackBitsBlock8(output, 0, bytes, 0, bits);

        for (int i = 0; i < 8; ++i) {
          assertEquals(output[i], input[i]);
        }
      }
    }
  }

  @Test
  public void packBlocksUnpackBits() {
    long value = 123L; // arbitrary starting value
    for (int n = 0; n < 10000; n++) {
      for (int bits = 1; bits <= 63; bits++) {
        final long mask = (1 << bits) - 1;
        long[] input = new long[8];
        for (int i = 0; i < 8; ++i) {
          input[i] = value & mask;
          value += Util.INVERSE_GOLDEN_U64;
        }

        byte[] bytes = new byte[8 * Long.BYTES];
        BitPacking.packBitsBlock8(input, 0, bytes, 0, bits);

        long[] output = new long[8];
        int bitOffset = 0;
        int bufOffset = 0;
        for (int i = 0; i < 8; ++i) {
          BitPacking.unpackBits(output, i, bits, bytes, bufOffset, bitOffset);
          bufOffset += (bitOffset + bits) >>> 3;
          bitOffset = (bitOffset + bits) & 7;
        }

        for (int i = 0; i < 8; ++i) {
          assertEquals(output[i], input[i]);
        }
      }
    }
  }

  void hexDump(byte[] bytes) {
    for (int i = 0; i < bytes.length; i++) {
      System.out.print(String.format("%02x ", bytes[i]));
    }
    System.out.println();
  }

}
