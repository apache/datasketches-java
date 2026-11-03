/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.datasketches.common;

import static org.apache.datasketches.common.ByteArrayUtil.putIntLE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.expectThrows;

import java.lang.foreign.MemorySegment;
import java.util.Arrays;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class StringSerDeBoundsTest {
  @DataProvider(name = "stringSerDes")
  public Object[][] stringSerDes() {
    return new Object[][] {
        {new ArrayOfStringsSerDe()},
        {new ArrayOfUtf16StringsSerDe()}
    };
  }

  @Test(dataProvider = "stringSerDes")
  public void rejectInvalidLengthsBeforeAllocation(final ArrayOfItemsSerDe<String> serDe) {
    for (final int length : new int[] {-1, Integer.MAX_VALUE, 5}) {
      final byte[] bytes = new byte[Integer.BYTES];
      putIntLE(bytes, 0, length);
      final MemorySegment seg = MemorySegment.ofArray(bytes);
      expectThrows(SketchesArgumentException.class,
          () -> serDe.deserializeFromMemorySegment(seg, 1));
      expectThrows(SketchesArgumentException.class, () -> serDe.sizeOf(seg, 0, 1));
    }
  }

  @Test(dataProvider = "stringSerDes")
  public void rejectImpossibleCountsBeforeAllocation(final ArrayOfItemsSerDe<String> serDe) {
    final MemorySegment seg = MemorySegment.ofArray(new byte[Integer.BYTES]);
    expectThrows(SketchesArgumentException.class,
        () -> serDe.deserializeFromMemorySegment(seg, Integer.MAX_VALUE));
    expectThrows(SketchesArgumentException.class,
        () -> serDe.deserializeFromMemorySegment(seg, 2));
  }

  @Test(dataProvider = "stringSerDes")
  public void rejectInvalidOffsetsAndTruncatedPrefixes(final ArrayOfItemsSerDe<String> serDe) {
    final MemorySegment seg = MemorySegment.ofArray(new byte[Integer.BYTES]);
    for (final long offset : new long[] {-1, Long.MAX_VALUE, 1, 4}) {
      expectThrows(SketchesArgumentException.class,
          () -> serDe.deserializeFromMemorySegment(seg, offset, 1));
    }
  }

  @Test(dataProvider = "stringSerDes")
  public void preserveValidOffsetUnicodeAndOwnership(final ArrayOfItemsSerDe<String> serDe) {
    final String[] expected = {"", "hello", "日本語", "😀", "a\u0000b"};
    final byte[] encoded = serDe.serializeToByteArray(expected);
    final byte[] padded = new byte[3 + encoded.length + 1];
    System.arraycopy(encoded, 0, padded, 3, encoded.length);
    final MemorySegment seg = MemorySegment.ofArray(padded);
    final String[] actual = serDe.deserializeFromMemorySegment(seg, 3, expected.length);
    assertEquals(actual, expected);
    assertEquals(serDe.sizeOf(seg, 3, expected.length), encoded.length);
    Arrays.fill(padded, (byte) 0);
    assertEquals(actual, expected);
  }

  @Test(dataProvider = "stringSerDes")
  public void preserveEmptyCount(final ArrayOfItemsSerDe<String> serDe) {
    assertEquals(serDe.deserializeFromMemorySegment(MemorySegment.ofArray(new byte[0]), 0).length, 0);
  }
}
