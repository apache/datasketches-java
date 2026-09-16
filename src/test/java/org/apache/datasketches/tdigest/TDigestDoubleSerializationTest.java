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

package org.apache.datasketches.tdigest;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

import org.apache.datasketches.common.Family;
import org.apache.datasketches.common.SketchesArgumentException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class TDigestDoubleSerializationTest {

  @DataProvider(name = "formats")
  public Object[][] formats() {
    return new Object[][] {{false, false}, {false, true}, {true, false}, {true, true}};
  }

  @DataProvider(name = "precisions")
  public Object[][] precisions() {
    return new Object[][] {{false}, {true}};
  }

  @Test(dataProvider = "formats")
  public void validCentroids(final boolean compat, final boolean isFloat) {
    // Equal means are valid and must remain in their original order.
    final byte[] bytes = serialize(compat, isFloat, 0, 10,
        new double[] {0, 5, 5, 10}, new double[] {1, 2, 3, 1}, new double[0]);
    final TDigestDouble td = TDigestDouble.heapify(MemorySegment.ofArray(bytes), isFloat);
    assertEquals(td.getTotalWeight(), 7);
    assertEquals(td.getMinValue(), 0.0);
    assertEquals(td.getMaxValue(), 10.0);
    assertEquals(td.getQuantile(0.5), 5.0);
    final TDigestDouble restored = TDigestDouble.heapify(MemorySegment.ofArray(td.toByteArray()));
    assertEquals(restored.getTotalWeight(), 7);
    assertEquals(restored.getQuantile(0.5), 5.0);
  }

  @Test
  public void quantileInterpolationApproachesNearerCentroid() {
    final byte[] bytes = serialize(false, false, -1, 21,
        new double[] {0, 10, 20}, new double[] {4, 4, 4}, new double[0]);
    final TDigestDouble td = TDigestDouble.heapify(MemorySegment.ofArray(bytes));
    // The first two centroids lie at ranks 2/12 and 6/12. Moving across this
    // interval must approach the right centroid, not move back toward the left.
    assertEquals(td.getQuantile(2.0 / 12), 0.0, 1e-12);
    assertEquals(td.getQuantile(3.0 / 12), 2.5, 1e-12);
    assertEquals(td.getQuantile(4.0 / 12), 5.0, 1e-12);
    assertEquals(td.getQuantile(5.0 / 12), 7.5, 1e-12);
    assertEquals(td.getQuantile(6.0 / 12), 10.0, 1e-12);
  }

  @Test
  public void queryTailsAreSymmetric() {
    final byte[] bytes = serialize(false, false, 0, 100,
        new double[] {10, 50, 90}, new double[] {10, 10, 10}, new double[0]);
    final TDigestDouble td = TDigestDouble.heapify(MemorySegment.ofArray(bytes));
    assertEquals(td.getQuantile(0.9), 95.0, 1e-12);
    assertEquals(td.getQuantile(29.0 / 30), 100.0, 1e-12);
    assertEquals(td.getQuantile(1.0 / 30), 0.0, 1e-12);
    assertEquals(td.getQuantile(5.0 / 30), 10.0, 1e-12);
    assertEquals(td.getRank(5), 0.1, 1e-12);
    assertEquals(td.getRank(95), 0.9, 1e-12);
    assertEquals(td.getCDF(new double[] {5, 95}), new double[] {0.1, 0.9, 1.0}, 1e-12);
    assertEquals(td.getPMF(new double[] {5, 95}), new double[] {0.1, 0.8, 0.1}, 1e-12);
  }

  @Test(dataProvider = "formats")
  public void centroidsBeyondLocalCapacity(final boolean compat, final boolean isFloat) {
    // k=100 normally allocates 210 centroid slots, but that is not a wire-format limit.
    final double[] means = new double[211];
    final double[] weights = new double[means.length];
    for (int i = 0; i < means.length; i++) {
      means[i] = i;
      weights[i] = 1;
    }
    final byte[] bytes = serialize(compat, isFloat, 0, 210, means, weights, new double[0]);
    final TDigestDouble td = TDigestDouble.heapify(MemorySegment.ofArray(bytes), isFloat);
    assertEquals(td.toByteArray(), serialize(false, false, 0, 210, means, weights, new double[0]));
    assertEquals(td.getQuantile(0.5), 105.0);
    final TDigestDouble merged = new TDigestDouble((short) 100);
    merged.merge(td);
    assertEquals(merged.getTotalWeight(), means.length);
    td.update(211);
    assertEquals(TDigestDouble.heapify(MemorySegment.ofArray(td.toByteArray())).getTotalWeight(), means.length + 1);
  }

  @Test(dataProvider = "precisions")
  public void bufferedValuesBeyondLocalCapacity(final boolean isFloat) {
    // A producer may serialize more than this implementation's 840 buffered values.
    // Exercise both buffer-only and mixed images, then cross several update boundaries.
    final double[] buffered = new double[841];
    Arrays.fill(buffered, 10);
    buffered[0] = 0;
    for (final double[] means : new double[][] {new double[0], {0, 10}}) {
      final double[] weights = new double[means.length];
      Arrays.fill(weights, 1);
      final byte[] bytes = serialize(false, isFloat, 0, 10, means, weights, buffered);
      final TDigestDouble td = TDigestDouble.heapify(MemorySegment.ofArray(bytes), isFloat);
      assertEquals(td.getTotalWeight(), buffered.length + means.length);
      for (int i = 0; i < 10000; i++) { td.update(10); }
      assertEquals(td.getTotalWeight(), 10000 + buffered.length + means.length);
      assertEquals(td.getMinValue(), 0.0);
      assertEquals(td.getMaxValue(), 10.0);
      assertEquals(td.getQuantile(0.5), 10.0);
      final TDigestDouble restored = TDigestDouble.heapify(MemorySegment.ofArray(td.toByteArray()));
      assertEquals(restored.getTotalWeight(), td.getTotalWeight());
    }
  }

  @Test(dataProvider = "formats")
  public void invalidExtremaAndMeans(final boolean compat, final boolean isFloat) {
    final double[][] extrema = {{2, 1}, {Double.NaN, 1}, {0, Double.POSITIVE_INFINITY}};
    for (final double[] range : extrema) {
      assertInvalid(serialize(compat, isFloat, range[0], range[1],
          new double[] {0, 1}, new double[] {1, 1}, new double[0]), isFloat);
    }
    final double[][] means = {{1, 0}, {-1, 1}, {0, 2}, {Double.NaN, 1}, {0, Double.POSITIVE_INFINITY}};
    for (final double[] values : means) {
      assertInvalid(serialize(compat, isFloat, 0, 1, values, new double[] {1, 1}, new double[0]), isFloat);
    }
    assertInvalid(serialize(compat, isFloat, 0, 1, new double[0], new double[0], new double[0]), isFloat);
  }

  @Test(dataProvider = "formats")
  public void nonpositiveWeights(final boolean compat, final boolean isFloat) {
    for (final double weight : new double[] {0, -1}) {
      assertInvalid(serialize(compat, isFloat, 0, 1,
          new double[] {0, 1}, new double[] {weight, 1}, new double[0]), isFloat);
    }
  }

  @Test(dataProvider = "precisions")
  public void invalidCompatibilityWeights(final boolean isFloat) {
    for (final double weight : new double[] {Double.NaN, Double.POSITIVE_INFINITY,
        Double.NEGATIVE_INFINITY, 0.5, 1.5, 0x1p63}) {
      assertInvalid(serialize(true, isFloat, 0, 1,
          new double[] {0, 1}, new double[] {weight, 1}, new double[0]), isFloat);
    }
    assertInvalid(serialize(true, isFloat, 0, 1,
        new double[] {0, 1}, new double[] {0x1p62, 0x1p62}, new double[0]), isFloat);
  }

  @Test
  public void compatibilityFloatMeansRoundedOutsideExtrema() {
    // asSmallBytes() retains double extrema but rounds means to floats in either direction.
    for (final double value : new double[] {0.1, 0.3, -0.1, -0.3}) {
      final byte[] bytes = serialize(true, true, value, value,
          new double[] {value, value}, new double[] {2, 2}, new double[0]);
      final TDigestDouble td = TDigestDouble.heapify(MemorySegment.ofArray(bytes));
      assertEquals(td.getTotalWeight(), 4);
      assertEquals(td.getMinValue(), value);
      assertEquals(td.getMaxValue(), value);
      assertEquals(td.getQuantile(0.5), value);
      final TDigestDouble restored = TDigestDouble.heapify(MemorySegment.ofArray(td.toByteArray()));
      assertEquals(restored.getQuantile(0.5), value);
    }
  }

  @Test
  public void overflowingNativeWeights() {
    final byte[] bytes = serialize(false, false, 0, 1,
        new double[] {0, 1}, new double[] {1, 1}, new double[0]);
    MemorySegment.ofArray(bytes).set(ValueLayout.JAVA_LONG_UNALIGNED, 40, Long.MAX_VALUE);
    assertInvalid(bytes, false);

    final byte[] buffered = serialize(false, false, 0, 1,
        new double[] {0}, new double[] {1}, new double[] {1});
    MemorySegment.ofArray(buffered).set(ValueLayout.JAVA_LONG_UNALIGNED, 40, Long.MAX_VALUE);
    assertInvalid(buffered, false);

    // The exact boundary is representable, including an uncompressed value.
    MemorySegment.ofArray(buffered).set(ValueLayout.JAVA_LONG_UNALIGNED, 40, Long.MAX_VALUE - 1);
    final TDigestDouble td = TDigestDouble.heapify(MemorySegment.ofArray(buffered));
    assertEquals(td.getTotalWeight(), Long.MAX_VALUE);
    final TDigestDouble restored = TDigestDouble.heapify(MemorySegment.ofArray(td.toByteArray()));
    assertEquals(restored.getTotalWeight(), Long.MAX_VALUE);
  }

  @Test
  public void weightOverflowDoesNotChangeDigest() {
    final byte[] bytes = serialize(false, false, 0, 0,
        new double[] {0}, new double[] {1}, new double[0]);
    MemorySegment.ofArray(bytes).set(ValueLayout.JAVA_LONG_UNALIGNED, 40, Long.MAX_VALUE - 1);
    final TDigestDouble td = TDigestDouble.heapify(MemorySegment.ofArray(bytes));
    td.update(1);
    assertEquals(td.getTotalWeight(), Long.MAX_VALUE);
    final byte[] before = td.toByteArray();
    assertThrows(ArithmeticException.class, () -> td.update(2));
    final TDigestDouble other = new TDigestDouble();
    other.update(2);
    assertThrows(ArithmeticException.class, () -> td.merge(other));
    assertThrows(ArithmeticException.class, () -> td.merge(td));
    assertEquals(td.toByteArray(), before);
    assertEquals(other.getTotalWeight(), 1);
  }

  @Test(dataProvider = "precisions")
  public void bufferedValues(final boolean isFloat) {
    // Buffered values may be unsorted, and may be the only stored values.
    for (final double[] means : new double[][] {new double[0], {0, 10}}) {
      final double[] weights = new double[means.length];
      Arrays.fill(weights, 1);
      final byte[] bytes = serialize(false, isFloat, 0, 10, means, weights, new double[] {10, 0, 5});
      final TDigestDouble td = TDigestDouble.heapify(MemorySegment.ofArray(bytes), isFloat);
      assertEquals(td.getTotalWeight(), means.length + 3);
      assertEquals(td.getQuantile(0.5), 5.0);
      assertEquals(TDigestDouble.heapify(MemorySegment.ofArray(td.toByteArray())).getTotalWeight(), means.length + 3);
    }
    for (final double value : new double[] {-1, 11, Double.NaN, Double.POSITIVE_INFINITY}) {
      assertInvalid(serialize(false, isFloat, 0, 10,
          new double[] {0, 10}, new double[] {1, 1}, new double[] {value}), isFloat);
    }
  }

  @Test
  public void invalidFlags() {
    final byte[] bytes = new TDigestDouble().toByteArray();
    bytes[5] = 3; // mutually exclusive empty and single-value flags
    assertInvalid(bytes, false);
    bytes[5] = (byte) 0x81; // unknown flag on an otherwise valid empty image
    assertInvalid(bytes, false);
  }

  @Test(dataProvider = "formats")
  public void invalidCountsAndTruncatedPayload(final boolean compat, final boolean isFloat) {
    final byte[] bytes = serialize(compat, isFloat, 0, 1,
        new double[] {0, 1}, new double[] {1, 1}, new double[0]);
    for (int length = 0; length < bytes.length; length++) {
      assertInvalid(Arrays.copyOf(bytes, length), isFloat);
    }
    final ByteBuffer buffer = ByteBuffer.wrap(bytes).order(compat ? ByteOrder.BIG_ENDIAN : ByteOrder.nativeOrder());
    if (compat && isFloat) {
      buffer.putShort(28, (short) -1);
    } else {
      buffer.putInt(compat ? 28 : 8, Integer.MAX_VALUE);
    }
    assertInvalid(bytes, isFloat);
    if (!(compat && isFloat)) {
      buffer.putInt(compat ? 28 : 8, -1);
      assertInvalid(bytes, isFloat);
    }
    if (!compat) {
      buffer.putInt(8, 2);
      buffer.putInt(12, -1);
      assertInvalid(bytes, isFloat);
      buffer.putInt(12, Integer.MAX_VALUE);
      assertInvalid(bytes, isFloat);
    }
  }

  private static void assertInvalid(final byte[] bytes, final boolean isFloat) {
    assertThrows(SketchesArgumentException.class, () -> TDigestDouble.heapify(MemorySegment.ofArray(bytes), isFloat));
  }

  private static byte[] serialize(final boolean compat, final boolean isFloat, final double min, final double max,
      final double[] means, final double[] weights, final double[] buffered) {
    final int valueBytes = isFloat ? Float.BYTES : Double.BYTES;
    final int headerBytes = compat ? (isFloat ? 30 : 32) : 16 + (2 * valueBytes);
    final ByteBuffer buffer = ByteBuffer.allocate(headerBytes + (means.length * 2 * valueBytes)
        + (buffered.length * valueBytes)).order(compat ? ByteOrder.BIG_ENDIAN : ByteOrder.nativeOrder());
    if (compat) {
      buffer.putInt(isFloat ? 2 : 1).putDouble(min).putDouble(max);
      putValue(buffer, 100, isFloat);
      if (isFloat) {
        buffer.putInt(0).putShort((short) means.length);
      } else {
        buffer.putInt(means.length);
      }
    } else {
      buffer.put((byte) 2).put((byte) 1).put((byte) Family.TDIGEST.getID()).putShort((short) 100);
      buffer.put((byte) 0).putShort((short) 0).putInt(means.length).putInt(buffered.length);
      putValue(buffer, min, isFloat);
      putValue(buffer, max, isFloat);
    }
    for (int i = 0; i < means.length; i++) {
      if (compat) {
        putValue(buffer, weights[i], isFloat);
        putValue(buffer, means[i], isFloat);
      } else {
        putValue(buffer, means[i], isFloat);
        if (isFloat) {
          buffer.putInt((int) weights[i]);
        } else {
          buffer.putLong((long) weights[i]);
        }
      }
    }
    for (final double value : buffered) {
      putValue(buffer, value, isFloat);
    }
    return buffer.array();
  }

  private static void putValue(final ByteBuffer buffer, final double value, final boolean isFloat) {
    if (isFloat) {
      buffer.putFloat((float) value);
    } else {
      buffer.putDouble(value);
    }
  }
}
