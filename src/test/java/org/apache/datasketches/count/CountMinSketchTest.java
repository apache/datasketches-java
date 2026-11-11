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

package org.apache.datasketches.count;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.HexFormat;
import java.util.Random;

import org.apache.datasketches.common.AllocationTracker;
import org.apache.datasketches.common.ByteArrayUtil;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.SketchesException;
import org.apache.datasketches.hash.MurmurHash3;
import org.apache.datasketches.hash.XxHash64;
import org.testng.annotations.Test;

public class CountMinSketchTest {

  @Test
  public void createNewCountMinSketchTest() throws Exception {
    assertThrows(SketchesArgumentException.class, () -> new CountMinSketch((byte) 5, 1, 123));
    assertThrows(SketchesArgumentException.class, () -> new CountMinSketch((byte) 4, (1 << 28), 123));
    final byte numHashes = 3;
    final int numBuckets = 5;
    final long seed = 1234567;
    final CountMinSketch c = new CountMinSketch(numHashes, numBuckets, seed);

    assertEquals(c.getNumHashes_(), numHashes);
    assertEquals(c.getNumBuckets_(), numBuckets);
    assertEquals(c.getSeed_(), seed);
    assertTrue(c.isEmpty());
  }

  @Test
  public void parameterSuggestionsTest() {
    // Bucket suggestions
    assertThrows("Relative error must be at least 0.", SketchesException.class, () -> CountMinSketch.suggestNumBuckets(-1.0));
    assertEquals(CountMinSketch.suggestNumBuckets(0.2), 14);
    assertEquals(CountMinSketch.suggestNumBuckets(0.1), 28);
    assertEquals(CountMinSketch.suggestNumBuckets(0.05), 55);
    assertEquals(CountMinSketch.suggestNumBuckets(0.01), 272);

    // Check that the sketch get_epsilon acts inversely to suggest_num_buckets
    final byte numHashes = 3;
    final long seed = 1234567;
    assertTrue(new CountMinSketch(numHashes, 14, seed).getRelativeError() <= 0.2);
    assertTrue(new CountMinSketch(numHashes, 28, seed).getRelativeError() <= 0.1);
    assertTrue(new CountMinSketch(numHashes, 55, seed).getRelativeError() <= 0.05);
    assertTrue(new CountMinSketch(numHashes, 272, seed).getRelativeError() <= 0.01);

    // Hash suggestions
    assertThrows("Confidence must be between 0 and 1.0 (inclusive).", SketchesException.class, () -> CountMinSketch.suggestNumHashes(10));
    assertThrows("Confidence must be between 0 and 1.0 (inclusive).", SketchesException.class, () -> CountMinSketch.suggestNumHashes(-1.0));
    assertEquals(CountMinSketch.suggestNumHashes(0.682689492), 2);
    assertEquals(CountMinSketch.suggestNumHashes(0.954499736), 4);
    assertEquals(CountMinSketch.suggestNumHashes(0.997300204), 6);
  }

  @Test
  public void countMinSketchOneUpdateTest() {
    final byte numHashes = 3;
    final int numBuckets = 5;
    final long seed = 1234567;
    long insertedWeights = 0;
    final CountMinSketch c = new CountMinSketch(numHashes, numBuckets, seed);
    final String x = "x";

    assertTrue(c.isEmpty());
    assertEquals(c.getEstimate(x), 0);
    c.update(x, 1);
    assertFalse(c.isEmpty());
    assertEquals(c.getEstimate(x), 1);
    insertedWeights++;

    final long w = 9;
    insertedWeights += w;
    c.update(x, w);
    assertEquals(c.getEstimate(x), insertedWeights);

    final double w1 = 10.0;
    insertedWeights += (long) w1;
    c.update(x, (long) w1);
    assertEquals(c.getEstimate(x), insertedWeights);
    assertEquals(c.getTotalWeight_(), insertedWeights);
    assertTrue(c.getEstimate(x) <= c.getUpperBound(x));
    assertTrue(c.getEstimate(x) >= c.getLowerBound(x));
  }

  @Test
  public void frequencyCancellationTest() {
    final CountMinSketch c = new CountMinSketch((byte) 1, 5, 123456);
    c.update("x", 1);
    c.update("y", -1);
    assertEquals(c.getTotalWeight_(), 2);
    assertEquals(c.getEstimate("x"), 1);
    assertEquals(c.getEstimate("y"), -1);
  }

  @Test
  public void frequencyEstimates() {
    final int numItems = 10;
    final long[] data = new long[numItems];
    final long[] frequencies = new long[numItems];

    for (int i = 0; i < numItems; i++) {
      data[i] = i;
      frequencies[i] = (long) 1 << (numItems - i);
    }

    final double relativeError = 0.1;
    final double confidence = 0.99;
    final int numBuckets = CountMinSketch.suggestNumBuckets(relativeError);
    final byte numHashes = CountMinSketch.suggestNumHashes(confidence);

    final CountMinSketch c = new CountMinSketch(numHashes, numBuckets, 1234567);
    for (int i = 0; i < numItems; i++) {
      final long value = data[i];
      final long freq = frequencies[i];
      c.update(value, freq);
    }

    for (final long i : data) {
      final long est = c.getEstimate(i);
      final long upp = c.getUpperBound(i);
      final long low = c.getLowerBound(i);
      assertTrue(est <= upp);
      assertTrue(est >= low);
    }
  }

  @Test
  public void mergeFailTest() {
    final double relativeError = 0.25;
    final double confidence = 0.9;
    final long seed = 1234567;
    final int numBuckets = CountMinSketch.suggestNumBuckets(relativeError);
    final byte numHashes = CountMinSketch.suggestNumHashes(confidence);
    final CountMinSketch s = new CountMinSketch(numHashes, numBuckets, seed);

    assertThrows("Cannot merge a sketch with itself.", SketchesException.class, () -> s.merge(s));

    final CountMinSketch s1 = new CountMinSketch((byte) (numHashes + 1), numBuckets, seed);
    final CountMinSketch s2 = new CountMinSketch(numHashes, numBuckets + 1, seed);
    final CountMinSketch s3 = new CountMinSketch(numHashes, numBuckets, seed + 1);

    final CountMinSketch[] sketches = {s1, s2, s3};
    for (final CountMinSketch sk : sketches) {
      assertThrows("Incompatible sketch configuration.", SketchesException.class, () -> s.merge(sk));
    }
  }

  @Test
  public void mergeTest() {
    final double relativeError = 0.25;
    final double confidence = 0.9;
    final long seed = 123456;
    final int numBuckets = CountMinSketch.suggestNumBuckets(relativeError);
    final byte numHashes = CountMinSketch.suggestNumHashes(confidence);
    final CountMinSketch c = new CountMinSketch(numHashes, numBuckets, seed);

    final byte sHashes = c.getNumHashes_();
    final int sBuckets = c.getNumBuckets_();
    final long sSeed = c.getSeed_();
    final CountMinSketch s = new CountMinSketch(sHashes, sBuckets, sSeed);

    c.merge(s);
    assertEquals(c.getTotalWeight_(), 0);

    final long[] data = {2, 3, 5, 7};
    for (final long d : data) {
      c.update(d, 1);
      s.update(d, 1);
    }
    c.merge(s);

    assertEquals(c.getTotalWeight_(), 2 * s.getTotalWeight_());

    for (final long d : data) {
      assertTrue(c.getEstimate(d) <= c.getUpperBound(d));
      assertTrue(s.getEstimate(d) <= 2);
    }
  }

  @Test
  public void serializeDeserializeEmptyTest() {
    final byte numHashes = 3;
    final int numBuckets = 32;
    final long seed = 123456;
    final CountMinSketch c = new CountMinSketch(numHashes, numBuckets, seed);

    final byte[] b = c.toByteArray();
    assertThrows(SketchesArgumentException.class, () -> CountMinSketch.deserialize(b, seed - 1));

    final CountMinSketch d = CountMinSketch.deserialize(b, seed);
    assertEquals(d.getNumHashes_(), c.getNumHashes_());
    assertEquals(d.getNumBuckets_(), c.getNumBuckets_());
    assertEquals(d.getSeed_(), c.getSeed_());
    final long zero = 0;
    assertEquals(d.getEstimate(zero), c.getEstimate(zero));
    assertEquals(d.getTotalWeight_(), c.getTotalWeight_());
  }

  @Test
  public void serializeDeserializeTest() {
    final byte numHashes = 5;
    final int numBuckets = 64;
    final long seed = 1234456;
    final CountMinSketch c = new CountMinSketch(numHashes, numBuckets, seed);
    for (long i = 0; i < 10; i++) {
      c.update(i, 10*i*i);
    }

    final byte[] b = c.toByteArray();

    assertThrows(SketchesArgumentException.class, () -> CountMinSketch.deserialize(b, seed - 1));
    final CountMinSketch d = CountMinSketch.deserialize(b, seed);

    assertEquals(d.getNumHashes_(), c.getNumHashes_());
    assertEquals(d.getNumBuckets_(), c.getNumBuckets_());
    assertEquals(d.getSeed_(), c.getSeed_());
    assertEquals(d.getTotalWeight_(), c.getTotalWeight_());

    for (long i = 0; i < 10; i++) {
      assertEquals(d.getEstimate(i), c.getEstimate(i));
    }
  }

  @Test
  public void emptyImageIsUsable() {
    final long seed = 123L;
    final CountMinSketch cms = CountMinSketch.deserialize(new CountMinSketch((byte) 3, 5, seed).toByteArray(), seed);
    final CountMinSketch other = new CountMinSketch((byte) 3, 5, seed);
    cms.merge(other); //both empty
    assertTrue(cms.isEmpty());
    other.update(1L, 2);
    cms.merge(other);
    assertEquals(cms.getEstimate(1L), 2);
    cms.update(1L, 3);
    assertEquals(cms.getEstimate(1L), 5);
    final CountMinSketch cms2 = CountMinSketch.deserialize(cms.toByteArray(), seed);
    assertEquals(cms2.getEstimate(1L), 5);
  }

  @Test
  public void truncatedImageThrows() {
    final long seed = 123L;
    final CountMinSketch cms = new CountMinSketch((byte) 3, 5, seed);
    cms.update(1L, 1);
    final byte[] bytes = cms.toByteArray();
    ByteArrayUtil.putIntLE(bytes, 8, 1 << 28); //numBuckets larger than the image holds
    final long allocated = AllocationTracker.allocatedBytes(() -> CountMinSketch.deserialize(bytes, seed));
    assertTrue(allocated < (1 << 20), "allocated " + allocated);
    assertThrows(SketchesArgumentException.class, () -> CountMinSketch.deserialize(bytes, seed));
    final byte[] shortBytes = java.util.Arrays.copyOf(bytes, 8);
    assertThrows(SketchesArgumentException.class, () -> CountMinSketch.deserialize(shortBytes, seed));
  }

  // ---- Serialization version 2: test vectors from the TCK spec docs/count-min-format.md, section 7 ----

  private static final long SPEC_SEED = 9001L;

  // 7.1 XXH64 and 7.2 row seeds
  @Test
  public void v2XxHashAndRowSeedVectors() {
    final byte[] empty = new byte[0];
    assertEquals(XxHash64.hashBytes(empty, 0, 0, 0), 0xEF46DB3751D8E999L);
    assertEquals(XxHash64.hashBytes(utf8("a"), 0, 1, 0), 0xD24EC4F1A98C6E5BL);
    assertEquals(XxHash64.hashBytes(utf8("abc"), 0, 3, 0), 0x44BC2CF5AD770999L);

    final long[] seeds9001 = { 0xD71D64972584A288L, 0xEFE8454A6499F311L, 0x23819BE21B1B1D9EL };
    final long[] seeds42 = { 0xB71B47EBDA15746CL, 0x9ED50FD59358D232L, 0xD19FE30002B9A3CDL };
    for (int i = 0; i < 3; i++) {
      assertEquals(XxHash64.hash(i, 9001L), seeds9001[i]);
      assertEquals(XxHash64.hash(i, 42L), seeds42[i]);
    }
  }

  // 7.3 item buckets, observed through the counters a single update sets
  @Test
  public void v2BucketVectors() {
    assertBuckets(SPEC_SEED, 1L, 185, 898, 4);
    assertBuckets(SPEC_SEED, 9L, 490, 217, 440);
    assertBuckets(SPEC_SEED, -1L, 256, 641, 308);
    assertBuckets(SPEC_SEED, Long.MIN_VALUE, 951, 0, 298);
    assertBuckets(SPEC_SEED, "apple", 130, 747, 144);
    assertBuckets(SPEC_SEED, "\u00e9t\u00e9", 806, 491, 163);
    assertBuckets(42L, 1L, 468, 459, 696);
    assertBuckets(42L, 9L, 841, 428, 322);
  }

  // 7.4 images
  @Test
  public void v2ImageVectors() {
    final CountMinSketch empty = new CountMinSketch((byte) 2, 9, SPEC_SEED);
    assertEquals(empty.getSerialVersion(), CountMinSketch.SERIAL_VERSION_2);
    assertEquals(empty.toByteArray(), hex("02021201000000000900000002cc9300"));

    final CountMinSketch cms = new CountMinSketch((byte) 2, 9, SPEC_SEED);
    cms.update(1L, 10);
    cms.update(9L, 810);
    cms.update("apple", 5);
    final byte[] expected = new byte[168];
    System.arraycopy(hex("02021200000000000900000002cc9300"), 0, expected, 0, 16);
    ByteArrayUtil.putLongLE(expected, 16, 825);
    ByteArrayUtil.putLongLE(expected, 24 + (8 * 0), 810);  // row 0, bucket 0
    ByteArrayUtil.putLongLE(expected, 24 + (8 * 4), 5);    // row 0, bucket 4
    ByteArrayUtil.putLongLE(expected, 24 + (8 * 6), 10);   // row 0, bucket 6
    ByteArrayUtil.putLongLE(expected, 24 + (8 * 10), 5);   // row 1, bucket 1
    ByteArrayUtil.putLongLE(expected, 24 + (8 * 11), 810); // row 1, bucket 2
    ByteArrayUtil.putLongLE(expected, 24 + (8 * 16), 10);  // row 1, bucket 7
    assertEquals(cms.toByteArray(), expected);

    final CountMinSketch read = CountMinSketch.deserialize(expected, SPEC_SEED);
    assertEquals(read.getSerialVersion(), CountMinSketch.SERIAL_VERSION_2);
    assertEquals(read.getEstimate(1L), 10);
    assertEquals(read.getEstimate(9L), 810);
    assertEquals(read.getEstimate("apple"), 5);
    assertEquals(read.toByteArray(), expected);
  }

  // The long overloads hash the 8 little-endian bytes of the item, like the byte[] overloads
  @Test
  public void v2LongMatchesLittleEndianBytes() {
    final CountMinSketch a = new CountMinSketch((byte) 4, 101, SPEC_SEED);
    final CountMinSketch b = new CountMinSketch((byte) 4, 101, SPEC_SEED);
    final long[] items = { 0L, 1L, -1L, Long.MIN_VALUE, Long.MAX_VALUE, 0x0123456789ABCDEFL };
    for (final long item : items) {
      final byte[] le = new byte[Long.BYTES];
      ByteArrayUtil.putLongLE(le, 0, item);
      a.update(item, 3);
      b.update(le, 3);
    }
    assertEquals(a.toByteArray(), b.toByteArray());
  }

  // Version 1 images keep the legacy hashing and are written back as version 1
  @Test
  public void v1ImageKeepsLegacyHashing() {
    final long seed = 1234L;
    final byte numHashes = 3;
    final int numBuckets = 17;
    final byte[] emptyV1 = new CountMinSketch(numHashes, numBuckets, seed).toByteArray();
    emptyV1[1] = CountMinSketch.SERIAL_VERSION_1;

    final CountMinSketch v1 = CountMinSketch.deserialize(emptyV1, seed);
    assertEquals(v1.getSerialVersion(), CountMinSketch.SERIAL_VERSION_1);
    v1.update(7L, 11);
    v1.update("legacy", 13);

    final byte[] image = v1.toByteArray();
    assertEquals(image[1], CountMinSketch.SERIAL_VERSION_1);

    // Independent version 1 computation: seeds from Random(seed), MurmurHash3, floorMod, native byte order
    final Random rand = new Random(seed);
    final long[] rowSeeds = new long[numHashes];
    for (int i = 0; i < numHashes; i++) { rowSeeds[i] = rand.nextLong(); }
    final byte[] item7 = java.nio.ByteBuffer.allocate(8).order(java.nio.ByteOrder.nativeOrder()).putLong(7L).array();
    final long[] expected = new long[numHashes * numBuckets];
    for (int i = 0; i < numHashes; i++) {
      expected[(i * numBuckets) + Math.floorMod(MurmurHash3.hash(item7, rowSeeds[i])[0], numBuckets)] += 11;
      expected[(i * numBuckets) + Math.floorMod(MurmurHash3.hash(utf8("legacy"), rowSeeds[i])[0], numBuckets)] += 13;
    }
    for (int k = 0; k < expected.length; k++) {
      assertEquals(ByteArrayUtil.getLongLE(image, 24 + (8 * k)), expected[k], "counter " + k);
    }

    final CountMinSketch reread = CountMinSketch.deserialize(image, seed);
    assertEquals(reread.getSerialVersion(), CountMinSketch.SERIAL_VERSION_1);
    assertEquals(reread.getEstimate(7L), v1.getEstimate(7L));
    assertEquals(reread.getEstimate("legacy"), v1.getEstimate("legacy"));
  }

  @Test
  public void mergeAcrossVersionsThrows() {
    final long seed = 1234L;
    final byte[] emptyV1 = new CountMinSketch((byte) 3, 17, seed).toByteArray();
    emptyV1[1] = CountMinSketch.SERIAL_VERSION_1;
    final CountMinSketch v1 = CountMinSketch.deserialize(emptyV1, seed);
    final CountMinSketch v2 = new CountMinSketch((byte) 3, 17, seed);
    assertThrows(SketchesException.class, () -> v1.merge(v2));
    assertThrows(SketchesException.class, () -> v2.merge(v1));
  }

  @Test
  public void unknownSerialVersionThrows() {
    final long seed = 1234L;
    final byte[] image = new CountMinSketch((byte) 3, 17, seed).toByteArray();
    image[1] = 3;
    assertThrows(SketchesArgumentException.class, () -> CountMinSketch.deserialize(image, seed));
    image[1] = 0;
    assertThrows(SketchesArgumentException.class, () -> CountMinSketch.deserialize(image, seed));
  }

  @Test
  public void nullAndEmptyItemsAreIgnored() {
    final CountMinSketch cms = new CountMinSketch((byte) 3, 17, SPEC_SEED);
    cms.update((byte[]) null, 5);
    cms.update(new byte[0], 5);
    cms.update((String) null, 5);
    cms.update("", 5);
    assertTrue(cms.isEmpty());
    assertEquals(cms.getEstimate((byte[]) null), 0);
    assertEquals(cms.getEstimate(new byte[0]), 0);
    assertEquals(cms.getEstimate((String) null), 0);
    assertEquals(cms.getEstimate(""), 0);
    assertEquals(cms.getUpperBound((byte[]) null), 0);
  }

  private static void assertBuckets(final long seed, final long item, final int... buckets) {
    final CountMinSketch cms = new CountMinSketch((byte) 3, 1000, seed);
    cms.update(item, 1);
    assertCounters(cms, buckets);
  }

  private static void assertBuckets(final long seed, final String item, final int... buckets) {
    final CountMinSketch cms = new CountMinSketch((byte) 3, 1000, seed);
    cms.update(item, 1);
    assertCounters(cms, buckets);
  }

  private static void assertCounters(final CountMinSketch cms, final int... buckets) {
    final byte[] image = cms.toByteArray();
    for (int row = 0; row < buckets.length; row++) {
      for (int b = 0; b < 1000; b++) {
        final long counter = ByteArrayUtil.getLongLE(image, 24 + (8 * ((row * 1000) + b)));
        assertEquals(counter, b == buckets[row] ? 1 : 0, "row " + row + ", bucket " + b);
      }
    }
  }

  private static byte[] utf8(final String s) {
    return s.getBytes(StandardCharsets.UTF_8);
  }

  private static byte[] hex(final String s) {
    return HexFormat.of().parseHex(s);
  }

}
