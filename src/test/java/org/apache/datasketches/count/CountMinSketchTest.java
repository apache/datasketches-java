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

import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.SketchesException;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.lang.annotation.Repeatable;
import java.nio.ByteBuffer;

import static org.testng.Assert.*;

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
}
