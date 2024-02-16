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

package org.apache.datasketches.sampling;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

import org.testng.annotations.Test;

public class EbppsItemsSampleTest {
  private static final double EPS = 1e-14;

  @Test
  public void basicInitialization() {
    EbppsItemsSample<Integer> sample = new EbppsItemsSample<>(0);
    assertEquals(sample.getC(), 0.0);
    assertEquals(sample.getNumRetainedItems(), 0);
    assertNull(sample.getSample());
  }

  @Test
  public void initializeWithData() {
    final double theta1 = 1.0;
    EbppsItemsSample<Integer> sample = new EbppsItemsSample<>(1);
    sample.replaceContent(-1, theta1);
    assertEquals(sample.getC(), theta1);
    assertEquals(sample.getNumRetainedItems(), 1);
    assertEquals(sample.getSample().size(), 1);
    assertEquals(sample.getSample().get(0), -1);    
    assertFalse(sample.hasPartialItem());

    final double theta2 = 1e-300;
    sample.replaceContent(-2, theta2);
    assertEquals(sample.getC(), theta2);
    assertEquals(sample.getNumRetainedItems(), 1);
    // next check assumes random number is > 1e-300
    assertNull(sample.getSample());
    assertTrue(sample.hasPartialItem());
  }

  @Test
  public void downsampleToZeroOrOneItem() {
    EbppsItemsSample<String> sample = new EbppsItemsSample<>(1);
    sample.replaceContent("a", 1.0);

    sample.downsample(2.0); // no-op
    assertEquals(sample.getC(), 1.0);
    assertEquals(sample.getNumRetainedItems(), 1);
    assertEquals(sample.getSample().get(0), "a");
    assertFalse(sample.hasPartialItem());

    // downsample and result in an empty sample
    ArrayList<String> items = new ArrayList<>(Arrays.asList("a", "b"));
    sample = new EbppsItemsSample<>(items, null, 1.8);
    sample.replaceRandom(new Random(85942));
    sample.downsample(0.5);
    assertEquals(sample.getC(), 0.9);
    assertEquals(sample.getNumRetainedItems(), 0);
    assertNull(sample.getSample());
    assertFalse(sample.hasPartialItem());

    // downsample and result in a sample with a partial item
    // create a new ArrayList each time to be sure it's clean
    items = new ArrayList<>(Arrays.asList("a", "b"));
    sample = new EbppsItemsSample<>(items, null, 1.5);
    sample.replaceRandom(new Random(15));
    sample.downsample(0.5);
    assertEquals(sample.getC(), 0.75);
    assertEquals(sample.getNumRetainedItems(), 1);
    assertTrue(sample.hasPartialItem());
    for (String s : sample.getSample()) {
      assertTrue("a".equals(s) || "b".equals(s));
    }
  }

  @Test
  public void downsampleMultipleItems() {
    // downsample to an exact integer c (7.5 * 0.8 = 6.0)
    ArrayList<String> items = new ArrayList<>(Arrays.asList("a", "b", "c", "d", "e", "f", "g"));
    String partial = "h";
    ArrayList<String> referenceItems = new ArrayList<>(items); // copy of inputs
    referenceItems.add("h"); // include the partial item

    EbppsItemsSample<String> sample = new EbppsItemsSample<>(items, partial, 7.5);
    sample.downsample(0.8);
    assertEquals(sample.getC(), 6.0);
    assertEquals(sample.getNumRetainedItems(), 6);
    assertFalse(sample.hasPartialItem());
    for (String s : sample.getSample()) {
      assertTrue(referenceItems.contains(s));
    }

    // downsample to c > 1 with partial item
    items = new ArrayList<>(referenceItems); // includes previous optional
    partial = "i";
    referenceItems.add("i");
    sample = new EbppsItemsSample<>(items, partial, 8.5);
    sample.downsample(0.8);
    assertEquals(sample.getC(), 6.8, EPS);
    assertEquals(sample.getNumRetainedItems(), 7);
    assertTrue(sample.hasPartialItem());
    for (String s : sample.getSample()) {
      assertTrue(referenceItems.contains(s));
    }
  }

  @Test
  public void mergeUnitSamples() {
    int k = 8;
    EbppsItemsSample<Integer> sample = new EbppsItemsSample<>(k);
    EbppsItemsSample<Integer> s = new EbppsItemsSample<>(1);

    for (int i = 1; i <= k; ++i) {
      s.replaceContent(i, 1.0);
      sample.merge(s);
      assertEquals(sample.getC(), i);
      assertEquals(sample.getNumRetainedItems(), i);
    }

    sample.reset();
    assertEquals(sample.getC(), 0.0);
    assertEquals(sample.getNumRetainedItems(), 0);
    assertFalse(sample.hasPartialItem());
  }
}
