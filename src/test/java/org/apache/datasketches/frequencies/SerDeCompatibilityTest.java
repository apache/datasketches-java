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

package org.apache.datasketches.frequencies;

import static org.apache.datasketches.common.TestUtil.GENERATE_JAVA_FILES;
import static org.apache.datasketches.common.TestUtil.javaPath;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;

import org.apache.datasketches.common.ArrayOfItemsSerDe;
import org.apache.datasketches.common.ArrayOfLongsSerDe;
import org.apache.datasketches.common.ArrayOfStringsSerDe;
import org.apache.datasketches.memory.WritableMemory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class SerDeCompatibilityTest {

  static final ArrayOfItemsSerDe<Long> serDe = new ArrayOfLongsSerDe();

  @Test
  public void itemsToLongs() {
    final ItemsSketch<Long> sketch1 = new ItemsSketch<>(8);
    sketch1.update(1L);
    sketch1.update(2L);
    sketch1.update(3L);
    sketch1.update(4L);

    final byte[] bytes = sketch1.toByteArray(serDe);
    final LongsSketch sketch2 = LongsSketch.getInstance(WritableMemory.writableWrap(bytes));
    sketch2.update(2L);
    sketch2.update(3L);
    sketch2.update(2L);

    Assert.assertFalse(sketch2.isEmpty());
    Assert.assertEquals(sketch2.getNumActiveItems(), 4);
    Assert.assertEquals(sketch2.getStreamLength(), 7);
    Assert.assertEquals(sketch2.getEstimate(1L), 1);
    Assert.assertEquals(sketch2.getEstimate(2L), 3);
    Assert.assertEquals(sketch2.getEstimate(3L), 2);
    Assert.assertEquals(sketch2.getEstimate(4L), 1);
  }

  @Test
  public void longsToItems() {
    final LongsSketch sketch1 = new LongsSketch(8);
    sketch1.update(1L);
    sketch1.update(2L);
    sketch1.update(3L);
    sketch1.update(4L);

    final byte[] bytes = sketch1.toByteArray();
    final ItemsSketch<Long> sketch2 = ItemsSketch.getInstance(WritableMemory.writableWrap(bytes), serDe);
    sketch2.update(2L);
    sketch2.update(3L);
    sketch2.update(2L);

    Assert.assertFalse(sketch2.isEmpty());
    Assert.assertEquals(sketch2.getNumActiveItems(), 4);
    Assert.assertEquals(sketch2.getStreamLength(), 7);
    Assert.assertEquals(sketch2.getEstimate(1L), 1);
    Assert.assertEquals(sketch2.getEstimate(2L), 3);
    Assert.assertEquals(sketch2.getEstimate(3L), 2);
    Assert.assertEquals(sketch2.getEstimate(4L), 1);
  }

  @Test(groups = {GENERATE_JAVA_FILES})
  public void generateBinariesForCompatibilityTestingLongsSketch() throws IOException {
    final int[] nArr = {0, 1, 10, 100, 1000, 10_000, 100_000, 1_000_000};
    for (int n: nArr) {
      LongsSketch sk = new LongsSketch(64);
      for (int i = 1; i <= n; i++) sk.update(i);
      assertTrue(n == 0 ? sk.isEmpty() : !sk.isEmpty());
      if (n > 10) { assertTrue(sk.getMaximumError() > 0); }
      else { assertEquals(sk.getMaximumError(), 0); }
      Files.newOutputStream(javaPath.resolve("frequent_long_n" + n + "_java.sk")).write(sk.toByteArray());
    }
  }

  @Test(groups = {GENERATE_JAVA_FILES})
  public void generateBinariesForCompatibilityTestingStringsSketch() throws IOException {
    final int[] nArr = {0, 1, 10, 100, 1000, 10_000, 100_000, 1_000_000};
    for (final int n: nArr) {
      final ItemsSketch<String> sk = new ItemsSketch<>(64);
      for (int i = 1; i <= n; i++) sk.update(Integer.toString(i));
      assertTrue(n == 0 ? sk.isEmpty() : !sk.isEmpty());
      if (n > 10) { assertTrue(sk.getMaximumError() > 0); }
      else { assertEquals(sk.getMaximumError(), 0); }
      Files.newOutputStream(javaPath.resolve("frequent_string_n" + n + "_java.sk"))
        .write(sk.toByteArray(new ArrayOfStringsSerDe()));
    }
  }

  @Test(groups = {GENERATE_JAVA_FILES})
  public void generateBinariesForCompatibilityTestingStringsSketchAscii() throws IOException {
    final ItemsSketch<String> sk = new ItemsSketch<>(64);
    sk.update("aaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 1);
    sk.update("bbbbbbbbbbbbbbbbbbbbbbbbbbbbb", 2);
    sk.update("ccccccccccccccccccccccccccccc", 3);
    sk.update("ddddddddddddddddddddddddddddd", 4);
    Files.newOutputStream(javaPath.resolve("frequent_string_ascii_java.sk"))
      .write(sk.toByteArray(new ArrayOfStringsSerDe()));
  }

  @Test(groups = {GENERATE_JAVA_FILES})
  public void generateBinariesForCompatibilityTestingStringsSketchUtf8() throws IOException {
    final ItemsSketch<String> sk = new ItemsSketch<>(64);
    sk.update("абвгд", 1);
    sk.update("еёжзи", 2);
    sk.update("йклмн", 3);
    sk.update("опрст", 4);
    sk.update("уфхцч", 5);
    sk.update("шщъыь", 6);
    sk.update("эюя", 7);
    Files.newOutputStream(javaPath.resolve("frequent_string_utf8_java.sk"))
      .write(sk.toByteArray(new ArrayOfStringsSerDe()));
  }

}
