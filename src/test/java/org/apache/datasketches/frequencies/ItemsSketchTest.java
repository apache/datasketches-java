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

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED;
import static org.apache.datasketches.frequencies.PreambleUtil.FAMILY_BYTE;
import static org.apache.datasketches.frequencies.PreambleUtil.FLAGS_BYTE;
import static org.apache.datasketches.frequencies.PreambleUtil.PREAMBLE_LONGS_BYTE;
import static org.apache.datasketches.frequencies.PreambleUtil.SER_VER_BYTE;
import static org.apache.datasketches.frequencies.Util.LG_MIN_MAP_SIZE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.lang.foreign.MemorySegment;
import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.datasketches.common.ArrayOfLongsSerDe2;
import org.apache.datasketches.common.ArrayOfStringsSerDe2;
import org.apache.datasketches.common.ArrayOfUtf16StringsSerDe2;
import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.frequencies.ErrorType;
import org.apache.datasketches.frequencies.ItemsSketch;
import org.apache.datasketches.frequencies.ReversePurgeItemHashMap;
import org.apache.datasketches.frequencies.ItemsSketch.Row;

public class ItemsSketchTest {

  @Test
  public void empty() {
    final ItemsSketch<String> sketch = new ItemsSketch<>(1 << LG_MIN_MAP_SIZE);
    Assert.assertTrue(sketch.isEmpty());
    Assert.assertEquals(sketch.getNumActiveItems(), 0);
    Assert.assertEquals(sketch.getStreamLength(), 0);
    Assert.assertEquals(sketch.getLowerBound("a"), 0);
    Assert.assertEquals(sketch.getUpperBound("a"), 0);
  }

  @Test
  public void nullInput() {
    final ItemsSketch<String> sketch = new ItemsSketch<>(1 << LG_MIN_MAP_SIZE);
    sketch.update(null);
    Assert.assertTrue(sketch.isEmpty());
    Assert.assertEquals(sketch.getNumActiveItems(), 0);
    Assert.assertEquals(sketch.getStreamLength(), 0);
    Assert.assertEquals(sketch.getLowerBound(null), 0);
    Assert.assertEquals(sketch.getUpperBound(null), 0);
  }

  @Test
  public void oneItem() {
    final ItemsSketch<String> sketch = new ItemsSketch<>(1 << LG_MIN_MAP_SIZE);
    sketch.update("a");
    Assert.assertFalse(sketch.isEmpty());
    Assert.assertEquals(sketch.getNumActiveItems(), 1);
    Assert.assertEquals(sketch.getStreamLength(), 1);
    Assert.assertEquals(sketch.getEstimate("a"), 1);
    Assert.assertEquals(sketch.getLowerBound("a"), 1);
  }

  @Test
  public void severalItems() {
    final ItemsSketch<String> sketch = new ItemsSketch<>(1 << LG_MIN_MAP_SIZE);
    sketch.update("a");
    sketch.update("b");
    sketch.update("c");
    sketch.update("d");
    sketch.update("b");
    sketch.update("c");
    sketch.update("b");
    Assert.assertFalse(sketch.isEmpty());
    Assert.assertEquals(sketch.getNumActiveItems(), 4);
    Assert.assertEquals(sketch.getStreamLength(), 7);
    Assert.assertEquals(sketch.getEstimate("a"), 1);
    Assert.assertEquals(sketch.getEstimate("b"), 3);
    Assert.assertEquals(sketch.getEstimate("c"), 2);
    Assert.assertEquals(sketch.getEstimate("d"), 1);

    ItemsSketch.Row<String>[] items = sketch.getFrequentItems(ErrorType.NO_FALSE_POSITIVES);
    Assert.assertEquals(items.length, 4);

    items = sketch.getFrequentItems(3, ErrorType.NO_FALSE_POSITIVES);
    Assert.assertEquals(items.length, 1);
    Assert.assertEquals(items[0].getItem(), "b");

    sketch.reset();
    Assert.assertTrue(sketch.isEmpty());
    Assert.assertEquals(sketch.getNumActiveItems(), 0);
    Assert.assertEquals(sketch.getStreamLength(), 0);
  }

  @Test
  public void estimationMode() {
    final ItemsSketch<Integer> sketch = new ItemsSketch<>(1 << LG_MIN_MAP_SIZE);
    sketch.update(1, 10);
    sketch.update(2);
    sketch.update(3);
    sketch.update(4);
    sketch.update(5);
    sketch.update(6);
    sketch.update(7, 15);
    sketch.update(8);
    sketch.update(9);
    sketch.update(10);
    sketch.update(11);
    sketch.update(12);

    Assert.assertFalse(sketch.isEmpty());
    Assert.assertEquals(sketch.getStreamLength(), 35);

    {
      final ItemsSketch.Row<Integer>[] items =
          sketch.getFrequentItems(ErrorType.NO_FALSE_POSITIVES);
      Assert.assertEquals(items.length, 2);
      // only 2 items (1 and 7) should have counts more than 1
      int count = 0;
      for (final ItemsSketch.Row<Integer> item: items) {
        if (item.getLowerBound() > 1) {
          count++;
        }
      }
      Assert.assertEquals(count, 2);
    }

    {
      final ItemsSketch.Row<Integer>[] items =
          sketch.getFrequentItems(ErrorType.NO_FALSE_NEGATIVES);
      Assert.assertTrue(items.length >= 2);
      // only 2 items (1 and 7) should have counts more than 1
      int count = 0;
      for (final ItemsSketch.Row<Integer> item: items) {
        if (item.getLowerBound() > 5) {
          count++;
        }
      }
      Assert.assertEquals(count, 2);
    }
  }

  @Test
  public void serializeStringDeserializeEmpty() {
    final ItemsSketch<String> sketch1 = new ItemsSketch<>(1 << LG_MIN_MAP_SIZE);
    final byte[] bytes = sketch1.toByteArray(new ArrayOfStringsSerDe2());
    final ItemsSketch<String> sketch2 =
        ItemsSketch.getInstance(MemorySegment.ofArray(bytes), new ArrayOfStringsSerDe2());
    Assert.assertTrue(sketch2.isEmpty());
    Assert.assertEquals(sketch2.getNumActiveItems(), 0);
    Assert.assertEquals(sketch2.getStreamLength(), 0);
  }

  @Test
  public void serializeDeserializeUft8Strings() {
    final ItemsSketch<String> sketch1 = new ItemsSketch<>(1 << LG_MIN_MAP_SIZE);
    sketch1.update("aaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    sketch1.update("bbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
    sketch1.update("ccccccccccccccccccccccccccccc");
    sketch1.update("ddddddddddddddddddddddddddddd");

    final byte[] bytes = sketch1.toByteArray(new ArrayOfStringsSerDe2());
    final ItemsSketch<String> sketch2 =
        ItemsSketch.getInstance(MemorySegment.ofArray(bytes), new ArrayOfStringsSerDe2());
    sketch2.update("bbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
    sketch2.update("ccccccccccccccccccccccccccccc");
    sketch2.update("bbbbbbbbbbbbbbbbbbbbbbbbbbbbb");

    Assert.assertFalse(sketch2.isEmpty());
    Assert.assertEquals(sketch2.getNumActiveItems(), 4);
    Assert.assertEquals(sketch2.getStreamLength(), 7);
    Assert.assertEquals(sketch2.getEstimate("aaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), 1);
    Assert.assertEquals(sketch2.getEstimate("bbbbbbbbbbbbbbbbbbbbbbbbbbbbb"), 3);
    Assert.assertEquals(sketch2.getEstimate("ccccccccccccccccccccccccccccc"), 2);
    Assert.assertEquals(sketch2.getEstimate("ddddddddddddddddddddddddddddd"), 1);
  }

  @Test
  public void serializeDeserializeUtf16Strings() {
    final ItemsSketch<String> sketch1 = new ItemsSketch<>(1 << LG_MIN_MAP_SIZE);
    sketch1.update("aaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    sketch1.update("bbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
    sketch1.update("ccccccccccccccccccccccccccccc");
    sketch1.update("ddddddddddddddddddddddddddddd");

    final byte[] bytes = sketch1.toByteArray(new ArrayOfUtf16StringsSerDe2());
    final ItemsSketch<String> sketch2 =
        ItemsSketch.getInstance(MemorySegment.ofArray(bytes), new ArrayOfUtf16StringsSerDe2());
    sketch2.update("bbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
    sketch2.update("ccccccccccccccccccccccccccccc");
    sketch2.update("bbbbbbbbbbbbbbbbbbbbbbbbbbbbb");

    Assert.assertFalse(sketch2.isEmpty());
    Assert.assertEquals(sketch2.getNumActiveItems(), 4);
    Assert.assertEquals(sketch2.getStreamLength(), 7);
    Assert.assertEquals(sketch2.getEstimate("aaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), 1);
    Assert.assertEquals(sketch2.getEstimate("bbbbbbbbbbbbbbbbbbbbbbbbbbbbb"), 3);
    Assert.assertEquals(sketch2.getEstimate("ccccccccccccccccccccccccccccc"), 2);
    Assert.assertEquals(sketch2.getEstimate("ddddddddddddddddddddddddddddd"), 1);
  }

  @Test
  public void forceResize() {
    final ItemsSketch<String> sketch1 = new ItemsSketch<>(2 << LG_MIN_MAP_SIZE);
    for (int i=0; i<32; i++) {
      sketch1.update(Integer.toString(i), i*i);
    }
  }

  @Test
  public void getRowHeader() {
    final String header = ItemsSketch.Row.getRowHeader();
    Assert.assertNotNull(header);
    Assert.assertTrue(header.length() > 0);
  }

  @Test
  public void serializeLongDeserialize() {
    final ItemsSketch<Long> sketch1 = new ItemsSketch<>(1 << LG_MIN_MAP_SIZE);
    sketch1.update(1L);
    sketch1.update(2L);
    sketch1.update(3L);
    sketch1.update(4L);

    final String s = sketch1.toString();
    println(s);

    final byte[] bytes = sketch1.toByteArray(new ArrayOfLongsSerDe2());
    final ItemsSketch<Long> sketch2 =
        ItemsSketch.getInstance(MemorySegment.ofArray(bytes), new ArrayOfLongsSerDe2());
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
  public void mergeExact() {
    final ItemsSketch<String> sketch1 = new ItemsSketch<>(1 << LG_MIN_MAP_SIZE);
    sketch1.update("a");
    sketch1.update("b");
    sketch1.update("c");
    sketch1.update("d");

    final ItemsSketch<String> sketch2 = new ItemsSketch<>(1 << LG_MIN_MAP_SIZE);
    sketch2.update("b");
    sketch2.update("c");
    sketch2.update("b");

    sketch1.merge(sketch2);
    Assert.assertFalse(sketch1.isEmpty());
    Assert.assertEquals(sketch1.getNumActiveItems(), 4);
    Assert.assertEquals(sketch1.getStreamLength(), 7);
    Assert.assertEquals(sketch1.getEstimate("a"), 1);
    Assert.assertEquals(sketch1.getEstimate("b"), 3);
    Assert.assertEquals(sketch1.getEstimate("c"), 2);
    Assert.assertEquals(sketch1.getEstimate("d"), 1);
  }

  @Test
  public void checkNullMapReturns() {
    final ReversePurgeItemHashMap<Long> map = new ReversePurgeItemHashMap<>(1 << LG_MIN_MAP_SIZE);
    Assert.assertNull(map.getActiveKeys());
    Assert.assertNull(map.getActiveValues());
  }

  @SuppressWarnings("unlikely-arg-type")
  @Test
  public void checkMisc() {
    final ItemsSketch<Long> sk1 = new ItemsSketch<>(1 << LG_MIN_MAP_SIZE);
    Assert.assertEquals(sk1.getCurrentMapCapacity(), 6);
    Assert.assertEquals(sk1.getEstimate(Long.valueOf(1)), 0);
    final ItemsSketch<Long> sk2 = new ItemsSketch<>(8);
    Assert.assertEquals(sk1.merge(sk2), sk1 );
    Assert.assertEquals(sk1.merge(null), sk1);
    sk1.update(Long.valueOf(1));
    final ItemsSketch.Row<Long>[] rows = sk1.getFrequentItems(ErrorType.NO_FALSE_NEGATIVES);
    final ItemsSketch.Row<Long> row = rows[0];
    Assert.assertTrue(row.hashCode() != 0);
    Assert.assertTrue(row.equals(row));
    Assert.assertFalse(row.equals(sk1));
    Assert.assertEquals((long)row.getItem(), 1L);
    Assert.assertEquals(row.getEstimate(), 1);
    Assert.assertEquals(row.getUpperBound(), 1);
    final String s = row.toString();
    println(s);
    final ItemsSketch.Row<Long> nullRow = null; //check equals(null)
    Assert.assertFalse(row.equals(nullRow));
  }

  @Test
  public void checkToString() {
    final ItemsSketch<Long> sk = new ItemsSketch<>(1 << LG_MIN_MAP_SIZE);
    sk.update(Long.valueOf(1));
    println(ItemsSketch.toString(sk.toByteArray(new ArrayOfLongsSerDe2())));
  }

  @Test
  public void checkGetFrequentItems1() {
    final ItemsSketch<Long> fis = new ItemsSketch<>(1 << LG_MIN_MAP_SIZE);
    fis.update(1L);
    final Row<Long>[] rowArr = fis.getFrequentItems(ErrorType.NO_FALSE_POSITIVES);
    final Row<Long> row = rowArr[0];
    assertNotNull(row);
    assertEquals(row.est, 1L);
    assertEquals(row.item, Long.valueOf(1L));
    assertEquals(row.lb, 1L);
    assertEquals(row.ub, 1L);
    Row<Long> newRow = new Row<>(row.item, row.est+1, row.ub, row.lb);
    assertFalse(row.equals(newRow));
    newRow = new Row<>(row.item, row.est, row.ub, row.lb);
    assertTrue(row.equals(newRow));
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkUpdateException() {
    final ItemsSketch<Long> sk1 = new ItemsSketch<>(1 << LG_MIN_MAP_SIZE);
    sk1.update(Long.valueOf(1), -1);
  }

  @Test
  public void checkMemExceptions() {
    final ItemsSketch<Long> sk1 = new ItemsSketch<>(1 << LG_MIN_MAP_SIZE);
    sk1.update(Long.valueOf(1), 1);
    final ArrayOfLongsSerDe2 serDe = new ArrayOfLongsSerDe2();
    final byte[] byteArr = sk1.toByteArray(serDe);
    final MemorySegment mem = MemorySegment.ofArray(byteArr);
    //FrequentItemsSketch<Long> sk2 = FrequentItemsSketch.getInstance(mem, serDe);
    //println(sk2.toString());
    final long pre0 = mem.get(JAVA_LONG_UNALIGNED, 0); //The correct first 8 bytes.
    //Now start corrupting
    tryBadMem(mem, PREAMBLE_LONGS_BYTE, 2); //Corrupt
    mem.set(JAVA_LONG_UNALIGNED, 0, pre0); //restore

    tryBadMem(mem, SER_VER_BYTE, 2); //Corrupt
    mem.set(JAVA_LONG_UNALIGNED, 0, pre0); //restore

    tryBadMem(mem, FAMILY_BYTE, 2); //Corrupt
    mem.set(JAVA_LONG_UNALIGNED, 0, pre0); //restore

    tryBadMem(mem, FLAGS_BYTE, 4); //Corrupt to true
    mem.set(JAVA_LONG_UNALIGNED, 0, pre0); //restore
  }

  @Test
  public void oneItemUtf8() {
    final ItemsSketch<String> sketch1 = new ItemsSketch<>(1 << LG_MIN_MAP_SIZE);
    sketch1.update("\u5fb5");
    Assert.assertFalse(sketch1.isEmpty());
    Assert.assertEquals(sketch1.getNumActiveItems(), 1);
    Assert.assertEquals(sketch1.getStreamLength(), 1);
    Assert.assertEquals(sketch1.getEstimate("\u5fb5"), 1);

    final byte[] bytes = sketch1.toByteArray(new ArrayOfStringsSerDe2());
    final ItemsSketch<String> sketch2 =
        ItemsSketch.getInstance(MemorySegment.ofArray(bytes), new ArrayOfStringsSerDe2());
    Assert.assertFalse(sketch2.isEmpty());
    Assert.assertEquals(sketch2.getNumActiveItems(), 1);
    Assert.assertEquals(sketch2.getStreamLength(), 1);
    Assert.assertEquals(sketch2.getEstimate("\u5fb5"), 1);
  }

  @Test
  public void checkGetEpsilon() {
    assertEquals(ItemsSketch.getEpsilon(1024), 3.5 / 1024, 0.0);
    try {
      ItemsSketch.getEpsilon(1000);
    } catch (final SketchesArgumentException e) { }
  }

  @Test
  public void checkGetAprioriError() {
    final double eps = 3.5 / 1024;
    assertEquals(ItemsSketch.getAprioriError(1024, 10_000), eps * 10_000);
  }

  @Test
  public void printlnTest() {
    println("PRINTING: " + this.getClass().getName());
  }

  //Restricted methods

  private static void tryBadMem(final MemorySegment mem, final int byteOffset, final int byteValue) {
    final ArrayOfLongsSerDe2 serDe = new ArrayOfLongsSerDe2();
    try {
      mem.set(JAVA_BYTE, byteOffset, (byte) byteValue); //Corrupt
      ItemsSketch.getInstance(mem, serDe);
      fail();
    } catch (final SketchesArgumentException e) {
      //expected
    }
  }


  /**
   * @param s value to print
   */
  static void println(final String s) {
    //System.out.println(s); //disable here
  }
}
