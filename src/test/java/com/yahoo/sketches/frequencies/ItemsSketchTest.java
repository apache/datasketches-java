/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.frequencies;

import static com.yahoo.sketches.frequencies.PreambleUtil.*;
import org.testng.annotations.Test;

import com.yahoo.sketches.ArrayOfLongsSerDe;
import com.yahoo.sketches.ArrayOfStringsSerDe;
import com.yahoo.sketches.ArrayOfUtf16StringsSerDe;
import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.memory.NativeMemory;

import static org.testng.Assert.fail;

import org.testng.Assert;

public class ItemsSketchTest {

  @Test
  public void empty() {
    ItemsSketch<String> sketch = new ItemsSketch<String>(8);
    Assert.assertTrue(sketch.isEmpty());
    Assert.assertEquals(sketch.getNumActiveItems(), 0);
    Assert.assertEquals(sketch.getStreamLength(), 0);
    Assert.assertEquals(sketch.getLowerBound("a"), 0);
    Assert.assertEquals(sketch.getUpperBound("a"), 0);
  }

  @Test
  public void nullInput() {
    ItemsSketch<String> sketch = new ItemsSketch<String>(8);
    sketch.update(null);
    Assert.assertTrue(sketch.isEmpty());
    Assert.assertEquals(sketch.getNumActiveItems(), 0);
    Assert.assertEquals(sketch.getStreamLength(), 0);
    Assert.assertEquals(sketch.getLowerBound(null), 0);
    Assert.assertEquals(sketch.getUpperBound(null), 0);
  }

  @Test
  public void oneItem() {
    ItemsSketch<String> sketch = new ItemsSketch<String>(8);
    sketch.update("a");
    Assert.assertFalse(sketch.isEmpty());
    Assert.assertEquals(sketch.getNumActiveItems(), 1);
    Assert.assertEquals(sketch.getStreamLength(), 1);
    Assert.assertEquals(sketch.getEstimate("a"), 1);
    Assert.assertEquals(sketch.getLowerBound("a"), 1);
  }

  @Test
  public void severalItems() {
    ItemsSketch<String> sketch = new ItemsSketch<String>(8);
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

    sketch.reset();
    Assert.assertTrue(sketch.isEmpty());
    Assert.assertEquals(sketch.getNumActiveItems(), 0);
    Assert.assertEquals(sketch.getStreamLength(), 0);
  }

  @Test
  public void estimationMode() {
    ItemsSketch<Integer> sketch = new ItemsSketch<Integer>(8);
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
      ItemsSketch.Row<Integer>[] items = 
          sketch.getFrequentItems(ErrorType.NO_FALSE_POSITIVES);
      Assert.assertEquals(items.length, 2);
      // only 2 items (1 and 7) should have counts more than 1
      int count = 0;
      for (ItemsSketch.Row<Integer> item: items) {
        if (item.getLowerBound() > 1) count++;
      }
      Assert.assertEquals(count, 2);
    }

    {
      ItemsSketch.Row<Integer>[] items = 
          sketch.getFrequentItems(ErrorType.NO_FALSE_NEGATIVES);
      Assert.assertTrue(items.length >= 2);
      // only 2 items (1 and 7) should have counts more than 1
      int count = 0;
      for (ItemsSketch.Row<Integer> item: items) {
        if (item.getLowerBound() > 5) {
          count++;
        }
      }
      Assert.assertEquals(count, 2);
    }
}

  @Test
  public void serializeStringDeserializeEmpty() {
    ItemsSketch<String> sketch1 = new ItemsSketch<String>(8);
    byte[] bytes = sketch1.toByteArray(new ArrayOfStringsSerDe());
    ItemsSketch<String> sketch2 = 
        ItemsSketch.getInstance(new NativeMemory(bytes), new ArrayOfStringsSerDe());
    Assert.assertTrue(sketch2.isEmpty());
    Assert.assertEquals(sketch2.getNumActiveItems(), 0);
    Assert.assertEquals(sketch2.getStreamLength(), 0);
  }

  @Test
  public void serializeDeserializeUft8Strings() {
    ItemsSketch<String> sketch1 = new ItemsSketch<String>(8);
    sketch1.update("aaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    sketch1.update("bbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
    sketch1.update("ccccccccccccccccccccccccccccc");
    sketch1.update("ddddddddddddddddddddddddddddd");

    byte[] bytes = sketch1.toByteArray(new ArrayOfStringsSerDe());
    ItemsSketch<String> sketch2 = 
        ItemsSketch.getInstance(new NativeMemory(bytes), new ArrayOfStringsSerDe());
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
    ItemsSketch<String> sketch1 = new ItemsSketch<String>(8);
    sketch1.update("aaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    sketch1.update("bbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
    sketch1.update("ccccccccccccccccccccccccccccc");
    sketch1.update("ddddddddddddddddddddddddddddd");

    byte[] bytes = sketch1.toByteArray(new ArrayOfUtf16StringsSerDe());
    ItemsSketch<String> sketch2 = 
        ItemsSketch.getInstance(new NativeMemory(bytes), new ArrayOfUtf16StringsSerDe());
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
    ItemsSketch<String> sketch1 = new ItemsSketch<String>(16);
    for (int i=0; i<32; i++) {
      sketch1.update(Integer.toString(i), i*i);
    }
  }
  
  @Test
  public void getRowHeader() {
    String header = new ItemsSketch.Row<String>("a", 0, 0, 0).getRowHeader();
    Assert.assertNotNull(header);
    Assert.assertTrue(header.length() > 0);
  }
  
  @SuppressWarnings("unused")
  @Test
  public void serializeLongDeserialize() {
    ItemsSketch<Long> sketch1 = new ItemsSketch<Long>(8);
    sketch1.update(1L);
    sketch1.update(2L);
    sketch1.update(3L);
    sketch1.update(4L);

    String s = sketch1.toString();
    //println(s);
    
    byte[] bytes = sketch1.toByteArray(new ArrayOfLongsSerDe());
    ItemsSketch<Long> sketch2 = 
        ItemsSketch.getInstance(new NativeMemory(bytes), new ArrayOfLongsSerDe());
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
    ItemsSketch<String> sketch1 = new ItemsSketch<String>(8);
    sketch1.update("a");
    sketch1.update("b");
    sketch1.update("c");
    sketch1.update("d");
    
    ItemsSketch<String> sketch2 = new ItemsSketch<String>(8);
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
    ReversePurgeItemHashMap<Long> map = new ReversePurgeItemHashMap<Long>(8);
    Assert.assertNull(map.getActiveKeys());
    Assert.assertNull(map.getActiveValues());
  }
  
  @SuppressWarnings({ "rawtypes", "unused" })
  @Test
  public void checkMisc() {
    ItemsSketch<Long> sk1 = new ItemsSketch<Long>(8);
    Assert.assertEquals(sk1.getCurrentMapCapacity(), 6);
    Assert.assertEquals(sk1.getEstimate(new Long(1)), 0);
    ItemsSketch<Long> sk2 = new ItemsSketch<Long>(8);
    Assert.assertEquals(sk1.merge(sk2), sk1 );
    Assert.assertEquals(sk1.merge(null), sk1);
    sk1.update(new Long(1));
    ItemsSketch.Row<Long>[] rows = sk1.getFrequentItems(ErrorType.NO_FALSE_NEGATIVES);
    ItemsSketch.Row<Long> row = rows[0];
    Long item = row.getItem();
    Assert.assertEquals(row.getEstimate(), 1);
    Assert.assertEquals(row.getUpperBound(), 1);
    String s = row.toString();
    //println(s);
  }
  
  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkUpdateException() {
    ItemsSketch<Long> sk1 = new ItemsSketch<Long>(8);
    sk1.update(new Long(1), -1);
  }
  
  @Test
  public void checkMemExceptions() {
    ItemsSketch<Long> sk1 = new ItemsSketch<Long>(8);
    sk1.update(new Long(1), 1);
    ArrayOfLongsSerDe serDe = new ArrayOfLongsSerDe();
    byte[] byteArr = sk1.toByteArray(serDe);
    Memory mem = new NativeMemory(byteArr);
    //FrequentItemsSketch<Long> sk2 = FrequentItemsSketch.getInstance(mem, serDe);
    //println(sk2.toString());
    long pre0 = mem.getLong(0); //The correct first 8 bytes.
    //Now start corrupting
    tryBadMem(mem, PREAMBLE_LONGS_BYTE, 2); //Corrupt
    mem.putLong(0, pre0); //restore
    
    tryBadMem(mem, SER_VER_BYTE, 2); //Corrupt
    mem.putLong(0, pre0); //restore
    
    tryBadMem(mem, FAMILY_BYTE, 2); //Corrupt
    mem.putLong(0, pre0); //restore
    
    tryBadMem(mem, FLAGS_BYTE, 4); //Corrupt to true
    mem.putLong(0, pre0); //restore
    
    tryBadMem(mem, SER_DE_ID_SHORT, 2);
  }

  @Test
  public void oneItemUtf8() {
    ItemsSketch<String> sketch1 = new ItemsSketch<String>(8);
    sketch1.update("\u5fb5");
    Assert.assertFalse(sketch1.isEmpty());
    Assert.assertEquals(sketch1.getNumActiveItems(), 1);
    Assert.assertEquals(sketch1.getStreamLength(), 1);
    Assert.assertEquals(sketch1.getEstimate("\u5fb5"), 1);

    byte[] bytes = sketch1.toByteArray(new ArrayOfStringsSerDe());
    ItemsSketch<String> sketch2 = 
        ItemsSketch.getInstance(new NativeMemory(bytes), new ArrayOfStringsSerDe());
    Assert.assertFalse(sketch2.isEmpty());
    Assert.assertEquals(sketch2.getNumActiveItems(), 1);
    Assert.assertEquals(sketch2.getStreamLength(), 1);
    Assert.assertEquals(sketch2.getEstimate("\u5fb5"), 1);
  }

  private static void tryBadMem(Memory mem, int byteOffset, int byteValue) {
    ArrayOfLongsSerDe serDe = new ArrayOfLongsSerDe();
    try {
      mem.putByte(byteOffset, (byte) byteValue); //Corrupt
      ItemsSketch.getInstance(mem, serDe);
      fail();
    } catch (SketchesArgumentException e) {
      //expected
    }
  }
  
  @Test
  public void printlnTest() {
    println("PRINTING: " + this.getClass().getName());
  }

  /**
   * @param s value to print
   */
  static void println(String s) {
    //System.err.println(s); //disable here
  }
}
