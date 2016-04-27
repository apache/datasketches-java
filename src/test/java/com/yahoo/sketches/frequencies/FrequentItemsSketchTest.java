package com.yahoo.sketches.frequencies;

import static com.yahoo.sketches.frequencies.PreambleUtil.*;
import org.testng.annotations.Test;

import com.yahoo.sketches.frequencies.FrequentItemsSketch.ErrorType;
import com.yahoo.sketches.frequencies.FrequentItemsSketch.Row;
import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.memory.NativeMemory;

import static org.testng.Assert.fail;

import org.testng.Assert;

public class FrequentItemsSketchTest {

  @Test
  public void empty() {
    FrequentItemsSketch<String> sketch = new FrequentItemsSketch<String>(8);
    Assert.assertTrue(sketch.isEmpty());
    Assert.assertEquals(sketch.getNumActiveItems(), 0);
    Assert.assertEquals(sketch.getStreamLength(), 0);
    Assert.assertEquals(sketch.getLowerBound("a"), 0);
    Assert.assertEquals(sketch.getUpperBound("a"), 0);
  }

  @Test
  public void nullInput() {
    FrequentItemsSketch<String> sketch = new FrequentItemsSketch<String>(8);
    sketch.update(null);
    Assert.assertTrue(sketch.isEmpty());
    Assert.assertEquals(sketch.getNumActiveItems(), 0);
    Assert.assertEquals(sketch.getStreamLength(), 0);
    Assert.assertEquals(sketch.getLowerBound(null), 0);
    Assert.assertEquals(sketch.getUpperBound(null), 0);
  }

  @Test
  public void oneItem() {
    FrequentItemsSketch<String> sketch = new FrequentItemsSketch<String>(8);
    sketch.update("a");
    Assert.assertFalse(sketch.isEmpty());
    Assert.assertEquals(sketch.getNumActiveItems(), 1);
    Assert.assertEquals(sketch.getStreamLength(), 1);
    Assert.assertEquals(sketch.getEstimate("a"), 1);
    Assert.assertEquals(sketch.getLowerBound("a"), 1);
  }

  @Test
  public void severalItems() {
    FrequentItemsSketch<String> sketch = new FrequentItemsSketch<String>(8);
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

    FrequentItemsSketch<String>.Row[] items = sketch.getFrequentItems(FrequentItemsSketch.ErrorType.NO_FALSE_POSITIVES);
    Assert.assertEquals(items.length, 4);

    sketch.reset();
    Assert.assertTrue(sketch.isEmpty());
    Assert.assertEquals(sketch.getNumActiveItems(), 0);
    Assert.assertEquals(sketch.getStreamLength(), 0);
  }

  @Test
  public void estimationMode() {
    FrequentItemsSketch<Integer> sketch = new FrequentItemsSketch<Integer>(8);
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
      FrequentItemsSketch<Integer>.Row[] items = 
          sketch.getFrequentItems(FrequentItemsSketch.ErrorType.NO_FALSE_POSITIVES);
      Assert.assertEquals(items.length, 2);
      // only 2 items (1 and 7) should have counts more than 1
      int count = 0;
      for (FrequentItemsSketch<Integer>.Row item: items) {
        if (item.getLowerBound() > 1) count++;
      }
      Assert.assertEquals(count, 2);
    }

    {
      FrequentItemsSketch<Integer>.Row[] items = 
          sketch.getFrequentItems(FrequentItemsSketch.ErrorType.NO_FALSE_NEGATIVES);
      Assert.assertTrue(items.length >= 2);
      // only 2 items (1 and 7) should have counts more than 1
      int count = 0;
      for (FrequentItemsSketch<Integer>.Row item: items) {
        if (item.getLowerBound() > 5) {
          count++;
        }
      }
      Assert.assertEquals(count, 2);
    }
}

  @Test
  public void serializeStringDeserializeEmpty() {
    FrequentItemsSketch<String> sketch1 = new FrequentItemsSketch<String>(8);
    byte[] bytes = sketch1.serializeToByteArray(new ArrayOfStringsSerDe());
    FrequentItemsSketch<String> sketch2 = 
        FrequentItemsSketch.getInstance(new NativeMemory(bytes), new ArrayOfStringsSerDe());
    Assert.assertTrue(sketch2.isEmpty());
    Assert.assertEquals(sketch2.getNumActiveItems(), 0);
    Assert.assertEquals(sketch2.getStreamLength(), 0);
  }

  @Test
  public void serializeStringDeserialize() {
    FrequentItemsSketch<String> sketch1 = new FrequentItemsSketch<String>(8);
    sketch1.update("a");
    sketch1.update("b");
    sketch1.update("c");
    sketch1.update("d");

    byte[] bytes = sketch1.serializeToByteArray(new ArrayOfStringsSerDe());
    FrequentItemsSketch<String> sketch2 = 
        FrequentItemsSketch.getInstance(new NativeMemory(bytes), new ArrayOfStringsSerDe());
    sketch2.update("b");
    sketch2.update("c");
    sketch2.update("b");

    Assert.assertFalse(sketch2.isEmpty());
    Assert.assertEquals(sketch2.getNumActiveItems(), 4);
    Assert.assertEquals(sketch2.getStreamLength(), 7);
    Assert.assertEquals(sketch2.getEstimate("a"), 1);
    Assert.assertEquals(sketch2.getEstimate("b"), 3);
    Assert.assertEquals(sketch2.getEstimate("c"), 2);
    Assert.assertEquals(sketch2.getEstimate("d"), 1);
  }

  @SuppressWarnings("unused")
  @Test
  public void serializeLongDeserialize() {
    FrequentItemsSketch<Long> sketch1 = new FrequentItemsSketch<Long>(8);
    sketch1.update(1L);
    sketch1.update(2L);
    sketch1.update(3L);
    sketch1.update(4L);

    String s = sketch1.toString();
    //println(s);
    
    byte[] bytes = sketch1.serializeToByteArray(new ArrayOfLongsSerDe());
    FrequentItemsSketch<Long> sketch2 = 
        FrequentItemsSketch.getInstance(new NativeMemory(bytes), new ArrayOfLongsSerDe());
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
    FrequentItemsSketch<String> sketch1 = new FrequentItemsSketch<String>(8);
    sketch1.update("a");
    sketch1.update("b");
    sketch1.update("c");
    sketch1.update("d");
    
    FrequentItemsSketch<String> sketch2 = new FrequentItemsSketch<String>(8);
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
    FrequentItemsSketch<Long> sk1 = new FrequentItemsSketch<Long>(4);
    Assert.assertEquals(sk1.getCurrentMapCapacity(), 3);
    Assert.assertEquals(sk1.getEstimate(new Long(1)), 0);
    FrequentItemsSketch<Long> sk2 = new FrequentItemsSketch<Long>(4);
    Assert.assertEquals(sk1.merge(sk2), sk1 );
    Assert.assertEquals(sk1.merge(null), sk1);
    sk1.update(new Long(1));
    Row[] rows = sk1.getFrequentItems(ErrorType.NO_FALSE_NEGATIVES);
    Row row = rows[0];
    Long item = (Long)row.getItem();
    Assert.assertEquals(row.getEstimate(), 1);
    Assert.assertEquals(row.getUpperBound(), 1);
    String s = row.toString();
    //println(s);
  }
  
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkUpdateException() {
    FrequentItemsSketch<Long> sk1 = new FrequentItemsSketch<Long>(4);
    sk1.update(new Long(1), -1);
  }
  
  @Test
  public void checkMemExceptions() {
    FrequentItemsSketch<Long> sk1 = new FrequentItemsSketch<Long>(4);
    sk1.update(new Long(1), 1);
    ArrayOfLongsSerDe serDe = new ArrayOfLongsSerDe();
    byte[] byteArr = sk1.serializeToByteArray(serDe);
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
    
    tryBadMem(mem, FREQ_SKETCH_TYPE_BYTE, 2);
  }
  
  private static void tryBadMem(Memory mem, int byteOffset, int byteValue) {
    ArrayOfLongsSerDe serDe = new ArrayOfLongsSerDe();
    try {
      mem.putByte(byteOffset, (byte) byteValue); //Corrupt
      FrequentItemsSketch.getInstance(mem, serDe);
      fail();
    } catch (IllegalArgumentException e) {
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
    System.err.println(s); //disable here
  }
}
