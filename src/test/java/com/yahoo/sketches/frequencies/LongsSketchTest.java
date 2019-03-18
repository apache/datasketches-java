/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.frequencies;

import static com.yahoo.sketches.Util.LS;
import static com.yahoo.sketches.frequencies.DistTest.randomGeometricDist;
import static com.yahoo.sketches.frequencies.PreambleUtil.FAMILY_BYTE;
import static com.yahoo.sketches.frequencies.PreambleUtil.FLAGS_BYTE;
import static com.yahoo.sketches.frequencies.PreambleUtil.PREAMBLE_LONGS_BYTE;
import static com.yahoo.sketches.frequencies.PreambleUtil.SER_VER_BYTE;
import static com.yahoo.sketches.frequencies.Util.LG_MIN_MAP_SIZE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.Util;
import com.yahoo.sketches.frequencies.LongsSketch.Row;

public class LongsSketchTest {

  @Test
  public void hashMapSerialTest() {
    ReversePurgeLongHashMap map = new ReversePurgeLongHashMap(8);
    map.adjustOrPutValue(10, 15);
    map.adjustOrPutValue(10, 5);
    map.adjustOrPutValue(1, 1);
    map.adjustOrPutValue(2, 3);
    String string = map.serializeToString();
    //println(string);
    //println(map.toString());
    ReversePurgeLongHashMap new_map =
        ReversePurgeLongHashMap.getInstance(string);
    String new_string = new_map.serializeToString();
    Assert.assertTrue(string.equals(new_string));
  }

  @Test
  public void frequentItemsStringSerialTest() {
    LongsSketch sketch = new LongsSketch(8);
    LongsSketch sketch2 = new LongsSketch(128);
    sketch.update(10, 100);
    sketch.update(10, 100);
    sketch.update(15, 3443);
    sketch.update(1000001, 1010230);
    sketch.update(1000002, 1010230);

    String string0 = sketch.serializeToString();
    LongsSketch new_sketch0 = LongsSketch.getInstance(string0);
    String new_string0 = new_sketch0.serializeToString();
    Assert.assertTrue(string0.equals(new_string0));
    Assert.assertTrue(new_sketch0.getMaximumMapCapacity() == sketch.getMaximumMapCapacity());
    Assert.assertTrue(new_sketch0.getCurrentMapCapacity() == sketch.getCurrentMapCapacity());

    sketch2.update(190, 12902390);
    sketch2.update(191, 12902390);
    sketch2.update(192, 12902390);
    sketch2.update(193, 12902390);
    sketch2.update(194, 12902390);
    sketch2.update(195, 12902390);
    sketch2.update(196, 12902390);
    sketch2.update(197, 12902390);
    sketch2.update(198, 12902390);
    sketch2.update(199, 12902390);
    sketch2.update(200, 12902390);
    sketch2.update(201, 12902390);
    sketch2.update(202, 12902390);
    sketch2.update(203, 12902390);
    sketch2.update(204, 12902390);
    sketch2.update(205, 12902390);
    sketch2.update(206, 12902390);
    sketch2.update(207, 12902390);
    sketch2.update(208, 12902390);

    String string2 = sketch2.serializeToString();
    LongsSketch new_sketch2 = LongsSketch.getInstance(string2);
    String new_string2 = new_sketch2.serializeToString();
    Assert.assertTrue(string2.equals(new_string2));
    Assert.assertTrue(new_sketch2.getMaximumMapCapacity() == sketch2.getMaximumMapCapacity());
    Assert.assertTrue(new_sketch2.getCurrentMapCapacity() == sketch2.getCurrentMapCapacity());
    Assert.assertTrue(new_sketch2.getStreamLength() == sketch2.getStreamLength());

    LongsSketch merged_sketch = sketch.merge(sketch2);

    String string = merged_sketch.serializeToString();
    LongsSketch new_sketch = LongsSketch.getInstance(string);
    String new_string = new_sketch.serializeToString();
    Assert.assertTrue(string.equals(new_string));
    Assert.assertTrue(new_sketch.getMaximumMapCapacity() == merged_sketch.getMaximumMapCapacity());
    Assert.assertTrue(new_sketch.getCurrentMapCapacity() == merged_sketch.getCurrentMapCapacity());
    Assert.assertTrue(new_sketch.getStreamLength() == merged_sketch.getStreamLength());
  }

  @Test
  public void frequentItemsByteSerialTest() {
    //Empty Sketch
    LongsSketch sketch = new LongsSketch(16);
    byte[] bytearray0 = sketch.toByteArray();
    WritableMemory mem0 = WritableMemory.wrap(bytearray0);
    LongsSketch new_sketch0 = LongsSketch.getInstance(mem0);
    String str0 = LongsSketch.toString(mem0);
    println(str0);
    String string0 = sketch.serializeToString();
    String new_string0 = new_sketch0.serializeToString();
    Assert.assertTrue(string0.equals(new_string0));

    LongsSketch sketch2 = new LongsSketch(128);
    sketch.update(10, 100);
    sketch.update(10, 100);
    sketch.update(15, 3443);
    sketch.update(1000001, 1010230);
    sketch.update(1000002, 1010230);

    byte[] bytearray1 = sketch.toByteArray();
    Memory mem1 = Memory.wrap(bytearray1);
    LongsSketch new_sketch1 = LongsSketch.getInstance(mem1);
    String str1 = LongsSketch.toString(bytearray1);
    println(str1);
    String string1 = sketch.serializeToString();
    String new_string1 = new_sketch1.serializeToString();
    Assert.assertTrue(string1.equals(new_string1));
    Assert.assertTrue(new_sketch1.getMaximumMapCapacity() == sketch.getMaximumMapCapacity());
    Assert.assertTrue(new_sketch1.getCurrentMapCapacity() == sketch.getCurrentMapCapacity());

    sketch2.update(190, 12902390);
    sketch2.update(191, 12902390);
    sketch2.update(192, 12902390);
    sketch2.update(193, 12902390);
    sketch2.update(194, 12902390);
    sketch2.update(195, 12902390);
    sketch2.update(196, 12902390);
    sketch2.update(197, 12902390);
    sketch2.update(198, 12902390);
    sketch2.update(199, 12902390);
    sketch2.update(200, 12902390);
    sketch2.update(201, 12902390);
    sketch2.update(202, 12902390);
    sketch2.update(203, 12902390);
    sketch2.update(204, 12902390);
    sketch2.update(205, 12902390);
    sketch2.update(206, 12902390);
    sketch2.update(207, 12902390);
    sketch2.update(208, 12902390);

    byte[] bytearray2 = sketch2.toByteArray();
    Memory mem2 = Memory.wrap(bytearray2);
    LongsSketch new_sketch2 = LongsSketch.getInstance(mem2);

    String string2 = sketch2.serializeToString();
    String new_string2 = new_sketch2.serializeToString();

    Assert.assertTrue(string2.equals(new_string2));
    Assert.assertTrue(new_sketch2.getMaximumMapCapacity() == sketch2.getMaximumMapCapacity());
    Assert.assertTrue(new_sketch2.getCurrentMapCapacity() == sketch2.getCurrentMapCapacity());
    Assert.assertTrue(new_sketch2.getStreamLength() == sketch2.getStreamLength());

    LongsSketch merged_sketch = sketch.merge(sketch2);

    byte[] bytearray = sketch.toByteArray();
    Memory mem = Memory.wrap(bytearray);
    LongsSketch new_sketch = LongsSketch.getInstance(mem);

    String string = sketch.serializeToString();
    String new_string = new_sketch.serializeToString();

    Assert.assertTrue(string.equals(new_string));
    Assert.assertTrue(new_sketch.getMaximumMapCapacity() == merged_sketch.getMaximumMapCapacity());
    Assert.assertTrue(new_sketch.getCurrentMapCapacity() == merged_sketch.getCurrentMapCapacity());
    Assert.assertTrue(new_sketch.getStreamLength() == merged_sketch.getStreamLength());
  }

  @Test
  public void frequentItemsByteResetAndEmptySerialTest() {
    LongsSketch sketch = new LongsSketch(16);
    sketch.update(10, 100);
    sketch.update(10, 100);
    sketch.update(15, 3443);
    sketch.update(1000001, 1010230);
    sketch.update(1000002, 1010230);
    sketch.reset();

    byte[] bytearray0 = sketch.toByteArray();
    Memory mem0 = Memory.wrap(bytearray0);
    LongsSketch new_sketch0 = LongsSketch.getInstance(mem0);

    String string0 = sketch.serializeToString();
    String new_string0 = new_sketch0.serializeToString();
    Assert.assertTrue(string0.equals(new_string0));
    Assert.assertTrue(new_sketch0.getMaximumMapCapacity() == sketch.getMaximumMapCapacity());
    Assert.assertTrue(new_sketch0.getCurrentMapCapacity() == sketch.getCurrentMapCapacity());
  }

  @Test
  public void checkFreqLongsMemSerDe() {
    int minSize = 1 << LG_MIN_MAP_SIZE;
    LongsSketch sk1 = new LongsSketch(minSize);
    sk1.update(10, 100);
    sk1.update(10, 100);
    sk1.update(15, 3443); println(sk1.toString());
    sk1.update(1000001, 1010230); println(sk1.toString());
    sk1.update(1000002, 1010230); println(sk1.toString());

    byte[] bytearray0 = sk1.toByteArray();
    Memory mem0 = Memory.wrap(bytearray0);
    LongsSketch sk2 = LongsSketch.getInstance(mem0);

    checkEquality(sk1, sk2);
  }

  @Test
  public void checkFreqLongsStringSerDe() {
    int minSize = 1 << LG_MIN_MAP_SIZE;
    LongsSketch sk1 = new LongsSketch(minSize);
    sk1.update(10, 100);
    sk1.update(10, 100);
    sk1.update(15, 3443);
    sk1.update(1000001, 1010230);
    sk1.update(1000002, 1010230);

    String string1 = sk1.serializeToString();
    LongsSketch sk2 = LongsSketch.getInstance(string1);

    checkEquality(sk1, sk2);
  }

  private static void checkEquality(LongsSketch sk1, LongsSketch sk2) {
    assertEquals(sk1.getNumActiveItems(), sk2.getNumActiveItems());
    assertEquals(sk1.getCurrentMapCapacity(), sk2.getCurrentMapCapacity());
    assertEquals(sk1.getMaximumError(), sk2.getMaximumError());
    assertEquals(sk1.getMaximumMapCapacity(), sk2.getMaximumMapCapacity());
    assertEquals(sk1.getStorageBytes(), sk2.getStorageBytes());
    assertEquals(sk1.getStreamLength(), sk2.getStreamLength());
    assertEquals(sk1.isEmpty(), sk2.isEmpty());

    ErrorType NFN = ErrorType.NO_FALSE_NEGATIVES;
    ErrorType NFP = ErrorType.NO_FALSE_POSITIVES;
    Row[] rowArr1 = sk1.getFrequentItems(NFN);
    Row[] rowArr2 = sk2.getFrequentItems(NFN);
    assertEquals(sk1.getFrequentItems(NFN).length, sk2.getFrequentItems(NFN).length);
    for (int i=0; i<rowArr1.length; i++) {
      String s1 = rowArr1[i].toString();
      String s2 = rowArr2[i].toString();
      assertEquals(s1, s2);
    }
    rowArr1 = sk1.getFrequentItems(NFP);
    rowArr2 = sk2.getFrequentItems(NFP);
    assertEquals(sk1.getFrequentItems(NFP).length, sk2.getFrequentItems(NFP).length);
    for (int i=0; i<rowArr1.length; i++) {
      String s1 = rowArr1[i].toString();
      String s2 = rowArr2[i].toString();
      assertEquals(s1, s2);
    }
  }

  @Test
  public void checkFreqLongsMemDeSerExceptions() {
    int minSize = 1 << LG_MIN_MAP_SIZE;
    LongsSketch sk1 = new LongsSketch(minSize);
    sk1.update(1L);

    byte[] bytearray0 = sk1.toByteArray();
    WritableMemory mem = WritableMemory.wrap(bytearray0);
    long pre0 = mem.getLong(0);

    tryBadMem(mem, PREAMBLE_LONGS_BYTE, 2); //Corrupt
    mem.putLong(0, pre0); //restore

    tryBadMem(mem, SER_VER_BYTE, 2); //Corrupt
    mem.putLong(0, pre0); //restore

    tryBadMem(mem, FAMILY_BYTE, 2); //Corrupt
    mem.putLong(0, pre0); //restore

    tryBadMem(mem, FLAGS_BYTE, 4); //Corrupt to true
    mem.putLong(0, pre0); //restore
  }

  private static void tryBadMem(WritableMemory mem, int byteOffset, int byteValue) {
    try {
      mem.putByte(byteOffset, (byte) byteValue); //Corrupt
      LongsSketch.getInstance(mem);
      fail();
    } catch (SketchesArgumentException e) {
      //expected
    }
  }

  @Test
  public void checkFreqLongsStringDeSerExceptions() {
    //FrequentLongsSketch sk1 = new FrequentLongsSketch(8);
    //String str1 = sk1.serializeToString();
    //String correct   = "1,10,2,4,0,0,0,4,";

    tryBadString("2,10,2,4,0,0,0,4,"); //bad SerVer of 2
    tryBadString("1,10,2,0,0,0,0,4,"); //bad empty of 0
    tryBadString(  "1,10,2,4,0,0,0,4,0,"); //one extra
  }

  private static void tryBadString(String badString) {
    try {
      LongsSketch.getInstance(badString);
      fail("Should have thrown SketchesArgumentException");
    } catch (SketchesArgumentException e) {
      //expected
    }
  }

  @Test
  public void checkFreqLongs(){
    int numSketches = 1;
    int n = 2222;
    double error_tolerance = 1.0/100;

    LongsSketch[] sketches = new LongsSketch[numSketches];
    for (int h = 0; h < numSketches; h++) {
      sketches[h] = newFrequencySketch(error_tolerance);
    }

    long item;
    double prob = .001;
    for (int i = 0; i < n; i++) {
      item = randomGeometricDist(prob) + 1;
      for (int h = 0; h < numSketches; h++) {
        sketches[h].update(item);
      }
    }

    for (int h = 0; h < numSketches; h++) {
      long threshold = sketches[h].getMaximumError();
      Row[] rows = sketches[h].getFrequentItems(ErrorType.NO_FALSE_NEGATIVES);
      for (int i = 0; i < rows.length; i++) {
        Assert.assertTrue(rows[i].getUpperBound() > threshold);
      }

      rows = sketches[h].getFrequentItems(ErrorType.NO_FALSE_POSITIVES);
      for (int i = 0; i < rows.length; i++) {
        Assert.assertTrue(rows[i].getLowerBound() > threshold);
      }

      rows = sketches[h].getFrequentItems(Long.MAX_VALUE, ErrorType.NO_FALSE_POSITIVES);
      Assert.assertEquals(rows.length, 0);
    }
  }

  @Test
  public void updateOneTime() {
    int size = 100;
    double error_tolerance = 1.0 / size;
    //double delta = .01;
    int numSketches = 1;
    for (int h = 0; h < numSketches; h++) {
      LongsSketch sketch = newFrequencySketch(error_tolerance);
      Assert.assertEquals(sketch.getUpperBound(13L), 0);
      Assert.assertEquals(sketch.getLowerBound(13L), 0);
      Assert.assertEquals(sketch.getMaximumError(), 0);
      Assert.assertEquals(sketch.getEstimate(13L), 0);
      sketch.update(13L);
      // Assert.assertEquals(sketch.getEstimate(13L), 1);
    }
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkGetInstanceMemory() {
    WritableMemory mem = WritableMemory.wrap(new byte[4]);
    LongsSketch.getInstance(mem);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkGetInstanceString() {
    String s = "";
    LongsSketch.getInstance(s);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkUpdateNegative() {
    int minSize = 1 << LG_MIN_MAP_SIZE;
    LongsSketch fls = new LongsSketch(minSize);
    fls.update(1, 0);
    fls.update(1, -1);
  }

  @SuppressWarnings("unlikely-arg-type")
  @Test
  public void checkGetFrequentItems1() {
    int minSize = 1 << LG_MIN_MAP_SIZE;
    LongsSketch fis = new LongsSketch(minSize);
    fis.update(1);
    Row[] rowArr = fis.getFrequentItems(ErrorType.NO_FALSE_POSITIVES);
    Row row = rowArr[0];
    assertTrue(row.hashCode() != 0);
    assertTrue(row.equals(row));
    assertFalse(row.equals(fis));
    assertNotNull(row);
    assertEquals(row.est, 1L);
    assertEquals(row.item, 1L);
    assertEquals(row.lb, 1L);
    assertEquals(row.ub, 1L);
    Row newRow = new Row(row.item, row.est+1, row.ub, row.lb);
    assertFalse(row.equals(newRow));
    newRow = new Row(row.item, row.est, row.ub, row.lb);
    assertTrue(row.equals(newRow));

  }

  @Test
  public void checkGetStorageBytes() {
    int minSize = 1 << LG_MIN_MAP_SIZE;
    LongsSketch fls = new LongsSketch(minSize);
    assertEquals(fls.toByteArray().length, fls.getStorageBytes());
    fls.update(1);
    assertEquals(fls.toByteArray().length, fls.getStorageBytes());
  }

  @Test
  public void checkDeSerFromStringArray() {
    int minSize = 1 << LG_MIN_MAP_SIZE;
    LongsSketch fls = new LongsSketch(minSize);
    String ser = fls.serializeToString();
    println(ser);
    fls.update(1);
    ser = fls.serializeToString();
    println(ser);
  }

  @Test
  public void checkMerge() {
    int minSize = 1 << LG_MIN_MAP_SIZE;
    LongsSketch fls1 = new LongsSketch(minSize);
    LongsSketch fls2 = null;
    LongsSketch fle = fls1.merge(fls2);
    assertTrue(fle.isEmpty());

    fls2 = new LongsSketch(minSize);
    fle = fls1.merge(fls2);
    assertTrue(fle.isEmpty());
  }

  @Test
  public void checkSortItems() {
    int numSketches = 1;
    int n = 2222;
    double error_tolerance = 1.0/100;
    int sketchSize = Util.ceilingPowerOf2((int) (1.0 /(error_tolerance*ReversePurgeLongHashMap.getLoadFactor())));
    //println("sketchSize: "+sketchSize);

    LongsSketch[] sketches = new LongsSketch[numSketches];
    for (int h = 0; h < numSketches; h++) {
      sketches[h] = new LongsSketch(sketchSize);
    }

    long item;
    double prob = .001;
    for (int i = 0; i < n; i++) {
      item = randomGeometricDist(prob) + 1;
      for (int h = 0; h < numSketches; h++) {
        sketches[h].update(item);
      }
    }

    for(int h=0; h<numSketches; h++) {
      long threshold = sketches[h].getMaximumError();
      Row[] rows = sketches[h].getFrequentItems(ErrorType.NO_FALSE_NEGATIVES);
      //println("ROWS: "+rows.length);
      for (int i = 0; i < rows.length; i++) {
        Assert.assertTrue(rows[i].ub > threshold);
      }
      Row first = rows[0];
      long anItem = first.getItem();
      long anEst  = first.getEstimate();
      long aLB    = first.getLowerBound();
      String s = first.toString();
      println(s);
      assertTrue(anEst >= 0);
      assertTrue(aLB >= 0);
      assertEquals(anItem, anItem); //dummy test
    }
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkGetAndCheckPreLongs() {
    byte[] byteArr = new byte[8];
    byteArr[0] = (byte) 2;
    PreambleUtil.checkPreambleSize(Memory.wrap(byteArr));
  }

  @Test
  public void checkToString1() {
    int size = 1 << LG_MIN_MAP_SIZE;
    printSketch(size, new long[] {1, 1, 1, 1, 1, 1, 1, 2, 3, 4, 5});
    printSketch(size, new long[] {5, 4, 3, 2, 1, 1, 1, 1, 1, 1, 1});
  }

  @Test
  public void checkStringDeserEmptyNotCorrupt() {
    int size = 1 << LG_MIN_MAP_SIZE;
    int thresh = (size * 3) / 4;
    String fmt = "%6d%10s%s";
    LongsSketch fls = new LongsSketch(size);
    println("Sketch Size: " + size);
    String s = null;
    int i = 0;
    for ( ; i <= thresh; i++) {
      fls.update(i+1, 1);
      s = fls.serializeToString();
      println(String.format("SER   " + fmt, (i + 1), fls.isEmpty() + " : ", s ));
      LongsSketch fls2 = LongsSketch.getInstance(s);
      println(String.format("DESER " + fmt, (i + 1), fls2.isEmpty() + " : ", s ));
    }
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkStringDeserEmptyCorrupt() {
    String s = "1,"  //serVer
             + "10," //FamID
             + "3,"  //lgMaxMapSz
             + "0,"  //Empty Flag = false ... corrupted, should be true
             + "7,"  //stream Len so far
             + "1,"  //error offset
             + "0,"  //numActive ...conflict with empty
             + "8,"; //curMapLen
    LongsSketch.getInstance(s);
  }

  @Test
  public void checkGetEpsilon() {
    assertEquals(LongsSketch.getEpsilon(1024), 3.5 / 1024, 0.0);
  }

  @Test
  public void checkGetAprioriError() {
    double eps = 3.5 / 1024;
    assertEquals(LongsSketch.getAprioriError(1024, 10_000), eps * 10_000);
  }

  @Test
  public void printlnTest() {
    println("PRINTING: " + this.getClass().getName());
  }

  //Restricted methods

  public void printSketch(int size, long[] freqArr) {
    LongsSketch fls = new LongsSketch(size);
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i<freqArr.length; i++) {
      fls.update(i+1, freqArr[i]);
    }
    sb.append("Sketch Size: "+size).append(LS);
    String s = fls.toString();
    sb.append(s);
    println(sb.toString());
    printRows(fls, ErrorType.NO_FALSE_NEGATIVES);
    println("");
    printRows(fls, ErrorType.NO_FALSE_POSITIVES);
    println("");
  }

  private static void printRows(LongsSketch fls, ErrorType eType) {
    Row[] rows = fls.getFrequentItems(eType);
    String s1 = eType.toString();
    println(s1);
    String hdr = Row.getRowHeader();
    println(hdr);
    for (int i=0; i<rows.length; i++) {
      Row row = rows[i];
      String s2 = row.toString();
      println(s2);
    }
    if (rows.length > 0) { //check equals null case
      Row nullRow = null;
      assertFalse(rows[0].equals(nullRow));
    }
  }

  /**
   * @param s value to print
   */
  static void println(String s) {
    //System.err.println(s); //disable here
  }

  private static LongsSketch newFrequencySketch(double eps) {
    double loadFactor = ReversePurgeLongHashMap.getLoadFactor();
    int maxMapSize = Util.ceilingPowerOf2((int) (1.0 /(eps*loadFactor)));
    return new LongsSketch(maxMapSize);
  }

}
