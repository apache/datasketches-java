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
import static org.apache.datasketches.common.Util.LS;
import static org.apache.datasketches.frequencies.DistTest.randomGeometricDist;
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

import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.common.Util;
import org.apache.datasketches.frequencies.ErrorType;
import org.apache.datasketches.frequencies.LongsSketch;
import org.apache.datasketches.frequencies.PreambleUtil;
import org.apache.datasketches.frequencies.ReversePurgeLongHashMap;
import org.apache.datasketches.frequencies.LongsSketch.Row;

public class LongsSketchTest {

  @Test
  public void hashMapSerialTest() {
    final ReversePurgeLongHashMap map = new ReversePurgeLongHashMap(8);
    map.adjustOrPutValue(10, 15);
    map.adjustOrPutValue(10, 5);
    map.adjustOrPutValue(1, 1);
    map.adjustOrPutValue(2, 3);
    final String string = map.serializeToString();
    //println(string);
    //println(map.toString());
    final ReversePurgeLongHashMap new_map =
        ReversePurgeLongHashMap.getInstance(string);
    final String new_string = new_map.serializeToString();
    Assert.assertTrue(string.equals(new_string));
  }

  @Test
  public void frequentItemsStringSerialTest() {
    final LongsSketch sketch = new LongsSketch(8);
    final LongsSketch sketch2 = new LongsSketch(128);
    sketch.update(10, 100);
    sketch.update(10, 100);
    sketch.update(15, 3443);
    sketch.update(1000001, 1010230);
    sketch.update(1000002, 1010230);

    final String string0 = sketch.serializeToString();
    final LongsSketch new_sketch0 = LongsSketch.getInstance(string0);
    final String new_string0 = new_sketch0.serializeToString();
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

    final String string2 = sketch2.serializeToString();
    final LongsSketch new_sketch2 = LongsSketch.getInstance(string2);
    final String new_string2 = new_sketch2.serializeToString();
    Assert.assertTrue(string2.equals(new_string2));
    Assert.assertTrue(new_sketch2.getMaximumMapCapacity() == sketch2.getMaximumMapCapacity());
    Assert.assertTrue(new_sketch2.getCurrentMapCapacity() == sketch2.getCurrentMapCapacity());
    Assert.assertTrue(new_sketch2.getStreamLength() == sketch2.getStreamLength());

    final LongsSketch merged_sketch = sketch.merge(sketch2);

    final String string = merged_sketch.serializeToString();
    final LongsSketch new_sketch = LongsSketch.getInstance(string);
    final String new_string = new_sketch.serializeToString();
    Assert.assertTrue(string.equals(new_string));
    Assert.assertTrue(new_sketch.getMaximumMapCapacity() == merged_sketch.getMaximumMapCapacity());
    Assert.assertTrue(new_sketch.getCurrentMapCapacity() == merged_sketch.getCurrentMapCapacity());
    Assert.assertTrue(new_sketch.getStreamLength() == merged_sketch.getStreamLength());
  }

  @Test
  public void frequentItemsByteSerialTest() {
    //Empty Sketch
    final LongsSketch sketch = new LongsSketch(16);
    final byte[] bytearray0 = sketch.toByteArray();
    final MemorySegment seg0 = MemorySegment.ofArray(bytearray0);
    final LongsSketch new_sketch0 = LongsSketch.getInstance(seg0);
    final String str0 = LongsSketch.toString(seg0);
    println(str0);
    final String string0 = sketch.serializeToString();
    final String new_string0 = new_sketch0.serializeToString();
    Assert.assertTrue(string0.equals(new_string0));

    final LongsSketch sketch2 = new LongsSketch(128);
    sketch.update(10, 100);
    sketch.update(10, 100);
    sketch.update(15, 3443);
    sketch.update(1000001, 1010230);
    sketch.update(1000002, 1010230);

    final byte[] bytearray1 = sketch.toByteArray();
    final MemorySegment seg1 = MemorySegment.ofArray(bytearray1);
    final LongsSketch new_sketch1 = LongsSketch.getInstance(seg1);
    final String str1 = LongsSketch.toString(bytearray1);
    println(str1);
    final String string1 = sketch.serializeToString();
    final String new_string1 = new_sketch1.serializeToString();
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

    final byte[] bytearray2 = sketch2.toByteArray();
    final MemorySegment seg2 = MemorySegment.ofArray(bytearray2);
    final LongsSketch new_sketch2 = LongsSketch.getInstance(seg2);

    final String string2 = sketch2.serializeToString();
    final String new_string2 = new_sketch2.serializeToString();

    Assert.assertTrue(string2.equals(new_string2));
    Assert.assertTrue(new_sketch2.getMaximumMapCapacity() == sketch2.getMaximumMapCapacity());
    Assert.assertTrue(new_sketch2.getCurrentMapCapacity() == sketch2.getCurrentMapCapacity());
    Assert.assertTrue(new_sketch2.getStreamLength() == sketch2.getStreamLength());

    final LongsSketch merged_sketch = sketch.merge(sketch2);

    final byte[] bytearray = sketch.toByteArray();
    final MemorySegment seg = MemorySegment.ofArray(bytearray);
    final LongsSketch new_sketch = LongsSketch.getInstance(seg);

    final String string = sketch.serializeToString();
    final String new_string = new_sketch.serializeToString();

    Assert.assertTrue(string.equals(new_string));
    Assert.assertTrue(new_sketch.getMaximumMapCapacity() == merged_sketch.getMaximumMapCapacity());
    Assert.assertTrue(new_sketch.getCurrentMapCapacity() == merged_sketch.getCurrentMapCapacity());
    Assert.assertTrue(new_sketch.getStreamLength() == merged_sketch.getStreamLength());
  }

  @Test
  public void frequentItemsByteResetAndEmptySerialTest() {
    final LongsSketch sketch = new LongsSketch(16);
    sketch.update(10, 100);
    sketch.update(10, 100);
    sketch.update(15, 3443);
    sketch.update(1000001, 1010230);
    sketch.update(1000002, 1010230);
    sketch.reset();

    final byte[] bytearray0 = sketch.toByteArray();
    final MemorySegment seg0 = MemorySegment.ofArray(bytearray0);
    final LongsSketch new_sketch0 = LongsSketch.getInstance(seg0);

    final String string0 = sketch.serializeToString();
    final String new_string0 = new_sketch0.serializeToString();
    Assert.assertTrue(string0.equals(new_string0));
    Assert.assertTrue(new_sketch0.getMaximumMapCapacity() == sketch.getMaximumMapCapacity());
    Assert.assertTrue(new_sketch0.getCurrentMapCapacity() == sketch.getCurrentMapCapacity());
  }

  @Test
  public void checkFreqLongsSegSerDe() {
    final int minSize = 1 << LG_MIN_MAP_SIZE;
    final LongsSketch sk1 = new LongsSketch(minSize);
    sk1.update(10, 100);
    sk1.update(10, 100);
    sk1.update(15, 3443); println(sk1.toString());
    sk1.update(1000001, 1010230); println(sk1.toString());
    sk1.update(1000002, 1010230); println(sk1.toString());

    final byte[] bytearray0 = sk1.toByteArray();
    final MemorySegment seg0 = MemorySegment.ofArray(bytearray0);
    final LongsSketch sk2 = LongsSketch.getInstance(seg0);

    checkEquality(sk1, sk2);
  }

  @Test
  public void checkFreqLongsStringSerDe() {
    final int minSize = 1 << LG_MIN_MAP_SIZE;
    final LongsSketch sk1 = new LongsSketch(minSize);
    sk1.update(10, 100);
    sk1.update(10, 100);
    sk1.update(15, 3443);
    sk1.update(1000001, 1010230);
    sk1.update(1000002, 1010230);

    final String string1 = sk1.serializeToString();
    final LongsSketch sk2 = LongsSketch.getInstance(string1);

    checkEquality(sk1, sk2);
  }

  private static void checkEquality(final LongsSketch sk1, final LongsSketch sk2) {
    assertEquals(sk1.getNumActiveItems(), sk2.getNumActiveItems());
    assertEquals(sk1.getCurrentMapCapacity(), sk2.getCurrentMapCapacity());
    assertEquals(sk1.getMaximumError(), sk2.getMaximumError());
    assertEquals(sk1.getMaximumMapCapacity(), sk2.getMaximumMapCapacity());
    assertEquals(sk1.getStorageBytes(), sk2.getStorageBytes());
    assertEquals(sk1.getStreamLength(), sk2.getStreamLength());
    assertEquals(sk1.isEmpty(), sk2.isEmpty());

    final ErrorType NFN = ErrorType.NO_FALSE_NEGATIVES;
    final ErrorType NFP = ErrorType.NO_FALSE_POSITIVES;
    Row[] rowArr1 = sk1.getFrequentItems(NFN);
    Row[] rowArr2 = sk2.getFrequentItems(NFN);
    assertEquals(sk1.getFrequentItems(NFN).length, sk2.getFrequentItems(NFN).length);
    for (int i=0; i<rowArr1.length; i++) {
      final String s1 = rowArr1[i].toString();
      final String s2 = rowArr2[i].toString();
      assertEquals(s1, s2);
    }
    rowArr1 = sk1.getFrequentItems(NFP);
    rowArr2 = sk2.getFrequentItems(NFP);
    assertEquals(sk1.getFrequentItems(NFP).length, sk2.getFrequentItems(NFP).length);
    for (int i=0; i<rowArr1.length; i++) {
      final String s1 = rowArr1[i].toString();
      final String s2 = rowArr2[i].toString();
      assertEquals(s1, s2);
    }
  }

  @Test
  public void checkFreqLongsSegDeSerExceptions() {
    final int minSize = 1 << LG_MIN_MAP_SIZE;
    final LongsSketch sk1 = new LongsSketch(minSize);
    sk1.update(1L);

    final byte[] bytearray0 = sk1.toByteArray();
    final MemorySegment seg = MemorySegment.ofArray(bytearray0);
    final long pre0 = seg.get(JAVA_LONG_UNALIGNED, 0);

    tryBadSeg(seg, PREAMBLE_LONGS_BYTE, 2); //Corrupt
    seg.set(JAVA_LONG_UNALIGNED, 0, pre0); //restore

    tryBadSeg(seg, SER_VER_BYTE, 2); //Corrupt
    seg.set(JAVA_LONG_UNALIGNED, 0, pre0); //restore

    tryBadSeg(seg, FAMILY_BYTE, 2); //Corrupt
    seg.set(JAVA_LONG_UNALIGNED, 0, pre0); //restore

    tryBadSeg(seg, FLAGS_BYTE, 4); //Corrupt to true
    seg.set(JAVA_LONG_UNALIGNED, 0, pre0); //restore
  }

  private static void tryBadSeg(final MemorySegment seg, final int byteOffset, final int byteValue) {
    try {
      seg.set(JAVA_BYTE, byteOffset, (byte) byteValue); //Corrupt
      LongsSketch.getInstance(seg);
      fail();
    } catch (final SketchesArgumentException e) {
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

  private static void tryBadString(final String badString) {
    try {
      LongsSketch.getInstance(badString);
      fail("Should have thrown SketchesArgumentException");
    } catch (final SketchesArgumentException e) {
      //expected
    }
  }

  @Test
  public void checkFreqLongs(){
    final int numSketches = 1;
    final int n = 2222;
    final double error_tolerance = 1.0/100;

    final LongsSketch[] sketches = new LongsSketch[numSketches];
    for (int h = 0; h < numSketches; h++) {
      sketches[h] = newFrequencySketch(error_tolerance);
    }

    long item;
    final double prob = .001;
    for (int i = 0; i < n; i++) {
      item = randomGeometricDist(prob) + 1;
      for (int h = 0; h < numSketches; h++) {
        sketches[h].update(item);
      }
    }

    for (int h = 0; h < numSketches; h++) {
      final long threshold = sketches[h].getMaximumError();
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
    final int size = 100;
    final double error_tolerance = 1.0 / size;
    //double delta = .01;
    final int numSketches = 1;
    for (int h = 0; h < numSketches; h++) {
      final LongsSketch sketch = newFrequencySketch(error_tolerance);
      Assert.assertEquals(sketch.getUpperBound(13L), 0);
      Assert.assertEquals(sketch.getLowerBound(13L), 0);
      Assert.assertEquals(sketch.getMaximumError(), 0);
      Assert.assertEquals(sketch.getEstimate(13L), 0);
      sketch.update(13L);
      // Assert.assertEquals(sketch.getEstimate(13L), 1);
    }
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkGetInstanceMemorySegment() {
    final MemorySegment seg = MemorySegment.ofArray(new byte[4]);
    LongsSketch.getInstance(seg);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkGetInstanceString() {
    final String s = "";
    LongsSketch.getInstance(s);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkUpdateNegative() {
    final int minSize = 1 << LG_MIN_MAP_SIZE;
    final LongsSketch fls = new LongsSketch(minSize);
    fls.update(1, 0);
    fls.update(1, -1);
  }

  @SuppressWarnings("unlikely-arg-type")
  @Test
  public void checkGetFrequentItems1() {
    final int minSize = 1 << LG_MIN_MAP_SIZE;
    final LongsSketch fis = new LongsSketch(minSize);
    fis.update(1);
    final Row[] rowArr = fis.getFrequentItems(ErrorType.NO_FALSE_POSITIVES);
    final Row row = rowArr[0];
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
    final int minSize = 1 << LG_MIN_MAP_SIZE;
    final LongsSketch fls = new LongsSketch(minSize);
    assertEquals(fls.toByteArray().length, fls.getStorageBytes());
    fls.update(1);
    assertEquals(fls.toByteArray().length, fls.getStorageBytes());
  }

  @Test
  public void checkDeSerFromStringArray() {
    final int minSize = 1 << LG_MIN_MAP_SIZE;
    final LongsSketch fls = new LongsSketch(minSize);
    String ser = fls.serializeToString();
    println(ser);
    fls.update(1);
    ser = fls.serializeToString();
    println(ser);
  }

  @Test
  public void checkMerge() {
    final int minSize = 1 << LG_MIN_MAP_SIZE;
    final LongsSketch fls1 = new LongsSketch(minSize);
    LongsSketch fls2 = null;
    LongsSketch fle = fls1.merge(fls2);
    assertTrue(fle.isEmpty());

    fls2 = new LongsSketch(minSize);
    fle = fls1.merge(fls2);
    assertTrue(fle.isEmpty());
  }

  @Test
  public void checkSortItems() {
    final int numSketches = 1;
    final int n = 2222;
    final double error_tolerance = 1.0/100;
    final int sketchSize = Util.ceilingPowerOf2((int) (1.0 /(error_tolerance*ReversePurgeLongHashMap.getLoadFactor())));
    //println("sketchSize: "+sketchSize);

    final LongsSketch[] sketches = new LongsSketch[numSketches];
    for (int h = 0; h < numSketches; h++) {
      sketches[h] = new LongsSketch(sketchSize);
    }

    long item;
    final double prob = .001;
    for (int i = 0; i < n; i++) {
      item = randomGeometricDist(prob) + 1;
      for (int h = 0; h < numSketches; h++) {
        sketches[h].update(item);
      }
    }

    for(int h=0; h<numSketches; h++) {
      final long threshold = sketches[h].getMaximumError();
      final Row[] rows = sketches[h].getFrequentItems(ErrorType.NO_FALSE_NEGATIVES);
      //println("ROWS: "+rows.length);
      for (int i = 0; i < rows.length; i++) {
        Assert.assertTrue(rows[i].ub > threshold);
      }
      final Row first = rows[0];
      final long anItem = first.getItem();
      final long anEst  = first.getEstimate();
      final long aLB    = first.getLowerBound();
      final String s = first.toString();
      println(s);
      assertTrue(anEst >= 0);
      assertTrue(aLB >= 0);
      assertEquals(anItem, anItem); //dummy test
    }
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkGetAndCheckPreLongs() {
    final byte[] byteArr = new byte[8];
    byteArr[0] = (byte) 2;
    PreambleUtil.checkPreambleSize(MemorySegment.ofArray(byteArr));
  }

  @Test
  public void checkToString1() {
    final int size = 1 << LG_MIN_MAP_SIZE;
    printSketch(size, new long[] {1, 1, 1, 1, 1, 1, 1, 2, 3, 4, 5});
    printSketch(size, new long[] {5, 4, 3, 2, 1, 1, 1, 1, 1, 1, 1});
  }

  @Test
  public void checkStringDeserEmptyNotCorrupt() {
    final int size = 1 << LG_MIN_MAP_SIZE;
    final int thresh = (size * 3) / 4;
    final String fmt = "%6d%10s%s";
    final LongsSketch fls = new LongsSketch(size);
    println("Sketch Size: " + size);
    String s = null;
    int i = 0;
    for ( ; i <= thresh; i++) {
      fls.update(i+1, 1);
      s = fls.serializeToString();
      println(String.format("SER   " + fmt, (i + 1), fls.isEmpty() + " : ", s ));
      final LongsSketch fls2 = LongsSketch.getInstance(s);
      println(String.format("DESER " + fmt, (i + 1), fls2.isEmpty() + " : ", s ));
    }
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkStringDeserEmptyCorrupt() {
    final String s = "1,"  //serVer
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
    try {
      LongsSketch.getEpsilon(1000);
    } catch (final SketchesArgumentException e) { }
  }

  @Test
  public void checkGetAprioriError() {
    final double eps = 3.5 / 1024;
    assertEquals(LongsSketch.getAprioriError(1024, 10_000), eps * 10_000);
  }

  @Test
  public void printlnTest() {
    println("PRINTING: " + this.getClass().getName());
  }

  //Restricted methods

  public void printSketch(final int size, final long[] freqArr) {
    final LongsSketch fls = new LongsSketch(size);
    final StringBuilder sb = new StringBuilder();
    for (int i = 0; i<freqArr.length; i++) {
      fls.update(i+1, freqArr[i]);
    }
    sb.append("Sketch Size: "+size).append(LS);
    final String s = fls.toString();
    sb.append(s);
    println(sb.toString());
    printRows(fls, ErrorType.NO_FALSE_NEGATIVES);
    println("");
    printRows(fls, ErrorType.NO_FALSE_POSITIVES);
    println("");
  }

  private static void printRows(final LongsSketch fls, final ErrorType eType) {
    final Row[] rows = fls.getFrequentItems(eType);
    final String s1 = eType.toString();
    println(s1);
    final String hdr = Row.getRowHeader();
    println(hdr);
    for (int i=0; i<rows.length; i++) {
      final Row row = rows[i];
      final String s2 = row.toString();
      println(s2);
    }
    if (rows.length > 0) { //check equals null case
      final Row nullRow = null;
      assertFalse(rows[0].equals(nullRow));
    }
  }

  /**
   * @param s value to print
   */
  static void println(final String s) {
    //System.err.println(s); //disable here
  }

  private static LongsSketch newFrequencySketch(final double eps) {
    final double loadFactor = ReversePurgeLongHashMap.getLoadFactor();
    final int maxMapSize = Util.ceilingPowerOf2((int) (1.0 /(eps*loadFactor)));
    return new LongsSketch(maxMapSize);
  }

}
