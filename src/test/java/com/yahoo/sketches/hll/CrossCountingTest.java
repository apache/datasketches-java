/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import static com.yahoo.sketches.hll.TgtHllType.HLL_4;
import static com.yahoo.sketches.hll.TgtHllType.HLL_6;
import static com.yahoo.sketches.hll.TgtHllType.HLL_8;
import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
@SuppressWarnings("unused")
public class CrossCountingTest {
  static final String LS = System.getProperty("line.separator");

  @Test
  public void crossCountingCheck() {
    int n = 10000; //16 & 1537
    int lgK = 14;
    boolean setOooFlag = false;

    HllSketch sk4 = buildSketch(n, lgK, HLL_4, setOooFlag);
    int s4csum = computeChecksum(sk4);
//    printSketchSummary(sk4, s4csum);
//    printSketchData(sk4);
//    printAuxData(sk4);

    int csum;

    HllSketch sk6 = buildSketch(n, lgK, HLL_6, setOooFlag);
    csum = computeChecksum(sk6);
    assertEquals(csum, s4csum);
//    printSketchSummary(sk6, csum);
//    printSketchData(sk6);

    HllSketch sk8 = buildSketch(n, lgK, HLL_8, setOooFlag);
    csum = computeChecksum(sk8);
    assertEquals(csum, s4csum);
//    printSketchSummary(sk8, csum);
//    printSketchData(sk8);

    //Conversions
//    println("\nConverted HLL_6 to HLL_4:");
    HllSketch sk6to4 = sk6.copyAs(HLL_4);
    csum = computeChecksum(sk6to4);
    assertEquals(csum, s4csum);
//    printSketchSummary(sk6to4, csum);
//    printSketchData(sk6to4);
//    printAuxData(sk4);

//    println("\nConverted HLL_8 to HLL_4:");
    HllSketch sk8to4 = sk8.copyAs(HLL_4);
    csum = computeChecksum(sk8to4);
    assertEquals(csum, s4csum);
//    printSketchSummary(sk8to4, csum);
//    printSketchData(sk8to4);
//    printAuxData(sk4);

//    println("\nConverted HLL_4 to HLL_6:");
    HllSketch sk4to6 = sk4.copyAs(HLL_6);
    csum = computeChecksum(sk4to6);
    assertEquals(csum, s4csum);
//    printSketchSummary(sk4to6, csum);
//    printSketchData(sk4to6);

//    println("\nConverted HLL_8 to HLL_6:");
    HllSketch sk8to6 = sk8.copyAs(HLL_6);
    csum = computeChecksum(sk8to6);
    assertEquals(csum, s4csum);
//    printSketchSummary(sk8to6, csum);
//    printSketchData(sk8to6);

//    println("\nConverted HLL_4 to HLL_8:");
    HllSketch sk4to8 = sk4.copyAs(HLL_8);
    csum = computeChecksum(sk4to8);
    assertEquals(csum, s4csum);
//    printSketchSummary(sk4to8, csum);
//    printSketchData(sk4to8);

//    println("\nConverted HLL_6 to HLL_8:");
    HllSketch sk6to8 = sk6.copyAs(HLL_8);
    csum = computeChecksum(sk6to8);
    assertEquals(csum, s4csum);
//    printSketchSummary(sk6to8, csum);
//    printSketchData(sk6to8);
  }

  private static HllSketch buildSketch(int n, int lgK, TgtHllType tgtHllType, boolean setOooFlag) {
    HllSketch sketch = new HllSketch(lgK, tgtHllType);
    for (int i = 0; i < n; i++) {
      sketch.update(i);
    }
    if (setOooFlag) { sketch.putOutOfOrderFlag(true); }
    return sketch;
  }

  private static int computeChecksum(HllSketch sketch) {
    PairIterator itr = sketch.getIterator();
    int checksum = 0;
    int key  = 0;
    while (itr.nextAll()) {
      checksum += itr.getPair();
      key = itr.getKey(); //dummy
    }
    return checksum;
  }


  private static void printSketchData(HllSketch sketch) {
    TgtHllType tgtHllType = sketch.getTgtHllType();
    println("Data for " + tgtHllType.toString());
    PairIterator itr = sketch.getIterator();
    println(itr.getHeader());
    while (itr.nextAll()) {
      println(itr.getString());
    }
  }

  private static void printAuxData(HllSketch sketch) {
    TgtHllType tgtHllType = sketch.getTgtHllType();
    assertEquals(tgtHllType, HLL_4);
    PairIterator auxItr = sketch.getAuxIterator();
    if (auxItr == null) {
      return;
    }
    println("Aux Data for " + tgtHllType.toString());
    println("\nAux Array:");
    println(auxItr.getHeader());
    while (auxItr.nextValid()) {
      println(auxItr.getString());
    }
  }

  private static void printSketchSummary(HllSketch sketch, int checksum) {
    String tgtHllTypeStr = sketch.getTgtHllType().toString();
    println("");
    println(tgtHllTypeStr + " Est: " + sketch.getEstimate());
    println(tgtHllTypeStr + " CurMin: " + sketch.getCurMin());
    println(tgtHllTypeStr + " NumAtCurMin: " + sketch.getNumAtCurMin());
    println(tgtHllTypeStr + " OooFlag    : " + sketch.isOutOfOrderFlag());
    println(tgtHllTypeStr + " CheckSum: " + checksum);
  }


  @Test
  public void printlnTest() {
    println("PRINTING: "+this.getClass().getName());
  }

  /**
   * @param s value to print
   */
  static void println(String s) {
    print(s + LS);
  }

  /**
   * @param s value to print
   */
  static void print(String s) {
    System.out.print(s); //disable here
  }
}
