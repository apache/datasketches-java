/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.cpc;

import static org.testng.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;

import org.testng.annotations.Test;

import com.yahoo.memory.MapHandle;
import com.yahoo.memory.Memory;

/**
 * @author Lee Rhodes
 */
public class CpcCBinariesTest {
  static PrintStream ps = System.out;
  static final String LS = System.getProperty("line.separator");

  @Test
  public void checkEmptyBin() {
    String fileName = "cpc-empty.bin";
    File file = new File(getClass().getClassLoader().getResource(fileName).getFile());
    try (MapHandle mh = Memory.map(file)) {
      Memory wmem = mh.get();
      println(PreambleUtil.toString(wmem, true));
      CpcSketch sk = CpcSketch.heapify(wmem);
      assertEquals(sk.getFlavor(), Flavor.EMPTY);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void checkSparseBin() {
    String fileName = "cpc-sparse.bin";
    File file = new File(getClass().getClassLoader().getResource(fileName).getFile());
    try (MapHandle mh = Memory.map(file)) {
      Memory mem = mh.get();
      println("CPP GENERATED SKETCH FROM BINARY FILE LgK=11, U0 to U99");
      println("PreambleUtil.toString(mem, true)" + LS);
      println(PreambleUtil.toString(mem, true));

      println(LS + LS + "################");
      println("CpcSketch sk = CpcSketch.heapify(mem);");
      println("sk.toString(true)" + LS);
      CpcSketch sk = CpcSketch.heapify(mem);
      println(sk.toString(true));
      assertEquals(sk.getFlavor(), Flavor.SPARSE);
      double est = sk.getEstimate();
      assertEquals(est, 100, 100 * .02);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void genSparseSketch() {
    CpcSketch sk = new CpcSketch(11);
    for (int i = 0; i < 100; i++) { sk.update(i); }
    println("JAVA GENERATED SKETCH LgK=11, U0 to U99");
    println("sketch.toString(true);" + LS);
    println(sk.toString(true));

    println(LS + LS + "################");
    byte[] byteArray = sk.toByteArray();
    println("sketch.toByteArray();");
    println("PreambleUtil.toString(byteArray, true);" + LS);
    println(PreambleUtil.toString(byteArray, true));

    println(LS + LS + "################");
    println("CpcSketch sk2 = CpcSketch.heapify(byteArray);");
    println("sk2.toString(true);" + LS);
    CpcSketch sk2 = CpcSketch.heapify(byteArray);
    println(sk2.toString(true));
  }

  //@Test
  public void checkHybridBin() {
    String fileName = "cpc-hybrid.bin";
    File file = new File(getClass().getClassLoader().getResource(fileName).getFile());
    try (MapHandle mh = Memory.map(file)) {
      Memory wmem = mh.get();
      CpcSketch sk = CpcSketch.heapify(wmem);
      assertEquals(sk.getFlavor(), Flavor.HYBRID);
      assertEquals(sk.getEstimate(), 200, 200 * .02);
      for (long i = 0; i < 200; i++) { sk.update(i); }
      assertEquals(sk.getEstimate(), 200, 200 * .02);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void printlnTest() {
    println("PRINTING: " + this.getClass().getName());
  }

  /**
   * @param format the string to print
   * @param args the arguments
   */
  static void printf(String format, Object... args) {
    //ps.printf(format, args);
  }

  /**
   * @param s value to print
   */
  static void println(String s) {
    //System.out.println(s); //disable here
  }


}
