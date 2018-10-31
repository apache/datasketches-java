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
 * Checks sketch images obtained from C++.
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
      double est1 = sk.getEstimate();
      assertEquals(est1, 100, 100 * .02);
      for (int i = 0; i < 100; i++) { sk.update(i); }
      double est2 = sk.getEstimate();
      assertEquals(est2, est1, 0); //assert no change
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void checkHybridBin() {
    String fileName = "cpc-hybrid.bin";
    File file = new File(getClass().getClassLoader().getResource(fileName).getFile());
    try (MapHandle mh = Memory.map(file)) {
      Memory mem = mh.get();
      println("CPP GENERATED SKETCH FROM BINARY FILE LgK=11, U0 to U199");
      println("PreambleUtil.toString(mem, true)" + LS);
      println(PreambleUtil.toString(mem, true));

      println(LS + LS + "################");
      println("CpcSketch sk = CpcSketch.heapify(mem);");
      println("sk.toString(true)" + LS);
      CpcSketch sk = CpcSketch.heapify(mem);
      println(sk.toString(true));
      assertEquals(sk.getFlavor(), Flavor.HYBRID);
      double est1 = sk.getEstimate();
      assertEquals(est1, 200, 200 * .02);
      for (long i = 0; i < 200; i++) { sk.update(i); }
      double est2 = sk.getEstimate();
      assertEquals(est2, est1, 0); //assert no change
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void checkPinnedBin() {
    String fileName = "cpc-pinned.bin";
    File file = new File(getClass().getClassLoader().getResource(fileName).getFile());
    try (MapHandle mh = Memory.map(file)) {
      Memory mem = mh.get();
      println("CPP GENERATED SKETCH FROM BINARY FILE LgK=11, U0 to U1999");
      println("PreambleUtil.toString(mem, true)" + LS);
      println(PreambleUtil.toString(mem, true));

      println(LS + LS + "################");
      println("CpcSketch sk = CpcSketch.heapify(mem);");
      println("sk.toString(true)" + LS);
      CpcSketch sk = CpcSketch.heapify(mem);
      println(sk.toString(true));
      assertEquals(sk.getFlavor(), Flavor.PINNED);
      double est1 = sk.getEstimate();
      assertEquals(est1, 2000, 2000 * .02);
      for (long i = 0; i < 2000; i++) { sk.update(i); }
      double est2 = sk.getEstimate();
      assertEquals(est2, est1, 0); //assert no change
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void checkSlidingBin() {
    String fileName = "cpc-sliding.bin";
    File file = new File(getClass().getClassLoader().getResource(fileName).getFile());
    try (MapHandle mh = Memory.map(file)) {
      Memory mem = mh.get();
      println("CPP GENERATED SKETCH FROM BINARY FILE LgK=11, U0 to U19999");
      println("PreambleUtil.toString(mem, true)" + LS);
      println(PreambleUtil.toString(mem, true));

      println(LS + LS + "################");
      println("CpcSketch sk = CpcSketch.heapify(mem);");
      println("sk.toString(true)" + LS);
      CpcSketch sk = CpcSketch.heapify(mem);
      println(sk.toString(true));
      assertEquals(sk.getFlavor(), Flavor.SLIDING);
      double est1 = sk.getEstimate();
      assertEquals(est1, 20000, 20000 * .02);
      for (long i = 0; i < 20000; i++) { sk.update(i); }
      double est2 = sk.getEstimate();
      assertEquals(est2, est1, 0);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  //Image checks

  @Test
  public void checkEmptyImages() {
    String fileName = "cpc-empty.bin";
    File file = new File(getClass().getClassLoader().getResource(fileName).getFile());
    try (MapHandle mh = Memory.map(file)) {
      Memory mem = mh.get();
      int cap = (int) mem.getCapacity();
      byte[] memByteArr = new byte[cap];
      mem.getByteArray(0, memByteArr, 0, cap);

      CpcSketch sk = new CpcSketch(11);
      byte[] mem2ByteArr = sk.toByteArray();
      Memory mem2 = Memory.wrap(mem2ByteArr);
      assertEquals(mem.getCapacity(), mem2.getCapacity());
      assertEquals(memByteArr, mem2ByteArr);
    }catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void checkSparseImages() {
    String fileName = "cpc-sparse.bin";
    File file = new File(getClass().getClassLoader().getResource(fileName).getFile());
    try (MapHandle mh = Memory.map(file)) {
      Memory mem = mh.get();
      int cap = (int) mem.getCapacity();
      byte[] memByteArr = new byte[cap];
      mem.getByteArray(0, memByteArr, 0, cap);

      CpcSketch sk = new CpcSketch(11);
      for (int i = 0; i < 100; i++) { sk.update(i); }
      byte[] mem2ByteArr = sk.toByteArray();
      Memory mem2 = Memory.wrap(mem2ByteArr);
      assertEquals(mem.getCapacity(), mem2.getCapacity());
      assertEquals(memByteArr, mem2ByteArr);
    }catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void checkHybridImages() {
    String fileName = "cpc-hybrid.bin";
    File file = new File(getClass().getClassLoader().getResource(fileName).getFile());
    try (MapHandle mh = Memory.map(file)) {
      Memory mem = mh.get();
      int cap = (int) mem.getCapacity();
      byte[] memByteArr = new byte[cap];
      mem.getByteArray(0, memByteArr, 0, cap);

      CpcSketch sk = new CpcSketch(11);
      for (int i = 0; i < 200; i++) { sk.update(i); }
      byte[] mem2ByteArr = sk.toByteArray();
      Memory mem2 = Memory.wrap(mem2ByteArr);
      assertEquals(mem.getCapacity(), mem2.getCapacity());
      assertEquals(memByteArr, mem2ByteArr);
    }catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void checkPinnedImages() {
    String fileName = "cpc-pinned.bin";
    File file = new File(getClass().getClassLoader().getResource(fileName).getFile());
    try (MapHandle mh = Memory.map(file)) {
      Memory mem = mh.get();
      int cap = (int) mem.getCapacity();
      byte[] cppMemByteArr = new byte[cap];
      mem.getByteArray(0, cppMemByteArr, 0, cap);

      CpcSketch sk = new CpcSketch(11);
      for (int i = 0; i < 2000; i++) { sk.update(i); }
      byte[] javaMemByteArr = sk.toByteArray();
      Memory mem2 = Memory.wrap(javaMemByteArr);
      assertEquals(mem.getCapacity(), mem2.getCapacity());
      assertEquals(cppMemByteArr, javaMemByteArr);
    }catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void checkSlidingImages() {
    String fileName = "cpc-sliding.bin";
    File file = new File(getClass().getClassLoader().getResource(fileName).getFile());
    try (MapHandle mh = Memory.map(file)) {
      Memory mem = mh.get();
      int cap = (int) mem.getCapacity();
      byte[] memByteArr = new byte[cap];
      mem.getByteArray(0, memByteArr, 0, cap);

      CpcSketch sk = new CpcSketch(11);
      for (int i = 0; i < 20000; i++) { sk.update(i); }
      byte[] mem2ByteArr = sk.toByteArray();
      Memory mem2 = Memory.wrap(mem2ByteArr);
      assertEquals(mem.getCapacity(), mem2.getCapacity());
      assertEquals(memByteArr, mem2ByteArr);
    }catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test //Internal consistency check
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
    //ps.println(s); //disable here
  }


}
