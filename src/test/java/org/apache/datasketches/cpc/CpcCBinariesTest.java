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

package org.apache.datasketches.cpc;

import static org.apache.datasketches.Util.getResourceFile;
import static org.testng.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;

import org.apache.datasketches.memory.MapHandle;
import org.apache.datasketches.memory.Memory;
import org.testng.annotations.Test;

/**
 * Checks sketch images obtained from C++.
 * @author Lee Rhodes
 */
@SuppressWarnings("javadoc")
public class CpcCBinariesTest {
  static PrintStream ps = System.out;
  static final String LS = System.getProperty("line.separator");

  @Test
  public void checkEmptyBin() {
    String fileName = "cpc-empty.bin";
    File file = getResourceFile(fileName);
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
    File file = getResourceFile(fileName);
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
    File file = getResourceFile(fileName);
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
    File file = getResourceFile(fileName);
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
    File file = getResourceFile(fileName);
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
    File file = getResourceFile(fileName);
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
    File file = getResourceFile(fileName);
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
    File file = getResourceFile(fileName);
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
    File file = getResourceFile(fileName);
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
    File file = getResourceFile(fileName);
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
