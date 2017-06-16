/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.quantiles;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.yahoo.memory.Memory;

public class ForwardCompatibilityTest {

  @Test
  //fullPath: sketches/src/test/resources/Qk128_n50_v0.3.0.bin
  //Median2: 26.0
  public void check030_50() {
    int n = 50;
    String ver = "0.3.0";
    double expected = 26;
    getAndCheck(ver, n, expected);
  }

  @Test
  //fullPath: sketches/src/test/resources/Qk128_n1000_v0.3.0.bin
  //Median2: 501.0
  public void check030_1000() {
    int n = 1000;
    String ver = "0.3.0";
    double expected = 501;
    getAndCheck(ver, n, expected);
  }

  @Test
  //fullPath: sketches/src/test/resources/Qk128_n50_v0.6.0.bin
  //Median2: 26.0
  public void check060_50() {
    int n = 50;
    String ver = "0.6.0";
    double expected = 26;
    getAndCheck(ver, n, expected);
  }

  @Test
  //fullPath: sketches/src/test/resources/Qk128_n1000_v0.6.0.bin
  //Median2: 501.0
  public void check060_1000() {
    int n = 1000;
    String ver = "0.6.0";
    double expected = 501;
    getAndCheck(ver, n, expected);
  }

  @Test
  //fullPath: sketches/src/test/resources/Qk128_n50_v0.8.0.bin
  //Median2: 26.0
  public void check080_50() {
    int n = 50;
    String ver = "0.8.0";
    double expected = 26;
    getAndCheck(ver, n, expected);
  }

  @Test
  //fullPath: sketches/src/test/resources/Qk128_n1000_v0.8.0.bin
  //Median2: 501.0
  public void check080_1000() {
    int n = 1000;
    String ver = "0.8.0";
    double expected = 501;
    getAndCheck(ver, n, expected);
  }

  @Test
  //fullPath: sketches/src/test/resources/Qk128_n50_v0.8.3.bin
  //Median2: 26.0
  public void check083_50() {
    int n = 50;
    String ver = "0.8.3";
    double expected = 26;
    getAndCheck(ver, n, expected);
  }

  @Test
  //fullPath: sketches/src/test/resources/Qk128_n1000_v0.8.0.bin
  //Median2: 501.0
  public void check083_1000() {
    int n = 1000;
    String ver = "0.8.3";
    double expected = 501;
    getAndCheck(ver, n, expected);
  }

  private void getAndCheck(String ver, int n, double quantile) {
    DoublesSketch.rand.setSeed(131); //make deterministic
    //create fileName
    int k = 128;
    double nf = 0.5;
    String fileName = String.format("Qk%d_n%d_v%s.bin", k, n, ver);
    println("fullName: "+ fileName);
    println("Old Median: " + quantile);
    //create & Read File
    File file = new File(getClass().getClassLoader().getResource(fileName).getFile());
    byte[] byteArr2 = readFile(file);
    Memory srcMem = Memory.wrap(byteArr2);

    // heapify as update sketch
    DoublesSketch qs2 = UpdateDoublesSketch.heapify(srcMem);
    //Test the quantile
    double q2 = qs2.getQuantile(nf);
    println("New Median: " + q2);
    Assert.assertEquals(q2, quantile, 0.0);

    // same thing with compact sketch
    qs2 = CompactDoublesSketch.heapify(srcMem);
    //Test the quantile
    q2 = qs2.getQuantile(nf);
    println("New Median: " + q2);
    Assert.assertEquals(q2, quantile, 0.0);
  }

  private static byte[] readFile(File file) {
    try ( FileInputStream streamIn = new FileInputStream(file) ) {
      byte[] byteArr = new byte[(int)file.length()];
      streamIn.read(byteArr);
      return byteArr;
    }
    catch (NullPointerException | IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void printlnTest() {
    println("PRINTING: "+this.getClass().getName());
  }

  /**
   * @param s value to print
   */
  static void println(String s) {
    //System.out.println(s); //disable here
  }

}
