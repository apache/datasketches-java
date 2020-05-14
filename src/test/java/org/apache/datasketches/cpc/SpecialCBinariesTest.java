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
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteOrder;
import java.nio.file.Files;

import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.memory.MapHandle;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMapHandle;
import org.apache.datasketches.memory.WritableMemory;
import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
@SuppressWarnings("javadoc")
public class SpecialCBinariesTest {
  static PrintStream ps = System.out;
  static final String LS = System.getProperty("line.separator");

  @Test
  @SuppressWarnings("unused")
  public void checkCpc10mBin() {
    String fileName = "cpc-10m.sk";
    File file = getResourceFile(fileName);
    try (MapHandle mh = Memory.map(file)) {
      Memory mem = mh.get();
      try {
        CpcSketch sk = CpcSketch.heapify(mem);
      } catch (SketchesArgumentException e) {} // Image was truncated by 4 bytes
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  //@Test
  public void checkFranksString() {
    String hex = "06011006001ACC938E000000010000000100000009000000"
        + "C0284BC1E001763B471D75617B0770CC9488E8DEE445D88A9347E97778C4A83E010600000D010000";
    int len = hex.length();
    byte[] byteArr = new byte[len/2];
    for (int i = 0; i < (len/2); i++) {
      String subStr = hex.substring(2*i, (2*i) + 2);
      byteArr[i] = (byte) (Integer.parseInt(subStr, 16) & 0XFF);
    }
    println(CpcSketch.toString(byteArr, true));
    CpcSketch sk = CpcSketch.heapify(byteArr);
    assertTrue(sk.validate());
    println(sk.toString(true));
    println("Est: " + sk.getEstimate());
    try {
      //byteArrToFile(byteArr, "FranksFile.sk");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  static void byteArrToFile(byte[] byteArr, String fileName) throws Exception {
    String userDir = System.getProperty("user.dir");
    String fullPathName = userDir + "/src/test/resources/" + fileName;
    File file = new File(fullPathName);
    if (file.exists()) { Files.delete(file.toPath()); }
    assertTrue(file.createNewFile());
    assertTrue(file.setWritable(true, false));
    assertTrue(file.isFile());

    try (WritableMapHandle wmh
        = WritableMemory.map(file, 0, byteArr.length, ByteOrder.nativeOrder())) {
      WritableMemory wmem = wmh.get();
      wmem.putByteArray(0, byteArr, 0, byteArr.length);
      wmh.force();
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
    //ps.println(s); //disable here
  }

}
