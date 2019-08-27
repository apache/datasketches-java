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

package org.apache.datasketches.quantiles;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

import org.apache.datasketches.memory.WritableMemory;

@SuppressWarnings("javadoc")
public class DoublesSketchBuilderTest {

  @Test
  public void checkBuilder() {
    int k = 256; //default is 128
    DoublesSketchBuilder bldr = DoublesSketch.builder();
    bldr.setK(k);
    assertEquals(bldr.getK(), k); //confirms new k
    println(bldr.toString());
    int bytes = DoublesSketch.getUpdatableStorageBytes(k, 0);
    byte[] byteArr = new byte[bytes];
    WritableMemory mem = WritableMemory.wrap(byteArr);
    DoublesSketch ds = bldr.build(mem);
    assertTrue(ds.isDirect());
    println(bldr.toString());

    bldr = DoublesSketch.builder();
    assertEquals(bldr.getK(), PreambleUtil.DEFAULT_K);
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
