/*
 * Copyright 2019, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.tuple.strings;

import org.testng.annotations.Test;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.tuple.DeserializeResult;
import com.yahoo.sketches.tuple.strings.ArrayOfStringsSummary;

/**
 * @author Lee Rhodes
 */
public class StringsSummaryTest {

  @Test
  public void checkToByteArray() {
    String[] strArr =  new String[] {"abcd", "abcd", "abcd"};
    ArrayOfStringsSummary nsum = new ArrayOfStringsSummary(strArr);
    byte[] out = nsum.toByteArray();

    Memory mem = Memory.wrap(out);
    ArrayOfStringsSummary nsum2 = new ArrayOfStringsSummary(mem);
    String[] nodesArr = nsum2.getValue();
    for (String s : nodesArr) {
      println(s);
    }

    println("\nfromMemory(mem)");
    DeserializeResult<ArrayOfStringsSummary> dres = ArrayOfStringsSummary.fromMemory(mem);
    ArrayOfStringsSummary nsum3 = dres.getObject();
    nodesArr = nsum3.getValue();
    for (String s : nodesArr) {
      println(s);
    }
  }

  static void println(String s) { System.out.println(s); }

}
