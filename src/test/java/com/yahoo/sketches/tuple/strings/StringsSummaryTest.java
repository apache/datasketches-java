/*
 * Copyright 2019, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.tuple.strings;

import org.testng.annotations.Test;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.tuple.DeserializeResult;
import com.yahoo.sketches.tuple.strings.StringsSummary;

/**
 * @author Lee Rhodes
 */
public class StringsSummaryTest {

  @Test
  public void checkToByteArray() {
    String[] strArr =  new String[] {"abcd", "abcd", "abcd"};
    StringsSummary nsum = new StringsSummary(strArr);
    byte[] out = nsum.toByteArray();

    Memory mem = Memory.wrap(out);
    StringsSummary nsum2 = new StringsSummary(mem);
    String[] nodesArr = nsum2.getValue();
    for (String s : nodesArr) {
      println(s);
    }

    println("\nfromMemory(mem)");
    DeserializeResult<StringsSummary> dres = StringsSummary.fromMemory(mem);
    StringsSummary nsum3 = dres.getObject();
    nodesArr = nsum3.getValue();
    for (String s : nodesArr) {
      println(s);
    }
  }

  static void println(String s) { System.out.println(s); }

}
