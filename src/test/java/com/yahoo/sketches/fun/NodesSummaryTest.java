/*
 * Copyright 2019, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.fun;

import org.testng.annotations.Test;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.tuple.DeserializeResult;

/**
 * @author Lee Rhodes
 */
public class NodesSummaryTest {

  @Test
  public void checkToByteArray() {
    String[] strArr =  new String[] {"abcd", "abcd", "abcd"};
    NodesSummary nsum = new NodesSummary(strArr);
    byte[] out = nsum.toByteArray();

    Memory mem = Memory.wrap(out);
    NodesSummary nsum2 = new NodesSummary(mem);
    String[] nodesArr = nsum2.getValue();
    for (String s : nodesArr) {
      println(s);
    }

    println("\nfromMemory(mem)");
    DeserializeResult<NodesSummary> dres = NodesSummary.fromMemory(mem);
    NodesSummary nsum3 = dres.getObject();
    nodesArr = nsum3.getValue();
    for (String s : nodesArr) {
      println(s);
    }
  }

  static void println(String s) { System.out.println(s); }

}
