/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.hll;

import static com.yahoo.sketches.hll.Hll6Array.get6Bit;
import static com.yahoo.sketches.hll.Hll6Array.put6Bit;
import static org.testng.Assert.assertEquals;

import com.yahoo.memory.WritableMemory;

/**
 * @author Lee Rhodes
 */
public class HllAlgosTest {

  //@Test
  public void listBytesPer6BitBkts() {
    for (int i = 1; i <= 47; i++) {
      println("Bkts: " + i + ", Bytes: " + byteArrSz(i));
    }
  }

  //@Test
  public void listStartEndShiftByte() {
    String header = String.format("%7s%7s%7s", "BktAdr", "Start", "Shift");
    println(header);
    for (int i = 0; i <= 11; i++) {
      String row = String.format("%7d%7d%7d", i, startByte(i), shift(i));
      println(row);
    }
  }

  //@Test
  public void checkBktWriteRead() {
    int bkts = 120;
    byte[] byteArr = new byte[byteArrSz(bkts)];
    println(""+byteArr.length);
    WritableMemory mem = WritableMemory.wrap(byteArr);
    //write
    for (int i = 0; i < bkts; i++) {
      put6Bit(mem, 0, i, (byte) (i & 0X3F));
    }

    //read
    for (int i = 0; i < bkts; i++) {
      byte b = (byte) get6Bit(mem, 0, i);
      assertEquals(b, i & 0X3F);
    }
  }

  static final int byteArrSz(final int numSlots) {
    return ((numSlots * 3) >> 2) + 1;
  }

  //For Hll6Array test only
  static final int startByte(final int slotIdx) {
    final int startBit = slotIdx * 6;
    return startBit / 8;
  }

  //for Hll6Array test only
  static final int shift(final int slotIdx) {
    final int startBit = slotIdx * 6;
    return (startBit % 8) & 0X7;
  }



  /**
   * @param s value to print
   */
  static void println(String s) {
    //System.out.println(s); //disable here
  }
}
