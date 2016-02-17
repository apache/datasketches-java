package com.yahoo.sketches.hll;

import com.yahoo.sketches.Util;
import com.yahoo.sketches.hash.MurmurHash3;

// This is a simplistic implementation for testing to produce a canonical representation of HLL Sketch

public class HllCanonical6Bit {
  byte[] table;

  public HllCanonical6Bit(int size) {
    table = new byte[size];
  }

  public void update(long[] input) {
    long[] hash = MurmurHash3.hash(input, Util.DEFAULT_UPDATE_SEED);
    int key = (int) hash[0] & (table.length - 1);
    byte value = (byte) (Long.numberOfLeadingZeros(hash[1]) + 1);
    if (table[key] < value) table[key] = value;
  }

  public byte[] getCanonicalRepresentation() {
    return table;
  }
}
