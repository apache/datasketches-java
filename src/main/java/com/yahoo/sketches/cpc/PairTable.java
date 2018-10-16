/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.cpc;

import static com.yahoo.sketches.cpc.RuntimeAsserts.rtAssertEquals;

import java.util.Arrays;

import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.SketchesStateException;

/**
 * Note: Definition of
 * <a href="{@docRoot}/resources/dictionary.html#SnowPlow">Snow Plow Effect</a>.
 * @author Lee Rhodes
 * @author Kevin Lang
 */
final class PairTable {
  private static final String LS = System.getProperty("line.separator");
  private static final int upsizeNumer = 3;
  private static final int upsizeDenom = 4;
  private static final int downsizeNumer = 1;
  private static final int downsizeDenom = 4;

  private int lgSize;
  private int validBits;
  private int numPairs;
  private int[] slots;

  PairTable(final int lgSize, final int numValidBits) {
    checkLgSize(lgSize);
    this.lgSize = lgSize;
    final int numSlots = 1 << lgSize;
    validBits = numValidBits;
    numPairs = 0;
    slots = new int[numSlots];
    for (int i = 0; i < numSlots; i++) { slots[i] = -1; }
  }

  //Factory
  static PairTable newInstanceFromPairsArray(final int[] pairs, final int numPairs, final int lgK) {
    int lgNumSlots = 2;
    while ((upsizeDenom * numPairs) > (upsizeNumer * (1 << lgNumSlots))) {
      lgNumSlots++;
    }
    final PairTable table = new PairTable(lgNumSlots, 6 + lgK);

    /* Note: there is a possible
     * <a href="{@docRoot}/resources/dictionary.html#SnowPlow">Snow Plow Effect</a> here because
     * the caller is passing in a sorted pairs array. However, we are starting out with the correct
     * final table size, so the problem is not likely to occur.
    */

    for (int i = 0; i < numPairs; i++) {
      mustInsert(table, pairs[i]);
    }
    table.numPairs = numPairs;
    return table;
  }

  PairTable clear() {
    Arrays.fill(slots, -1);
    numPairs = 0;
    return this;
  }

  PairTable copy() {
    final PairTable copy = new PairTable(lgSize, validBits);
    copy.numPairs = numPairs;
    copy.slots = slots.clone(); //slots can never be null
    return copy;
  }

  int getLgSize() {
    return lgSize;
  }

  int getNumPairs() {
    return numPairs;
  }

  int[] getSlots() {
    return slots;
  }

  int getValidBits() {
    return validBits;
  }

  /**
   * Rebuilds to a larger size. NumItems and validBits remain unchanged.
   * @param newLgSize the new size
   * @return a larger PairTable
   */
  PairTable rebuild(final int newLgSize) {
    checkLgSize(newLgSize);
    final int newSize = 1 << newLgSize;
    final int oldSize = 1 << lgSize;
    assert newSize > numPairs;
    final int[] oldSlots = slots;
    slots = new int[newSize];
    Arrays.fill(slots, -1);
    lgSize = newLgSize;
    for (int i = 0; i < oldSize; i++) {
      final int item = oldSlots[i];
      if (item != -1) { mustInsert(this, item); }
    }
    return this;
  }

  @Override
  public String toString() {
    return PairTable.toString(this, false);
  }

  private static void mustInsert(final PairTable table, final int item) {
    //SHARED CODE (implemented as a macro in C and expanded here)
    final int lgSize = table.lgSize;
    final int tableSize = 1 << lgSize;
    final int mask = tableSize - 1;
    final int shift = table.validBits - lgSize;
    int probe = item >>> shift; //extract high tablesize bits
    assert (probe >= 0) && (probe <= mask);
    final int[] arr = table.slots;
    int fetched = arr[probe];
    while ((fetched != item) && (fetched != -1)) {
      probe = (probe + 1) & mask;
      fetched = arr[probe];
    }
    //END SHARED CODE
    if (fetched == item) { throw new SketchesStateException("u32TableMustInsert failed"); }
    else {
      assert (fetched == -1);
      arr[probe] = item;
      // counts and resizing must be handled by the caller.
    }
  }

  static boolean maybeInsert(final PairTable table, final int item) {
    //SHARED CODE (implemented as a macro in C and expanded here)
    final int lgSize = table.lgSize;
    final int tableSize = 1 << lgSize;
    final int mask = tableSize - 1;
    final int shift = table.validBits - lgSize;
    int probe = item >>> shift;
    assert (probe >= 0) && (probe <= mask);
    final int[] arr = table.slots;
    int fetched = arr[probe];
    while ((fetched != item) && (fetched != -1)) {
      probe = (probe + 1) & mask;
      fetched = arr[probe];
    }
    //END SHARED CODE
    if (fetched == item) { return false; }
    else {
      assert (fetched == -1);
      arr[probe] = item;
      table.numPairs += 1;
      while ((upsizeDenom * table.numPairs) > (upsizeNumer * (1 << table.lgSize))) {
        table.rebuild(table.lgSize + 1);
      }
      return true;
    }
  }

  static boolean maybeDelete(final PairTable table, final int item) {
    //SHARED CODE (implemented as a macro in C and expanded here)
    final int lgSize = table.lgSize;
    final int tableSize = 1 << lgSize;
    final int mask = tableSize - 1;
    final int shift = table.validBits - lgSize;
    int probe = item >>> shift;
    assert (probe >= 0) && (probe <= mask);
    final int[] arr = table.slots;
    int fetched = arr[probe];
    while ((fetched != item) && (fetched != -1)) {
      probe = (probe + 1) & mask;
      fetched = arr[probe];
    }
    //END SHARED CODE
    if (fetched == -1) { return false; }
    else {
      assert (fetched == item);
      // delete the item
      arr[probe] = -1;
      table.numPairs -= 1; assert (table.numPairs >= 0);

      // re-insert all items between the freed slot and the next empty slot
      probe = (probe + 1) & mask; fetched = arr[probe];
      while (fetched != -1) {
        arr[probe] = -1;
        mustInsert(table, fetched);
        probe = (probe + 1) & mask; fetched = arr[probe];
      }

      // shrink if necessary
      while (((downsizeDenom * table.numPairs)
              < (downsizeNumer * (1 << table.lgSize))) && (table.lgSize > 2)) {
        table.rebuild(table.lgSize - 1);
      }
      return true;
    }
  }

  /**
   * While extracting the items from a linear probing hashtable,
   * this will usually undo the wrap-around provided that the table
   * isn't too full. Experiments suggest that for sufficiently large tables
   * the load factor would have to be over 90 percent before this would fail frequently,
   * and even then the subsequent sort would fix things up.
   * @param table the given table to unwrap
   * @param numPairs the number of valid pairs in the table
   * @return the unwrapped table
   */
  static int[] unwrappingGetItems(final PairTable table, final int numPairs) {
    if (numPairs < 1) { return null; }
    final int[] slots = table.slots;
    final int tableSize = 1 << table.lgSize;
    final int[] result = new int[numPairs];
    int i = 0;
    int l = 0;
    int r = numPairs - 1;

    // Special rules for the region before the first empty slot.
    final int hiBit = 1 << (table.validBits - 1);
    while ((i < tableSize) && (slots[i] != -1)) {
      final int item = slots[i++];
      if ((item & hiBit) != 0) { result[r--] = item; } // This item was probably wrapped, so move to end.
      else                     { result[l++] = item; }
    }

    // The rest of the table is processed normally.
    while (i < tableSize) {
      final int look = slots[i++];
      if (look != -1) { result[l++] = look; }
    }
    assert l == (r + 1);
    return result;
  }

  /**
   * In applications where the input array is already nearly sorted,
   * insertion sort runs in linear time with a very small constant.
   * This introspective version of insertion sort protects against
   * the quadratic cost of sorting bad input arrays.
   * It keeps track of how much work has been done, and if that exceeds a
   * constant times the array length, it switches to a different sorting algorithm.
   * @param a the array to sort
   * @param l points AT the leftmost element, i.e., inclusive.
   * @param r points AT the rightmost element, i.e., inclusive.
   */
  static void introspectiveInsertionSort(final int[] a, final int l, final int r) {
    final int length = (r - l) + 1;
    long cost = 0;
    final long costLimit = 8 * length;
    for (int i = l + 1; i <= r; i++) {
      int j = i;
      final long v = a[i] & 0XFFFF_FFFFL; //v must be long
      while ((j >= (l + 1)) && (v < ((a[j - 1]) & 0XFFFF_FFFFL))) {
        a[j] = a[j - 1];
        j -= 1;
      }
      a[j] = (int) v;
      cost += (i - j); // distance moved is a measure of work

      if (cost > costLimit) {
        //we need an unsigned sort, but it is linear time and very rarely occurs
        final long[] b = new long[a.length];
        for (int m = 0; m < a.length; m++) { b[m] = a[m] & 0XFFFF_FFFFL; }
        Arrays.sort(b, l, r + 1);
        for (int m = 0; m < a.length; m++) { a[m] = (int) b[m]; }
        // The following sanity check can be used during development
        //int bad = 0;
        //for (int m = l; m < (r - 1); m++) {
        //  final long b1 = a[m] & 0XFFFF_FFFFL;
        //  final long b2 = a[m + 1] & 0XFFFF_FFFFL;
        //  if (b1 > b2) { bad++; }
        //}
        //assert (bad == 0);
        return;
      }
    }
    // The following sanity check can be used during development
    //    int bad = 0;
    //    for (int m = l; m < (r - 1); m++) {
    //      final long b1 = a[m] & 0XFFFF_FFFFL;
    //      final long b2 = a[m + 1] & 0XFFFF_FFFFL;
    //      if (b1 > b2) { bad++; }
    //    }
    //    assert (bad == 0);
  }

  static void merge(
      final int[] arrA, final int startA, final int lengthA, //input
      final int[] arrB, final int startB, final int lengthB, //input
      final int[] arrC, final int startC) { //output

    final int lengthC = lengthA + lengthB;
    final int limA = startA + lengthA;
    final int limB = startB + lengthB;
    final int limC = startC + lengthC;
    int a = startA;
    int b = startB;
    int c = startC;
    for ( ; c < limC ; c++) {
      if      (b >= limB)         { arrC[c] = arrA[a++]; }
      else if (a >= limA)         { arrC[c] = arrB[b++]; }
      else {
        final long aa = arrA[a] & 0XFFFF_FFFFL;
        final long bb = arrB[b] & 0XFFFF_FFFFL;
        if (aa < bb)    { arrC[c] = arrA[a++]; }
        else            { arrC[c] = arrB[b++]; }
      }
    }
    assert (a == limA);
    assert (b == limB);
  }

  static boolean equals(final PairTable a, final PairTable b) {
    if ((a == null) && (b == null)) { return true; }
    if ((a == null) || (b == null)) {
      throw new SketchesArgumentException("PairTable " + ((a == null) ? "a" : "b") + " is null");
    }
    //Note: lgSize array sizes may not be equal, but contents still can be equal
    rtAssertEquals(a.validBits, b.validBits);
    final int numPairsA = a.numPairs;
    final int numPairsB = b.numPairs;
    rtAssertEquals(numPairsA, numPairsB);
    final int[] pairsA = PairTable.unwrappingGetItems(a, numPairsA);
    final int[] pairsB = PairTable.unwrappingGetItems(b, numPairsB);
    PairTable.introspectiveInsertionSort(pairsA, 0, numPairsA - 1);
    PairTable.introspectiveInsertionSort(pairsB, 0, numPairsB - 1);
    rtAssertEquals(pairsA, pairsB);
    return true;
  }

  static String toString(final PairTable table, final boolean detail) {
    final StringBuilder sb = new StringBuilder();
    final int tableSize = 1 << table.lgSize;
    sb.append("PairTable").append(LS);
    sb.append("  LgSize        : ").append(table.lgSize).append(LS);
    sb.append("  Size          : ").append(tableSize).append(LS);
    sb.append("  Valid Bits    : ").append(table.validBits).append(LS);
    sb.append("  Num Items     : ").append(table.numPairs).append(LS);
    if (detail) {
      sb.append("  DATA (hex) : ").append(LS);
      final String hdr = String.format("%8s %9s %9s %4s", "Index","Word","Row","Col");
      sb.append(hdr).append(LS);
      final int[] slots = table.slots;
      for (int i = 0; i < tableSize; i++) {
        final int word = slots[i];
        if (word == -1) { //empty
          final String h = String.format("%8d %9s", i, "--");
          sb.append(h).append(LS);
        } else { //data
          final int row = word >>> 6;
          final int col = word & 0X3F;
          final String rowStr = Integer.toHexString(row);
          final String colStr = Integer.toHexString(col);
          final String wordStr = Long.toHexString(word & 0XFFFF_FFFFL);
          final String data = String.format("%8d %9s %9s %4s", i, wordStr, rowStr, colStr);
          sb.append(data).append(LS);
        }
      }
    }
    return sb.toString();
  }

  private static void checkLgSize(final int lgSize) {
    if ((lgSize < 2) || (lgSize > 26)) {
      throw new SketchesArgumentException("Illegal LgSize: " + lgSize);
    }
  }

}
