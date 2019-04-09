/*
 * Copyright 2019, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.tuple.strings;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.yahoo.memory.Buffer;
import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableBuffer;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.hash.MurmurHash3v2;
import com.yahoo.sketches.tuple.DeserializeResult;
import com.yahoo.sketches.tuple.UpdatableSummary;

/**
 * @author Lee Rhodes
 */
public class ArrayOfStringsSummary implements UpdatableSummary<String[]> {
  private static final int PRIME = 0x7A3C_CA71;
  private String[] nodesArr = null;

  ArrayOfStringsSummary() { //required for ArrayOfStringsSummaryFactory
    nodesArr = null;
  }

  ArrayOfStringsSummary(final String[] nodesArr) {
    this.nodesArr = nodesArr.clone();
    checkNumNodes(nodesArr.length);
  }

  //used by deserialization
  ArrayOfStringsSummary(final Memory mem) {
    final Buffer buf = mem.asBuffer();
    final int totBytes = buf.getInt();
    checkInBytes(mem, totBytes);
    final int nodes = buf.getByte();
    checkNumNodes(nodes);
    final String[] nodesArr = new String[nodes];
    for (int i = 0; i < nodes; i++) {
      final int len = buf.getInt();
      final byte[] byteArr = new byte[len];
      buf.getByteArray(byteArr, 0, len);
      nodesArr[i] = new String(byteArr, UTF_8);
    }
    this.nodesArr = nodesArr;
  }

  private static class ComputeBytes {
    final byte numNodes_;
    final int[] nodeLengthsArr_;
    final byte[][] nodeBytesArr_;
    final int totBytes_;

    ComputeBytes(final String[] nodesArr) {
      numNodes_ = (byte) nodesArr.length;
      checkNumNodes(numNodes_);
      nodeLengthsArr_ = new int[numNodes_];
      nodeBytesArr_ = new byte[numNodes_][];
      int sumNodeBytes = 0;
      for (int i = 0; i < numNodes_; i++) {
        nodeBytesArr_[i] = nodesArr[i].getBytes(UTF_8);
        nodeLengthsArr_[i] = nodeBytesArr_[i].length;
        sumNodeBytes += nodeLengthsArr_[i];
      }
      totBytes_ = sumNodeBytes + ((numNodes_ + 1) * Integer.BYTES) + 1;
    }
  }

  @Override
  public ArrayOfStringsSummary copy() {
    final ArrayOfStringsSummary nodes = new ArrayOfStringsSummary(nodesArr);
    return nodes;
  }

  public String[] getValue() {
    return nodesArr.clone();
  }

  @Override
  public byte[] toByteArray() {
    final ComputeBytes cb = new ComputeBytes(nodesArr);
    final int totBytes = cb.totBytes_;
    final byte[] out = new byte[totBytes];
    final WritableMemory wmem = WritableMemory.wrap(out);
    final WritableBuffer wbuf = wmem.asWritableBuffer();
    wbuf.putInt(totBytes);
    wbuf.putByte(cb.numNodes_);
    for (int i = 0; i < cb.numNodes_; i++) {
      wbuf.putInt(cb.nodeLengthsArr_[i]);
      wbuf.putByteArray(cb.nodeBytesArr_[i], 0, cb.nodeLengthsArr_[i]);
    }
    assert wbuf.getPosition() == totBytes;
    return out;
  }

  /**
   *
   * @param mem the given memory
   * @return the DeserializeResult
   */
  public static DeserializeResult<ArrayOfStringsSummary> fromMemory(final Memory mem) {
    final ArrayOfStringsSummary nsum = new ArrayOfStringsSummary(mem);
    final int totBytes = mem.getInt(0);
    return new DeserializeResult<>(nsum, totBytes);
  }

  @Override
  public void update(final String[] value) {
    if (nodesArr == null) {
      nodesArr = value.clone();
    }
    //otherwise do not update.
  }

  @Override
  public int hashCode() {
    final int[] arr = ArrayOfStringsSketch.computeKey(nodesArr);
    final int hash = (int) MurmurHash3v2.hash(arr, PRIME)[0];
    return hash;
  }

  @Override
  public boolean equals(final Object summary) {
    if ((summary == null) || !(summary instanceof ArrayOfStringsSummary)) {
      return false;
    }
    final String thatStr = stringConcat(((ArrayOfStringsSummary) summary).nodesArr);
    final String thisStr = stringConcat(nodesArr);
    return thisStr.equals(thatStr);
  }

  private static String stringConcat(final String[] strArr) {
    final StringBuilder sb = new StringBuilder();
    for (int i = 0; i < strArr.length; i++) { sb.append(strArr[i]); }
    return sb.toString();
  }

  /**
   * @param key to be hashed
   * @return an index into the hash table This hash function is taken from the internals of
   * Austin Appleby's MurmurHash3 algorithm. It is also used by the Trove for Java libraries.
   */
  static long hash(long key) {
    key ^= key >>> 33;
    key *= 0xff51afd7ed558ccdL;
    key ^= key >>> 33;
    key *= 0xc4ceb9fe1a85ec53L;
    key ^= key >>> 33;
    return key;
  }

  static void checkNumNodes(final int numNodes) {
    if (numNodes > 127)  {
      throw new SketchesArgumentException("Number of nodes cannot exceed 127.");
    }
  }

  static void checkInBytes(final Memory mem, final int totBytes) {
    if (mem.getCapacity() < totBytes) {
      throw new SketchesArgumentException("Incoming Memory has insufficient capacity.");
    }
  }

}
