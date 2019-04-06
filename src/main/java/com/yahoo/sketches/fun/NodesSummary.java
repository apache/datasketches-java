/*
 * Copyright 2019, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.fun;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.yahoo.memory.Buffer;
import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableBuffer;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.tuple.DeserializeResult;
import com.yahoo.sketches.tuple.UpdatableSummary;

/**
 * @author Lee Rhodes
 */
public class NodesSummary implements UpdatableSummary<String[]> {
  private String[] nodesArr = null;

  NodesSummary() { //required for NodesSummaryFactory
    nodesArr = null;
  }

  NodesSummary(final String[] nodesArr) {
    this.nodesArr = nodesArr.clone();
    checkNumNodes(nodesArr.length);
  }

  //used by deserialization
  NodesSummary(final Memory mem) {
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

  private class ComputeBytes {
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
  public NodesSummary copy() { //shallow copy
    final NodesSummary nodes = new NodesSummary(nodesArr);
    return nodes;
  }

  public String[] getValue() {
    return nodesArr;
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
  public static DeserializeResult<NodesSummary> fromMemory(final Memory mem) {
    final NodesSummary nsum = new NodesSummary(mem);
    final int totBytes = mem.getInt(0);
    return new DeserializeResult<>(nsum, totBytes);
  }

  @Override
  public void update(final String[] value) {
    if (nodesArr == null) {
      nodesArr = value;
    }
    //otherwise do not update.
  }

  private static void checkNumNodes(final int numNodes) {
    if (numNodes > 127)  {
      throw new SketchesArgumentException("Number of nodes cannot exceed 127.");
    }
  }

  private static void checkInBytes(final Memory mem, final int totBytes) {
    if (mem.getCapacity() < totBytes) {
      throw new SketchesArgumentException("Incoming Memory has insufficient capacity.");
    }
  }

}
