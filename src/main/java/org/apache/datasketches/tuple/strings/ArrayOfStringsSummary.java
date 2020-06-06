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

package org.apache.datasketches.tuple.strings;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.datasketches.tuple.Util.stringArrHash;
import static org.apache.datasketches.tuple.Util.stringConcat;

import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.memory.Buffer;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableBuffer;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.datasketches.tuple.UpdatableSummary;

/**
 * @author Lee Rhodes
 */
public class ArrayOfStringsSummary implements UpdatableSummary<String[]> {

  private String[] nodesArr = null;

  ArrayOfStringsSummary() { //required for ArrayOfStringsSummaryFactory
    nodesArr = null;
  }

  //Used by copy() and in test
  ArrayOfStringsSummary(final String[] nodesArr) {
    this.nodesArr = nodesArr.clone();
    checkNumNodes(nodesArr.length);
  }

  //used by fromMemory and in test
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

  @Override
  public ArrayOfStringsSummary copy() {
    final ArrayOfStringsSummary nodes = new ArrayOfStringsSummary(nodesArr);
    return nodes;
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

  //From UpdatableSummary

  @Override
  public void update(final String[] value) {
    if (nodesArr == null) {
      nodesArr = value.clone();
    }
    //otherwise do not update.
  }

  //From Object

  @Override
  public int hashCode() {
    return (int) stringArrHash(nodesArr);
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

  /**
   * @return the nodes array for this summary.
   */
  public String[] getValue() {
    return nodesArr.clone();
  }

  //also used in test
  static void checkNumNodes(final int numNodes) {
    if (numNodes > 127)  {
      throw new SketchesArgumentException("Number of nodes cannot exceed 127.");
    }
  }

  //also used in test
  static void checkInBytes(final Memory mem, final int totBytes) {
    if (mem.getCapacity() < totBytes) {
      throw new SketchesArgumentException("Incoming Memory has insufficient capacity.");
    }
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

}
