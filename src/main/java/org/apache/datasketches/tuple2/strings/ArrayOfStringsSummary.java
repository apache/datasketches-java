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

package org.apache.datasketches.tuple2.strings;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_INT_UNALIGNED;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.datasketches.tuple2.Util.stringArrHash;
import static org.apache.datasketches.tuple2.Util.stringConcat;

import java.lang.foreign.MemorySegment;

import org.apache.datasketches.common.SketchesArgumentException;
import org.apache.datasketches.tuple2.UpdatableSummary;

/**
 * Implements UpdatableSummary&lt;String[]&gt;
 * @author Lee Rhodes
 */
public final class ArrayOfStringsSummary implements UpdatableSummary<String[]> {

  private String[] stringArr = null;

  ArrayOfStringsSummary() { //required for ArrayOfStringsSummaryFactory
    stringArr = null;
  }

  //Used by copy() and in test
  ArrayOfStringsSummary(final String[] stringArr) {
    this.stringArr = stringArr.clone();
    checkNumNodes(stringArr.length);
  }

  //used by fromMemorySegment and in test
  /**
   * This reads a MemorySegment that has a layout similar to the C struct:
   * {@snippet :
   *   typedef struct {
   *     int totBytes;
   *     byte nodes;   //number of Nodes.
   *     Node[nodes] = { Node[0], Node[1], ... }
   *   }
   * }
   * Where a Node has a layout similar to the C struct:
   * {@snippet :
   *   typedef struct {
   *     int numBytes;
   *     byte[] byteArray; //UTF-8 byte array. Not null terminated.
   *   }
   * }
   * @param seg the MemorySegment containing the Summary data
   */
  ArrayOfStringsSummary(final MemorySegment seg) {
    int pos = 0;
    final int totBytes = seg.get(JAVA_INT_UNALIGNED, pos); pos += Integer.BYTES;
    checkInBytes(seg, totBytes);
    final int nodes = seg.get(JAVA_BYTE, pos); pos += Byte.BYTES;
    checkNumNodes(nodes);
    final String[] stringArr = new String[nodes];
    for (int i = 0; i < nodes; i++) {
      final int len = seg.get(JAVA_INT_UNALIGNED, pos); pos += Integer.BYTES;
      final byte[] byteArr = new byte[len];
      MemorySegment.copy(seg, JAVA_BYTE, pos, byteArr, 0, len); pos += len;
      stringArr[i] = new String(byteArr, UTF_8);
    }
    assert pos == totBytes;
    this.stringArr = stringArr;
  }

  @Override
  public ArrayOfStringsSummary copy() {
    final ArrayOfStringsSummary nodes = new ArrayOfStringsSummary(stringArr);
    return nodes;
  }

  @Override
  public byte[] toByteArray() {
    final ComputeBytes cb = new ComputeBytes(stringArr);
    final int totBytes = cb.totBytes_;
    final byte[] out = new byte[totBytes];
    final MemorySegment wseg = MemorySegment.ofArray(out);
    int pos = 0;
    wseg.set(JAVA_INT_UNALIGNED, pos, totBytes); pos += Integer.BYTES;
    final int numNodes = cb.numNodes_;
    wseg.set(JAVA_BYTE, pos, (byte)numNodes); pos += Byte.BYTES;
    for (int i = 0; i < numNodes; i++) {
      final int nodeLen = cb.nodeLengthsArr_[i];
      wseg.set(JAVA_INT_UNALIGNED, pos, nodeLen); pos += Integer.BYTES;
      MemorySegment.copy(cb.nodeBytesArr_[i], 0, wseg, JAVA_BYTE, pos, nodeLen); pos += nodeLen;
    }
    assert pos == totBytes;
    return out;
  }

  //From UpdatableSummary

  @Override
  public ArrayOfStringsSummary update(final String[] value) {
    if (stringArr == null) {
      stringArr = value.clone();
    }
    return this;
  }

  //From Object

  @Override
  public int hashCode() {
    return (int) stringArrHash(stringArr);
  }

  @Override
  public boolean equals(final Object summary) {
    if (summary == null || !(summary instanceof ArrayOfStringsSummary)) {
      return false;
    }
    final String thatStr = stringConcat(((ArrayOfStringsSummary) summary).stringArr);
    final String thisStr = stringConcat(stringArr);
    return thisStr.equals(thatStr);
  }

  /**
   * Returns the nodes array for this summary.
   * @return the nodes array for this summary.
   */
  public String[] getValue() {
    return stringArr.clone();
  }

  //also used in test
  static void checkNumNodes(final int numNodes) {
    if (numNodes > 127 || numNodes < 0)  {
      throw new SketchesArgumentException("Number of nodes cannot exceed 127 or be negative.");
    }
  }

  //also used in test
  static void checkInBytes(final MemorySegment seg, final int totBytes) {
    if (seg.byteSize() < totBytes) {
      throw new SketchesArgumentException("Incoming MemorySegment has insufficient capacity.");
    }
  }

  /**
   * Computes total bytes and number of nodes from the given string array.
   */
  private static class ComputeBytes {
    final byte numNodes_;
    final int[] nodeLengthsArr_;
    final byte[][] nodeBytesArr_;
    final int totBytes_;

    ComputeBytes(final String[] stringArr) {
      numNodes_ = (byte) stringArr.length;
      checkNumNodes(numNodes_);
      nodeLengthsArr_ = new int[numNodes_];
      nodeBytesArr_ = new byte[numNodes_][];
      int sumNodeBytes = 0;
      for (int i = 0; i < numNodes_; i++) {
        nodeBytesArr_[i] = stringArr[i].getBytes(UTF_8);
        nodeLengthsArr_[i] = nodeBytesArr_[i].length;
        sumNodeBytes += nodeLengthsArr_[i];
      }
      totBytes_ = sumNodeBytes + (numNodes_ + 1) * Integer.BYTES + 1;
    }
  }

}
