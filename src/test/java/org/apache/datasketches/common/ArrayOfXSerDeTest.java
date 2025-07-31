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

package org.apache.datasketches.common;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static org.testng.Assert.assertEquals;

import java.lang.foreign.MemorySegment;

import org.testng.annotations.Test;

public class ArrayOfXSerDeTest {

  @Test
  public void checkBooleanItems() {
    int bytes;
    byte[] byteArr;
    final int offset = 10;
    MemorySegment wseg;
    final ArrayOfBooleansSerDe serDe = new ArrayOfBooleansSerDe();

    final Boolean[] items = {true,false,true,false,true,false,true,false,true,false};
    bytes = serDe.sizeOf(items);
    byteArr = serDe.serializeToByteArray(items);
    assertEquals(byteArr.length, bytes);

    wseg = MemorySegment.ofArray(new byte[offset + byteArr.length]);
    MemorySegment.copy(byteArr, 0, wseg, JAVA_BYTE, offset, byteArr.length);
    final Boolean[] deSer = serDe.deserializeFromMemorySegment(wseg, offset, items.length);
    assertEquals(deSer, items);
    assertEquals(serDe.sizeOf(wseg, offset, items.length), bytes);

    final Boolean item = true;
    bytes = serDe.sizeOf(item);
    byteArr = serDe.serializeToByteArray(item);
    assertEquals(byteArr.length, bytes);
    assertEquals(serDe.toString(item), item.toString());

    wseg = MemorySegment.ofArray(new byte[offset + byteArr.length]);
    MemorySegment.copy(byteArr, 0, wseg, JAVA_BYTE, offset, byteArr.length);
    final Boolean deItem = serDe.deserializeFromMemorySegment(wseg, offset, 1)[0];
    assertEquals(deItem, item);
    assertEquals(serDe.sizeOf(wseg, offset, 1), bytes);
  }

  @Test
  public void checkDoubleItems() {
    int bytes;
    byte[] byteArr;
    final int offset = 10;
    MemorySegment wseg;
    final ArrayOfDoublesSerDe serDe = new ArrayOfDoublesSerDe();

    final Double[] items = {1.0, 2.0, 3.0, 4.0, 5.0, 6.0};
    bytes = serDe.sizeOf(items);
    byteArr = serDe.serializeToByteArray(items);
    assertEquals(byteArr.length, bytes);

    wseg = MemorySegment.ofArray(new byte[offset + byteArr.length]);
    MemorySegment.copy(byteArr, 0, wseg, JAVA_BYTE, offset, byteArr.length);
    final Double[] deSer = serDe.deserializeFromMemorySegment(wseg, offset, items.length);
    assertEquals(deSer,items);
    assertEquals(serDe.sizeOf(wseg, offset, items.length), bytes);

    final Double item = 13.0;
    bytes = serDe.sizeOf(item);
    byteArr = serDe.serializeToByteArray(item);
    assertEquals(byteArr.length, bytes);
    assertEquals(serDe.sizeOf(item), bytes);
    assertEquals(serDe.toString(item), item.toString());

    wseg = MemorySegment.ofArray(new byte[offset + byteArr.length]);
    MemorySegment.copy(byteArr, 0, wseg, JAVA_BYTE, offset, byteArr.length);
    final Double deItem = serDe.deserializeFromMemorySegment(wseg, offset, 1)[0];
    assertEquals(deItem, item);
    assertEquals(serDe.sizeOf(wseg, offset, 1), bytes);
  }

  @Test
  public void checkLongItems() {
    int bytes;
    byte[] byteArr;
    final int offset = 10;
    MemorySegment wseg;
    final ArrayOfLongsSerDe serDe = new ArrayOfLongsSerDe();

    final Long[] items = {1L, 2L, 3L, 4L, 5L, 6L};
    bytes = serDe.sizeOf(items);
    byteArr = serDe.serializeToByteArray(items);
    assertEquals(byteArr.length, bytes);

    wseg = MemorySegment.ofArray(new byte[offset + byteArr.length]);
    MemorySegment.copy(byteArr, 0, wseg, JAVA_BYTE, offset, byteArr.length);
    final Long[] deSer = serDe.deserializeFromMemorySegment(wseg, offset, items.length);
    assertEquals(deSer,items);
    assertEquals(serDe.sizeOf(wseg, offset, items.length), bytes);

    final Long item = 13L;
    bytes = serDe.sizeOf(item);
    byteArr = serDe.serializeToByteArray(item);
    assertEquals(byteArr.length, bytes);
    assertEquals(serDe.sizeOf(item), bytes);
    assertEquals(serDe.toString(item), item.toString());

    wseg = MemorySegment.ofArray(new byte[offset + byteArr.length]);
    MemorySegment.copy(byteArr, 0, wseg, JAVA_BYTE, offset, byteArr.length);
    final Long deItem = serDe.deserializeFromMemorySegment(wseg, offset, 1)[0];
    assertEquals(deItem, item);
    assertEquals(serDe.sizeOf(wseg, offset, 1), bytes);
  }

  @Test
  public void checkNumberItems() {
    int bytes;
    byte[] byteArr;
    final int offset = 10;
    MemorySegment wseg;
    final ArrayOfNumbersSerDe serDe = new ArrayOfNumbersSerDe();

    Number item = (double)5;
    bytes = serDe.sizeOf(item);
    byteArr = serDe.serializeToByteArray(item);
    assertEquals(byteArr.length, bytes);

    wseg = MemorySegment.ofArray(new byte[offset + byteArr.length]);
    MemorySegment.copy(byteArr, 0, wseg, JAVA_BYTE, offset, byteArr.length);
    final Number deSer1 = serDe.deserializeFromMemorySegment(wseg, offset, 1)[0];
    assertEquals(deSer1,item);
    assertEquals(serDe.sizeOf(wseg, offset, 1), bytes);

    final Number[] items = {(long)1, (int)2, (short)3, (byte)4, (double)5, (float)6};
    bytes = serDe.sizeOf(items);
    byteArr = serDe.serializeToByteArray(items);
    assertEquals(byteArr.length, bytes);

    wseg = MemorySegment.ofArray(new byte[offset + byteArr.length]);
    MemorySegment.copy(byteArr, 0, wseg, JAVA_BYTE, offset, byteArr.length);
    final Number[] deSer = serDe.deserializeFromMemorySegment(wseg, offset, items.length);
    assertEquals(deSer,items);
    assertEquals(serDe.sizeOf(wseg, offset, items.length), bytes);

    item = 13.0;
    bytes = serDe.sizeOf(item);
    byteArr = serDe.serializeToByteArray(new Number[] {item});
    assertEquals(byteArr.length, bytes);
    assertEquals(serDe.sizeOf(item), bytes);
    assertEquals(serDe.toString(item), item.toString());

    wseg = MemorySegment.ofArray(new byte[offset + byteArr.length]);
    MemorySegment.copy(byteArr, 0, wseg, JAVA_BYTE, offset, byteArr.length);
    final Number[] deItem = serDe.deserializeFromMemorySegment(wseg, offset, 1);
    assertEquals(deItem[0], item);
    assertEquals(serDe.sizeOf(wseg, offset, 1), bytes);
  }

  @Test
  public void checkUTF8Items() {
    int bytes;
    byte[] byteArr;
    final int offset = 10;
    MemorySegment wseg;
    final ArrayOfStringsSerDe serDe = new ArrayOfStringsSerDe();

    String item = "abcdefghijklmnopqr";
    bytes = serDe.sizeOf(item);
    byteArr = serDe.serializeToByteArray(item);
    assertEquals(byteArr.length, bytes);

    wseg = MemorySegment.ofArray(new byte[offset + byteArr.length]);
    MemorySegment.copy(byteArr, 0, wseg, JAVA_BYTE, offset, byteArr.length);
    final String deSer1 = serDe.deserializeFromMemorySegment(wseg, offset, 1)[0];
    assertEquals(deSer1,item);
    assertEquals(serDe.sizeOf(wseg, offset, 1), bytes);

    final String[] items = {"abc","def","ghi","jkl","mno","pqr"};
    bytes = serDe.sizeOf(items);
    byteArr = serDe.serializeToByteArray(items);
    assertEquals(byteArr.length, bytes);

    wseg = MemorySegment.ofArray(new byte[offset + byteArr.length]);
    MemorySegment.copy(byteArr, 0, wseg, JAVA_BYTE, offset, byteArr.length);
    final String[] deSer = serDe.deserializeFromMemorySegment(wseg, offset, items.length);
    assertEquals(deSer,items);
    assertEquals(serDe.sizeOf(wseg, offset, items.length), bytes);

    item = "13.0";
    bytes = serDe.sizeOf(item);
    byteArr = serDe.serializeToByteArray(new String[] {item});
    assertEquals(byteArr.length, bytes);
    assertEquals(serDe.sizeOf(item), bytes);
    assertEquals(serDe.toString(item), item);

    wseg = MemorySegment.ofArray(new byte[offset + byteArr.length]);
    MemorySegment.copy(byteArr, 0, wseg, JAVA_BYTE, offset, byteArr.length);
    final String[] deItem = serDe.deserializeFromMemorySegment(wseg, offset, 1);
    assertEquals(deItem[0], item);
    assertEquals(serDe.sizeOf(wseg, offset, 1), bytes);
  }

  @Test
  public void checkUTF16Items() {
    int bytes;
    byte[] byteArr;
    final int offset = 10;
    MemorySegment wseg;
    final ArrayOfUtf16StringsSerDe serDe = new ArrayOfUtf16StringsSerDe();

    String item = "abcdefghijklmnopqr";
    bytes = serDe.sizeOf(item);
    byteArr = serDe.serializeToByteArray(item);
    assertEquals(byteArr.length, bytes);

    wseg = MemorySegment.ofArray(new byte[offset + byteArr.length]);
    MemorySegment.copy(byteArr, 0, wseg, JAVA_BYTE, offset, byteArr.length);
    final String deSer1 = serDe.deserializeFromMemorySegment(wseg, offset, 1)[0];
    assertEquals(deSer1,item);
    assertEquals(serDe.sizeOf(wseg, offset, 1), bytes);

    final String[] items = {"abc","def","ghi","jkl","mno","pqr"};
    bytes = serDe.sizeOf(items);
    byteArr = serDe.serializeToByteArray(items);
    assertEquals(byteArr.length, bytes);

    wseg = MemorySegment.ofArray(new byte[offset + byteArr.length]);
    MemorySegment.copy(byteArr, 0, wseg, JAVA_BYTE, offset, byteArr.length);
    final String[] deSer = serDe.deserializeFromMemorySegment(wseg, offset, items.length);
    assertEquals(deSer,items);
    assertEquals(serDe.sizeOf(wseg, offset, items.length), bytes); //

    item = "13.0";
    bytes = serDe.sizeOf(item);
    byteArr = serDe.serializeToByteArray(new String[] {item});
    assertEquals(byteArr.length, bytes);
    assertEquals(serDe.sizeOf(item), bytes);
    assertEquals(serDe.toString(item), item);

    wseg = MemorySegment.ofArray(new byte[offset + byteArr.length]);
    MemorySegment.copy(byteArr, 0, wseg, JAVA_BYTE, offset, byteArr.length);
    final String[] deItem = serDe.deserializeFromMemorySegment(wseg, offset, 1);
    assertEquals(deItem[0], item);
    assertEquals(serDe.sizeOf(wseg, offset, 1), bytes);
  }

}
