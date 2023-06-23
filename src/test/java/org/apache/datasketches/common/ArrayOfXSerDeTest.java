package org.apache.datasketches.common;

import static org.testng.Assert.assertEquals;

import org.apache.datasketches.memory.WritableMemory;
import org.testng.annotations.Test;

public class ArrayOfXSerDeTest {

  @Test
  public void checkBooleanItems() {
    int bytes;
    byte[] byteArr;
    int offset = 10;
    WritableMemory wmem;
    ArrayOfBooleansSerDe serDe = new ArrayOfBooleansSerDe();

    Boolean[] items = {true,false,true,true,false,false};
    bytes = serDe.sizeOf(items);
    byteArr = serDe.serializeToByteArray(items);
    assertEquals(byteArr.length, bytes);

    wmem = WritableMemory.allocate(offset + byteArr.length);
    wmem.putByteArray(offset, byteArr, 0, byteArr.length);
    Boolean[] deSer = serDe.deserializeFromMemory(wmem, offset, items.length);
    assertEquals(deSer, items);
    assertEquals(serDe.sizeOf(wmem, offset, items.length), bytes);

    Boolean item = true;
    bytes = serDe.sizeOf(item);
    byteArr = serDe.serializeToByteArray(item);
    assertEquals(byteArr.length, bytes);

    wmem = WritableMemory.allocate(offset + byteArr.length);
    wmem.putByteArray(offset, byteArr, 0, byteArr.length);
    Boolean deItem = serDe.deserializeOneFromMemory(wmem, offset);
    assertEquals(deItem, item);
    assertEquals(serDe.sizeOf(wmem, offset, 1), bytes);
  }

  @Test
  public void checkDoubleItems() {
    int bytes;
    byte[] byteArr;
    int offset = 10;
    WritableMemory wmem;
    ArrayOfDoublesSerDe serDe = new ArrayOfDoublesSerDe();

    Double[] items = {1.0, 2.0, 3.0, 4.0, 5.0, 6.0};
    bytes = serDe.sizeOf(items);
    byteArr = serDe.serializeToByteArray(items);
    assertEquals(byteArr.length, bytes);

    wmem = WritableMemory.allocate(offset + byteArr.length);
    wmem.putByteArray(offset, byteArr, 0, byteArr.length);
    Double[] deSer = serDe.deserializeFromMemory(wmem, offset, items.length);
    assertEquals(deSer,items);
    assertEquals(serDe.sizeOf(wmem, offset, items.length), bytes);

    Double item = 13.0;
    bytes = serDe.sizeOf(item);
    byteArr = serDe.serializeToByteArray(item);
    assertEquals(byteArr.length, bytes);
    assertEquals(serDe.sizeOf(item), bytes);

    wmem = WritableMemory.allocate(offset + byteArr.length);
    wmem.putByteArray(offset, byteArr, 0, byteArr.length);
    Double deItem = serDe.deserializeOneFromMemory(wmem, offset);
    assertEquals(deItem, item);
    assertEquals(serDe.sizeOf(wmem, offset, 1), bytes);
  }

  @Test
  public void checkLongItems() {
    int bytes;
    byte[] byteArr;
    int offset = 10;
    WritableMemory wmem;
    ArrayOfLongsSerDe serDe = new ArrayOfLongsSerDe();

    Long[] items = {1L, 2L, 3L, 4L, 5L, 6L};
    bytes = serDe.sizeOf(items);
    byteArr = serDe.serializeToByteArray(items);
    assertEquals(byteArr.length, bytes);

    wmem = WritableMemory.allocate(offset + byteArr.length);
    wmem.putByteArray(offset, byteArr, 0, byteArr.length);
    Long[] deSer = serDe.deserializeFromMemory(wmem, offset, items.length);
    assertEquals(deSer,items);
    assertEquals(serDe.sizeOf(wmem, offset, items.length), bytes);

    Long item = 13L;
    bytes = serDe.sizeOf(item);
    byteArr = serDe.serializeToByteArray(item);
    assertEquals(byteArr.length, bytes);
    assertEquals(serDe.sizeOf(item), bytes);

    wmem = WritableMemory.allocate(offset + byteArr.length);
    wmem.putByteArray(offset, byteArr, 0, byteArr.length);
    Long deItem = serDe.deserializeOneFromMemory(wmem, offset);
    assertEquals(deItem, item);
    assertEquals(serDe.sizeOf(wmem, offset, 1), bytes);
  }

  @Test
  public void checkNumberItems() {
    int bytes;
    byte[] byteArr;
    final int offset = 10;
    WritableMemory wmem;
    ArrayOfNumbersSerDe serDe = new ArrayOfNumbersSerDe();

    Number item = (double)5;
    bytes = serDe.sizeOf(item);
    byteArr = serDe.serializeToByteArray(item);
    assertEquals(byteArr.length, bytes);

    wmem = WritableMemory.allocate(offset + byteArr.length);
    wmem.putByteArray(offset, byteArr, 0, byteArr.length);
    Number deSer1 = serDe.deserializeOneFromMemory(wmem, offset);
    assertEquals(deSer1,item);
    assertEquals(serDe.sizeOf(wmem, offset, 1), bytes);

    Number[] items = {(long)1, (int)2, (short)3, (byte)4, (double)5, (float)6};
    bytes = serDe.sizeOf(items);
    byteArr = serDe.serializeToByteArray(items);
    assertEquals(byteArr.length, bytes);

    wmem = WritableMemory.allocate(offset + byteArr.length);
    wmem.putByteArray(offset, byteArr, 0, byteArr.length);
    Number[] deSer = serDe.deserializeFromMemory(wmem, offset, items.length);
    assertEquals(deSer,items);
    assertEquals(serDe.sizeOf(wmem, offset, items.length), bytes);

    item = 13.0;
    bytes = serDe.sizeOf(item);
    byteArr = serDe.serializeToByteArray(new Number[] {item});
    assertEquals(byteArr.length, bytes);
    assertEquals(serDe.sizeOf(item), bytes);

    wmem = WritableMemory.allocate(offset + byteArr.length);
    wmem.putByteArray(offset, byteArr, 0, byteArr.length);
    Number[] deItem = serDe.deserializeFromMemory(wmem, offset, 1);
    assertEquals(deItem[0], item);
    assertEquals(serDe.sizeOf(wmem, offset, 1), bytes);
  }

  @Test
  public void checkUTF8Items() {
    int bytes;
    byte[] byteArr;
    final int offset = 10;
    WritableMemory wmem;
    ArrayOfStringsSerDe serDe = new ArrayOfStringsSerDe();

    String item = "abcdefghijklmnopqr";
    bytes = serDe.sizeOf(item);
    byteArr = serDe.serializeToByteArray(item);
    assertEquals(byteArr.length, bytes);

    wmem = WritableMemory.allocate(offset + byteArr.length);
    wmem.putByteArray(offset, byteArr, 0, byteArr.length);
    String deSer1 = serDe.deserializeOneFromMemory(wmem, offset);
    assertEquals(deSer1,item);
    assertEquals(serDe.sizeOf(wmem, offset, 1), bytes);

    String[] items = {"abc","def","ghi","jkl","mno","pqr"};
    bytes = serDe.sizeOf(items);
    byteArr = serDe.serializeToByteArray(items);
    assertEquals(byteArr.length, bytes);

    wmem = WritableMemory.allocate(offset + byteArr.length);
    wmem.putByteArray(offset, byteArr, 0, byteArr.length);
    String[] deSer = serDe.deserializeFromMemory(wmem, offset, items.length);
    assertEquals(deSer,items);
    assertEquals(serDe.sizeOf(wmem, offset, items.length), bytes);

    item = "13.0";
    bytes = serDe.sizeOf(item);
    byteArr = serDe.serializeToByteArray(new String[] {item});
    assertEquals(byteArr.length, bytes);
    assertEquals(serDe.sizeOf(item), bytes);

    wmem = WritableMemory.allocate(offset + byteArr.length);
    wmem.putByteArray(offset, byteArr, 0, byteArr.length);
    String[] deItem = serDe.deserializeFromMemory(wmem, offset, 1);
    assertEquals(deItem[0], item);
    assertEquals(serDe.sizeOf(wmem, offset, 1), bytes);
  }

  @Test
  public void checkUTF16Items() {
    int bytes;
    byte[] byteArr;
    final int offset = 10;
    WritableMemory wmem;
    ArrayOfUtf16StringsSerDe serDe = new ArrayOfUtf16StringsSerDe();

    String item = "abcdefghijklmnopqr";
    bytes = serDe.sizeOf(item);
    byteArr = serDe.serializeToByteArray(item);
    assertEquals(byteArr.length, bytes);

    wmem = WritableMemory.allocate(offset + byteArr.length);
    wmem.putByteArray(offset, byteArr, 0, byteArr.length);
    String deSer1 = serDe.deserializeOneFromMemory(wmem, offset);
    assertEquals(deSer1,item);
    assertEquals(serDe.sizeOf(wmem, offset, 1), bytes);

    String[] items = {"abc","def","ghi","jkl","mno","pqr"};
    bytes = serDe.sizeOf(items);
    byteArr = serDe.serializeToByteArray(items);
    assertEquals(byteArr.length, bytes);

    wmem = WritableMemory.allocate(offset + byteArr.length);
    wmem.putByteArray(offset, byteArr, 0, byteArr.length);
    String[] deSer = serDe.deserializeFromMemory(wmem, offset, items.length);
    assertEquals(deSer,items);
    assertEquals(serDe.sizeOf(wmem, offset, items.length), bytes); //

    item = "13.0";
    bytes = serDe.sizeOf(item);
    byteArr = serDe.serializeToByteArray(new String[] {item});
    assertEquals(byteArr.length, bytes);
    assertEquals(serDe.sizeOf(item), bytes);

    wmem = WritableMemory.allocate(offset + byteArr.length);
    wmem.putByteArray(offset, byteArr, 0, byteArr.length);
    String[] deItem = serDe.deserializeFromMemory(wmem, offset, 1);
    assertEquals(deItem[0], item);
    assertEquals(serDe.sizeOf(wmem, offset, 1), bytes);
  }

}
