package org.apache.datasketches.common;

import static org.testng.Assert.assertEquals;

import org.apache.datasketches.memory.Memory;
import org.testng.annotations.Test;

public class ArrayOfXSerDeTest {

  @Test
  public void checkBooleanItems() {
    int bytes;
    byte[] byteArr;
    Memory mem;
    ArrayOfBooleansSerDe serDe = new ArrayOfBooleansSerDe();

    Boolean[] items = {true,false,true,true,false,false};
    bytes = serDe.sizeOf(items);
    byteArr = serDe.serializeToByteArray(items);
    assertEquals(byteArr.length, bytes);

    mem = Memory.wrap(byteArr);
    Boolean[] deSer = serDe.deserializeFromMemory(mem, items.length);
    assertEquals(deSer, items);
    assertEquals(serDe.sizeOf(mem, 0, items.length), bytes);

    Boolean item = true;
    bytes = serDe.sizeOf(item);
    byteArr = serDe.serializeToByteArray(new Boolean[] {item});
    assertEquals(byteArr.length, bytes);

    mem = Memory.wrap(byteArr);
    Boolean[] deItem = serDe.deserializeFromMemory(mem, 1);
    assertEquals(deItem[0], item);
    assertEquals(serDe.sizeOf(mem, 0, 1), bytes);
  }

  @Test
  public void checkDoubleItems() {
    int bytes;
    byte[] byteArr;
    ArrayOfDoublesSerDe serDe = new ArrayOfDoublesSerDe();

    Double[] items = {1.0, 2.0, 3.0, 4.0, 5.0, 6.0};
    bytes = serDe.sizeOf(items);
    byteArr = serDe.serializeToByteArray(items);
    assertEquals(byteArr.length, bytes);

    Memory mem = Memory.wrap(byteArr);
    Double[] deSer = serDe.deserializeFromMemory(mem, items.length);
    assertEquals(deSer,items);
    assertEquals(serDe.sizeOf(mem, 0, items.length), bytes);

    Double item = 13.0;
    bytes = serDe.sizeOf(item);
    byteArr = serDe.serializeToByteArray(new Double[] {item});
    assertEquals(byteArr.length, bytes);
    assertEquals(serDe.sizeOf(item), bytes);

    mem = Memory.wrap(byteArr);
    Double[] deItem = serDe.deserializeFromMemory(mem, 1);
    assertEquals(deItem[0], item);
    assertEquals(serDe.sizeOf(mem, 0, 1), bytes);
  }

  @Test
  public void checkLongItems() {
    int bytes;
    byte[] byteArr;
    ArrayOfLongsSerDe serDe = new ArrayOfLongsSerDe();

    Long[] items = {1L, 2L, 3L, 4L, 5L, 6L};
    bytes = serDe.sizeOf(items);
    byteArr = serDe.serializeToByteArray(items);
    assertEquals(byteArr.length, bytes);

    Memory mem = Memory.wrap(byteArr);
    Long[] deSer = serDe.deserializeFromMemory(mem, items.length);
    assertEquals(deSer,items);
    assertEquals(serDe.sizeOf(mem, 0, items.length), bytes);

    Long item = 13L;
    bytes = serDe.sizeOf(item);
    byteArr = serDe.serializeToByteArray(new Long[] {item});
    assertEquals(byteArr.length, bytes);
    assertEquals(serDe.sizeOf(item), bytes);

    mem = Memory.wrap(byteArr);
    Long[] deItem = serDe.deserializeFromMemory(mem, 1);
    assertEquals(deItem[0], item);
    assertEquals(serDe.sizeOf(mem, 0, 1), bytes);
  }

  @Test
  public void checkNumberItems() {
    int bytes;
    byte[] byteArr;
    ArrayOfNumbersSerDe serDe = new ArrayOfNumbersSerDe();

    Number[] items = {(long)1, (int)2, (short)3, (byte)4, (double)5, (float)6};
    bytes = serDe.sizeOf(items);
    byteArr = serDe.serializeToByteArray(items);
    assertEquals(byteArr.length, bytes);

    Memory mem = Memory.wrap(byteArr);
    Number[] deSer = serDe.deserializeFromMemory(mem, items.length);
    assertEquals(deSer,items);
    assertEquals(serDe.sizeOf(mem, 0, items.length), bytes);

    Number item = 13.0;
    bytes = serDe.sizeOf(item);
    byteArr = serDe.serializeToByteArray(new Number[] {item});
    assertEquals(byteArr.length, bytes);
    assertEquals(serDe.sizeOf(item), bytes);

    mem = Memory.wrap(byteArr);
    Number[] deItem = serDe.deserializeFromMemory(mem, 1);
    assertEquals(deItem[0], item);
    assertEquals(serDe.sizeOf(mem, 0, 1), bytes);
  }

  @Test
  public void checkUTF8Items() {
    int bytes;
    byte[] byteArr;
    ArrayOfStringsSerDe serDe = new ArrayOfStringsSerDe();

    String[] items = {"abc","def","ghi","jkl","mno","pqr"};
    bytes = serDe.sizeOf(items);
    byteArr = serDe.serializeToByteArray(items);
    assertEquals(byteArr.length, bytes);

    Memory mem = Memory.wrap(byteArr);
    String[] deSer = serDe.deserializeFromMemory(mem, items.length);
    assertEquals(deSer,items);
    assertEquals(serDe.sizeOf(mem, 0, items.length), bytes);

    String item = "13.0";
    bytes = serDe.sizeOf(item);
    byteArr = serDe.serializeToByteArray(new String[] {item});
    assertEquals(byteArr.length, bytes);
    assertEquals(serDe.sizeOf(item), bytes);

    mem = Memory.wrap(byteArr);
    String[] deItem = serDe.deserializeFromMemory(mem, 1);
    assertEquals(deItem[0], item);
    assertEquals(serDe.sizeOf(mem, 0, 1), bytes);
  }

  @Test
  public void checkUTF16Items() {
    int bytes;
    byte[] byteArr;
    ArrayOfUtf16StringsSerDe serDe = new ArrayOfUtf16StringsSerDe();

    String[] items = {"abc","def","ghi","jkl","mno","pqr"};
    bytes = serDe.sizeOf(items);
    byteArr = serDe.serializeToByteArray(items);
    assertEquals(byteArr.length, bytes);

    Memory mem = Memory.wrap(byteArr);
    String[] deSer = serDe.deserializeFromMemory(mem, items.length);
    assertEquals(deSer,items);
    assertEquals(serDe.sizeOf(mem, 0, items.length), bytes); //

    String item = "13.0";
    bytes = serDe.sizeOf(item);
    byteArr = serDe.serializeToByteArray(new String[] {item});
    assertEquals(byteArr.length, bytes);
    assertEquals(serDe.sizeOf(item), bytes);

    mem = Memory.wrap(byteArr);
    String[] deItem = serDe.deserializeFromMemory(mem, 1);
    assertEquals(deItem[0], item);
    assertEquals(serDe.sizeOf(mem, 0, 1), bytes);
  }

}
