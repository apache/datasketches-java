package org.apache.datasketches;

import org.apache.datasketches.memory.Memory;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class ArrayOfStringsSerDeTest {

  @Test
  public void testArray() {
    testSerde("abc", "def", "xyz");
    testSerde("abc", "123", "456.0");
  }

  @Test
  public void testSingletonArray() {
    testSerde("abc");
    testSerde("xyz");
  }

  @Test
  public void testEmptyArray() {
    testSerde();
  }

  @Test
  public void testArrayWithNullString() {
    testSerde((String) null);
    testSerde("abc", null, "def");
    testSerde(null, null, null);
  }

  @Test
  public void testArrayWithEmptyString() {
    testSerde("");
    testSerde("abc", "def", "");
    testSerde("", "", "");
    testSerde("", null, "abc");
  }

  @Test(
      expectedExceptions = SketchesArgumentException.class,
      expectedExceptionsMessageRegExp
          = "All Strings must be non-null in non null-safe mode.")
  public void testArrayWithNullsInUnsafeMode() {
    final ArrayOfStringsSerDe serDe = new ArrayOfStringsSerDe();
    serDe.serializeToByteArray(new String[]{null});
  }

  @Test(
      expectedExceptions = SketchesArgumentException.class,
      expectedExceptionsMessageRegExp
          = "Unrecognized String length reading entry 0: -1")
  public void testUtf8IllegalLength() {
    // bytes for length = -1
    final byte[] bytes = {-1, -1, -1, -1};
    new ArrayOfStringsSerDe().deserializeFromMemory(Memory.wrap(bytes), 1);
  }

  @Test(
      expectedExceptions = SketchesArgumentException.class,
      expectedExceptionsMessageRegExp
          = "Unrecognized String length reading entry 0: -2")
  public void testUtf8IllegalLengthInNullSafe() {
    // bytes for length = -2
    final byte[] bytes = {-2, -1, -1, -1};
    new ArrayOfStringsSerDe(true).deserializeFromMemory(Memory.wrap(bytes), 1);
  }

  @Test(
      expectedExceptions = SketchesArgumentException.class,
      expectedExceptionsMessageRegExp
          = "Unrecognized String length reading entry 0: -1")
  public void testUtf16IllegalLength() {
    // bytes for length = -1
    final byte[] bytes = {-1, -1, -1, -1};
    new ArrayOfUtf16StringsSerDe().deserializeFromMemory(Memory.wrap(bytes), 1);
  }

  @Test(
      expectedExceptions = SketchesArgumentException.class,
      expectedExceptionsMessageRegExp
          = "Unrecognized String length reading entry 0: -2")
  public void testUtf16IllegalLengthInNullSafe() {
    // bytes for length = -2
    final byte[] bytes = {-2, -1, -1, -1};
    new ArrayOfUtf16StringsSerDe(true).deserializeFromMemory(Memory.wrap(bytes), 1);
  }

  /**
   * Tests serialization/deserialization of the given array of Strings using both
   * utf8 and utf16 SerDe.
   */
  private void testSerde(String... inputArray) {
    testUtf8Serde(inputArray);
    testUtf16Serde(inputArray);
  }

  private void testUtf8Serde(String... inputArray) {
    final ArrayOfStringsSerDe serde = new ArrayOfStringsSerDe(true);
    byte[] bytes = serde.serializeToByteArray(inputArray);
    String[] deserialized = serde.deserializeFromMemory(Memory.wrap(bytes), inputArray.length);
    assertEquals(inputArray, deserialized);
  }

  private void testUtf16Serde(String... inputArray) {
    final ArrayOfUtf16StringsSerDe serde = new ArrayOfUtf16StringsSerDe(true);
    byte[] bytes = serde.serializeToByteArray(inputArray);
    String[] deserialized = serde.deserializeFromMemory(Memory.wrap(bytes), inputArray.length);
    assertEquals(inputArray, deserialized);
  }

}