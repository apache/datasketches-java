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

package org.apache.datasketches.kll;

import static org.apache.datasketches.kll.KllPreambleUtil.*;

import org.apache.datasketches.Family;
import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.memory.WritableMemory;
import org.testng.annotations.Test;

@SuppressWarnings("unused")
public class MemoryValidateTest {

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkInvalidFamily() {
    KllFloatsSketch sk = new KllFloatsSketch();
    byte[] byteArr = sk.toByteArray();
    WritableMemory wmem = WritableMemory.writableWrap(byteArr);
    insertFamilyID(wmem, Family.KLL.getID() - 1);
    MemoryValidate memVal = new MemoryValidate(wmem);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkInvalidSerVer() {
    KllFloatsSketch sk = new KllFloatsSketch();
    byte[] byteArr = sk.toByteArray();
    WritableMemory wmem = WritableMemory.writableWrap(byteArr);
    insertSerVer(wmem, SERIAL_VERSION_EMPTY_FULL - 1);
    MemoryValidate memVal = new MemoryValidate(wmem);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkInvalidEmptyAndSingle() {
    KllFloatsSketch sk = new KllFloatsSketch();
    byte[] byteArr = sk.toByteArray();
    WritableMemory wmem = WritableMemory.writableWrap(byteArr);
    insertFlags(wmem, EMPTY_BIT_MASK | SINGLE_ITEM_BIT_MASK);
    MemoryValidate memVal = new MemoryValidate(wmem);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkInvalidUpdatableAndSerVer() {
    KllFloatsSketch sk = new KllFloatsSketch();
    byte[] byteArr = sk.toByteArray();
    WritableMemory wmem = WritableMemory.writableWrap(byteArr);
    insertFlags(wmem, UPDATABLE_BIT_MASK);
    insertSerVer(wmem, SERIAL_VERSION_EMPTY_FULL);
    MemoryValidate memVal = new MemoryValidate(wmem);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkInvalidPreIntsAndSingle() {
    KllFloatsSketch sk = new KllFloatsSketch();
    byte[] byteArr = sk.toByteArray();
    WritableMemory wmem = WritableMemory.writableWrap(byteArr);
    insertFlags(wmem, UPDATABLE_BIT_MASK);
    insertSerVer(wmem, SERIAL_VERSION_SINGLE);
    MemoryValidate memVal = new MemoryValidate(wmem);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkInvalidSerVerAndSingle2() {
    KllFloatsSketch sk = new KllFloatsSketch();
    byte[] byteArr = sk.toByteArray();
    WritableMemory wmem = WritableMemory.writableWrap(byteArr);
    insertFlags(wmem, SINGLE_ITEM_BIT_MASK);
    insertSerVer(wmem, SERIAL_VERSION_EMPTY_FULL);
    MemoryValidate memVal = new MemoryValidate(wmem);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkInvalidPreIntsAndSingle2() {
    KllFloatsSketch sk = new KllFloatsSketch();
    byte[] byteArr = sk.toByteArray();
    WritableMemory wmem = WritableMemory.writableWrap(byteArr);
    insertFlags(wmem, SINGLE_ITEM_BIT_MASK);
    insertPreInts(wmem, PREAMBLE_INTS_EMPTY_SINGLE);
    MemoryValidate memVal = new MemoryValidate(wmem);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkInvalidPreIntsAndDouble() {
    KllFloatsSketch sk = new KllFloatsSketch();
    byte[] byteArr = sk.toByteArray();
    WritableMemory wmem = WritableMemory.writableWrap(byteArr);
    insertFlags(wmem, DOUBLES_SKETCH_BIT_MASK);
    insertPreInts(wmem, PREAMBLE_INTS_DOUBLE);
    insertSerVer(wmem, SERIAL_VERSION_SINGLE);
    MemoryValidate memVal = new MemoryValidate(wmem);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkInvalidDoubleCompactAndSingle() {
    KllFloatsSketch sk = new KllFloatsSketch();
    byte[] byteArr = sk.toByteArray();
    WritableMemory wmem = WritableMemory.writableWrap(byteArr);
    insertFlags(wmem, SINGLE_ITEM_BIT_MASK | DOUBLES_SKETCH_BIT_MASK);
    insertPreInts(wmem, PREAMBLE_INTS_EMPTY_SINGLE);
    insertSerVer(wmem, SERIAL_VERSION_EMPTY_FULL);
    MemoryValidate memVal = new MemoryValidate(wmem);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkInvalidDoubleUpdatableAndSerVer() {
    KllFloatsSketch sk = new KllFloatsSketch();
    byte[] byteArr = sk.toByteArray();
    WritableMemory wmem = WritableMemory.writableWrap(byteArr);
    insertSerVer(wmem, SERIAL_VERSION_UPDATABLE);
    insertFlags(wmem, DOUBLES_SKETCH_BIT_MASK | UPDATABLE_BIT_MASK);
    insertPreInts(wmem, PREAMBLE_INTS_DOUBLE - 1);
    MemoryValidate memVal = new MemoryValidate(wmem);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkInvalidFloatFullAndPreInts() {
    KllFloatsSketch sk = new KllFloatsSketch();
    byte[] byteArr = sk.toByteArray();
    WritableMemory wmem = WritableMemory.writableWrap(byteArr);
    insertFlags(wmem, 0); //float full
    insertSerVer(wmem, SERIAL_VERSION_SINGLE); //should be 1
    insertPreInts(wmem, PREAMBLE_INTS_FLOAT);
    MemoryValidate memVal = new MemoryValidate(wmem);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkInvalidFloatUpdatableFullAndPreInts() {
    KllFloatsSketch sk = new KllFloatsSketch();
    byte[] byteArr = sk.toByteArray();
    WritableMemory wmem = WritableMemory.writableWrap(byteArr);
    insertFlags(wmem, UPDATABLE_BIT_MASK); //float updatable full
    insertSerVer(wmem, SERIAL_VERSION_UPDATABLE);
    insertPreInts(wmem, 0);//should be 5
    MemoryValidate memVal = new MemoryValidate(wmem);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkInvalidDoubleCompactSingleAndPreInts() {
    KllFloatsSketch sk = new KllFloatsSketch();
    byte[] byteArr = sk.toByteArray();
    WritableMemory wmem = WritableMemory.writableWrap(byteArr);
    insertFlags(wmem, DOUBLES_SKETCH_BIT_MASK | SINGLE_ITEM_BIT_MASK);
    insertPreInts(wmem, 5);//should be 2
    insertSerVer(wmem, SERIAL_VERSION_SINGLE); //should be 2
    MemoryValidate memVal = new MemoryValidate(wmem);
  }

}

