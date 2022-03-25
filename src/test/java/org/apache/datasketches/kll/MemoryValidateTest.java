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
    insertFamilyID(wmem, 14);
    MemoryValidate memVal = new MemoryValidate(wmem);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkInvalidSerVer() {
    KllFloatsSketch sk = new KllFloatsSketch();
    byte[] byteArr = sk.toByteArray();
    WritableMemory wmem = WritableMemory.writableWrap(byteArr);
    insertSerVer(wmem, 4);
    MemoryValidate memVal = new MemoryValidate(wmem);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkInvalidEmptyAndSingle() {
    KllFloatsSketch sk = new KllFloatsSketch();
    byte[] byteArr = sk.toByteArray();
    WritableMemory wmem = WritableMemory.writableWrap(byteArr);
    insertFlags(wmem, 5);
    MemoryValidate memVal = new MemoryValidate(wmem);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkInvalidUpdatableAndSerVer() {
    KllFloatsSketch sk = new KllFloatsSketch();
    byte[] byteArr = sk.toByteArray();
    WritableMemory wmem = WritableMemory.writableWrap(byteArr);
    insertFlags(wmem, 16);
    insertSerVer(wmem, 2);
    MemoryValidate memVal = new MemoryValidate(wmem);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkInvalidPreIntsAndSingle() {
    KllFloatsSketch sk = new KllFloatsSketch();
    byte[] byteArr = sk.toByteArray();
    WritableMemory wmem = WritableMemory.writableWrap(byteArr);
    insertFlags(wmem, 16);
    insertSerVer(wmem, 2);
    MemoryValidate memVal = new MemoryValidate(wmem);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkInvalidSerVerAndSingle2() {
    KllFloatsSketch sk = new KllFloatsSketch();
    byte[] byteArr = sk.toByteArray();
    WritableMemory wmem = WritableMemory.writableWrap(byteArr);
    insertFlags(wmem, 4);
    insertSerVer(wmem, 1);
    MemoryValidate memVal = new MemoryValidate(wmem);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkInvalidPreIntsAndSingle2() {
    KllFloatsSketch sk = new KllFloatsSketch();
    byte[] byteArr = sk.toByteArray();
    WritableMemory wmem = WritableMemory.writableWrap(byteArr);
    insertFlags(wmem, 4);
    insertPreInts(wmem, 1);
    MemoryValidate memVal = new MemoryValidate(wmem);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkInvalidPreIntsAndDouble() {
    KllFloatsSketch sk = new KllFloatsSketch();
    byte[] byteArr = sk.toByteArray();
    WritableMemory wmem = WritableMemory.writableWrap(byteArr);
    insertFlags(wmem, 8);
    insertPreInts(wmem, 6);
    insertSerVer(wmem, 2);
    MemoryValidate memVal = new MemoryValidate(wmem);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkInvalidDoubleCompactAndSingle() {
    KllFloatsSketch sk = new KllFloatsSketch();
    byte[] byteArr = sk.toByteArray();
    WritableMemory wmem = WritableMemory.writableWrap(byteArr);
    insertFlags(wmem, 12); //double & single
    insertPreInts(wmem, 2);//should be 2
    insertSerVer(wmem, 1); //should be 2
    MemoryValidate memVal = new MemoryValidate(wmem);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkInvalidDoubleUpdatableAndSerVer() {
    KllFloatsSketch sk = new KllFloatsSketch();
    byte[] byteArr = sk.toByteArray();
    WritableMemory wmem = WritableMemory.writableWrap(byteArr);
    insertSerVer(wmem, 3);
    insertFlags(wmem, 24); //double & updatable
    insertPreInts(wmem, 5);//should be 6
    MemoryValidate memVal = new MemoryValidate(wmem);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkInvalidFloatFullAndPreInts() {
    KllFloatsSketch sk = new KllFloatsSketch();
    byte[] byteArr = sk.toByteArray();
    WritableMemory wmem = WritableMemory.writableWrap(byteArr);
    insertFlags(wmem, 0); //float full
    insertSerVer(wmem, 2); //should be 1
    insertPreInts(wmem, 5);//should be 5
    MemoryValidate memVal = new MemoryValidate(wmem);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkInvalidFloatUpdatableFullAndPreInts() {
    KllFloatsSketch sk = new KllFloatsSketch();
    byte[] byteArr = sk.toByteArray();
    WritableMemory wmem = WritableMemory.writableWrap(byteArr);
    insertFlags(wmem, 16); //float updatable full
    insertSerVer(wmem, 3); //should be 3
    insertPreInts(wmem, 6);//should be 5
    MemoryValidate memVal = new MemoryValidate(wmem);
  }

  @Test(expectedExceptions = SketchesArgumentException.class)
  public void checkInvalidDoubleCompactSingleAndPreInts() {
    KllFloatsSketch sk = new KllFloatsSketch();
    byte[] byteArr = sk.toByteArray();
    WritableMemory wmem = WritableMemory.writableWrap(byteArr);
    insertFlags(wmem, 12); //double & single
    insertPreInts(wmem, 5);//should be 2
    insertSerVer(wmem, 2); //should be 2
    MemoryValidate memVal = new MemoryValidate(wmem);
  }

}

