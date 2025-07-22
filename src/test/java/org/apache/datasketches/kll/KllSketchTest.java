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

import static org.apache.datasketches.kll.KllSketch.SketchStructure.COMPACT_EMPTY;
import static org.apache.datasketches.kll.KllSketch.SketchStructure.COMPACT_FULL;
import static org.apache.datasketches.kll.KllSketch.SketchStructure.COMPACT_SINGLE;
import static org.apache.datasketches.kll.KllSketch.SketchStructure.UPDATABLE;
import static org.apache.datasketches.kll.KllSketch.SketchStructure.getSketchStructure;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import org.apache.datasketches.common.SketchesArgumentException;
import org.testng.annotations.Test;

public class KllSketchTest {

  @Test
  public void checkSketchStructureEnum() {
    assertEquals(getSketchStructure(2,1), COMPACT_EMPTY);
    assertEquals(getSketchStructure(2,2), COMPACT_SINGLE);
    assertEquals(getSketchStructure(5,1), COMPACT_FULL);
    assertEquals(getSketchStructure(5,3), UPDATABLE);
    try { getSketchStructure(5,2); fail(); } catch (final SketchesArgumentException e) { }
    try { getSketchStructure(2,3); fail(); } catch (final SketchesArgumentException e) { }
  }

  private final static boolean enablePrinting = false;

  /**
   * @param o the Object to println
   */
  static final void println(final Object o) {
    if (enablePrinting) { System.out.println(o.toString()); }
  }

}

