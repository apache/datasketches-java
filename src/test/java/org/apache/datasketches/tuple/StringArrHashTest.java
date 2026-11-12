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


package org.apache.datasketches.tuple;

import static java.nio.charset.StandardCharsets.UTF_16LE;
import static org.testng.Assert.assertEquals;

import org.apache.datasketches.hash.XxHash64;
import org.testng.annotations.Test;

/**
 * Checks the key hash of the Array of Strings tuple sketch against the definition in the javadoc of
 * {@link Util#stringArrHash(String[])}.
 */
public class StringArrHashTest {
  private static final long SEED = 0x7A3CCA71L;

  private static long expected(final String joined) {
    final byte[] bytes = joined.getBytes(UTF_16LE); // no byte-order mark
    return XxHash64.hashBytes(bytes, 0, bytes.length, SEED);
  }

  @Test
  public void hashesCommaJoinedUtf16LittleEndian() {
    assertEquals(Util.stringArrHash(new String[] { "a" }), expected("a"));
    assertEquals(Util.stringArrHash(new String[] { "a", "b", "c" }), expected("a,b,c"));
    assertEquals(Util.stringArrHash(new String[] { "" }), expected(""));
    assertEquals(Util.stringArrHash(new String[] { }), expected(""));
    assertEquals(Util.stringArrHash(new String[] { "é", "日本", "😀" }),
        expected("é,日本,😀"));
  }

  @Test
  public void commasAreNotEscaped() {
    assertEquals(Util.stringArrHash(new String[] { "a,b" }), Util.stringArrHash(new String[] { "a", "b" }));
  }

  @Test
  public void nullElementIsHashedAsTheWordNull() {
    assertEquals(Util.stringArrHash(new String[] { "a", null }), expected("a,null"));
  }
}
