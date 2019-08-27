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

package org.apache.datasketches.cpc;

import static org.apache.datasketches.cpc.CompressionData.decodingTablesForHighEntropyByte;
import static org.apache.datasketches.cpc.CompressionData.encodingTablesForHighEntropyByte;
import static org.apache.datasketches.cpc.CompressionData.lengthLimitedUnaryDecodingTable65;
import static org.apache.datasketches.cpc.CompressionData.lengthLimitedUnaryEncodingTable65;
import static org.apache.datasketches.cpc.CompressionData.validateDecodingTable;

import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
@SuppressWarnings("javadoc")
public class CompressionDataTest {

  @Test
  public static void checkTables() {
    validateDecodingTable(lengthLimitedUnaryDecodingTable65, lengthLimitedUnaryEncodingTable65);

    for (int i = 0; i < (16 + 6); i++) {
      validateDecodingTable(decodingTablesForHighEntropyByte[i], encodingTablesForHighEntropyByte[i]);
    }
  }

}
