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

import org.apache.datasketches.tuple.adouble.DoubleSummary;
import org.apache.datasketches.tuple.adouble.DoubleSummary.Mode;
import org.apache.datasketches.tuple.adouble.DoubleSummaryFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
@SuppressWarnings("javadoc")
public class MiscTest {

  @Test
  public void checkUpdatableSketchBuilderReset() {
    final DoubleSummary.Mode mode = Mode.Sum;
    UpdatableSketchBuilder<Double, DoubleSummary> bldr =
        new UpdatableSketchBuilder<>(new DoubleSummaryFactory(mode));
    bldr.reset();
  }

  @Test
  public void checkStringToByteArray() {
    Util.stringToByteArray("");
  }

  @Test
  public void checkDoubleToLongArray() {
    final long[] v = Util.doubleToLongArray(-0.0);
    Assert.assertEquals(v[0], 0);
  }

}
