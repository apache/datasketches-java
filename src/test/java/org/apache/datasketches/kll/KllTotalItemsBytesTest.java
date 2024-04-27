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

import org.apache.datasketches.common.*;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.function.BiConsumer;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class KllTotalItemsBytesTest {

    @DataProvider(name = "emptySketchesItems")
    public Object[][] emptySketches() {
        return new Object[][]{
                new Object[]{KllItemsSketch.newHeapInstance(Double::compareTo, new ArrayOfDoublesSerDe())},
                new Object[]{KllItemsSketch.newHeapInstance(Long::compareTo, new ArrayOfLongsSerDe())},
                new Object[]{KllItemsSketch.newHeapInstance(String::compareTo, new ArrayOfStringsSerDe())},
                new Object[]{KllItemsSketch.newHeapInstance(String::compareTo, new ArrayOfUtf16StringsSerDe())},
                new Object[]{KllItemsSketch.newHeapInstance(Boolean::compareTo, new ArrayOfBooleansSerDe())},
        };
    }

    @Test(dataProvider = "emptySketchesItems")
    public void testEmptySketches(KllSketch sketch) {
        assertEquals(sketch.getTotalItemsNumBytes(), 0);
    }

    @DataProvider(name = "emptySketchesNative")
    public Object[][] emptySketchesNative() {
        return new Object[][]{
                new Object[]{KllFloatsSketch.newHeapInstance(), Float.BYTES},
                new Object[]{KllDoublesSketch.newHeapInstance(), Double.BYTES},
        };
    }

    @Test(dataProvider = "emptySketchesNative")
    public void testEmptySketchesNative(KllSketch sketch, int singleItemSize) {
        assertEquals(sketch.getTotalItemsNumBytes(), singleItemSize * sketch.getK());
    }


    @SuppressWarnings({"rawtypes", "unchecked"})
    @DataProvider(name = "sketchSingleItem")
    public Object[][] sketchSingleItem() {
        return new Object[][]{
                new Object[]{KllItemsSketch.newHeapInstance(Double::compareTo, new ArrayOfDoublesSerDe()), 1.0d, (BiConsumer<KllItemsSketch, Double>) KllItemsSketch::update},
                new Object[]{KllItemsSketch.newHeapInstance(Long::compareTo, new ArrayOfLongsSerDe()), 1L, (BiConsumer<KllItemsSketch, Long>) KllItemsSketch::update},
                new Object[]{KllItemsSketch.newHeapInstance(Boolean::compareTo, new ArrayOfBooleansSerDe()), true, (BiConsumer<KllItemsSketch, Boolean>) KllItemsSketch::update},
                new Object[]{KllItemsSketch.newHeapInstance(String::compareTo, new ArrayOfStringsSerDe()), "1", (BiConsumer<KllItemsSketch, String>) KllItemsSketch::update},
                new Object[]{KllItemsSketch.newHeapInstance(String::compareTo, new ArrayOfUtf16StringsSerDe()), "1", (BiConsumer<KllItemsSketch, String>) KllItemsSketch::update},
        };
    }

    @SuppressWarnings("unchecked")
    @Test(dataProvider = "sketchSingleItem")
    public void testSingleItemIncreases(KllSketch sketch, Object item, BiConsumer<KllSketch, Object> update) {
        assertEquals(sketch.getTotalItemsNumBytes(), 0);
        assertEquals(sketch.getNumRetained(), 0);
        update.accept(sketch, item);
        assertTrue(sketch.getNumRetained() > 0);
        if (sketch.getSerDe().isFixedWidth()) {
            assertEquals(((ArrayOfItemsSerDe<Object>) sketch.getSerDe()).sizeOf(item) * sketch.getK(), sketch.getTotalItemsNumBytes());
        } else {
            assertEquals(((ArrayOfItemsSerDe<Object>) sketch.getSerDe()).sizeOf(item), sketch.getTotalItemsNumBytes());
        }
    }

    @Test(dataProvider = "sketchSingleItem")
    public void testManyItemIncreases(KllSketch sketch, Object item, BiConsumer<KllSketch, Object> update) {
        assertEquals(sketch.getTotalItemsNumBytes(), 0);
        assertEquals(sketch.getNumRetained(), 0);
        for (int i = 0; i < 4096; i++) {
            update.accept(sketch, item);
        }
        assertTrue(sketch.getNumRetained() > 0);
        assertTrue(sketch.getTotalItemsNumBytes() > 0);
    }
}
