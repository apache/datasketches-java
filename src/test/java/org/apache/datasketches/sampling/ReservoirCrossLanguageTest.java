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

package org.apache.datasketches.sampling;

import org.apache.datasketches.common.ArrayOfDoublesSerDe;
import org.apache.datasketches.common.ArrayOfLongsSerDe;
import org.apache.datasketches.common.ArrayOfStringsSerDe;
import org.apache.datasketches.common.ResizeFactor;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;

import static org.apache.datasketches.common.TestUtil.GENERATE_JAVA_FILES;
import static org.apache.datasketches.common.TestUtil.javaPath;

/**
 * Serialize binary sketches to be tested by other language code.
 * Test deserialization of binary sketches serialized by other language code.
 */
public class ReservoirCrossLanguageTest {

  @Test(groups = {GENERATE_JAVA_FILES})
  public void generateReservoirLongsSketchEmpty() throws IOException {
    final int k = 128;
    final ReservoirLongsSketch sk = ReservoirLongsSketch.newInstance(k);

    Files.newOutputStream(javaPath.resolve("reservoir_longs_empty_k" + k + "_java.sk"))
        .write(sk.toByteArray());
  }

  @Test(groups = {GENERATE_JAVA_FILES})
  public void generateReservoirLongsSketchExact() throws IOException {
    final int k = 128;
    final int[] nArr = {1, 10, 32, 100, 128};

    for (final int n : nArr) {
      final ReservoirLongsSketch sk = ReservoirLongsSketch.newInstance(k);
      for (int i = 0; i < n; i++) {
        sk.update(i);
      }
      Files.newOutputStream(javaPath.resolve("reservoir_longs_exact_n" + n + "_k" + k + "_java.sk"))
          .write(sk.toByteArray());
    }
  }

  @Test(groups = {GENERATE_JAVA_FILES})
  public void generateReservoirLongsSketchSampling() throws IOException {
    final int[] kArr = {32, 64, 128};
    final long n = 1000;

    for (final int k : kArr) {
      final long[] predeterminedSamples = new long[k];
      for (int i = 0; i < k; i++) {
        predeterminedSamples[i] = i * 2;
      }

      final ReservoirLongsSketch sk = ReservoirLongsSketch.getInstance(
          predeterminedSamples,
          n,
          ResizeFactor.X8,
          k
      );

      Files.newOutputStream(javaPath.resolve("reservoir_longs_sampling_n" + n + "_k" + k + "_java.sk"))
          .write(sk.toByteArray());
    }
  }

  @Test(groups = {GENERATE_JAVA_FILES})
  public void generateReservoirLongsUnionEmpty() throws IOException {
    int maxK = 128;
    ReservoirLongsUnion union = ReservoirLongsUnion.newInstance(maxK);

    Files.newOutputStream(javaPath.resolve("reservoir_longs_union_empty_maxk" + maxK + "_java.sk"))
        .write(union.toByteArray());
  }

  @Test(groups = {GENERATE_JAVA_FILES})
  public void generateReservoirLongsUnionExact() throws IOException {
    int maxK = 128;
    int[] nArr = {1, 10, 32, 100, 128};

    for (int n : nArr) {
      ReservoirLongsUnion union = ReservoirLongsUnion.newInstance(maxK);
      for (int i = 0; i < n; i++) {
        union.update(i);
      }
      Files.newOutputStream(javaPath.resolve("reservoir_longs_union_exact_n" + n + "_maxk" + maxK + "_java.sk"))
          .write(union.toByteArray());
    }
  }

  @Test(groups = {GENERATE_JAVA_FILES})
  public void generateReservoirLongsUnionSampling() throws IOException {
    int[] maxKArr = {32, 64, 128};
    long n = 1000;

    for (int maxK : maxKArr) {
      long[] predeterminedSamples = new long[maxK];
      for (int i = 0; i < maxK; i++) {
        predeterminedSamples[i] = i * 2;
      }

      ReservoirLongsSketch sk = ReservoirLongsSketch.getInstance(
          predeterminedSamples,
          n,
          ResizeFactor.X8,
          maxK
      );

      ReservoirLongsUnion union = ReservoirLongsUnion.newInstance(maxK);
      union.update(sk);

      Files.newOutputStream(javaPath.resolve("reservoir_longs_union_sampling_n" + n + "_maxk" + maxK + "_java.sk"))
          .write(union.toByteArray());
    }
  }

  @Test(groups = {GENERATE_JAVA_FILES})
  public void generateReservoirItemsSketchLongEmpty() throws IOException {
    final int k = 128;
    final ReservoirItemsSketch<Long> sk = ReservoirItemsSketch.newInstance(k);

    Files.newOutputStream(javaPath.resolve("reservoir_items_long_empty_k" + k + "_java.sk"))
        .write(sk.toByteArray(new ArrayOfLongsSerDe()));
  }

  @Test(groups = {GENERATE_JAVA_FILES})
  public void generateReservoirItemsSketchLongExact() throws IOException {
    final int k = 128;
    final int[] nArr = {1, 10, 32, 100, 128};

    for (final int n : nArr) {
      final ReservoirItemsSketch<Long> sk = ReservoirItemsSketch.newInstance(k);
      for (int i = 0; i < n; i++) {
        sk.update((long) i);
      }
      Files.newOutputStream(javaPath.resolve("reservoir_items_long_exact_n" + n + "_k" + k + "_java.sk"))
          .write(sk.toByteArray(new ArrayOfLongsSerDe()));
    }
  }

  @Test(groups = {GENERATE_JAVA_FILES})
  public void generateReservoirItemsSketchLongSampling() throws IOException {
    final int[] kArr = {32, 64, 128};
    final long n = 1000;

    for (final int k : kArr) {
      final java.util.ArrayList<Long> predeterminedSamples = new java.util.ArrayList<>();
      for (int i = 0; i < k; i++) {
        predeterminedSamples.add((long) (i * 2));
      }

      final ReservoirItemsSketch<Long> sk = ReservoirItemsSketch.newInstance(
          predeterminedSamples,
          n,
          ResizeFactor.X8,
          k
      );

      Files.newOutputStream(javaPath.resolve("reservoir_items_long_sampling_n" + n + "_k" + k + "_java.sk"))
          .write(sk.toByteArray(new ArrayOfLongsSerDe()));
    }
  }

  @Test(groups = {GENERATE_JAVA_FILES})
  public void generateReservoirItemsSketchDoubleEmpty() throws IOException {
    final int k = 128;
    final ReservoirItemsSketch<Double> sk = ReservoirItemsSketch.newInstance(k);

    Files.newOutputStream(javaPath.resolve("reservoir_items_double_empty_k" + k + "_java.sk"))
        .write(sk.toByteArray(new ArrayOfDoublesSerDe()));
  }

  @Test(groups = {GENERATE_JAVA_FILES})
  public void generateReservoirItemsSketchDoubleExact() throws IOException {
    final int k = 128;
    final int[] nArr = {1, 10, 32, 100, 128};

    for (final int n : nArr) {
      final ReservoirItemsSketch<Double> sk = ReservoirItemsSketch.newInstance(k);
      for (int i = 0; i < n; i++) {
        sk.update((double) i);
      }
      Files.newOutputStream(javaPath.resolve("reservoir_items_double_exact_n" + n + "_k" + k + "_java.sk"))
          .write(sk.toByteArray(new ArrayOfDoublesSerDe()));
    }
  }

  @Test(groups = {GENERATE_JAVA_FILES})
  public void generateReservoirItemsSketchDoubleSampling() throws IOException {
    final int[] kArr = {32, 64, 128};
    final long n = 1000;

    for (final int k : kArr) {
      final java.util.ArrayList<Double> predeterminedSamples = new java.util.ArrayList<>();
      for (int i = 0; i < k; i++) {
        predeterminedSamples.add((double) (i * 2));
      }

      final ReservoirItemsSketch<Double> sk = ReservoirItemsSketch.newInstance(
          predeterminedSamples,
          n,
          ResizeFactor.X8,
          k
      );

      Files.newOutputStream(javaPath.resolve("reservoir_items_double_sampling_n" + n + "_k" + k + "_java.sk"))
          .write(sk.toByteArray(new ArrayOfDoublesSerDe()));
    }
  }

  @Test(groups = {GENERATE_JAVA_FILES})
  public void generateReservoirItemsSketchStringEmpty() throws IOException {
    final int k = 128;
    final ReservoirItemsSketch<String> sk = ReservoirItemsSketch.newInstance(k);

    Files.newOutputStream(javaPath.resolve("reservoir_items_string_empty_k" + k + "_java.sk"))
        .write(sk.toByteArray(new ArrayOfStringsSerDe()));
  }

  @Test(groups = {GENERATE_JAVA_FILES})
  public void generateReservoirItemsSketchStringExact() throws IOException {
    final int k = 128;
    final int[] nArr = {1, 10, 32, 100, 128};

    for (final int n : nArr) {
      final ReservoirItemsSketch<String> sk = ReservoirItemsSketch.newInstance(k);
      for (int i = 0; i < n; i++) {
        sk.update("item" + i);
      }
      Files.newOutputStream(javaPath.resolve("reservoir_items_string_exact_n" + n + "_k" + k + "_java.sk"))
          .write(sk.toByteArray(new ArrayOfStringsSerDe()));
    }
  }

  @Test(groups = {GENERATE_JAVA_FILES})
  public void generateReservoirItemsSketchStringSampling() throws IOException {
    final int[] kArr = {32, 64, 128};
    final long n = 1000;

    for (final int k : kArr) {
      final java.util.ArrayList<String> predeterminedSamples = new java.util.ArrayList<>();
      for (int i = 0; i < k; i++) {
        predeterminedSamples.add("item" + (i * 2));
      }

      final ReservoirItemsSketch<String> sk = ReservoirItemsSketch.newInstance(
          predeterminedSamples,
          n,
          ResizeFactor.X8,
          k
      );

      Files.newOutputStream(javaPath.resolve("reservoir_items_string_sampling_n" + n + "_k" + k + "_java.sk"))
          .write(sk.toByteArray(new ArrayOfStringsSerDe()));
    }
  }

  @Test(groups = {GENERATE_JAVA_FILES})
  public void generateReservoirItemsUnionLongEmpty() throws IOException {
    int maxK = 128;
    ReservoirItemsUnion<Long> union = ReservoirItemsUnion.newInstance(maxK);

    Files.newOutputStream(javaPath.resolve("reservoir_items_union_long_empty_maxk" + maxK + "_java.sk"))
        .write(union.toByteArray(new ArrayOfLongsSerDe()));
  }

  @Test(groups = {GENERATE_JAVA_FILES})
  public void generateReservoirItemsUnionLongExact() throws IOException {
    int maxK = 128;
    int[] nArr = {1, 10, 32, 100, 128};

    for (int n : nArr) {
      ReservoirItemsUnion<Long> union = ReservoirItemsUnion.newInstance(maxK);
      for (int i = 0; i < n; i++) {
        union.update((long) i);
      }
      Files.newOutputStream(javaPath.resolve("reservoir_items_union_long_exact_n" + n + "_maxk" + maxK + "_java.sk"))
          .write(union.toByteArray(new ArrayOfLongsSerDe()));
    }
  }

  @Test(groups = {GENERATE_JAVA_FILES})
  public void generateReservoirItemsUnionLongSampling() throws IOException {
    int[] maxKArr = {32, 64, 128};
    long n = 1000;

    for (int maxK : maxKArr) {
      ArrayList<Long> predeterminedSamples = new ArrayList<>();
      for (int i = 0; i < maxK; i++) {
        predeterminedSamples.add((long) (i * 2));
      }

      ReservoirItemsSketch<Long> sk = ReservoirItemsSketch.newInstance(
          predeterminedSamples,
          n,
          ResizeFactor.X8,
          maxK
      );

      ReservoirItemsUnion<Long> union = ReservoirItemsUnion.newInstance(maxK);
      union.update(sk);

      Files.newOutputStream(javaPath.resolve("reservoir_items_union_long_sampling_n" + n + "_maxk" + maxK + "_java.sk"))
          .write(union.toByteArray(new ArrayOfLongsSerDe()));
    }
  }

  @Test(groups = {GENERATE_JAVA_FILES})
  public void generateReservoirItemsUnionDoubleEmpty() throws IOException {
    int maxK = 128;
    ReservoirItemsUnion<Double> union = ReservoirItemsUnion.newInstance(maxK);

    Files.newOutputStream(javaPath.resolve("reservoir_items_union_double_empty_maxk" + maxK + "_java.sk"))
        .write(union.toByteArray(new ArrayOfDoublesSerDe()));
  }

  @Test(groups = {GENERATE_JAVA_FILES})
  public void generateReservoirItemsUnionDoubleExact() throws IOException {
    int maxK = 128;
    int[] nArr = {1, 10, 32, 100, 128};

    for (int n : nArr) {
      ReservoirItemsUnion<Double> union = ReservoirItemsUnion.newInstance(maxK);
      for (int i = 0; i < n; i++) {
        union.update((double) i);
      }
      Files.newOutputStream(javaPath.resolve("reservoir_items_union_double_exact_n" + n + "_maxk" + maxK + "_java.sk"))
          .write(union.toByteArray(new ArrayOfDoublesSerDe()));
    }
  }

  @Test(groups = {GENERATE_JAVA_FILES})
  public void generateReservoirItemsUnionDoubleSampling() throws IOException {
    int[] maxKArr = {32, 64, 128};
    long n = 1000;

    for (int maxK : maxKArr) {
      ArrayList<Double> predeterminedSamples = new ArrayList<>();
      for (int i = 0; i < maxK; i++) {
        predeterminedSamples.add((double) (i * 2));
      }

      ReservoirItemsSketch<Double> sk = ReservoirItemsSketch.newInstance(
          predeterminedSamples,
          n,
          ResizeFactor.X8,
          maxK
      );

      ReservoirItemsUnion<Double> union = ReservoirItemsUnion.newInstance(maxK);
      union.update(sk);

      Files.newOutputStream(javaPath.resolve("reservoir_items_union_double_sampling_n" + n + "_maxk" + maxK + "_java.sk"))
          .write(union.toByteArray(new ArrayOfDoublesSerDe()));
    }
  }

  @Test(groups = {GENERATE_JAVA_FILES})
  public void generateReservoirItemsUnionStringEmpty() throws IOException {
    int maxK = 128;
    ReservoirItemsUnion<String> union = ReservoirItemsUnion.newInstance(maxK);

    Files.newOutputStream(javaPath.resolve("reservoir_items_union_string_empty_maxk" + maxK + "_java.sk"))
        .write(union.toByteArray(new ArrayOfStringsSerDe()));
  }

  @Test(groups = {GENERATE_JAVA_FILES})
  public void generateReservoirItemsUnionStringExact() throws IOException {
    int maxK = 128;
    int[] nArr = {1, 10, 32, 100, 128};

    for (int n : nArr) {
      ReservoirItemsUnion<String> union = ReservoirItemsUnion.newInstance(maxK);
      for (int i = 0; i < n; i++) {
        union.update("item" + i);
      }
      Files.newOutputStream(javaPath.resolve("reservoir_items_union_string_exact_n" + n + "_maxk" + maxK + "_java.sk"))
          .write(union.toByteArray(new ArrayOfStringsSerDe()));
    }
  }

  @Test(groups = {GENERATE_JAVA_FILES})
  public void generateReservoirItemsUnionStringSampling() throws IOException {
    int[] maxKArr = {32, 64, 128};
    long n = 1000;

    for (int maxK : maxKArr) {
      ArrayList<String> predeterminedSamples = new ArrayList<>();
      for (int i = 0; i < maxK; i++) {
        predeterminedSamples.add("item" + (i * 2));
      }

      ReservoirItemsSketch<String> sk = ReservoirItemsSketch.newInstance(
          predeterminedSamples,
          n,
          ResizeFactor.X8,
          maxK
      );

      ReservoirItemsUnion<String> union = ReservoirItemsUnion.newInstance(maxK);
      union.update(sk);

      Files.newOutputStream(javaPath.resolve("reservoir_items_union_string_sampling_n" + n + "_maxk" + maxK + "_java.sk"))
          .write(union.toByteArray(new ArrayOfStringsSerDe()));
    }
  }
}
