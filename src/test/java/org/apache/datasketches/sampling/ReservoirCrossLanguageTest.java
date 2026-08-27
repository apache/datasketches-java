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

import static org.apache.datasketches.common.UtilityIO.CHECK_CPP_FILES;
import static org.apache.datasketches.common.UtilityIO.CHECK_GO_FILES;
import static org.apache.datasketches.common.UtilityIO.CHECK_JAVA_FILES;
import static org.apache.datasketches.common.UtilityIO.GENERATE_JAVA_FILES;
import static org.apache.datasketches.common.UtilityIO.getFileBytes;
import static org.apache.datasketches.common.UtilityIO.putBytesToJavaPath;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.ArrayList;

import org.apache.datasketches.common.ArrayOfDoublesSerDe;
import org.apache.datasketches.common.ArrayOfLongsSerDe;
import org.apache.datasketches.common.ArrayOfStringsSerDe;
import org.apache.datasketches.common.ResizeFactor;
import org.apache.datasketches.common.UtilityIO.GroupLanguage;
import org.testng.annotations.Test;

/**
 * Serialize binary sketches to be tested by other language code.
 * Test deserialization of binary sketches serialized by other language code.
 */
public class ReservoirCrossLanguageTest {

  @Test(groups = {GENERATE_JAVA_FILES})
  public void generateReservoirLongsSketchEmpty() throws IOException { //1
    final int k = 128;
    final ReservoirLongsSketch sk = ReservoirLongsSketch.newInstance(k);
    putBytesToJavaPath("reservoir_longs_empty_k" + k + "_java.sk",  sk.toByteArray());
  }

  @Test(groups = {GENERATE_JAVA_FILES})
  public void generateReservoirLongsSketchExact() throws IOException { //2
    final int k = 128;
    final int[] nArr = {1, 10, 32, 100, 128};

    for (final int n : nArr) {
      final ReservoirLongsSketch sk = ReservoirLongsSketch.newInstance(k);
      for (int i = 0; i < n; i++) {
        sk.update(i);
      }

      putBytesToJavaPath("reservoir_longs_exact_n" + n + "_k" + k + "_java.sk",  sk.toByteArray());
    }
  }

  @Test(groups = {GENERATE_JAVA_FILES})
  public void generateReservoirLongsSketchSampling() throws IOException { //3
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

      putBytesToJavaPath("reservoir_longs_sampling_n" + n + "_k" + k + "_java.sk",  sk.toByteArray());
    }
  }

  @Test(groups = {GENERATE_JAVA_FILES})
  public void generateReservoirLongsUnionEmpty() throws IOException { //4
    int maxK = 128;
    ReservoirLongsUnion union = ReservoirLongsUnion.newInstance(maxK);

    putBytesToJavaPath("reservoir_longs_union_empty_maxk" + maxK + "_java.sk",  union.toByteArray());
  }

  @Test(groups = {GENERATE_JAVA_FILES})
  public void generateReservoirLongsUnionExact() throws IOException { //5
    int maxK = 128;
    int[] nArr = {1, 10, 32, 100, 128};

    for (int n : nArr) {
      ReservoirLongsUnion union = ReservoirLongsUnion.newInstance(maxK);
      for (int i = 0; i < n; i++) {
        union.update(i);
      }
      putBytesToJavaPath("reservoir_longs_union_exact_n" + n + "_maxk" + maxK + "_java.sk",  union.toByteArray());
    }
  }

  @Test(groups = {GENERATE_JAVA_FILES})
  public void generateReservoirLongsUnionSampling() throws IOException { //6
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

      putBytesToJavaPath("reservoir_longs_union_sampling_n" + n + "_maxk" + maxK + "_java.sk",  union.toByteArray());
    }
  }

  @Test(groups = {GENERATE_JAVA_FILES})
  public void generateReservoirItemsSketchLongEmpty() throws IOException { //7
    final int k = 128;
    final ReservoirItemsSketch<Long> sk = ReservoirItemsSketch.newInstance(k);

    putBytesToJavaPath("reservoir_items_long_empty_k" + k + "_java.sk",  sk.toByteArray(new ArrayOfLongsSerDe()));
  }

  @Test(groups = {GENERATE_JAVA_FILES})
  public void generateReservoirItemsSketchLongExact() throws IOException { //8
    final int k = 128;
    final int[] nArr = {1, 10, 32, 100, 128};

    for (final int n : nArr) {
      final ReservoirItemsSketch<Long> sk = ReservoirItemsSketch.newInstance(k);
      for (int i = 0; i < n; i++) {
        sk.update((long) i);
      }
      putBytesToJavaPath("reservoir_items_long_exact_n" + n + "_k" + k + "_java.sk",  sk.toByteArray(new ArrayOfLongsSerDe()));
    }
  }

  @Test(groups = {GENERATE_JAVA_FILES})
  public void generateReservoirItemsSketchLongSampling() throws IOException { //9
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

      putBytesToJavaPath("reservoir_items_long_sampling_n" + n + "_k" + k + "_java.sk",  sk.toByteArray(new ArrayOfLongsSerDe()));
    }
  }

  @Test(groups = {GENERATE_JAVA_FILES})
  public void generateReservoirItemsSketchDoubleEmpty() throws IOException { //10
    final int k = 128;
    final ReservoirItemsSketch<Double> sk = ReservoirItemsSketch.newInstance(k);

    putBytesToJavaPath("reservoir_items_double_empty_k" + k + "_java.sk",  sk.toByteArray(new ArrayOfDoublesSerDe()));
  }

  @Test(groups = {GENERATE_JAVA_FILES})
  public void generateReservoirItemsSketchDoubleExact() throws IOException { //11
    final int k = 128;
    final int[] nArr = {1, 10, 32, 100, 128};

    for (final int n : nArr) {
      final ReservoirItemsSketch<Double> sk = ReservoirItemsSketch.newInstance(k);
      for (int i = 0; i < n; i++) {
        sk.update((double) i);
      }
      putBytesToJavaPath("reservoir_items_double_exact_n" + n + "_k" + k + "_java.sk",  sk.toByteArray(new ArrayOfDoublesSerDe()));
    }
  }

  @Test(groups = {GENERATE_JAVA_FILES})
  public void generateReservoirItemsSketchDoubleSampling() throws IOException { //12
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

      putBytesToJavaPath("reservoir_items_double_sampling_n" + n + "_k" + k + "_java.sk",  sk.toByteArray(new ArrayOfDoublesSerDe()));
    }
  }

  @Test(groups = {GENERATE_JAVA_FILES})
  public void generateReservoirItemsSketchStringEmpty() throws IOException { //13
    final int k = 128;
    final ReservoirItemsSketch<String> sk = ReservoirItemsSketch.newInstance(k);

    putBytesToJavaPath("reservoir_items_string_empty_k" + k + "_java.sk",  sk.toByteArray(new ArrayOfStringsSerDe()));
  }

  @Test(groups = {GENERATE_JAVA_FILES})
  public void generateReservoirItemsSketchStringExact() throws IOException { //14
    final int k = 128;
    final int[] nArr = {1, 10, 32, 100, 128};

    for (final int n : nArr) {
      final ReservoirItemsSketch<String> sk = ReservoirItemsSketch.newInstance(k);
      for (int i = 0; i < n; i++) {
        sk.update("item" + i);
      }
      putBytesToJavaPath("reservoir_items_string_exact_n" + n + "_k" + k + "_java.sk",  sk.toByteArray(new ArrayOfStringsSerDe()));
    }
  }

  @Test(groups = {GENERATE_JAVA_FILES})
  public void generateReservoirItemsSketchStringSampling() throws IOException { //15
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
      putBytesToJavaPath("reservoir_items_string_sampling_n" + n + "_k" + k + "_java.sk",  sk.toByteArray(new ArrayOfStringsSerDe()));
    }
  }

  @Test(groups = {GENERATE_JAVA_FILES})
  public void generateReservoirItemsUnionLongEmpty() throws IOException { //16
    int maxK = 128;
    ReservoirItemsUnion<Long> union = ReservoirItemsUnion.newInstance(maxK);

    putBytesToJavaPath("reservoir_items_union_long_empty_maxk" + maxK + "_java.sk",  union.toByteArray(new ArrayOfLongsSerDe()));
  }

  @Test(groups = {GENERATE_JAVA_FILES})
  public void generateReservoirItemsUnionLongExact() throws IOException { //17
    int maxK = 128;
    int[] nArr = {1, 10, 32, 100, 128};

    for (int n : nArr) {
      ReservoirItemsUnion<Long> union = ReservoirItemsUnion.newInstance(maxK);
      for (int i = 0; i < n; i++) {
        union.update((long) i);
      }
      putBytesToJavaPath("reservoir_items_union_long_exact_n" + n + "_maxk" + maxK + "_java.sk",
          union.toByteArray(new ArrayOfLongsSerDe()));
    }
  }

  @Test(groups = {GENERATE_JAVA_FILES})
  public void generateReservoirItemsUnionLongSampling() throws IOException { //18
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

      putBytesToJavaPath("reservoir_items_union_long_sampling_n" + n + "_maxk" + maxK + "_java.sk",
          union.toByteArray(new ArrayOfLongsSerDe()));
    }
  }

  @Test(groups = {GENERATE_JAVA_FILES})
  public void generateReservoirItemsUnionDoubleEmpty() throws IOException { //19
    int maxK = 128;
    ReservoirItemsUnion<Double> union = ReservoirItemsUnion.newInstance(maxK);

    putBytesToJavaPath("reservoir_items_union_double_empty_maxk" + maxK + "_java.sk",  union.toByteArray(new ArrayOfDoublesSerDe()));
  }

  @Test(groups = {GENERATE_JAVA_FILES})
  public void generateReservoirItemsUnionDoubleExact() throws IOException { //20
    int maxK = 128;
    int[] nArr = {1, 10, 32, 100, 128};

    for (int n : nArr) {
      ReservoirItemsUnion<Double> union = ReservoirItemsUnion.newInstance(maxK);
      for (int i = 0; i < n; i++) {
        union.update((double) i);
      }
      putBytesToJavaPath("reservoir_items_union_double_exact_n" + n + "_maxk" + maxK + "_java.sk",
          union.toByteArray(new ArrayOfDoublesSerDe()));
    }
  }

  @Test(groups = {GENERATE_JAVA_FILES})
  public void generateReservoirItemsUnionDoubleSampling() throws IOException { //21
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

      putBytesToJavaPath("reservoir_items_union_double_sampling_n" + n + "_maxk" + maxK + "_java.sk",
          union.toByteArray(new ArrayOfDoublesSerDe()));
    }
  }

  @Test(groups = {GENERATE_JAVA_FILES})
  public void generateReservoirItemsUnionStringEmpty() throws IOException { //22
    int maxK = 128;
    ReservoirItemsUnion<String> union = ReservoirItemsUnion.newInstance(maxK);

    putBytesToJavaPath("reservoir_items_union_string_empty_maxk" + maxK + "_java.sk",  union.toByteArray(new ArrayOfStringsSerDe()));
  }

  @Test(groups = {GENERATE_JAVA_FILES})
  public void generateReservoirItemsUnionStringExact() throws IOException { //23
    int maxK = 128;
    int[] nArr = {1, 10, 32, 100, 128};

    for (int n : nArr) {
      ReservoirItemsUnion<String> union = ReservoirItemsUnion.newInstance(maxK);
      for (int i = 0; i < n; i++) {
        union.update("item" + i);
      }
      putBytesToJavaPath("reservoir_items_union_string_exact_n" + n + "_maxk" + maxK + "_java.sk",
          union.toByteArray(new ArrayOfStringsSerDe()));
    }
  }

  @Test(groups = {GENERATE_JAVA_FILES})
  public void generateReservoirItemsUnionStringSampling() throws IOException { //24
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

      putBytesToJavaPath("reservoir_items_union_string_sampling_n" + n + "_maxk" + maxK + "_java.sk",
          union.toByteArray(new ArrayOfStringsSerDe()));
    }
  }
  /*****************************************************/
  /*****************************************************/

  @Test(groups = {CHECK_JAVA_FILES})
  public void checkJava() {
    checkReservoirLongsSketchEmpty(GroupLanguage.JAVA);
    checkReservoirLongsSketchExact(GroupLanguage.JAVA);
    checkReservoirLongsSketchSampling(GroupLanguage.JAVA);
    checkReservoirLongsUnionEmpty(GroupLanguage.JAVA);
    checkReservoirLongsUnionExact(GroupLanguage.JAVA);
    checkReservoirLongsUnionSampling(GroupLanguage.JAVA);
    checkReservoirItemsSketchLongEmpty(GroupLanguage.JAVA);
    checkReservoirItemsSketchLongExact(GroupLanguage.JAVA);
    checkReservoirItemsSketchLongSampling(GroupLanguage.JAVA);
    checkReservoirItemsSketchDoubleEmpty(GroupLanguage.JAVA);
    checkReservoirItemsSketchDoubleExact(GroupLanguage.JAVA);
    checkReservoirItemsSketchDoubleSampling(GroupLanguage.JAVA);
    checkReservoirItemsSketchStringEmpty(GroupLanguage.JAVA);
    checkReservoirItemsSketchStringExact(GroupLanguage.JAVA);
    checkReservoirItemsSketchStringSampling(GroupLanguage.JAVA);
    checkReservoirItemsUnionLongEmpty(GroupLanguage.JAVA);
    checkReservoirItemsUnionLongExact(GroupLanguage.JAVA);
    checkReservoirItemsUnionLongSampling(GroupLanguage.JAVA);
    checkReservoirItemsUnionDoubleEmpty(GroupLanguage.JAVA);
    checkReservoirItemsUnionDoubleExact(GroupLanguage.JAVA);
    checkReservoirItemsUnionDoubleSampling(GroupLanguage.JAVA);
    checkReservoirItemsUnionStringEmpty(GroupLanguage.JAVA);
    checkReservoirItemsUnionStringExact(GroupLanguage.JAVA);
    checkReservoirItemsUnionStringSampling(GroupLanguage.JAVA);
  }

  @Test(groups = {CHECK_CPP_FILES})
  public void checkCpp() {
    checkReservoirLongsSketchEmpty(GroupLanguage.CPP);
    checkReservoirLongsSketchExact(GroupLanguage.CPP);
    checkReservoirLongsSketchSampling(GroupLanguage.CPP);
    checkReservoirLongsUnionEmpty(GroupLanguage.CPP);
    checkReservoirLongsUnionExact(GroupLanguage.CPP);
    checkReservoirLongsUnionSampling(GroupLanguage.CPP);
    checkReservoirItemsSketchLongEmpty(GroupLanguage.CPP);
    checkReservoirItemsSketchLongExact(GroupLanguage.CPP);
    checkReservoirItemsSketchLongSampling(GroupLanguage.CPP);
    checkReservoirItemsSketchDoubleEmpty(GroupLanguage.CPP);
    checkReservoirItemsSketchDoubleExact(GroupLanguage.CPP);
    checkReservoirItemsSketchDoubleSampling(GroupLanguage.CPP);
    checkReservoirItemsSketchStringEmpty(GroupLanguage.CPP);
    checkReservoirItemsSketchStringExact(GroupLanguage.CPP);
    checkReservoirItemsSketchStringSampling(GroupLanguage.CPP);
    checkReservoirItemsUnionLongEmpty(GroupLanguage.CPP);
    checkReservoirItemsUnionLongExact(GroupLanguage.CPP);
    checkReservoirItemsUnionLongSampling(GroupLanguage.CPP);
    checkReservoirItemsUnionDoubleEmpty(GroupLanguage.CPP);
    checkReservoirItemsUnionDoubleExact(GroupLanguage.CPP);
    checkReservoirItemsUnionDoubleSampling(GroupLanguage.CPP);
    checkReservoirItemsUnionStringEmpty(GroupLanguage.CPP);
    checkReservoirItemsUnionStringExact(GroupLanguage.CPP);
    checkReservoirItemsUnionStringSampling(GroupLanguage.CPP);
  }

  @Test(groups = {CHECK_GO_FILES})
  public void checkGo() {
    checkReservoirLongsSketchEmpty(GroupLanguage.GO);
    checkReservoirLongsSketchExact(GroupLanguage.GO);
    checkReservoirLongsSketchSampling(GroupLanguage.GO);
    checkReservoirLongsUnionEmpty(GroupLanguage.GO);
    checkReservoirLongsUnionExact(GroupLanguage.GO);
    checkReservoirLongsUnionSampling(GroupLanguage.GO);
    checkReservoirItemsSketchLongEmpty(GroupLanguage.GO);
    checkReservoirItemsSketchLongExact(GroupLanguage.GO);
    checkReservoirItemsSketchLongSampling(GroupLanguage.GO);
    checkReservoirItemsSketchDoubleEmpty(GroupLanguage.GO);
    checkReservoirItemsSketchDoubleExact(GroupLanguage.GO);
    checkReservoirItemsSketchDoubleSampling(GroupLanguage.GO);
    checkReservoirItemsSketchStringEmpty(GroupLanguage.GO);
    checkReservoirItemsSketchStringExact(GroupLanguage.GO);
    checkReservoirItemsSketchStringSampling(GroupLanguage.GO);
    checkReservoirItemsUnionLongEmpty(GroupLanguage.GO);
    checkReservoirItemsUnionLongExact(GroupLanguage.GO);
    checkReservoirItemsUnionLongSampling(GroupLanguage.GO);
    checkReservoirItemsUnionDoubleEmpty(GroupLanguage.GO);
    checkReservoirItemsUnionDoubleExact(GroupLanguage.GO);
    checkReservoirItemsUnionDoubleSampling(GroupLanguage.GO);
    checkReservoirItemsUnionStringEmpty(GroupLanguage.GO);
    checkReservoirItemsUnionStringExact(GroupLanguage.GO);
    checkReservoirItemsUnionStringSampling(GroupLanguage.GO);
  }

  //ReservoirLongsSketch

  private static void checkReservoirLongsSketchEmpty(final GroupLanguage lang) { //1
    final int k = 128;
    final String fileName = "reservoir_longs_empty_k" + k + lang.sfx + ".sk";
    final byte[] bytes = getFileBytes(lang.pth, fileName);
    if (bytes.length == 0) { return;}
    //System.out.println(fileName);
    final ReservoirLongsSketch sk = ReservoirLongsSketch.heapify(MemorySegment.ofArray(bytes));
    assertEquals(sk.getK(), k);
    assertEquals(sk.getN(), 0);
    assertEquals(sk.getNumSamples(), 0);
  }

  private static void checkReservoirLongsSketchExact(final GroupLanguage lang) { //1
    final int k = 128;
    final int[] nArr = {1, 10, 32, 100, 128};
    for (final int n : nArr) {
      final String fileName = "reservoir_longs_exact_n" + n + "_k" + k + lang.sfx + ".sk";
      final byte[] bytes = getFileBytes(lang.pth, fileName);
      if (bytes.length == 0) { continue;}
      //System.out.println(fileName);
      final ReservoirLongsSketch sk = ReservoirLongsSketch.heapify(MemorySegment.ofArray(bytes));
      assertEquals(sk.getK(), k);
      assertEquals(sk.getN(), n);
      assertEquals(sk.getNumSamples(), n);
    }
  }

  private static void checkReservoirLongsSketchSampling(final GroupLanguage lang) { //3
    final int[] kArr = {32, 64, 128};
    final long n = 1000;
    for (final int k : kArr) {
      final String fileName = "reservoir_longs_sampling_n" + n + "_k" + k + lang.sfx + ".sk";
      final byte[] bytes = getFileBytes(lang.pth, fileName);
      if (bytes.length == 0) { continue;}
      //System.out.println(fileName);
      final ReservoirLongsSketch sk = ReservoirLongsSketch.heapify(MemorySegment.ofArray(bytes));
      assertEquals(sk.getK(), k);
      assertEquals(sk.getN(), n);
      assertEquals(sk.getNumSamples(), k);
    }
  }

  //ReservoirLongsUnion

  private static void checkReservoirLongsUnionEmpty(final GroupLanguage lang) { //4
    int maxK = 128;
    final String fileName = "reservoir_longs_union_empty_maxk" + maxK + lang.sfx + ".sk";
    final byte[] bytes = getFileBytes(lang.pth, fileName);
    if (bytes.length == 0) { return;}
    //System.out.println(fileName);
    final ReservoirLongsUnion rlu = ReservoirLongsUnion.heapify(MemorySegment.ofArray(bytes));
    assertEquals(rlu.getMaxK(), maxK);
    //gadget is null
  }

  private static void checkReservoirLongsUnionExact(final GroupLanguage lang) { //5
    int maxK = 128;
    int[] nArr = {1, 10, 32, 100, 128};
    for (int n : nArr) {
      final String fileName = "reservoir_longs_union_exact_n" + n + "_maxk" + maxK + lang.sfx + ".sk";
      final byte[] bytes = getFileBytes(lang.pth, fileName);
      if (bytes.length == 0) { continue;}
      //System.out.println(fileName);
      final ReservoirLongsUnion rlu = ReservoirLongsUnion.heapify(MemorySegment.ofArray(bytes));
      assertEquals(rlu.getMaxK(), maxK);
      final ReservoirLongsSketch sk = rlu.getResult();
      assertTrue(sk.getN() == n);
      assertTrue(sk.getNumSamples() == n);
    }
  }

  private static void checkReservoirLongsUnionSampling(final GroupLanguage lang) { //6
    int[] maxKArr = {32, 64, 128};
    long n = 1000;
    for (int maxK : maxKArr) {
      final String fileName = "reservoir_longs_union_sampling_n" + n + "_maxk" + maxK + lang.sfx + ".sk";
      final byte[] bytes = getFileBytes(lang.pth, fileName);
      if (bytes.length == 0) { continue;}
      //System.out.println(fileName);
      final ReservoirLongsUnion rlu = ReservoirLongsUnion.heapify(MemorySegment.ofArray(bytes));
      assertEquals(rlu.getMaxK(), maxK);
      final ReservoirLongsSketch sk = rlu.getResult();
      assertTrue(sk.getN() == n);
      assertTrue(sk.getNumSamples() == maxK);
    }
  }

  //ReservoirItemsSketch<Long>

  private static void checkReservoirItemsSketchLongEmpty(final GroupLanguage lang) { //7
    final int k = 128;
    final String fileName = "reservoir_items_long_empty_k" + k + lang.sfx + ".sk";
    final byte[] bytes = getFileBytes(lang.pth, fileName);
    if (bytes.length == 0) { return;}
    //System.out.println(fileName);
    final ReservoirItemsSketch<Long> sk = ReservoirItemsSketch.heapify(
        MemorySegment.ofArray(bytes), new ArrayOfLongsSerDe());
    assertEquals(sk.getK(), k);
    assertEquals(sk.getN(), 0);
    assertEquals(sk.getNumSamples(), 0);
  }

  private static void checkReservoirItemsSketchLongExact(final GroupLanguage lang) { //8
    final int k = 128;
    final int[] nArr = {1, 10, 32, 100, 128};
    for (int n : nArr) {
      final String fileName = "reservoir_items_long_exact_n" + n + "_k" + k + lang.sfx + ".sk";
      final byte[] bytes = getFileBytes(lang.pth, fileName);
      if (bytes.length == 0) { continue;}
      //System.out.println(fileName);
      final ReservoirItemsSketch<Long> sk = ReservoirItemsSketch.heapify(
          MemorySegment.ofArray(bytes), new ArrayOfLongsSerDe());
      assertEquals(sk.getK(), k);
      assertEquals(sk.getN(), n);
      assertEquals(sk.getNumSamples(), n);
    }
  }

  private static void checkReservoirItemsSketchLongSampling(final GroupLanguage lang) { //9
    final int[] kArr = {32, 64, 128};
    final long n = 1000;
    for (final int k : kArr) {
      final String fileName = "reservoir_items_long_sampling_n" + n + "_k" + k + lang.sfx + ".sk";
      final byte[] bytes = getFileBytes(lang.pth, fileName);
      if (bytes.length == 0) { continue;}
      //System.out.println(fileName);
      final ReservoirItemsSketch<Long> sk = ReservoirItemsSketch.heapify(
          MemorySegment.ofArray(bytes), new ArrayOfLongsSerDe());
      assertEquals(sk.getK(), k);
      assertEquals(sk.getN(), n);
      assertEquals(sk.getNumSamples(), k);
    }
  }

  //ReservoirItemsSketch<Double>

  private static void checkReservoirItemsSketchDoubleEmpty(final GroupLanguage lang) { //10
    final int k = 128;
    final String fileName = "reservoir_items_double_empty_k" + k + lang.sfx + ".sk";
    final byte[] bytes = getFileBytes(lang.pth, fileName);
    if (bytes.length == 0) { return;}
    //System.out.println(fileName);
    final ReservoirItemsSketch<Double> sk = ReservoirItemsSketch.heapify(
        MemorySegment.ofArray(bytes), new ArrayOfDoublesSerDe());
    assertEquals(sk.getK(), k);
    assertEquals(sk.getN(), 0);
    assertEquals(sk.getNumSamples(), 0);
  }

  private static void checkReservoirItemsSketchDoubleExact(final GroupLanguage lang) { //11
    final int k = 128;
    final int[] nArr = {1, 10, 32, 100, 128};
    for (int n : nArr) {
      final String fileName = "reservoir_items_double_exact_n" + n + "_k" + k + lang.sfx + ".sk";
      final byte[] bytes = getFileBytes(lang.pth, fileName);
      if (bytes.length == 0) { continue;}
      //System.out.println(fileName);
      final ReservoirItemsSketch<Double> sk = ReservoirItemsSketch.heapify(
          MemorySegment.ofArray(bytes), new ArrayOfDoublesSerDe());
      assertEquals(sk.getK(), k);
      assertEquals(sk.getN(), n);
      assertEquals(sk.getNumSamples(), n);
    }
  }

  private static void checkReservoirItemsSketchDoubleSampling(final GroupLanguage lang) { //12
    final int[] kArr = {32, 64, 128};
    final long n = 1000;
    for (final int k : kArr) {
      final String fileName = "reservoir_items_double_sampling_n" + n + "_k" + k + lang.sfx + ".sk";
      final byte[] bytes = getFileBytes(lang.pth, fileName);
      if (bytes.length == 0) { continue;}
      //System.out.println(fileName);
      final ReservoirItemsSketch<Double> sk = ReservoirItemsSketch.heapify(
          MemorySegment.ofArray(bytes), new ArrayOfDoublesSerDe());
      assertEquals(sk.getK(), k);
      assertEquals(sk.getN(), n);
      assertEquals(sk.getNumSamples(), k);
    }
  }

  //ReservoirItemsSketch<String>

  private static void checkReservoirItemsSketchStringEmpty(final GroupLanguage lang) { //13
    final int k = 128;
    final String fileName = "reservoir_items_string_empty_k" + k + lang.sfx + ".sk";
    final byte[] bytes = getFileBytes(lang.pth, fileName);
    if (bytes.length == 0) { return;}
    //System.out.println(fileName);
    final ReservoirItemsSketch<String> sk = ReservoirItemsSketch.heapify(
        MemorySegment.ofArray(bytes), new ArrayOfStringsSerDe());
    assertEquals(sk.getK(), k);
    assertEquals(sk.getN(), 0);
    assertEquals(sk.getNumSamples(), 0);
  }

  private static void checkReservoirItemsSketchStringExact(final GroupLanguage lang) { //14
    final int k = 128;
    final int[] nArr = {1, 10, 32, 100, 128};
    for (int n : nArr) {
      final String fileName = "reservoir_items_string_exact_n" + n + "_k" + k + lang.sfx + ".sk";
      final byte[] bytes = getFileBytes(lang.pth, fileName);
      if (bytes.length == 0) { continue;}
      //System.out.println(fileName);
      final ReservoirItemsSketch<String> sk = ReservoirItemsSketch.heapify(
          MemorySegment.ofArray(bytes), new ArrayOfStringsSerDe());
      assertEquals(sk.getK(), k);
      assertEquals(sk.getN(), n);
      assertEquals(sk.getNumSamples(), n);
    }
  }

  private static void checkReservoirItemsSketchStringSampling(final GroupLanguage lang) { //15
    final int[] kArr = {32, 64, 128};
    final long n = 1000;
    for (final int k : kArr) {
      final String fileName = "reservoir_items_string_sampling_n" + n + "_k" + k + lang.sfx + ".sk";
      final byte[] bytes = getFileBytes(lang.pth, fileName);
      if (bytes.length == 0) { continue;}
      //System.out.println(fileName);
      final ReservoirItemsSketch<String> sk = ReservoirItemsSketch.heapify(
          MemorySegment.ofArray(bytes), new ArrayOfStringsSerDe());
      assertEquals(sk.getK(), k);
      assertEquals(sk.getN(), n);
      assertEquals(sk.getNumSamples(), k);
    }
  }

  //ReservoirItemsUnion<Long>

  private static void checkReservoirItemsUnionLongEmpty(final GroupLanguage lang) { //16
    int maxK = 128;
    final String fileName = "reservoir_items_union_long_empty_maxk" + maxK + lang.sfx + ".sk";
    final byte[] bytes = getFileBytes(lang.pth, fileName);
    if (bytes.length == 0) { return;}
    //System.out.println(fileName);
    final ReservoirItemsUnion<Long> riu = ReservoirItemsUnion.heapify(
        MemorySegment.ofArray(bytes), new ArrayOfLongsSerDe());
    assertEquals(riu.getMaxK(), maxK);
    //gadget is null
  }

  private static void checkReservoirItemsUnionLongExact(final GroupLanguage lang) { //17
    int maxK = 128;
    int[] nArr = {1, 10, 32, 100, 128};
    for (int n : nArr) {
      final String fileName = "reservoir_items_union_long_exact_n" + n + "_maxk" + maxK + lang.sfx + ".sk";
      final byte[] bytes = getFileBytes(lang.pth, fileName);
      if (bytes.length == 0) { continue;}
      //System.out.println(fileName);
      final ReservoirItemsUnion<Long> riu = ReservoirItemsUnion.heapify(
          MemorySegment.ofArray(bytes), new ArrayOfLongsSerDe());
      assertEquals(riu.getMaxK(), maxK);
      final ReservoirItemsSketch<Long> sk = riu.getResult();
      assertEquals(sk.getN(), n);
      assertEquals(sk.getNumSamples(), n);
    }
  }

  private static void checkReservoirItemsUnionLongSampling(final GroupLanguage lang) { //18
    int[] maxKArr = {32, 64, 128};
    long n = 1000;
    for (int maxK : maxKArr) {
      final String fileName = "reservoir_items_union_long_sampling_n" + n + "_maxk" + maxK + lang.sfx + ".sk";
      final byte[] bytes = getFileBytes(lang.pth, fileName);
      if (bytes.length == 0) { continue;}
      //System.out.println(fileName);
      final ReservoirItemsUnion<Long> riu = ReservoirItemsUnion.heapify(
          MemorySegment.ofArray(bytes), new ArrayOfLongsSerDe());
      assertEquals(riu.getMaxK(), maxK);
      final ReservoirItemsSketch<Long> sk = riu.getResult();
      assertEquals(sk.getN(), n);
      assertEquals(sk.getNumSamples(), maxK);
    }
  }

  //ReservoirItemsUnion<Double>

  private static void checkReservoirItemsUnionDoubleEmpty(final GroupLanguage lang) { //19
    int maxK = 128;
    final String fileName = "reservoir_items_union_double_empty_maxk" + maxK + lang.sfx + ".sk";
    final byte[] bytes = getFileBytes(lang.pth, fileName);
    if (bytes.length == 0) { return;}
    //System.out.println(fileName);
    final ReservoirItemsUnion<Double> riu = ReservoirItemsUnion.heapify(
        MemorySegment.ofArray(bytes), new ArrayOfDoublesSerDe());
    assertEquals(riu.getMaxK(), maxK);
    //gadget is null
  }

  private static void checkReservoirItemsUnionDoubleExact(final GroupLanguage lang) { //20
    int maxK = 128;
    int[] nArr = {1, 10, 32, 100, 128};
    for (int n : nArr) {
      final String fileName = "reservoir_items_union_double_exact_n" + n + "_maxk" + maxK + lang.sfx + ".sk";
      final byte[] bytes = getFileBytes(lang.pth, fileName);
      if (bytes.length == 0) { continue;}
      //System.out.println(fileName);
      final ReservoirItemsUnion<Double> riu = ReservoirItemsUnion.heapify(
          MemorySegment.ofArray(bytes), new ArrayOfDoublesSerDe());
      assertEquals(riu.getMaxK(), maxK);
      final ReservoirItemsSketch<Double> sk = riu.getResult();
      assertEquals(sk.getN(), n);
      assertEquals(sk.getNumSamples(), n);
    }
  }

  private static void checkReservoirItemsUnionDoubleSampling(final GroupLanguage lang) { //21
    int[] maxKArr = {32, 64, 128};
    long n = 1000;
    for (int maxK : maxKArr) {
      final String fileName = "reservoir_items_union_double_sampling_n" + n + "_maxk" + maxK + lang.sfx + ".sk";
      final byte[] bytes = getFileBytes(lang.pth, fileName);
      if (bytes.length == 0) { continue;}
      //System.out.println(fileName);
      final ReservoirItemsUnion<Double> riu = ReservoirItemsUnion.heapify(
          MemorySegment.ofArray(bytes), new ArrayOfDoublesSerDe());
      assertEquals(riu.getMaxK(), maxK);
      final ReservoirItemsSketch<Double> sk = riu.getResult();
      assertEquals(sk.getN(), n);
      assertEquals(sk.getNumSamples(), maxK);
    }
  }

  //ReservoirItemsUnion<String>

  private static void checkReservoirItemsUnionStringEmpty(final GroupLanguage lang) { //22
    int maxK = 128;
    final String fileName = "reservoir_items_union_string_empty_maxk" + maxK + lang.sfx + ".sk";
    final byte[] bytes = getFileBytes(lang.pth, fileName);
    if (bytes.length == 0) { return;}
    //System.out.println(fileName);
    final ReservoirItemsUnion<String> riu = ReservoirItemsUnion.heapify(
        MemorySegment.ofArray(bytes), new ArrayOfStringsSerDe());
    assertEquals(riu.getMaxK(), maxK);
    //gadget is null
  }

  private static void checkReservoirItemsUnionStringExact(final GroupLanguage lang) { //23
    int maxK = 128;
    int[] nArr = {1, 10, 32, 100, 128};
    for (int n : nArr) {
      final String fileName = "reservoir_items_union_string_exact_n" + n + "_maxk" + maxK + lang.sfx + ".sk";
      final byte[] bytes = getFileBytes(lang.pth, fileName);
      if (bytes.length == 0) { continue;}
      //System.out.println(fileName);
      final ReservoirItemsUnion<String> riu = ReservoirItemsUnion.heapify(
          MemorySegment.ofArray(bytes), new ArrayOfStringsSerDe());
      assertEquals(riu.getMaxK(), maxK);
      final ReservoirItemsSketch<String> sk = riu.getResult();
      assertEquals(sk.getN(), n);
      assertEquals(sk.getNumSamples(), n);
    }
  }

  private static void checkReservoirItemsUnionStringSampling(final GroupLanguage lang) { //24
    int[] maxKArr = {32, 64, 128};
    long n = 1000;
    for (int maxK : maxKArr) {
      final String fileName = "reservoir_items_union_string_sampling_n" + n + "_maxk" + maxK + lang.sfx + ".sk";
      final byte[] bytes = getFileBytes(lang.pth, fileName);
      if (bytes.length == 0) { continue;}
      //System.out.println(fileName);
      final ReservoirItemsUnion<String> riu = ReservoirItemsUnion.heapify(
          MemorySegment.ofArray(bytes), new ArrayOfStringsSerDe());
      assertEquals(riu.getMaxK(), maxK);
      final ReservoirItemsSketch<String> sk = riu.getResult();
      assertEquals(sk.getN(), n);
      assertEquals(sk.getNumSamples(), maxK);
    }
  }
}
