/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.tuple;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TestUtil {
  public static List<Double> asList(double[] array) {
    List<Double> list = new ArrayList<Double>(array.length);
    for (int i = 0; i < array.length; i++) list.add(array[i]);
    return list;
  }

  public static List<Float> asList(float[] array) {
    List<Float> list = new ArrayList<Float>(array.length);
    for (int i = 0; i < array.length; i++) list.add(array[i]);
    return list;
  }

  public static List<Long> asList(long[] array) {
    List<Long> list = new ArrayList<Long>(array.length);
    for (int i = 0; i < array.length; i++) list.add(array[i]);
    return list;
  }

  public static void writeBytesToFile(byte[] bytes, String fileName) throws IOException {
    FileOutputStream out = new FileOutputStream(new File(fileName));
    out.write(bytes);
    out.close();
  }
}
