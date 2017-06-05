/*
 * Copyright 2015-16, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.tuple;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
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
    try (FileOutputStream out = new FileOutputStream(new File(fileName))) {
      out.write(bytes);
    }
  }
  
  public static byte[] readBytesFromFile(String fileName) throws IOException {
    try (FileInputStream in = new FileInputStream(new File(fileName))) {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      byte[] bytes = new byte[1024];
      int len;
      while ((len = in.read(bytes, 0, bytes.length)) != -1) {
        out.write(bytes, 0, len);
      }
      return out.toByteArray();
    }
  }

}
