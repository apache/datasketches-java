package com.yahoo.sketches.tuple;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import com.yahoo.sketches.Family;
import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.memory.MemoryRegion;
import com.yahoo.sketches.memory.NativeMemory;

public class SerializerDeserializer {
  public static enum SketchType { QuickSelectSketch, CompactSketch, ArrayOfDoublesQuickSelectSketch, ArrayOfDoublesCompactSketch }
  static final int TYPE_BYTE_OFFSET = 3;

  private static final Map<String, Method> deserializeMethodCache = new HashMap<String, Method>();

  static void validateFamily(byte familyId, byte preambleLongs) {
    Family family = Family.idToFamily(familyId);
    if (family.equals(Family.TUPLE)) {
      if (preambleLongs != Family.TUPLE.getMinPreLongs()) {
        throw new IllegalArgumentException("Possible corruption: Invalid PreambleLongs value for family TUPLE: " + preambleLongs);
      }
    } else {
      throw new IllegalArgumentException("Possible corruption: Invalid Family: " + family.toString());
    }
  }

  static void validateType(byte sketchTypeByte, SketchType expectedType) {
    SketchType sketchType = getSketchType(sketchTypeByte);
    if (!sketchType.equals(expectedType)) throw new RuntimeException("Sketch Type mismatch. Expected " + expectedType.name() + ", got " + sketchType.name());
  }

  public static SketchType getSketchTypeAbsolute(byte[] buffer) {
    byte sketchTypeByte = buffer[TYPE_BYTE_OFFSET];
    return getSketchType(sketchTypeByte);
  }

  static byte[] toByteArray(Object object) {
    try {
      String className = object.getClass().getName();
      byte[] objectBytes = ((byte[]) object.getClass().getMethod("toByteArray", (Class<?>[])null).invoke(object));
      byte[] bytes = new byte[1 + className.length() + objectBytes.length];
      Memory mem = new NativeMemory(bytes);
      int offset = 0;
      mem.putByte(offset++, (byte)className.length());
      mem.putByteArray(offset, className.getBytes(), 0, className.length());
      offset += className.length();
      mem.putByteArray(offset, objectBytes, 0, objectBytes.length);
      return bytes;
    } catch (NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }

  static <T> DeserializeResult<T> deserializeFromMemory(Memory mem, int offset) {
    int classNameLength = mem.getByte(offset);
    byte[] classNameBuffer = new byte[classNameLength];
    mem.getByteArray(offset + 1, classNameBuffer, 0, classNameLength);
    String className = new String(classNameBuffer);
    DeserializeResult<T> result = deserializeFromMemory(mem, offset + classNameLength + 1, className);
    return new DeserializeResult<T>(result.getObject(), result.getSize() + classNameLength + 1);
  }

  @SuppressWarnings("unchecked")
  static <T> DeserializeResult<T> deserializeFromMemory(Memory mem, int offset, String className) {
    try {
      Method method = deserializeMethodCache.get(className);
      if (method == null) {
          method = Class.forName(className).getMethod("fromMemory", Memory.class);
          deserializeMethodCache.put(className, method);
      }
      return (DeserializeResult<T>) method.invoke(null, new MemoryRegion(mem, offset, mem.getCapacity() - offset));
    } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException | NoSuchMethodException | SecurityException | ClassNotFoundException e) {
      throw new RuntimeException("Couldn't deserialize class " + className + " " + e);
    }
  }

  private static SketchType getSketchType(byte sketchTypeByte) {
    if (sketchTypeByte < 0 || sketchTypeByte >= SketchType.values().length) throw new RuntimeException("Invalid Sketch Type " + sketchTypeByte);
    SketchType sketchType = SketchType.values()[sketchTypeByte];
    return sketchType;
  }
}
