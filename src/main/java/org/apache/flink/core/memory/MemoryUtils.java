package org.apache.flink.core.memory;

import java.lang.reflect.Field;
import java.nio.ByteOrder;
import sun.misc.Unsafe;

/**
 * created by pengmingguo on 1/22/19
 */
public class MemoryUtils {

  public static final sun.misc.Unsafe UNSAFE = getUnsafe();

  /*
        long a = unsafe.allocateMemory(8);
        try {
            unsafe.putLong(a, 0x0102030405060708L);
            byte b = unsafe.getByte(a);
            switch (b) {
            case 0x01: byteOrder = ByteOrder.BIG_ENDIAN;     break;
            case 0x08: byteOrder = ByteOrder.LITTLE_ENDIAN;  break;
            default:
                assert false;
                byteOrder = null;
            }
    1) Little-Endian就是低位字节排放在内存的低地址端，高位字节排放在内存的高地址端。
    2) Big-Endian就是高位字节排放在内存的低地址端，低位字节排放在内存的高地址端。
      因为malloc出来的地址是递增的
   */
  public static final ByteOrder NATIVE_BYTE_ORDER = ByteOrder.nativeOrder();

  private static sun.misc.Unsafe getUnsafe() {
    try {
      Field unsafeField = Unsafe.class.getDeclaredField("theUnsafe");
      unsafeField.setAccessible(true);
      return (Unsafe) unsafeField.get(null);
    } catch (SecurityException e) {
      throw new RuntimeException("Could not access the sun.misc");
    } catch (NoSuchFieldException e){
      throw new RuntimeException("The static handle field in sun.misc.Unsafe was not found");
    } catch (IllegalArgumentException e){
      throw new RuntimeException("");
    } catch (IllegalAccessException e){
      throw new RuntimeException("fff");
    } catch (Throwable t ){
      throw new RuntimeException("dd");
    }
  }

  private MemoryUtils() {
  }
}
