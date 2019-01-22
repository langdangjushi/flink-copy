package lan;

import org.junit.Test;
import sun.misc.Unsafe;

/**
 * created by pengmingguo on 1/22/19
 */
public class UnsafeTest {

  @Test
  public void testUnsage() {
    Unsafe unsafe = Unsafe.getUnsafe();
    long addr = unsafe.allocateMemory(1);
    unsafe.setMemory(addr,0,(byte)23);
    byte value = unsafe.getByte(addr);
    assert value==23;
  }
}
