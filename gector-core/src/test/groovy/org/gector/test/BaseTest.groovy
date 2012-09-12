package org.gector.test

import org.codehaus.groovy.runtime.InvokerHelper
import org.codehaus.groovy.runtime.typehandling.DefaultTypeTransformation
import org.junit.Assert

abstract class BaseTest
{
  
  public static void assertTrue( String message, boolean value ) {
    Assert.assertTrue( message, value );  
  }
  
  public static void assertFalse( String message, boolean value ) {
    Assert.assertFalse( message, value );  
  }
  
  /**
   * Does groovy equals comparison, taking into account transformations such as GStrings and java.lang.String
   * @param message
   * @param expected
   * @param actual
   */
  public static void assertEquals(String message, Object expected, Object actual) {
    if (expected == null && actual == null)
			return;
		if (expected != null && DefaultTypeTransformation.compareEqual(expected, actual))
			return;
		Assert.failNotEquals(message, expected, actual);
  }

  static void assertEquals(Object expected, Object actual) {
    assertEquals(null, expected, actual);
	}

	static void assertEquals(String expected, String actual) {
    assertEquals(null, expected, actual);
	}
	static void fail(String message) {
    Assert.fail( message );
	}
	static void assertNotNull(Object object) {
    Assert.assertNotNull( object );
	}
	static void assertNotNull(String message, Object object) {
    Assert.assertNotNull( message, object );
	}
	static void assertNull(Object object) {
    Assert.assertNull( object );
	}
	static void assertNull(String message, Object object) {
    Assert.assertNull( message, object );
	}
 
	static void assertArrayEquals(Object[] expected, Object[] value) {
	    String message =
	        "expected array: " + InvokerHelper.toString(expected) + " value array: " + InvokerHelper.toString(value);
	    assertNotNull(message + ": expected should not be null", expected);
	    assertNotNull(message + ": value should not be null", value);
	    assertEquals(message, expected.length, value.length);
      int size=expected.length;
	    for (int i=0 ; i < size; i++) {
	        assertEquals("value[" + i + "] when " + message, expected[i], value[i]);
	    }
	}
  
  static void assertArrayEquals( byte[] b1, byte[] b2 ) {
    if( b1 == null || b2 == null ) {
      Assert.assertTrue( "Arrays do not match (null)", b1 == b2 );
    }
    else {
      Assert.assertTrue( "Arrays are not same length", b1.length == b2.length );
      for( int i=0 ; i < b1.length ; i++ ) {
        Assert.assertTrue( "Array index [${i}] does not match", b1[i] == b2[i] );
      }
    }
  }
   
}
