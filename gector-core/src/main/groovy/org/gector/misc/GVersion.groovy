package org.gector.misc

/**
 * Utility class to help with Grape. Much of this has not been utilized. It may be
 * removed in the future.
 * 
 * @author david
 * @since Mar 29, 2011
 */
class GVersion
{
  public static final String HECTOR = 'me.prettyprint:hector-core:0.8.0-2';
  public static final String POI = 'org.apache.poi:poi:3.7'; 
  public static final boolean SYSLOADER = false; // Seems to need to be true when in Eclipse

  private static ClassLoader root = new GroovyClassLoader();
    
  public static ClassLoader getRootClassLoader() {
    return root;
    // return getRootClassLoader(GVersion.class);
  }
  
  public static ClassLoader getRootClassLoader(Class cls ) {
    def classLoader = cls.getClassLoader();
    println "Initial class loader: " + classLoader;
    while (classLoader && !classLoader.getClass().getName().equals("org.codehaus.groovy.tools.RootLoader")) {
      if( classLoader.getParent() == null ) {
        break; 
      }
      classLoader = classLoader.getParent()
      println "Next class loader: " + classLoader;
    }
    println "Root Class Loader: " + classLoader;
    return classLoader;
  } 
}
