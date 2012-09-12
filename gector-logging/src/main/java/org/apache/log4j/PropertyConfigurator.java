package org.apache.log4j;

/**
 * This is just a supplement to the SLF4J log4j bridge to allow Cassandra to skip past log4j initialization
 * 
 * @author david
 * @since May 13, 2011
 */
public class PropertyConfigurator
{
  static public void configureAndWatch(String configFilename, long delay) {
  }

}
