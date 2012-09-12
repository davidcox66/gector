package org.apache.log4j;

// import org.apache.log4j.spi.LoggingEvent;

/**
   Extend this abstract class to create your own log layout format.
   
   @author Ceki G&uuml;lc&uuml;

*/
  
public abstract class Layout {

  // abstract public String format(LoggingEvent event);

  public String getContentType() {
    return "text/plain";
  }

  public String getHeader() {
    return null;
  }

  public String getFooter() {
    return null;
  }

  abstract public boolean ignoresThrowable();
}
