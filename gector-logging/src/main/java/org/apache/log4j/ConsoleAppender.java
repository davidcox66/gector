package org.apache.log4j;

import java.io.Writer;

public class ConsoleAppender implements Appender {

  public ConsoleAppender() { }
  public ConsoleAppender(Layout layout) { }
  public ConsoleAppender(Layout layout, String target) { }

  public void setWriter(Writer writer) {   }
  public void setLayout(Layout layout) { }

  /*
  public void setTarget(String value) { }
  public String getTarget() { return null; }
  public final void setFollow(final boolean newValue) { }
  public final boolean getFollow() { return false; }
  void targetWarn(String val) { }
  public void activateOptions() { }
  protected final void closeWriter() { }
  */
}
