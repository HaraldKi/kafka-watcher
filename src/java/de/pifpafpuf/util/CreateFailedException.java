package de.pifpafpuf.util;

public class CreateFailedException extends Exception {
  public CreateFailedException(String msg, Throwable t) {
    super(msg);
    if (t!=null) {
      initCause(t);
    }
  }
  public CreateFailedException(String msg) {
    this(msg, null);
  }
}
