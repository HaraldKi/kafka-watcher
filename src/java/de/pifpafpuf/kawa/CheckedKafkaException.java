package de.pifpafpuf.kawa;

import org.apache.kafka.common.KafkaException;

public class CheckedKafkaException extends Exception {
  public CheckedKafkaException(String msg, KafkaException cause) {
    super(msg);
    initCause(cause);
  }
}
