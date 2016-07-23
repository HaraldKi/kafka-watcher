package de.pifpafpuf.util;

/**
 * similar to {@link java.function.Supplier} but the {@link #get} may fail
 * with an exception.
 *
 * @param <T>
 */
public interface FailableFactory<T> {

  T get() throws CreateFailedException;
}
