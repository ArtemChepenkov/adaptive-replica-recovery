package ru.nsu.replicarecovery.runner.client;

public final class RemoteCallException extends RuntimeException {
  private final int statusCode;
  public RemoteCallException(String message, int statusCode) {
    super(message);
    this.statusCode = statusCode;
  }
  public RemoteCallException(String message, Throwable cause) {
    super(message, cause);
    statusCode = 0;
  }
  public int statusCode() { return statusCode; }
}
