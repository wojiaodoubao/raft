package raft;

public class Time {
  private static final long NANOSECONDS_PER_MILLISECOND = 1000000;

  // Current time from arbitrary time base in the past.
  public static long monotonicNow() {
    return System.nanoTime() / NANOSECONDS_PER_MILLISECOND;
  }
}
