package raft;

import org.codehaus.jettison.json.JSONException;
import raft.LogQueue.LogEntry;

import java.io.IOException;
import java.net.SocketTimeoutException;

public abstract class RaftNode {
  protected int id;
  public abstract boolean requestVote(int srcId, int electionTerm, int term, long commit, int timeout)
      throws IOException, JSONException;
  public abstract boolean appendEntries(int srcId, LogEntry logPre, LogEntry toAppend, int timeout) throws SocketTimeoutException, IOException, JSONException;
  public abstract boolean setKeyValue(String key, String value, int timeout) throws SocketTimeoutException, IOException, JSONException;
  public abstract String getValue(String key, int timeout) throws SocketTimeoutException, IOException, JSONException;

  public static class NotLeaderException extends IOException {
    public NotLeaderException(String msg) {
      super(msg);
    }
  }

  public static class SafeModeException extends IOException {
    public SafeModeException(String msg) {
      super(msg);
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    } else {
      if (obj instanceof RaftNode) {
        RaftNode rni = (RaftNode) obj;
        if (rni.id == this.id) {
          return true;
        }
      }
    }
    return false;
  }
}
