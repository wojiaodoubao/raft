package raft;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class LogQueue {

  private static Logger LOG = LoggerFactory.getLogger(LogQueue.class);

  public static class LogEntry implements Comparable<LogEntry> {
    int term;
    long lid;
    String key;
    String value;
    long position;
    private LogEntry pre;
    private LogEntry next;

    public LogEntry(int term, long rid, String key, String value) {
      this.term = term;
      this.lid = rid;
      this.key = key;
      this.value = value;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      } else if (obj instanceof LogEntry) {
        LogEntry o = (LogEntry) obj;
        if (o.term == term && o.lid == lid) {
          return true;
        }
      }
      return false;
    }

    public void setPosition(long position) {
      this.position = position;
    }

    @Override
    public String toString() {
      return "{" + term + " " + lid + " " + key + " " + value + "}";
    }

    public JSONObject toJson() throws JSONException {
      JSONObject jobj = new JSONObject();
      jobj.put("term", term);
      jobj.put("lid", lid);
      jobj.put("key", key);
      jobj.put("value", value);
      return jobj;
    }

    public void fromJson(JSONObject jobj) throws JSONException {
      term = jobj.getInt("term");
      lid = jobj.getInt("lid");
      key = jobj.getString("key");
      value = jobj.getString("value");
    }

    @Override
    public int compareTo(LogEntry o) {
      if (o.term < term) {
        return -1;
      } else if (o.term > term) {
        return 1;
      } else {
        if (o.lid < lid) {
          return -1;
        } else if (o.lid > lid) {
          return 1;
        } else {
          return 0;
        }
      }
    }

    byte[] toByte() {
      return (lid + ";" + term + ";" + key + ";" + value + ";\n").getBytes();
    }
  }

  LogEntry head;
  LogEntry tail;
  Lock logLock = new ReentrantLock();
  final File LOG_FILE;
  FileOutputStream fos;
  FileInputStream fis;
  RaftNodeImpl raftNode;

  public LogQueue(String logFile, RaftNodeImpl raftNode) throws IOException {
    this.raftNode = raftNode;
    head = new LogEntry(-1, -1, "", "");
    tail = head;
    this.LOG_FILE = new File(logFile);
    if (!LOG_FILE.exists()) {
      if (!LOG_FILE.createNewFile()) {
        throw new RuntimeException("Couldn't create log file " + LOG_FILE);
      }
    } else if (!LOG_FILE.isFile()) {
      throw new RuntimeException("Bad log file " + LOG_FILE);
    }
    this.fos = new FileOutputStream(logFile, true);
    this.fis = new FileInputStream(logFile);
    // load from disk
    loadLog();
  }

  LogEntry getLastLog() {
    logLock.lock();
    try {
      return tail;
    } finally {
      logLock.unlock();
    }
  }

  /**
   * Append log to the end of the logQueue.
   *
   * @return true if entry is added to logQueue.
   *         false if entry is less or equals to the tail of logQueue.
   * */
  boolean appendLog(LogEntry entry) {
    logLock.lock();
    try {
      if (entry.term == tail.term && entry.lid == tail.lid) {
        return false;
      }
      tail.next = entry;
      entry.pre = tail;
      entry.next = null;
      tail = entry;
      return true;
    } finally {
      logLock.unlock();
    }
  }

  boolean appendLog(LogEntry logPre, LogEntry entry) {
    logLock.lock();
    try {
      LogEntry next = logPre.next;
      if (next != null && next.term == entry.term && next.lid == entry.lid) {
        return false;
      }
      if (next != null) {
        LOG.debug("truncate all after " + logPre);
      }
      tail = logPre;
      tail.next = entry;
      entry.pre = tail;
      tail = entry;
      return true;
    } finally {
      logLock.unlock();
    }
  }

  LogEntry appendLog(String key, String value) {
    logLock.lock();
    try {
      int term = tail.term;
      long lid = tail.lid + 1;
      LogEntry entry = new LogEntry(term, lid, key, value);
      tail.next = entry;
      entry.pre = tail;
      entry.next = null;
      tail = entry;
      return tail;
    } finally {
      logLock.unlock();
    }
  }

  LogEntry getPre(LogEntry entry) {
    logLock.lock();
    try {
      return entry.pre;
    } finally {
      logLock.unlock();
    }
  }

  LogEntry getNext(LogEntry entry) {
    logLock.lock();
    try {
      return entry.next;
    } finally {
      logLock.unlock();
    }
  }

  void writeLog(LogEntry log) { // Fast fail if any error occurs.
    logLock.lock();
    try {
      long position = fos.getChannel().size();
      fos.getChannel().position(position);
      fos.write(log.toByte());
      fos.flush();
      log.setPosition(position);
      LOG.info(raftNode.id + " append log " + log);
    } catch (IOException e) {
      LOG.error("Fatal error when write log " + log, e);
      System.exit(-1);
    } finally {
      logLock.unlock();
    }
  }

  void writeLog(LogEntry logPre, LogEntry log) { // Fast fail if any error occurs.
    logLock.lock();
    try {
      if (logPre == head) {
        writeLog(log);
        return;
      }
      LogEntry tmp = tail;
      while (tmp != null) {
        if (tmp.compareTo(logPre) == 0) {
          long nextPosition = tmp.position + tmp.toByte().length;
          if (nextPosition < fos.getChannel()
              .size()) { // truncate
            fos.getChannel().truncate(nextPosition);
            raftNode.restoreMap();
            LOG.info("truncate " + nextPosition);
          }
          writeLog(log);
          return;
        } else {
          tmp = tmp.pre;
        }
      }
      throw new IOException("Couldn't find the index of logPre.");
    } catch (IOException e) {
      LOG.error("Fatal error when write log " + logPre + " " + log, e);
      System.exit(-1);
    } finally {
      logLock.unlock();
    }
  }

  void loadLog() throws IOException {
    LOG.info("Start loading logs.");
    logLock.lock();
    BufferedReader br = null;
    try {
      LOG.info("loading " + LOG_FILE);
      fis.getChannel().position(0);
      br = new BufferedReader(new InputStreamReader(fis));
      String logStr = null;
      long position = 0;
      while ((logStr = br.readLine()) != null) {
        String[] items = logStr.split(";");
        int logId = Integer.parseInt(items[0]);
        int term = Integer.parseInt(items[1]);
        String key = "";
        String value = "";
        if (items.length == 4) {
          key = items[2];
          value = items[3];
        }
        LogEntry entry = new LogEntry(term, logId, key, value);
        entry.setPosition(position);
        appendLog(entry);
        position = fis.getChannel().position();
      }
    } finally {
      LOG.info("Load logs done.");
      logLock.unlock();
    }
  }

  void restoreKeyValue(Map<String, String> map) {
    logLock.lock();
    try {
      map.clear();
      LogEntry tmp = head;
      while (tmp != null) {
        if (tmp.term >= 0 && tmp.key != null && tmp.value != null) {
          map.put(tmp.key, tmp.value);
        }
        tmp = tmp.next;
      }
    } finally {
      logLock.unlock();
    }
  }
}
