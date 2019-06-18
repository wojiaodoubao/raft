package raft.ipc;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.Configuration;
import raft.LogQueue;
import raft.RaftNode;
import raft.Time;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;

public class JsonRaftClient extends RaftNode {

  InetSocketAddress socketAddress;
  private static Logger LOG = LoggerFactory.getLogger(JsonRaftClient.class);

  public JsonRaftClient(int id, Configuration conf) throws IOException {
    String ip = conf.get(id +".ip");
    int port = Integer.parseInt(conf.get(id+".port"));
    socketAddress = new InetSocketAddress(ip, port);
    this.id = id;
  }

  @Override
  public boolean requestVote(int srcId, int electionTerm, int term,
      long commit, int timeout) throws IOException, JSONException {
    LOG.debug("requestVote to " + id + ", " + socketAddress);// debug
    long startTime = Time.monotonicNow();
    Socket rpcSocket = new Socket();
    try {
      rpcSocket.connect(socketAddress, timeout);
      JSONObject request = new JSONObject();
      request.put("id", srcId);
      request.put("method", "requestVote");
      request.put("electionTerm", electionTerm);
      request.put("term", term);
      request.put("commit", commit);
      DataOutputStream out = new DataOutputStream(rpcSocket.getOutputStream());
      out.writeInt(request.toString().getBytes().length);
      out.write(request.toString().getBytes());
      long timeCost = Time.monotonicNow() - startTime;
      if (timeCost >= timeout) {
        throw new SocketTimeoutException("rpc timeout");
      }
      rpcSocket.setSoTimeout((int) (timeout - timeCost));
      DataInputStream in = new DataInputStream(rpcSocket.getInputStream());
      int len = in.readInt();
      byte[] buffer = new byte[len];
      timeCost = Time.monotonicNow() - startTime;
      if (timeCost >= timeout) {
        throw new SocketTimeoutException("rpc timeout");
      }
      rpcSocket.setSoTimeout((int) (timeout - timeCost));
      if (in.read(buffer) != buffer.length) {
        throw new IOException("Connection error.");
      }
      JSONObject res = new JSONObject(new String(buffer));
      return res.getBoolean("vote");
    } finally {
      if (rpcSocket != null) {
        rpcSocket.close();
      }
    }
  }

  @Override
  public boolean appendEntries(int srcId, LogQueue.LogEntry logPre,
      LogQueue.LogEntry log, int timeout)
      throws SocketTimeoutException, IOException, JSONException {
    LOG.debug(srcId + " appendEntries to " + id + ", " + socketAddress);// debug
    long startTime = Time.monotonicNow();
    Socket rpcSocket = new Socket();
    try {
      rpcSocket.connect(socketAddress, timeout);
      JSONObject request = new JSONObject();
      request.put("id", srcId);
      request.put("method", "appendEntries");
      request.put("logPre", logPre.toJson());
      request.put("log", log.toJson());
      DataOutputStream out = new DataOutputStream(rpcSocket.getOutputStream());
      out.writeInt(request.toString().getBytes().length);
      out.write(request.toString().getBytes());
      long timeCost = Time.monotonicNow() - startTime;
      if (timeCost >= timeout) {
        throw new SocketTimeoutException("rpc timeout");
      }
      rpcSocket.setSoTimeout((int) (timeout - timeCost));
      DataInputStream in = new DataInputStream(rpcSocket.getInputStream());
      int len = in.readInt();
      byte[] buffer = new byte[len];
      timeCost = Time.monotonicNow() - startTime;
      if (timeCost >= timeout) {
        throw new SocketTimeoutException("rpc timeout");
      }
      rpcSocket.setSoTimeout((int) (timeout - timeCost));
      if (in.read(buffer) != buffer.length) {
        throw new IOException("Connection error.");
      }
      JSONObject res = new JSONObject(new String(buffer));
      return res.getBoolean("append");
    } finally {
      if (rpcSocket != null) {
        rpcSocket.close();
      }
    }
  }

  @Override
  public boolean setKeyValue(String key, String value, int timeout)
      throws SocketTimeoutException, IOException, JSONException {
    long startTime = Time.monotonicNow();
    Socket rpcSocket = new Socket();
    try {
      rpcSocket.connect(socketAddress, timeout);
      JSONObject request = new JSONObject();
      request.put("method", "setKeyValue");
      request.put("key", key);
      request.put("value", value);
      DataOutputStream out = new DataOutputStream(rpcSocket.getOutputStream());
      out.writeInt(request.toString().getBytes().length);
      out.write(request.toString().getBytes());
      long timeCost = Time.monotonicNow() - startTime;
      if (timeCost >= timeout) {
        throw new SocketTimeoutException("rpc timeout");
      }
      rpcSocket.setSoTimeout((int) (timeout - timeCost));
      DataInputStream in = new DataInputStream(rpcSocket.getInputStream());
      int len = in.readInt();
      byte[] buffer = new byte[len];
      timeCost = Time.monotonicNow() - startTime;
      if (timeCost >= timeout) {
        throw new SocketTimeoutException("rpc timeout");
      }
      rpcSocket.setSoTimeout((int) (timeout - timeCost));
      if (in.read(buffer) != buffer.length) {
        throw new IOException("Connection error.");
      }
      JSONObject res = new JSONObject(new String(buffer));
      handleException(res);
      return res.getBoolean("result");
    } finally {
      if (rpcSocket != null) {
        rpcSocket.close();
      }
    }
  }

  @Override
  public String getValue(String key, int timeout)
      throws SocketTimeoutException, IOException, JSONException {
    long startTime = Time.monotonicNow();
    Socket rpcSocket = new Socket();
    try {
      rpcSocket.connect(socketAddress, timeout);
      JSONObject request = new JSONObject();
      request.put("method", "getValue");
      request.put("key", key);
      DataOutputStream out = new DataOutputStream(rpcSocket.getOutputStream());
      out.writeInt(request.toString().getBytes().length);
      out.write(request.toString().getBytes());
      long timeCost = Time.monotonicNow() - startTime;
      if (timeCost >= timeout) {
        throw new SocketTimeoutException("rpc timeout");
      }
      rpcSocket.setSoTimeout((int) (timeout - timeCost));
      DataInputStream in = new DataInputStream(rpcSocket.getInputStream());
      int len = in.readInt();
      byte[] buffer = new byte[len];
      timeCost = Time.monotonicNow() - startTime;
      if (timeCost >= timeout) {
        throw new SocketTimeoutException("rpc timeout");
      }
      rpcSocket.setSoTimeout((int) (timeout - timeCost));
      if (in.read(buffer) != buffer.length) {
        throw new IOException("Connection error.");
      }
      JSONObject res = new JSONObject(new String(buffer));
      handleException(res);
      if (!res.getBoolean("isNull")) {
        return res.getString("result");
      } else {
        return null;
      }
    } finally {
      if (rpcSocket != null) {
        rpcSocket.close();
      }
    }
  }

  void handleException(JSONObject obj) throws IOException {
    IOException e = null;
    try {
      if (obj.has("ExceptionClass")) {
        String exceptionClassStr = obj.getString("ExceptionClass");
        String message = obj.getString("ExceptionMessage");
        Class clazz = Class.forName(exceptionClassStr);
        clazz.asSubclass(IOException.class);
        Constructor constructor = clazz.getConstructor(String.class);
        constructor.setAccessible(true);
        e = (IOException) constructor.newInstance(message);
      }
    } catch (Exception ee) {
      throw new IOException(ee);
    }
    if (e != null) {
      throw e;
    }
  }

  public static RaftNode[] getRaftClient(Configuration conf) throws IOException {
    JsonRaftClient[] nodes = new JsonRaftClient[3];
    for (int i = 0; i < 3; i++) {
      nodes[i] = new JsonRaftClient(i, conf);
    }
    return nodes;
  }
}
