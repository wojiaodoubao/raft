package raft.ipc;

import org.codehaus.jettison.json.JSONObject;
import raft.LogQueue;
import raft.RaftNode;

import java.io.IOException;

public class JsonRaftServer extends SimpleRpcServer {

  RaftNode impl;

  public JsonRaftServer(int port, RaftNode node) throws IOException {
    super(port);
    this.impl = node;
  }

  @Override
  public byte[] handleRpc(byte[] request) throws Exception {
    String jsonStr = new String(request);
    JSONObject jobj = new JSONObject(jsonStr);
    String method = jobj.getString("method");
    try {
      if (method.equals("requestVote")) {
        int nodeId = jobj.getInt("id");
        int rElectionTerm = jobj.getInt("electionTerm");
        int rTerm = jobj.getInt("term");
        long rCommit = jobj.getLong("commit");
        boolean res = impl.requestVote(nodeId, rElectionTerm, rTerm, rCommit, 0);
        JSONObject robj = new JSONObject();
        robj.put("vote", res);
        return robj.toString().getBytes();
      } else if (method.equals("appendEntries")) {
        int nodeId = jobj.getInt("id");
        LogQueue.LogEntry toAppend = new LogQueue.LogEntry(-1, -1, null, null);
        toAppend.fromJson(jobj.getJSONObject("log"));
        LogQueue.LogEntry logPre = new LogQueue.LogEntry(-1, -1, null, null);
        logPre.fromJson(jobj.getJSONObject("logPre"));
        boolean res = impl.appendEntries(nodeId, logPre, toAppend, 0);
        JSONObject robj = new JSONObject();
        robj.put("append", res);
        return robj.toString().getBytes();
      } else if (method.equals("setKeyValue")) {
        String key = jobj.getString("key");
        String value = jobj.getString("value");
        boolean res = impl.setKeyValue(key, value, 0);
        JSONObject robj = new JSONObject();
        robj.put("result", res);
        return robj.toString().getBytes();
      } else if (method.equals("getValue")) {
        String key = jobj.getString("key");
        String res = impl.getValue(key, 0);
        JSONObject robj = new JSONObject();
        if (res == null) {
          robj.put("isNull", true);
        } else {
          robj.put("isNull", false);
          robj.put("result", res);
        }
        return robj.toString().getBytes();
      }
      return new byte[0];
    } catch (IOException e) {
      JSONObject robj = new JSONObject();
      robj.put("ExceptionClass", e.getClass().getName());
      robj.put("ExceptionMessage", e.getMessage());
      return robj.toString().getBytes();
    }
  }
}
