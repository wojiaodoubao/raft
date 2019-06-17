package raft;

import raft.ipc.JsonRaftClient;

import java.io.IOException;

/**
 * SMain
 * */
public class SMain {
  public static void main(String args[]) throws IOException {
    JsonRaftClient[] nodes = new JsonRaftClient[3];
    for (int i = 0; i < 3; i++) {
      nodes[i] = new JsonRaftClient("localhost", 9990 + i, i);
    }
    if (args == null || args.length == 0) {
      RaftNodeImpl[] impls = new RaftNodeImpl[3];
      for (int i = 0; i < 3; i++) {
        impls[i] = new RaftNodeImpl("localhost", 9990 + i, i,
            "/home/ljl/raft/node-" + i);
        impls[i].init(nodes);
        new Thread(impls[i]).start();
      }
    } else {
      int index = Integer.parseInt(args[0]);
      RaftNodeImpl impl = new RaftNodeImpl("localhost", 9990 + index, index,
          "/home/lijinglun/raft/node-" + index);
      impl.init(nodes);
      new Thread(impl).start();
    }
  }
}
