package raft;

import org.codehaus.jettison.json.JSONException;
import raft.ipc.JsonRaftClient;

import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.util.Scanner;

/**
 * RaftClientShell
 * */
public class RaftClientShell {
  RaftNode[] nodes;
  private int leaderIndex = 0;

  public RaftClientShell(RaftNode[] nodes) {
    this.nodes = nodes;
  }

  public boolean set(String key, String value)
      throws IOException, JSONException {
    for (int i = 0; i < nodes.length; i++) {
      try {
        boolean res = nodes[(leaderIndex + i) % nodes.length]
            .setKeyValue(key, value, 3000);
        return res;
      } catch (SocketTimeoutException ste) {
        leaderIndex = (leaderIndex + 1) % nodes.length;
        ste.printStackTrace();
      } catch (ConnectException ce) {
        leaderIndex = (leaderIndex + 1) % nodes.length;
        ce.printStackTrace();
      } catch (RaftNode.NotLeaderException nle) {
        leaderIndex = (leaderIndex + 1) % nodes.length;
      }
    }
    throw new IOException("Couldn't connect to leader.");
  }

  public String get(String key) throws IOException, JSONException {
    for (int i = 0; i < nodes.length; i++) {
      try {
        String value =
            nodes[(leaderIndex + i) % nodes.length].getValue(key, 3000);
        return value;
      } catch (SocketTimeoutException ste) {
        leaderIndex = (leaderIndex + 1) % nodes.length;
        ste.printStackTrace();
      } catch (ConnectException ce) {
        leaderIndex = (leaderIndex + 1) % nodes.length;
        ce.printStackTrace();
      } catch (RaftNode.NotLeaderException nle) {
        leaderIndex = (leaderIndex + 1) % nodes.length;
      }
    }
    throw new IOException("Couldn't connect to leader.");
  }

  public static void main(String args[]) throws Exception {
    String command;
    String[] items;
    if (args == null || args.length == 0) {
      Scanner sc = new Scanner(System.in);
      String line = sc.nextLine();
      args = line.split(" ");
    }
    command = args[0];
    items = new String[args.length - 1];
    for (int i = 1; i < args.length; i++) {
      items[i - 1] = args[i];
    }

    Configuration conf = new Configuration();
    RaftNode[] nodes = JsonRaftClient.getRaftClient(conf);
    if (command.equals("set")) {
      System.out.println(new RaftClientShell(nodes).set(items[0], items[1]));
    } else if (command.equals("get")) {
      System.out.println(new RaftClientShell(nodes).get(items[0]));
    }
  }
}
