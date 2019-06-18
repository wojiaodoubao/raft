package raft;

import org.xml.sax.SAXException;
import raft.ipc.JsonRaftClient;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;

/**
 * SMain
 */
public class SMain {
  public static void main(String args[])
      throws IOException, ParserConfigurationException, SAXException {
    Configuration conf = new Configuration();
    int index = Integer.parseInt(args[0]);
    RaftNode[] nodes = JsonRaftClient.getRaftClient(conf);
    RaftNodeImpl impl = new RaftNodeImpl(index, conf);
    impl.init(nodes);
    new Thread(impl).start();
  }
}
