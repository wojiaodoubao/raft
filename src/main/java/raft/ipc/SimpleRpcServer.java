package raft.ipc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.LinkedList;

public abstract class SimpleRpcServer implements Runnable {
  private static Logger LOG = LoggerFactory.getLogger(SimpleRpcServer.class);
  int port;
  ServerSocket socketServer;
  volatile boolean running;
  LinkedList<Socket> requestQueue;
  public static int TIMEOUT;

  public SimpleRpcServer(int port) throws IOException {
    this.port = port;
    this.running = false;
    this.requestQueue = new LinkedList<>();
  }

  @Override
  public void run() {
    for (int i = 0; i < 1; i++) {
      new Thread(new RpcRequestHandler()).start();
    }
    while (running) {
      try {
        socketServer = new ServerSocket(port);
        LOG.info("rpc server listening on " + port);
        while (running) {
          try {
            Socket s = socketServer.accept();
            s.setSoTimeout(TIMEOUT);
            synchronized (requestQueue) {
              requestQueue.addLast(s);
              requestQueue.notifyAll();
            }
          } catch (Throwable t) {
            LOG.warn("rpc handler exception", t);
          }
        }
      } catch (IOException e) {
        LOG.warn("Rpc server listen " + port + " failed.", e);
      } finally {
        if (socketServer != null) {
          try {
            socketServer.close();
          } catch (IOException e) {
          }
        }
        socketServer = null;
      }
    }
  }

  public void start() {
    running = true;
    new Thread(this).start();
  }

  public void close() throws IOException {
    running = false;
    synchronized (requestQueue) {
      requestQueue.notifyAll();
    }
    ServerSocket tmp = socketServer;
    socketServer = null;
    tmp.close();
  }

  abstract public byte[] handleRpc(byte[] request) throws Exception;

  class RpcRequestHandler implements Runnable {

    @Override
    public void run() {
      while (running) {
        Socket socket = null;
        try {
          synchronized (requestQueue) {
            while (running && requestQueue.size() == 0) {
              try {
                requestQueue.wait();
              } catch (InterruptedException e) {
              }
            }
            socket = requestQueue.removeFirst();
          }
          if (!running) {
            break;
          }
          DataInputStream in = new DataInputStream(socket.getInputStream());
          byte[] buffer = new byte[in.readInt()];
          if (in.read(buffer) != buffer.length) {
            // Because we are using java io Socket, the read will return either
            // buffer.length or -1 or throw a SocketTimeoutException. There's no
            // need to repeat reading until buffer is full.
            throw new IOException("Connection error.");
          }
          byte[] response = handleRpc(buffer);
          DataOutputStream out = new DataOutputStream(socket.getOutputStream());
          out.writeInt(response.length);
          out.write(response);
          out.flush();
          out.close();
          in.close();
        } catch (Throwable t) {
          String logInfo = "Error when handle rpc ";
          if (socket != null) {
            logInfo += socket.getInetAddress();
          }
          LOG.warn(logInfo, t);
        } finally {
          if (socket != null) {
            try {
              socket.close();
            } catch (IOException e) {
            }
          }
        }
      }
    }
  }
}
