package raft;

import org.codehaus.jettison.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.LogQueue.LogEntry;
import raft.ipc.JsonRaftServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * RaftNode implementation.
 * */
public class RaftNodeImpl extends RaftNode implements Runnable {
  Logger LOG = LoggerFactory.getLogger(RaftNodeImpl.class);
  InetSocketAddress socketAddress;

  final int LEADER_ELECTION_TIMEOUT;
  final int HEART_BEAT_TIMEOUT;
  final int RPC_TIMEOUT;
  String logFile;
  HashMap<String,String> map = new HashMap<>();
  LogQueue logQueue = null;

  enum Stat {
    LOOK, LEAD, FOLLOW
  }

  int electionTerm;
  RaftNode[] nodes;
  List<FollowerHandler> fHandlers;
  Lock fHandlerLock = new ReentrantLock();
  Condition fHandlerCondition = fHandlerLock.newCondition();
  LogEntry commit = new LogEntry(-1, -1, "", "");// 当client请求的term & commit < 这个commit即可返回；
  LogEntry finalLogOfLastTerm = null;
  int QUORUM;

  Stat stat;
  long lastStatUpdateTime;
  long lastVoteTime;
  int followerNum;
  Lock statAndFollowLock = new ReentrantLock();
  Condition safCondition = statAndFollowLock.newCondition();

  // Vote
  RaftNode voteTo;

  public RaftNodeImpl(int id, Configuration conf) throws IOException {
    LEADER_ELECTION_TIMEOUT = Integer.parseInt(conf.get("leader.election.timeout"));
    HEART_BEAT_TIMEOUT = Integer.parseInt(conf.get("heart.beat.timeout"));
    RPC_TIMEOUT = Integer.parseInt(conf.get("rpc.timeout"));

    String ip = conf.get(id +".ip");
    int port = Integer.parseInt(conf.get(id+".port"));
    socketAddress = new InetSocketAddress(ip, port);
    this.id = id;
    this.logFile = conf.get(id+".log");
  }

  public void init(RaftNode[] nodes) throws IOException {
    this.followerNum = 0;
    this.voteTo = null;
    this.nodes = nodes;
    this.QUORUM = nodes.length / 2 + 1;
    this.logQueue = new LogQueue(logFile, this);
    restoreMap();
    this.electionTerm = logQueue.getLastLog().term;
    transitionToLook();
    new JsonRaftServer(socketAddress.getPort(), this).start();
  }

  @Override
  public void run() {
    while (true) {
      try {
        Stat s = getStat();
        if (s == Stat.LOOK) {
          leaderElection();
        } else if (s == Stat.FOLLOW) {
          follow();
        } else if (s == Stat.LEAD) {
          lead();
        }
      } catch (Throwable t) {
        LOG.warn("something goes wrong", t);
      }
    }
  }

  void lead() {
    LOG.info(id + " I'm leading.");
    LogEntry newTermLog = new LogEntry(electionTerm, 0, "", "");
    logQueue.appendLog(newTermLog);
    logQueue.writeLog(newTermLog);
    setFollowerNum(0);
    startFHandlers();
    try {
      Thread.sleep(HEART_BEAT_TIMEOUT);
    } catch (InterruptedException e) {
    }
    while (getStat() == Stat.LEAD && getFollowerNum() >= QUORUM) {
      statAndFollowLock.lock();
      try {
        safCondition.await();
      } catch (InterruptedException e) {
      }
      statAndFollowLock.unlock();
    }
    stopFHandlers();
    transitionToLook();
  }

  void follow() {
    LOG.info(id + " I'm following.");
    while (true) {
      try {
        statAndFollowLock.lock();
        if (getStat() == Stat.FOLLOW
            && Time.monotonicNow() - getLastStatUpdateTime()
            < HEART_BEAT_TIMEOUT) {
          try {
            safCondition.await(HEART_BEAT_TIMEOUT - (Time.monotonicNow()
                - getLastStatUpdateTime()), TimeUnit.MILLISECONDS);
          } catch (InterruptedException e) {
          }
        } else {
          break;
        }
      } finally {
        statAndFollowLock.unlock();
      }
    }
    if (Time.monotonicNow() - getLastStatUpdateTime() >= HEART_BEAT_TIMEOUT) {
      transitionToLook();
    }
  }

  void leaderElection() {
    if (getStat() != Stat.LOOK) {
      return;
    }
    long start = Time.monotonicNow();
    electionTerm = electionTerm + 1;
    LOG.info(id + " starts election. term " + electionTerm);
    ThreadPoolExecutor tpe =
        new ThreadPoolExecutor(nodes.length, nodes.length, 1,
            TimeUnit.MILLISECONDS, new LinkedBlockingQueue());
    CompletionService<Boolean> cs = new ExecutorCompletionService<Boolean>(tpe);
    for (int i = 0; i < nodes.length; i++) {
      final int index = i;
      cs.submit(new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          try {
            LogEntry lastLog = logQueue.getLastLog();
            return nodes[index]
                .requestVote(id, electionTerm, lastLog.term, lastLog.lid,
                    RPC_TIMEOUT);
          } catch (Throwable t) {
            LOG.warn(id + " requestVote to " + index + " failed.", t);
            return false;
          }
        }
      });
    }
    int voteNum = 0;
    for (int i = 0; i < nodes.length && getStat() == Stat.LOOK; i++) {
      boolean res = false;
      try {
         res = cs.take().get();
      } catch (InterruptedException e) {
        LOG.warn("poll error", e);
      } catch (ExecutionException e) {
        LOG.warn("poll error", e);
      }
      if (res) {
        voteNum++;
      }
      if (voteNum >= QUORUM) {
        tpe.shutdown();
        setStat(Stat.LEAD);
        // sleep HEART_BEAT_TIMEOUT so last leader could find it's not leading.
        // sleep RPC_TIMEOUT so all requestVote rpc will end.
        try {
          long sleepTime = HEART_BEAT_TIMEOUT;
          if (HEART_BEAT_TIMEOUT < RPC_TIMEOUT) {
            sleepTime = RPC_TIMEOUT;
          }
          Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
        }
        return;
      }
    }
    tpe.shutdown();

    // random sleep before next round election.
    long timeLeft = LEADER_ELECTION_TIMEOUT - (Time.monotonicNow() - start);
    if (timeLeft < 0) {
      timeLeft = 0;
    }
    statAndFollowLock.lock();
    if (getStat() == Stat.LOOK) {
      try {
        long random = (long) (Math.random() * LEADER_ELECTION_TIMEOUT);
        LOG.info(id + " wait " + (timeLeft + random) + "ms before next election");
        safCondition.await((timeLeft + random), TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
      }
    }
    statAndFollowLock.unlock();
  }

  @Override
  public boolean requestVote(int srcId, int electionTerm, int term,
      long lid, int timeout)
      throws SocketTimeoutException, IOException, JSONException {
    RaftNode sourceNode = nodes[srcId];
    if (Time.monotonicNow() - lastVoteTime >= LEADER_ELECTION_TIMEOUT) {
      // Last round of election is over. Clear voteTo so we can vote to new one.
      setVoteTo(null);
    }
    RaftNode vt = getVoteTo();
    if (getStat() != Stat.LOOK) {
      if (electionTerm > this.electionTerm) {
        transitionToLook();
      } else { // The election of rElectionTerm has finished.
        LOG.info(id + " doesn't vote to " + srcId + " because it's a timeout vote");
        return false;
      }
    }
    boolean res = false;
    LogEntry lastLog = logQueue.getLastLog();
    if (vt != null) {
      if (sourceNode == vt) {
        res = true;
      } else {
        res = false;
      }
    } else if (lastLog.term > term) {
      res = false;
    } else if (lastLog.term == term && lastLog.lid > lid) {
      res = false;
    } else if (electionTerm < this.electionTerm) {
      res = false;
    } else {
      setVoteTo(sourceNode);
      res = true;
    }
    if (res) {
      LOG.info(id + " votes to " + srcId);
    } else {
      LOG.info(id + " doesn't vote to " + srcId);
    }
    return res;
  }

  @Override public boolean appendEntries(int srcId, LogEntry logPre,
      LogEntry toAppend, int timeout)
      throws SocketTimeoutException, IOException, JSONException {
    boolean res = false;
    RaftNode sourceNode = nodes[srcId];
    if (this.equals(sourceNode)) {
      res = true;
    } else {
      setStat(Stat.FOLLOW);
      if (toAppend.term > electionTerm) {
        electionTerm = toAppend.term;
      }
      // do append
      LogEntry toCompare = logQueue.getLastLog();
      while (true) {
        // Since every node has the same head(term=-1,lid=-1), and each
        // successful election leave a log with new term, we will eventually
        // get equals.
        int tmp = logPre.compareTo(toCompare);
        if (tmp == 0) {
          if (logQueue.appendLog(toCompare, toAppend)) {
            logQueue.writeLog(toCompare, toAppend);
          }
          res = true;
          return res;
        } else if (tmp > 0) { // toCompare > logPre, truncate.
          toCompare = logQueue.getPre(toCompare);
          if (toCompare == null) {
            break;
          }
        } else { // tmp < logPre, return false to remote and wait
                 // for smaller logPre.
          break;
        }
      }
      res = false;
      LOG.debug("append log return false." + toAppend);// TODO:debug
    }
    return res;
  }

  @Override
  public boolean setKeyValue(String key, String value, int timeout)
      throws SocketTimeoutException, IOException, JSONException {
    boolean res = false;
    if (getStat() != Stat.LEAD) {
      throw new NotLeaderException(id + " is not leader.");
    } else {
      if (key == null || value == null) {
        res = false;
      } else if (key.length() == 0 || value.length() == 0) {
        return false;
      } else if (key.contains(";") || value.contains(";")) {
        res = false;
      } else {
        LogEntry en = logQueue.appendLog(key, value);
        logQueue.writeLog(en);
        fHandlerLock.lock();
        try {
          while (!(commit.term > en.term || (commit.term == en.term
              && commit.lid >= en.lid))) {
            try {
              fHandlerCondition.await();
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
          }
        } finally {
          fHandlerLock.unlock();
        }
        synchronized (this) {
          map.put(key, value);
        }
        res = true;
      }
    }
    return res;
  }

  @Override
  public String getValue(String key, int timeout)
      throws SocketTimeoutException, IOException, JSONException {
    if (getStat() != Stat.LEAD) {
      throw new NotLeaderException(id + " is not leader.");
    } else {
      if (key == null) {
        return null;
      } else {
        fHandlerLock.lock();
        try {
          if (finalLogOfLastTerm == null
              || commit.term < finalLogOfLastTerm.term || (
              commit.term == finalLogOfLastTerm.term
                  && commit.lid < finalLogOfLastTerm.lid)) {
            throw new SafeModeException(
                id + " is in safe mode. Expect:" + finalLogOfLastTerm + " Wait:"
                    + commit);
          }
        } finally {
          fHandlerLock.unlock();
        }
        String value = null;
        synchronized (this) {
          value = map.get(key);
        }
        return value;
      }
    }
  }

  void restoreMap() {
    logQueue.restoreKeyValue(map);
  }

  /**
   * Do appendEntries to one Follower / Acceptor.
   * */
  class FollowerHandler extends Thread {

    RaftNode node;
    long lastContactTime = -1;
    boolean follow =false;
    boolean running;
    volatile int termId;
    volatile long commitId;
    LogEntry appendLog;

    public FollowerHandler(RaftNode node) {
      this.node = node;
      this.running = true;
      this.termId = -1;
      this.commitId = 0;
      this.appendLog = logQueue.getLastLog();
    }

    @Override
    public void run() {
      boolean hasNext = true;
      while (running) {
        try {
          if (!hasNext) {
            LogEntry next = logQueue.getNext(appendLog);
            if (next != null) {
              appendLog = next;
            }
          }
          if (node.appendEntries(id, logQueue.getPre(appendLog), appendLog,
              RPC_TIMEOUT)) {
            termId = appendLog.term;
            commitId = appendLog.lid;
            LogEntry next = logQueue.getNext(appendLog);
            if (next != null) {
              appendLog = next;
              hasNext = true;
            } else {
              hasNext = false;
            }
            updateCommit();
          } else {
            appendLog = logQueue.getPre(appendLog);
            hasNext = true;
          }
          lastContactTime = Time.monotonicNow();
        } catch (Exception e) {
        } finally {
          if (Time.monotonicNow() - lastContactTime > HEART_BEAT_TIMEOUT) {
            if (follow) {
              follow = false;
              decFollowerNum();
            }
          } else {
            if (!follow) {
              follow = true;
              incFollowerNum();
            }
          }
        }
        // If hasNext to send, then skip sleep so the Follower can catch up as
        // fast as it can. If the Follower has already catch up, send heartbeat
        // every HEART_BEAT_TIMEOUT / 2.
        if (!hasNext) {
          if (lastContactTime > 0 && running && (
              Time.monotonicNow() - lastContactTime < HEART_BEAT_TIMEOUT / 2)) {
            try {
              Thread.sleep(HEART_BEAT_TIMEOUT / 2);
            } catch (InterruptedException e) {
            }
          }
        }
      }
    }
  }

  // start FollowerHandler threads for each Acceptor / Follower.
  void startFHandlers() {
    fHandlerLock.lock();
    try {
      while (fHandlers != null) {
        stopFHandlers();
      }
      fHandlers = new ArrayList<>();
      for (int i = 0; i < nodes.length; i++) {
        FollowerHandler fh = new FollowerHandler(nodes[i]);
        fh.start();
        fHandlers.add(fh);
      }
    } finally {
      fHandlerLock.unlock();
    }
  }

  void stopFHandlers() {
    fHandlerLock.lock();
    try {
      if (fHandlers != null) {
        LOG.info(id + " quorum failed. Give up leader.");
        for (int i = 0; i < fHandlers.size(); i++) {
          fHandlers.get(i).running = false;
          fHandlers.get(i).interrupt();
        }
        fHandlers = null;
      }
    } finally {
      fHandlerLock.unlock();
    }
  }

  void updateCommit() {
    fHandlerLock.lock();
    try {
      if (fHandlers != null) {
        fHandlers.sort((FollowerHandler o1, FollowerHandler o2) -> {
          if (o1.termId < o2.termId) {
            return -1;
          } else if (o1.termId > o2.termId) {
            return 1;
          } else if (o1.commitId < o2.commitId) {
            return -1;
          } else if (o1.commitId > o2.commitId) {
            return 1;
          } else {
            return 0;
          }
        });
        FollowerHandler fh = fHandlers.get(fHandlers.size() - QUORUM);
        this.commit.term = fh.termId;
        this.commit.lid = fh.commitId;
        fHandlerCondition.signalAll();
      }
    } finally {
      fHandlerLock.unlock();
    }
  }

  void transitionToLook() {
    setStat(Stat.LOOK);
    setVoteTo(null);
    fHandlerLock.lock();
    try {
      commit.lid = -1;
      commit.term = -1;
      finalLogOfLastTerm = logQueue.getLastLog();
    } finally {
      fHandlerLock.unlock();
    }
    // end and clean last stat
  }

  synchronized void setVoteTo(RaftNode node) {
    voteTo = node;
    if (voteTo != null) {
      lastVoteTime = Time.monotonicNow();
    }
  }

  synchronized RaftNode getVoteTo() {
    return voteTo;
  }

  Stat getStat() {
    statAndFollowLock.lock();
    Stat tmp = stat;
    statAndFollowLock.unlock();
    return tmp;
  }

  long getLastStatUpdateTime() {
    statAndFollowLock.lock();
    long tmp = lastStatUpdateTime;
    statAndFollowLock.unlock();
    return tmp;
  }

  void setStat(Stat stat) {
    statAndFollowLock.lock();
    if (this.stat != stat) {
      LOG.info(id + " stat change from " + this.stat + " to " + stat);
    }
    this.stat = stat;
    this.lastStatUpdateTime = Time.monotonicNow();
    safCondition.signalAll();
    statAndFollowLock.unlock();
  }

  void setFollowerNum(int num) {
    statAndFollowLock.lock();
    followerNum = num;
    safCondition.signalAll();
    statAndFollowLock.unlock();
  }

  void incFollowerNum() {
    statAndFollowLock.lock();
    followerNum++;
    safCondition.signalAll();
    statAndFollowLock.unlock();
  }

  void decFollowerNum() {
    statAndFollowLock.lock();
    if (followerNum > 0)
      followerNum--;
    safCondition.signalAll();
    statAndFollowLock.unlock();
  }

  int getFollowerNum() {
    statAndFollowLock.lock();
    int tmp = followerNum;
    statAndFollowLock.unlock();
    return tmp;
  }
}
