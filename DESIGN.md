Raft程序设计思想

1. 每一个Raft Node同时具有两个身份，Proposer和Acceptor。Proposer是主动方，负责给Acceptor发送rpc；Acceptor只负责被动响应。对于一个Proposer来说，其他所有Acceptor是等价的，不存在要对自身的Acceptor特殊处理；对于一个Acceptor来说，其他所有Proposer是等价的，也不存在对自身Proposer的特殊响应。RaftNodeImpl.run()线程对应了Proposer，RaftNodeImpl启动的JsonRaftServer对应Acceptor。

2. Proposer的工作模式：
* 如果处于LOOK状态，就发起一轮选举，同时向所有Acceptor发送requestVote rpc，如果rpc返回true则记得到一票，其他情况（rpc返回false，rpc超时）都不记得票；在一个rpc超时的时间内Proposer一定得到了所有的投票结果，如果得到了QUORUM支持，就转变为leader，同时sleep一个HEART_BEAT_TIMEOUT，以便于上一任leader发现自己已经不是leader了，避免脑裂；如果没有QUORUM支持，就进入随机回退；
* 如果处于LEAD状态，就给每个Acceptor启动一个线程，如raft论文所描述的，初始appendEntries发送的是Proposer本地最新的LogEntry，如果得到的反馈是false就向前推一个，直到Acceptor追上，在Acceptor追的过程中是发送完一个马上发下一个，当Acceptor追上后就以HEART_BEAT_TIMEOUT/2为周期向Acceptor反复发送最新一个Log Entry；如果超过HEART_BEAT_TIMEOUT时间都没有成功的appendEntries就记为这个Acceptor不再follow；当follow的数量不足QUORUM时，Proposer会停掉所有到Acceptor的线程，并转为LOOK。
* 如果处于FOLLOW状态，就不断的比较上一次心跳时间距现在是否已经超过HEART_BEAT_TIMEOUT，如果超过就进入LOOK状态，否则就保持follow状态，sleep并等待下次心跳检查。

3. Acceptor工作模式：
**响应requestVote：**
* 如果收到requestVote时Acceptor不是LOOK状态的话，就检查一下票的electionTerm，如果小于等于自己的electionTerm的话说明是个过期票，直接返回false；否则将自己转成LOOK状态，后续处理就和收到时已经是LOOK状态一样了；
* Proposer传递来了它记录的最后一个LogEntry的term & lid，Acceptor比较如果自己的term * lid更大，就投反对票，如果对方大于等于自己就投赞成票；投赞成票后这个投票要保持LEADER_ELECTION_TIMEOUT，这期间收到其他requestVote也不会改自己的票，详细内容参考Raft变票部分的说明； **响应appendEntries：**
* 收到appendEntries说明leader已经产生，进入Follower状态，更新自己的electionTerm；
* 将收到的logPre与自己记录的所有log进行比较，如果自己的log中找不到logPre就返回false；找到的话就将logPre后面部分截断，并在logPre后接上log；除了内存修改这里也要做持久化的内容，包括对日志文件的truncate；如果有truncate发生，就清空map，并重放日志（为了保证日志重放不花费太长时间，要让集群经常做snapshot）；
**响应setKeyValue：**
* 如果状态不是Leader的话就返回一个NotLeaderException；否则新建一个LogEntry并加入到log尾，等待Proposer线程将其自动同步到其他Follower；比较commit的term & lid与本次log的term与lid，如果commit更大的话就可以给返回成功；
**响应getValue：**
* 如果状态不是Leader的话就返回一个NotLeaderException；否则返回map查到的值；
* 这里有重要一点，在raft论文中也强调过，即选举刚刚完成后，前面日志的commit要由后面commit来间接的确认，这是因为本地的log尾巴部分可能很多都还没有达到commit状态，在raft论文中就需要下一次entry被提出时，通过commit掉这个新entry来间接的commit掉之前的entries。这里实现的时候做了点改动，每次选举前记录一个finalLogOfLastTerm，每次选举后，leader都主动的写一条没有内容的log entry，收到getValue()调用时，如果Leader的commit要小于finalLogOfLastTerm，则说明间接确认还没有完成，Follower还处于追的状态，这时就返回SafeModeException；否则说明间接确认已经完成，可以正常的返回map查到的值；

4. Raft在选举过程不会变票，即任何Acceptor在给一个Proposer投票后，都将保持LEADER_ELECTION_TIMEOUT这么长时间不变票；如果超过了这个时间还没有leader出现，说明上一次选举中产生了分票情况，这时候就可以放弃掉之前的投票准备投给新的Proposer；当然上一轮的选举可能在LEADER_ELECTION_TIMEOUT之前就已经结束了，但是我们这里只要保证结束即可，时间落后一些是无所谓的；下一轮选举开始前的那段随机回退，可以保证下一轮的第一个Proposer发起的投票，在上一轮最后一个Acceptor经过LEADER_ELECTION_TIMEOUT之后（通过设置超时时间和随机回退策略来实现）。

5. 对于Raft Client的保证：
* 如果setKeyValue()返回了成功，那么一定被成功commit，所有人都能看到它被commit；
* 如果setKeyValue()返回了失败，那么一定不会被commit，所有人都不会看到它被commit；
* 如果setKeyValue()超时，那么行为就是未定义的，需要client自行确认；

6. 其他：
* 每一次选举都有一个electionTerm，不论是否选出了leader；
* electionTerm保留在内存中，启动的时候term就是从盘里commit序列里找到的最大term，如果没有文件就初始化为0；之后随着选举term变大；
* 因为不会变票所以永远只会有一个Proposer asks others to follow；

7. TODO
* snapshot；
* 节点替换；
