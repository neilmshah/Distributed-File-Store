package com.grpc.raft;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.grpc.proxy.ProxyServer;
import com.util.ConfigUtil;
import com.util.Connection;
import grpc.Raft;
import grpc.RaftServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.netty.util.internal.logging.Log4J2LoggerFactory;

import javax.annotation.Nullable;

/**
 * Node in a raft cluster. Contains all of the stored data and methods
 * related to the internal workings of the node itself. See Impl classes
 * for networking implementations.
 */
public class RaftServer {

	private final static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(RaftServer.class);
	private final static int LEADER_TIMEOUT = 3000;
	private final static int HEARTBEAT_SCHEDULE = 1000;

	//Raft Internals
	protected ConcurrentHashMap<String, String> data;
	protected int numEntries;
	protected int raftState; //0 for follower, 1 for candidate, 2 for leader
	protected long term;
	protected long currentLeaderIndex;
	protected boolean [] syncUsers;
	protected int index;
	protected ConcurrentLinkedQueue<String> changes;

	private Timer electionTimer, heartbeatTimer;
	public TimerTask electionEvent, heartbeatEvent;

	private AtomicInteger votes;
	private AtomicInteger accepts; //number of nodes that accepted a change
	protected boolean hasVoted;

	//Raft Sending Messages
	private List<ManagedChannel> channels;
	protected List<RaftServiceGrpc.RaftServiceFutureStub> stubs;

	public RaftServer(int index){
		data = new ConcurrentHashMap<String, String>();
		data.put("test.txt#0", "2$10.0.20.1:8000,10.0.20.3:8000");
		numEntries = 0;
		raftState = 0;
		term = 0;
		currentLeaderIndex = index;
		syncUsers = new boolean[ConfigUtil.raftNodes.size()];
		changes = new ConcurrentLinkedQueue<String>();

		electionTimer = new Timer();
		heartbeatTimer = new Timer();
		resetTimeoutTimer(); //TODO uncomment when ready to test raft itself!
		this.index = index;

		votes = new AtomicInteger(0);
		accepts = new AtomicInteger(0);

		channels = new ArrayList<ManagedChannel>();
		stubs = new ArrayList<RaftServiceGrpc.RaftServiceFutureStub>();
		for(int i = 0; i < ConfigUtil.raftNodes.size(); i++){
			String target = ConfigUtil.raftNodes.get(i).getIP() + ":" + ConfigUtil.raftNodes.get(i).getPort();
			ManagedChannel channel = ManagedChannelBuilder.forTarget(target).usePlaintext(true).build();
			//ManagedChannelBuilder.forAddress(ConfigUtil.raftNodes.get(i).getIP(),ConfigUtil.raftNodes.get(i).getPort()).build();
			RaftServiceGrpc.RaftServiceFutureStub stub = RaftServiceGrpc.newFutureStub(channel);

			channels.add(channel);
			stubs.add(stub);
		}
	}

    public static void main( String[] args ) {
		new ConfigUtil();

		System.out.println("Hello Raft!");
        if(args.length != 1){
        	System.err.println("Improper number of arguments. Must be 1!");
        	System.err.println("Must be integer that represents index of machine in config.json");
        	System.exit(1);
		}
		int index = -1;
		try{
			index = Integer.parseInt(args[0]);
		}catch (NumberFormatException e){
        	System.err.println("Argument must be an integer!");
        	System.exit(1);
		}

		//Get index of this computer/server in config file, start server
        Connection connection =	ConfigUtil.raftNodes.get(index);
        RaftServer myRaftServer = new RaftServer(index);
        Server server = ServerBuilder.forPort(ConfigUtil.raftNodes.get(index).getPort())
          .addService(new DataTransferServiceImpl(myRaftServer))
          .addService(new RaftServiceImpl(myRaftServer))
          .addService(new TeamClusterServiceImpl(myRaftServer))
          .build();

        try {
			server.start();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

        System.out.println("Started Raft Node: "+index);
        
        try {
			server.awaitTermination();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("Awaiting server termination");
    }

	/*
	 * Sets/resets the deadline of the election timeout
	 */
	public void resetTimeoutTimer(){
		System.out.println("Resetting leader timeout timer");
		//Cancel previous startElection event if initialized, clean timer
		if(electionEvent != null)
			electionEvent.cancel();
		electionTimer.purge();

		//Create new startElection event, exec after 150-300 ms
		electionEvent = new TimerTask() {
			@Override
			public void run() {
				leaderTimeout();
			}
		};
		electionTimer.schedule(electionEvent, (long)(LEADER_TIMEOUT*(Math.random()+1)));
	}


	public void resetHeartbeatTimer(){
		if(heartbeatEvent != null)
			heartbeatEvent.cancel();
		heartbeatTimer.purge();

		//Create new startElection event, exec after 150-300 ms
		heartbeatEvent = new TimerTask() {
			@Override
			public void run() {
				sendHeartbeat();
			}
		};
		heartbeatTimer.schedule(heartbeatEvent, HEARTBEAT_SCHEDULE);
	}

    //The following methods are called via Timer, thus will run on a new thread
	//Just account for synchronization when doing this

    /*
    Called when a leader times out
     */
	public void leaderTimeout(){
		System.out.println("Leader Timeout! Initiating election for node "+index);
    	//Become candidate, reset voting mechanism, increment term
		raftState = 1;
		votes.set(1); //vote for self, so start at 1
		term++;
		hasVoted = true;
		//Send async messages asking for votes
		for(int i = 0; i < stubs.size(); i++){
			if(i == index) //don't bother asking self for vote, auto yes
				continue;
			RaftServiceGrpc.RaftServiceFutureStub stub = stubs.get(i);
			final int stubIndex = i;
			Raft.VoteRequest request = Raft.VoteRequest.newBuilder()
					.setMyindex(index)
					.setAppendedEntries(numEntries)
					.setTerm(term)
					.build();

			Futures.addCallback(stub.requestVote(request), new FutureCallback<Raft.Response>() {
				@Override
				public void onSuccess(@Nullable Raft.Response response) {
					countVotes(stubIndex, response.getAccept());
				}

				@Override
				public void onFailure(Throwable throwable) {
					System.err.println("Error occured on grpc response!");
					throwable.printStackTrace();
				}
			});
		}
		resetTimeoutTimer();
	}

	private void countVotes(long voter, boolean acceptVote){

		//Ignore if already leader or just a candidate
		if(raftState == 0 || raftState == 2)
			return;
		//Count the vote
		if(acceptVote)
			votes.incrementAndGet();
		System.out.println("Current Votes: "+votes.get()+" / "+ConfigUtil.raftNodes.size());
		//Declare this node the leader, cancel the timeout timer
		if(votes.get() > ConfigUtil.raftNodes.size()/2){
			System.out.println("Node "+index+" Elected Leader!");
			raftState = 2;
			votes.set(0);
			electionEvent.cancel();
			resetHeartbeatTimer();
		}
	}

	public void sendHeartbeat(){
		System.out.println("Sending heartbeat to nodes!");
		//Send async heartbeat messages with timeouts
		//If syncUsers[index] = true, send whole map with it as well, then set syncUsers[index] to false
		//If response asks for sync, set syncUsers to true for that user
		for(int i = 0; i < stubs.size(); i++){
			if(i == index) //no need to send heartbeat to own node
				continue;
			RaftServiceGrpc.RaftServiceFutureStub stub = stubs.get(i);
			final int stubIndex = i;
			Raft.EntryAppend request = Raft.EntryAppend.newBuilder()
					.setTerm(term)
					.setLeader(index)
					.build();

			Futures.addCallback(stub.appendEntries(request), new FutureCallback<Raft.Response>() {
				@Override
				public void onSuccess(@Nullable Raft.Response response) {
					if(response.getRequireUpdate()){
						//Send message with hashmap
						ArrayList<Raft.Entry> entryList = new ArrayList<Raft.Entry>();
						for(java.util.Map.Entry<String, String> key : data.entrySet()){
							entryList.add(
									Raft.Entry.newBuilder()
											.setKey(key.getKey())
											.setValue(key.getValue())
											.build()
							);
						}
						Raft.EntryFix entries = Raft.EntryFix.newBuilder()
								.addAllMap(entryList)
								.build();
					}
					else if(!response.getRequireUpdate()){
						//Do nothing, either everything is fine, or this leader is being ignored
						//If ignored, expect a heartbeat from the true leader soon.
					}
				}

				@Override
				public void onFailure(Throwable throwable) {
					System.err.println("Error occured on grpc response!");
					throwable.printStackTrace();
				}
			});
		}
		resetHeartbeatTimer();
	}


}
