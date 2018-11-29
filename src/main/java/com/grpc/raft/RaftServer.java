package com.grpc.raft;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import com.util.ConfigUtil;
import com.util.Connection;
import grpc.RaftServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;

/**
 * Node in a raft cluster. Contains all of the stored data and methods
 * related to the internal workings of the node itself. See Impl classes
 * for networking implementations.
 */
public class RaftServer {

	//Raft Internals
	protected ConcurrentHashMap<String, String> data;
	protected int numEntries;
	protected int raftState; //0 for follower, 1 for candidate, 2 for leader
	protected long term;
	protected long currentLeaderIndex;
	protected boolean [] syncUsers;

	private Timer electionTimer;
	private TimerTask startElection;

	//Raft Sending Messages
	private List<ManagedChannel> channels;
	protected List<RaftServiceGrpc.RaftServiceFutureStub> stubs;

	public RaftServer(int index){
		data = new ConcurrentHashMap<String, String>();
		numEntries = 0;
		raftState = 0;
		term = 0;
		currentLeaderIndex = index;
		syncUsers = new boolean[ConfigUtil.raftNodes.size()];

		electionTimer = new Timer();
		resetTimer();

		channels = new ArrayList<ManagedChannel>();
		stubs = new ArrayList<RaftServiceGrpc.RaftServiceFutureStub>();
		for(int i = 0; i < ConfigUtil.raftNodes.size(); i++){
			ManagedChannel channel = ManagedChannelBuilder.forAddress(
					ConfigUtil.raftNodes.get(i).getIP(),
					ConfigUtil.raftNodes.get(i).getPort()
			).build();
			RaftServiceGrpc.RaftServiceFutureStub stub = RaftServiceGrpc.newFutureStub(channel);

			channels.add(channel);
			stubs.add(stub);
		}
	}

    public static void main( String[] args ) {
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
        Server server = ServerBuilder.forPort(8080)
          .addService(new DataTransferServiceImpl(myRaftServer))
          .addService(new RaftServiceImpl(myRaftServer))
          .build();

        try {
			server.start();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		//Begin timeout


        System.out.println("Raft Server started");
        
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
	public void resetTimer(){
		//Cancel previous startElection event if initialized, clean timer
		if(startElection != null)
			startElection.cancel();
		electionTimer.purge();

		//Create new startElection event, exec after 150-300 ms
		startElection = new TimerTask() {
			@Override
			public void run() {
				leaderTimeout();
			}
		};
		electionTimer.schedule(startElection, (long)(150*(Math.random()+1)));
	}

    //The following methods are called via Timer, thus will run on a new thread
	//Just account for synchronization when doing this

    /*
    Called when a leader times out
     */
	public void leaderTimeout(){
    	//Become candidate
		raftState = 1;
		//Send async messages asking for votes
		//When accepted vote count > majority, set to leader
		//Else if timeout occurs on the messages, set to follower
			//and restart timeout countdown
	}

	public void sendHeartbeat(){
		//Send async heartbeat messages with timeouts
		//If syncUsers[index] = true, send whole map with it as well, then set syncUsers[index] to false
		//If response asks for sync, set syncUsers to true for that user
	}

	/*
	 * Leader told of new value to store, send to other nodes to poll them
	 */
	public void appendEntry(){
		//Create async messages with timeouts
		//Increment counter when receive an "accept" response
		//When counter > ConfigUtil.raftNodes.size(), send a confirmation message
	}


}
