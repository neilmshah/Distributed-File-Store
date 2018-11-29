package com.grpc.raft;

import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import io.grpc.Server;
import io.grpc.ServerBuilder;

/**
 * Node in a raft cluster. Contains all of the stored data and methods
 * related to the internal workings of the node itself. See Impl classes
 * for networking implementations.
 */
public class RaftServer {

	private ConcurrentHashMap data;
	private int raftState; //0 for follower, 1 for candidate, 2 for leader
	private int term;

    public static void main( String[] args ) {
        System.out.println( "Hello Raft!" );
        RaftServer myRaftServer = new RaftServer();
        Server server = ServerBuilder.forPort(8080)
          .addService(new DataTransferServiceImpl(myRaftServer.data))
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
    Called when a leader times out
     */
	public void leaderTimeout(){
    	//Become candidate, send out vote
		raftState = 1;
		
	}
}
