package com.grpc.raft;

import grpc.Raft;
import grpc.RaftServiceGrpc;
import io.grpc.stub.StreamObserver;

public class RaftServiceImpl extends RaftServiceGrpc.RaftServiceImplBase{

	private RaftServer server;

	private String setkey = null;
	private String setvalue = null;

	private long voteIndex = -1; //who this server is voting for

	public RaftServiceImpl(RaftServer serv){
		super();
		server = serv;
	}

	/**
	 * Heartbeat message that is sent by the leader. May contain an entry to append.
	 * Response accepts if the leader is valid (>= term #, >= # of entries)
	 * @param request
	 * @param responseObserver
	 */
	@Override
	public void appendEntries(Raft.EntryAppend request,
							  StreamObserver<Raft.Response> responseObserver){
		System.out.println("heartbeat from "+request.getLeader()+"; term "+request.getTerm()+" vs "+server.term);
		//If this term > sent term
		if(server.term > request.getTerm()){
			Raft.Response response = Raft.Response.newBuilder()
					.setAccept(false)
					.setRequireUpdate(false)
					.build();
			responseObserver.onNext(response);
			responseObserver.onCompleted();
			return;
		}

		//If this term < sent term
		else if(server.term <= request.getTerm()){
			//Set this term as request's term, step down, reset election timer
			server.hasVoted = false;
			server.term = request.getTerm();
			server.raftState = 0; //0 is follower
			server.currentLeaderIndex = request.getLeader();
			server.resetTimeoutTimer();
			if(server.heartbeatEvent != null)
				server.heartbeatEvent.cancel();
			Raft.Response response = Raft.Response.newBuilder()
					.setAccept(true)
					.setRequireUpdate(true)
					.build();
			responseObserver.onNext(response);
			responseObserver.onCompleted();
			return;
		}

		//Mismatched stored data
		if(request.getAppendedEntries() > server.numEntries){ //requester ahead
			server.hasVoted = false;
			server.resetTimeoutTimer();
			server.currentLeaderIndex = request.getLeader();
			Raft.Response response = Raft.Response.newBuilder()
					.setAccept(false)
					.setRequireUpdate(true)
					.build();
			responseObserver.onNext(response);
			responseObserver.onCompleted();
			return;
		}else if(request.getAppendedEntries() < server.numEntries){ //this server is ahead
			server.hasVoted = false;
			server.resetTimeoutTimer();
			server.currentLeaderIndex = request.getLeader();
			Raft.Response response = Raft.Response.newBuilder()
					.setAccept(false)
					.setRequireUpdate(false)
					.build();
			responseObserver.onNext(response);
			responseObserver.onCompleted();
			return;
		}

		//Everything is fine
		server.hasVoted = false;
		server.resetTimeoutTimer();
		server.currentLeaderIndex = request.getLeader();
		//If received a heartbeat after receiving an entry to add, commit it
		if(setkey != null){
			server.data.put(setkey, setvalue);
			setkey = null;
			setvalue = null;
		}
		/* This should not be needed anymore, since value setting is handled by a new rpc function
		//If received an entry to add, store it for now, return acceptance
		if(request.getEntry() != null){
			//System.out.println("Received non-null entry!");
			//System.out.println(request.getEntry().getKey());
			setkey = request.getEntry().getKey();
			setvalue = request.getEntry().getValue();
		}*/

		Raft.Response response = Raft.Response.newBuilder()
				.setAccept(true)
				.setRequireUpdate(false)
				.build();
		responseObserver.onNext(response);
		responseObserver.onCompleted();

	}

	/**
	 * Reset the server's data field, fill it with the sent values
	 * @param request
	 * @param responseObserver
	 */
	@Override
	public void fixEntries(Raft.EntryFix request,
						   StreamObserver<Raft.Response> responseObserver){
		System.out.println("Entries being fixed!");
		server.data.clear();
		for(Raft.Entry entry : request.getMapList()){
			server.data.put(entry.getKey(), entry.getValue());
		}

		server.numEntries = (int)request.getNumEntries();
		Raft.Response response = Raft.Response.newBuilder()
				.setAccept(true)
				.setRequireUpdate(false)
				.build();
		responseObserver.onNext(response);
		responseObserver.onCompleted();
	}

	/**
	 * Called by a candidate requesting this node's vote. Will return
	 * an accepting vote if the candidate is valid and this node hasn't
	 * already voted for someone of the same term #.
	 * @param request
	 * @param responseObserver
	 */
	@Override
	public void requestVote(Raft.VoteRequest request,
							StreamObserver<Raft.Response> responseObserver){
		System.out.println("Received vote request from "+request.getMyindex());

		//Candidate has higher term, vote yes and set own values
		Raft.Response response = null;
		if(request.getTerm() > server.term){
			System.out.println(request.getMyindex()+" is of higher term, voting for it!");
			server.term = request.getTerm();
			server.raftState = 0;
			if(server.heartbeatEvent != null)
				server.heartbeatEvent.cancel();
			server.resetTimeoutTimer();
			server.hasVoted = true;
		//	voteIndex = request.getLeader();
			response = Raft.Response.newBuilder()
					.setAccept(true)
					.setRequireUpdate(false)
					.build();
		}
		//Candidate has lower term, vote no
		else if(request.getTerm() < server.term){
			System.out.println(request.getMyindex()+" is of lower term, vote against");
			response = Raft.Response.newBuilder()
					.setAccept(false)
					.setRequireUpdate(false)
					.build();
		}
		else if(!server.hasVoted){
			System.out.println("Not running, voting for "+request.getMyindex());
			response = Raft.Response.newBuilder()
					.setAccept(true)
					.setRequireUpdate(false)
					.build();
			server.hasVoted = true;
		}else{
			System.out.println("Already voted, voting against "+request.getMyindex());
			response = Raft.Response.newBuilder()
					.setAccept(false)
					.setRequireUpdate(false)
					.build();
		}
		responseObserver.onNext(response);
		responseObserver.onCompleted();
	}

	/**
	 * Called when the leader wants to poll a value set
	 * @param request
	 * @param responseObserver
	 */
	@Override
	public void pollEntry(Raft.EntryAppend request,
						  StreamObserver<Raft.Response> responseObserver){

		boolean accept = false;
		if(server.term <= request.getTerm() && server.numEntries <= request.getAppendedEntries()){
			accept = true;
		}

		if(server.numEntries > request.getAppendedEntries())
			System.out.println("pollEntry: follower says leader is behind");

		Raft.Response response = Raft.Response.newBuilder().setAccept(accept).build();
		responseObserver.onNext(response);
		responseObserver.onCompleted();
	}

	/**
	 * Called by the leader when a majority of followers accept a value set.
	 * When a follower receives this, it should set the key-value pair.
	 * @param request
	 * @param responseObserver
	 */
	@Override
	public void acceptEntry(Raft.EntryAppend request,
							StreamObserver<Raft.Response> responseObserver){

		System.out.println("Accepting entry addition from leader!");
		System.out.println("Key="+request.getEntry().getKey()+"Value="+request.getEntry().getValue());
		server.data.put(request.getEntry().getKey(), request.getEntry().getValue());
		server.numEntries++;
		System.out.println("Number of entries: "+server.numEntries);

		boolean accept = true;
		Raft.Response response = Raft.Response.newBuilder().setAccept(accept).build();
		responseObserver.onNext(response);
		responseObserver.onCompleted();
	}
}
