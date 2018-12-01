package com.grpc.raft;

import grpc.Raft;
import grpc.RaftServiceGrpc;
import io.grpc.stub.StreamObserver;

public class RaftServiceImpl extends RaftServiceGrpc.RaftServiceImplBase{

	private RaftServer server;

	private String setkey = null;
	private String setvalue = null;

	private boolean hasVoted = false;
	private long voteIndex = -1; //who this server is voting for

	public RaftServiceImpl(RaftServer serv){
		super();
		server = serv;
	}

	public void AppendEntries(Raft.EntryAppend request,
							  StreamObserver<Raft.Response> responseObserver){
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
		else if(server.term < request.getTerm()){
			//Set this term as request's term, step down, reset election timer
			server.term = request.getTerm();
			server.raftState = 0; //0 is follower
			server.resetTimeoutTimer();
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
			server.resetTimeoutTimer();
			Raft.Response response = Raft.Response.newBuilder()
					.setAccept(false)
					.setRequireUpdate(true)
					.build();
			responseObserver.onNext(response);
			responseObserver.onCompleted();
			return;
		}else if(request.getAppendedEntries() < server.numEntries){ //this server is ahead
			Raft.Response response = Raft.Response.newBuilder()
					.setAccept(false)
					.setRequireUpdate(false)
					.build();
			responseObserver.onNext(response);
			responseObserver.onCompleted();
			return;
		}

		//Everything is fine
		server.resetTimeoutTimer();
		//If received a heartbeat after receiving an entry to add, commit it
		if(setkey != null){
			server.data.put(setkey, setvalue);
			setkey = null;
			setvalue = null;
		}
		//If received an entry to add, store it for now, return acceptance
		if(request.getEntry() != null){
			System.out.println("Received non-null entry!");
			setkey = request.getEntry().getKey();
			setvalue = request.getEntry().getValue();
		}

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
	public void FixEntries(Raft.EntryFix request,
						   StreamObserver<Raft.Response> responseObserver){
		server.data.clear();
		for(Raft.Entry entry : request.getMapList()){
			server.data.put(entry.getKey(), entry.getValue());
		}

		Raft.Response response = Raft.Response.newBuilder()
				.setAccept(true)
				.setRequireUpdate(false)
				.build();
		responseObserver.onNext(response);
		responseObserver.onCompleted();
	}

	public void RequestVote(Raft.VoteRequest request,
							StreamObserver<Raft.Response> responseObserver){
		//Candidate has higher term, vote yes and set own values
		Raft.Response response = null;
		if(request.getTerm() > server.term){
			server.term = request.getTerm();
			server.raftState = 0;
			hasVoted = true;
		//	voteIndex = request.getLeader();
			response = Raft.Response.newBuilder()
					.setAccept(true)
					.setRequireUpdate(false)
					.build();
		}
		//Candidate has lower term, vote no
		else if(request.getTerm() < server.term){
			response = Raft.Response.newBuilder()
					.setAccept(false)
					.setRequireUpdate(false)
					.build();
		}
		else{
			response = Raft.Response.newBuilder()
					.setAccept(true)
					.setRequireUpdate(false)
					.build();
		}
		responseObserver.onNext(response);
	}
}
