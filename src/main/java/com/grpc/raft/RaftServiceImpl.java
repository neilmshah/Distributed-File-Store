package com.grpc.raft;

import grpc.Raft;
import grpc.RaftServiceGrpc;
import io.grpc.stub.StreamObserver;

public class RaftServiceImpl extends RaftServiceGrpc.RaftServiceImplBase{

	private RaftServer server;

	public RaftServiceImpl(RaftServer serv){
		super();
		server = serv;
	}

	public void AppendEntries(Raft.EntryAppend request,
							  StreamObserver<Raft.Response> responseObserver){
		//
	}

	public void RequestVote(Raft.VoteRequest request,
							StreamObserver<Raft.Response> responseObserver){
		//
	}
}
