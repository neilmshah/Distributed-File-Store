package com.grpc.raft;

import static io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import com.util.ConfigUtil;
import com.util.Connection;
import grpc.Raft;
import grpc.RaftServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.log4j.Logger;

import grpc.Team;
import grpc.TeamClusterServiceGrpc;
import grpc.Team.Ack;
import grpc.Team.ChunkLocations;
import io.grpc.stub.StreamObserver;

/**
 * This class communicates the the RAFT Servers
 * @author Sricheta's computer
 *
 */
public class TeamClusterServiceImpl extends TeamClusterServiceGrpc.TeamClusterServiceImplBase{

	private final String KEY_DELIMINATOR= "#";
	private RaftServer server;
	final static Logger logger = Logger.getLogger(TeamClusterServiceImpl.class);
	
	public TeamClusterServiceImpl(){
		super();
	}
	
	public TeamClusterServiceImpl(RaftServer serv){
		super();
		server = serv;
	}
	
	
	@Override
	public void heartbeat(Team.Ack request, StreamObserver<Team.Ack> responseObserver) {
		
		Team.Ack  response = Team.Ack.newBuilder().setIsAck(true).setMessageId(request.getMessageId()).build();
		responseObserver.onNext(response);
	    responseObserver.onCompleted();
		
	}

	/**
	 * This method updates the RAFT HashMap with the DB address where it stored the chunk
	 * @param request
	 * @param responseObserver
	 */
	@Override
	public void updateChunkLocations(Team.ChunkLocations request, StreamObserver<Team.Ack> responseObserver) {

		//Forward command to leader and return that
		if(server.raftState != 2) {
			Connection con = ConfigUtil.raftNodes.get((int) server.currentLeaderIndex);
			ManagedChannel channel = ManagedChannelBuilder
					.forTarget(con.getIP() + ":" + con.getPort()).usePlaintext(true).build();
			TeamClusterServiceGrpc.TeamClusterServiceBlockingStub stub = TeamClusterServiceGrpc.newBlockingStub(channel);

			responseObserver.onNext(stub.updateChunkLocations(request));
			responseObserver.onCompleted();
			return;
		}

		logger.debug("UpdateChunkLocations started.. ");
		String key = request.getFileName()+KEY_DELIMINATOR+ request.getChunkId()+KEY_DELIMINATOR+request.getMessageId();
		String value = server.data.get(key);
		logger.debug("Value stored in hashmap for key "+key+": "+value);
		
		//MaxChunks$IP1,IP2,IP3
		// If file is getting uploaded for the first time.
		if(value == null) {
			value = request.getMaxChunks()+"$";
			StringBuilder builder = new StringBuilder();
			builder.append(value);
			for(int i =0;i<request.getDbAddressesCount();i++) {
				builder.append(request.getDbAddresses(i));
				builder.append(",");
			}
			value = builder.toString();
			value = value.substring(0, value.length() - 1);
			//server.data.put(key, value);
			logger.debug("Put key in server! "+server.data.get(key));
		}else {
			//If key is already there, only update the db addresses.
			String[] valArr = value.split("\\$");
			StringBuilder builder = new StringBuilder();
			for(int i =0;i<request.getDbAddressesCount();i++) {
				builder.append(request.getDbAddresses(i));
				builder.append(",");
			}
			valArr[1] = builder.toString().substring(0, builder.length() - 1);
			String newValue = valArr[0] + "$"+ valArr[1];
			value = newValue;
			//server.data.put(key, newValue);
			
		}

		//TODO how to tell server to share a value with the others
		//server.changes.add(key+"\\"+value);
		if(pollValueChange(key, value))
			confirmValueChange(key, value);
		
		Team.Ack response = Team.Ack.newBuilder()
				.setMessageId(request.getMessageId())
				.setIsAck(true)
				.build();

		// Use responseObserver to send a single response back
		responseObserver.onNext(response);

		logger.debug("UpdateChunkLocations ended.. ");
		responseObserver.onCompleted();

	}

	private boolean pollValueChange(String key, String value){
		//Check with other nodes if we can make a change
		int accepts = 1;
		for(int i = 0; i < ConfigUtil.raftNodes.size(); i++){
			if(i == server.index)
				continue;

			Connection con = ConfigUtil.raftNodes.get(i);
			ManagedChannel channel = ManagedChannelBuilder
					.forTarget(con.getIP()+":"+con.getPort())
					.usePlaintext(true).build();

			RaftServiceGrpc.RaftServiceBlockingStub stub =
					RaftServiceGrpc.newBlockingStub(channel);

			Raft.Entry entry = Raft.Entry.newBuilder()
					.setKey(key).setValue(value).build();
			Raft.EntryAppend voteReq = Raft.EntryAppend.newBuilder()
					.setEntry(entry)
					.setTerm(server.term)
					.setLeader(server.index)
					.setAppendedEntries(server.numEntries)
					.build();

		//	Raft.Response vote = stub.withDeadlineAfter(25, TimeUnit.MILLISECONDS).pollEntry(voteReq);
			/*if(vote.getAccept())
				accepts++;
*/
			//Check if no point continuing to vote
			if(accepts > ConfigUtil.raftNodes.size()/2)
				return true;
		}
		if(accepts > ConfigUtil.raftNodes.size()/2)
			return true;
		else
			return false;
	}

	private void confirmValueChange(String key, String value){
		for(int i = 0; i < ConfigUtil.raftNodes.size(); i++){
			if(i == server.index)
				continue;

			Connection con = ConfigUtil.raftNodes.get(i);
			ManagedChannel channel = ManagedChannelBuilder
					.forTarget(con.getIP()+":"+con.getPort())
					.usePlaintext(true).build();

			RaftServiceGrpc.RaftServiceBlockingStub stub =
					RaftServiceGrpc.newBlockingStub(channel);

			Raft.Entry entry = Raft.Entry.newBuilder()
					.setKey(key).setValue(value).build();
			Raft.EntryAppend voteReq = Raft.EntryAppend.newBuilder()
					.setEntry(entry)
					.setTerm(server.term)
					.setLeader(server.index)
					.setAppendedEntries(server.numEntries)
					.build();

			//Raft.Response vote = stub.withDeadlineAfter(25, TimeUnit.MILLISECONDS).acceptEntry(voteReq);
		}
	}
	
	/**
	 * Proxy to fetch file locations of a particular file and chunk from RAFT HashMap
	 * 
	 * rpc GetChunkLocations (FileData) returns (ChunkLocations)
	 * 
	 */
	
	@Override
	public void getChunkLocations(grpc.Team.FileData request,
		        io.grpc.stub.StreamObserver<grpc.Team.ChunkLocations> responseObserver) {
		String key = request.getFileName()+KEY_DELIMINATOR+ request.getChunkId()+KEY_DELIMINATOR+request.getMessageId();
		String value = server.data.get(key);

		String[] valArr = value.split("\\$");
		ArrayList<String> arrayList = new ArrayList<String>(Arrays.asList(valArr[1]));
		
		ChunkLocations ch = ChunkLocations.newBuilder().setChunkId(request.getChunkId())
				.setFileName(request.getFileName())
				.addAllDbAddresses(arrayList)
				.setMessageId(request.getMessageId())
				.setMaxChunks(Long.parseLong(valArr[0]))
				.build();
		responseObserver.onNext(ch);
		responseObserver.onCompleted();
	}


}
