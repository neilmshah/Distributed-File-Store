package com.grpc.raft;

import static io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.log4j.Logger;

import grpc.Team;
import grpc.TeamClusterServiceGrpc;
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

	public TeamClusterServiceImpl(RaftServer serv){
		super();
		server = serv;
	}

	/**
	 * This method updates the RAFT HashMap with the DB address where it stored the chunk
	 * @param request
	 * @param responseObserver
	 */
	@Override
	public void updateChunkLocations(Team.ChunkLocations request, StreamObserver<Team.Ack> responseObserver) {

		logger.debug("UpdateChunkLocations started.. ");
		String key = request.getFileName()+KEY_DELIMINATOR+ request.getChunkId();
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
			server.data.put(key, value);
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
			server.data.put(key, newValue);
			
		}
		
		Team.Ack response = Team.Ack.newBuilder()
				.setMessageId(request.getMessageId())
				.setIsAck(true)
				.build();

		// Use responseObserver to send a single response back
		responseObserver.onNext(response);

		logger.debug("UpdateChunkLocations ended.. ");
		responseObserver.onCompleted();

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
		String key = request.getFileName()+KEY_DELIMINATOR+ request.getChunkId();
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
