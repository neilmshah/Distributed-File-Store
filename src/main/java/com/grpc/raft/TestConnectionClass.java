package com.grpc.raft;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import grpc.FileTransfer;
import grpc.RaftServiceGrpc;
import grpc.Team;
import grpc.TeamClusterServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import javax.annotation.Nullable;

/**
 * Contains test code for the RaftServer
 */
public class TestConnectionClass {

	public static void main(String [] args){
		ManagedChannel channel = ManagedChannelBuilder
				.forTarget("localhost:8700").usePlaintext(true).build();
		TeamClusterServiceGrpc.TeamClusterServiceBlockingStub stub =
				TeamClusterServiceGrpc.newBlockingStub(channel);

		Team.ChunkLocations request = Team.ChunkLocations.newBuilder()
				.setFileName("poop.jpg")
				.setChunkId(0)
				.addDbAddresses("localhost")
				.setMaxChunks(2)
				.build();
		System.out.println(stub.updateChunkLocations(request).getIsAck());
		/*
		Futures.addCallback(stub.updateChunkLocations(request), new FutureCallback<Team.Ack>() {
			@Override
			public void onSuccess(@Nullable Team.Ack ack) {
				System.out.println("Successful response! "+ack.getIsAck());
			}

			@Override
			public void onFailure(Throwable throwable) {
				System.out.println("Something broke!");
				throwable.printStackTrace();
			}
		});
		*/
	}
}
