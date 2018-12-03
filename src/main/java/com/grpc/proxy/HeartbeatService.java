package com.grpc.proxy;

import java.util.Random;

import com.util.ConfigUtil;
import com.util.Connection;

import grpc.Team;
import grpc.TeamClusterServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class HeartbeatService {
	
	/**
	 * This is a Heartbeat implementation origination from raft to proxy.
	 * @return
	 */
	public boolean[] getProxyStatus() {
		
		boolean[] proxies = new boolean[ConfigUtil.proxyNodes.size()];
		
		ManagedChannel channel = null;
		int i =0;
		for(Connection proxy : ConfigUtil.proxyNodes) {
			
			channel = ManagedChannelBuilder.forTarget(proxy.getIP()+":"+ proxy.getPort())
					.usePlaintext(true)
					.build();
			
			TeamClusterServiceGrpc.TeamClusterServiceBlockingStub stub = TeamClusterServiceGrpc.newBlockingStub(channel);	

			Team.Ack request = Team.Ack.newBuilder()
			          .setIsAck(false)
			          .setMessageId(new Random().nextLong())
			          .build();

			proxies[i] = false;
			try {
				Team.Ack response = stub.heartbeat(request);
				if(response.getIsAck())
					proxies[i] = true;
			}
			catch (Exception e){}
			
			i++;
			channel.shutdownNow();

			
		}
		
		return proxies;
	}

}
