package com.grpc.client;

import com.util.ConfigUtil;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class Client {
	
	
	public static void main(String[] args) {
		
		loadConfig();
		

	}

	private static void loadConfig() {
		ConfigUtil c = new ConfigUtil();
		final ManagedChannel channel = ManagedChannelBuilder.forTarget("localhost:8080")
		        .usePlaintext(true)
		        .build();

		      // It is up to the client to determine whether to block the call
		      // Here we create a blocking stub, but an async stub,
		      // or an async stub with Future are always possible.
		      GreetingServiceGrpc.GreetingServiceBlockingStub stub = GreetingServiceGrpc.newBlockingStub(channel);
		      GreetingServiceOuterClass.HelloRequest request =
		        GreetingServiceOuterClass.HelloRequest.newBuilder()
		          .setName("Ray")
		          .build();

		      // Finally, make the call using the stub
		      GreetingServiceOuterClass.HelloResponse response = 
		        stub.greeting(request);

		      System.out.println(response);

		      // A Channel should be shutdown before stopping the process.
		      channel.shutdownNow();

		
	}

}
