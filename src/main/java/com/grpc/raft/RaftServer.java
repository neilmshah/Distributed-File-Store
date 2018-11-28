package com.grpc.raft;

import java.io.IOException;

import io.grpc.Server;
import io.grpc.ServerBuilder;

/**
 * Server participating in RAFT
 */
public class RaftServer 
{
    public static void main( String[] args )
    {
        System.out.println( "Hello Raft!" );
        Server server = ServerBuilder.forPort(8080)
          .addService(new DataTransferServiceImpl())
          .build();


        try {
			server.start();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

        System.out.println("Raft Server started");
        
        try {
			server.awaitTermination();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
}
