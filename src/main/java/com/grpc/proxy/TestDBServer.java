package com.grpc.proxy;

import java.io.IOException;

import org.apache.log4j.Logger;

import io.grpc.Server;
import io.grpc.ServerBuilder;

public class TestDBServer {


	final static Logger logger = Logger.getLogger(TestDBServer.class);

	public static void main( String[] args )
	{
		logger.info( "Hello DB!" );
		Server server = ServerBuilder.forPort(9000)
				.addService(new TestDBDataTransferServiceImpl())
				.build();


		try {
			server.start();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		System.out.println("DB Server started");

		try {
			server.awaitTermination();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}


}
