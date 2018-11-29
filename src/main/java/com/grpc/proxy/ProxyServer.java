package com.grpc.proxy;

import java.io.IOException;

import org.apache.log4j.Logger;

import io.grpc.Server;
import io.grpc.ServerBuilder;

public class ProxyServer {
	
	final static Logger logger = Logger.getLogger(ProxyServer.class);
	
	 public static void main( String[] args )
	    {
		   logger.info( "Hello Proxy!" );
	        Server server = ServerBuilder.forPort(3000)
	          .addService(new ProxyDataTransferServiceImpl())
	          .build();


	        try {
				server.start();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

	        System.out.println("Proxy Server started");
	        
	        try {
				server.awaitTermination();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    }

}
