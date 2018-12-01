package com.grpc.db;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import com.util.ConfigUtil;

import io.grpc.Server;
import io.grpc.ServerBuilder;

/**
 * DB server 
 * @author Sricheta's computer
 *
 */
public class DBServer {
	
	final static Logger logger = Logger.getLogger(DBServer.class);
	
	 public static void main( String[] args )
	    {
		 
		   ConcurrentHashMap map= null;
		   logger.info( "Hello DB!" );
		   new ConfigUtil();
	        Server server = ServerBuilder.forPort(9000)
	          .addService(new DBTransferServiceImpl(map))
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
