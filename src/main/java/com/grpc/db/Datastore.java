package com.grpc.db;

import com.util.ConfigUtil;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

public class Datastore {

    final static Logger logger = Logger.getLogger(Datastore.class);
    public static ConfigUtil config;
    private Server server;
    DatabaseDataTransferServiceImpl dataTrasferServiceImpl;

    Datastore(){
        config = new ConfigUtil();
        dataTrasferServiceImpl = new DatabaseDataTransferServiceImpl();
        server = ServerBuilder.forPort(config.databaseNodes.get(0).getPort()).addService(dataTrasferServiceImpl).build();
    }

    private void start(){
        try {
            server.start();
            System.out.println("Datastore grpc server started");
            server.awaitTermination();
        }catch (IOException e){
            System.err.println("IO excpetion with starting the grpc server!");
            e.printStackTrace();
            System.exit(1);
        }catch (InterruptedException e){
            System.err.println("Interrupt excpetion with awaiting termination of the grpc server!");
            e.printStackTrace();
            System.exit(1);
        }
    }
    protected void stop() {
        server.shutdown();
    }

    private void blockUntilShutdown() throws Exception {
        server.awaitTermination();
    }

    public static void main(String[] args){
        Datastore myDs = new Datastore();
        myDs.start();
        try {
            myDs.blockUntilShutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
