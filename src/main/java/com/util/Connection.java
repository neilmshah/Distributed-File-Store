package com.util;

public class Connection {
	private String IP;
	private int port;
	
	Connection(String IP, int port){
		this.IP = IP;
		this.port = port;
	}
	public String getIP() {
		return IP;
	}
	public void setIP(String iP) {
		IP = iP;
	}
	public int getPort() {
		return port;
	}
	public void setPort(int port) {
		this.port = port;
	}
}
