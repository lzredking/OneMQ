/**
 * 
 */
package org.one.register;

/**
 * @author yangkunguo
 *
 */
public class RegisterConf {

	private String ip;
	private int port;
	private boolean isLoad=true;

	public boolean isLoad() {
		return isLoad;
	}

	public void setLoad(boolean isLoad) {
		this.isLoad = isLoad;
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}
	
	
}
