/**
 * 
 */
package org.one.remote.server;

import java.io.IOException;

import org.one.remote.common.Const;
import org.tio.core.Node;
import org.tio.server.ServerGroupContext;
import org.tio.server.TioServer;
import org.tio.server.intf.ServerAioHandler;
import org.tio.server.intf.ServerAioListener;

/**
 * @author yangkunguo
 *
 */
public class RemotingServerStarter {

	static ServerAioHandler aioHandler = new RemotingServerHandler();
	static ServerAioListener aioListener = new RemotingServerListener();
	static ServerGroupContext serverGroupContext = new ServerGroupContext(aioHandler, aioListener);
	static TioServer tioServer = new TioServer(serverGroupContext); //可以为空

	static String serverIp = null;
	static int serverPort = Const.PORT;

	public static void main(String[] args) throws IOException {
		tioServer.start(serverIp, serverPort);
	}
	
	/**服务端启动
	 * @param aioHandler
	 * @param aioListener
	 * @param node
	 * @throws IOException
	 */
	public static TioServer start(ServerAioHandler aioHandler,ServerAioListener aioListener,Node node) throws IOException {
		
		ServerGroupContext serverGroupContext = new ServerGroupContext(aioHandler, aioListener);
		serverGroupContext.setHeartbeatTimeout(1000*30);//30s一次心跳
		TioServer tioServer = new TioServer(serverGroupContext); //可以为空
		tioServer.start(node.getIp(), node.getPort());
		
		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
				@Override
				public void run() {
					stop(tioServer);
					
				}
	        }));
		
		return tioServer;
	}
	
	
	public static void stop(TioServer tioServer) {
		if(tioServer!=null) {
			tioServer.stop();
		}
	}
}
