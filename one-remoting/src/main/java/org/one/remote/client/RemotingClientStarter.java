/**
 * 
 */
package org.one.remote.client;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.one.remote.cmd.Command;
import org.one.remote.cmd.OneMessage;
import org.one.remote.common.Const;
import org.one.remote.common.OneClient;
import org.one.remote.common.OneMQException;
import org.one.remote.common.enums.RequestType;
import org.one.remote.producer.SendCache;
import org.tio.client.ClientChannelContext;
import org.tio.client.ClientGroupContext;
import org.tio.client.ReconnConf;
import org.tio.client.TioClient;
import org.tio.client.intf.ClientAioHandler;
import org.tio.client.intf.ClientAioListener;
import org.tio.core.ChannelContext;
import org.tio.core.Node;
import org.tio.core.Tio;
import org.tio.utils.json.Json;

/**
 * @author yangkunguo
 *
 */
public class RemotingClientStarter {

	// 服务器节点
	public static Node serverNode = new Node(Const.SERVER, Const.PORT);

	// handler, 包括编码、解码、消息处理
	public static ClientAioHandler tioClientHandler = new RemotingClientHandler();

	// 事件监听器，可以为null，但建议自己实现该接口，可以参考showcase了解些接口
	public static ClientAioListener aioListener = null;

	// 断链后自动连接的，不想自动连接请设为null
	private static ReconnConf reconnConf = new ReconnConf(5000L);

	// 一组连接共用的上下文对象
	public static ClientGroupContext clientGroupContext = new ClientGroupContext(tioClientHandler, aioListener,
			reconnConf);

	public static TioClient tioClient = null;
	public static ClientChannelContext clientChannelContext = null;

	/**
	 * 启动程序入口
	 */
//	public static void main(String[] args) throws Exception {
//		clientGroupContext.setHeartbeatTimeout(Const.TIMEOUT);
//		tioClient = new TioClient(clientGroupContext);
//		clientChannelContext = tioClient.connect(serverNode);
//		// 连上后，发条消息玩玩
//		send();
//	}
//
//	private static void send() throws Exception {
//		Command packet = new Command();
//		packet.setBody("hello world".getBytes(Command.CHARSET));
//		Tio.send(clientChannelContext, packet);
//	}
	
	public static ClientChannelContext start(ClientAioHandler handler, Node serverNode) throws Exception {
		return start(handler, null, serverNode);
	}

	/**客户端启动
	 * @param handler
	 * @param aioListener
	 * @param serverNode 本机IP，prot
	 * @return
	 * @throws Exception
	 */
	public static ClientChannelContext start(ClientAioHandler handler, ClientAioListener aioListener,Node serverNode) throws Exception {
		
		return clientChannelContext = start(handler,aioListener).connect(serverNode);
	}
	
	/**启动连接
	 * @param handler
	 * @param aioListener
	 * @return
	 * @throws Exception
	 */
	public static TioClient start(ClientAioHandler handler, ClientAioListener aioListener) throws Exception {
		if(StringUtils.isBlank(serverNode.getIp())) {
			throw new OneMQException(-1,"请配置服务IP");
		}
		clientGroupContext=new ClientGroupContext(handler, aioListener,
				reconnConf);
		clientGroupContext.setHeartbeatTimeout(Const.TIMEOUT);
		tioClient = new TioClient(clientGroupContext);
		
		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
			@Override
			public void run() {
				tioClient.stop();
			}
        }));
		return tioClient;
	}
	
	/**注册
	 * @param clientChannelContext
	 * @param message
	 * @param reqType
	 * @throws Exception
	 */
	public static boolean send(ChannelContext clientChannelContext,OneClient client,byte reqType) throws Exception {
		Command packet = new Command();
		packet.setReqType(reqType);
		
		if(client!=null) {
			packet.setBody(Json.toJson(client).getBytes(Command.CHARSET));
		}
		return Tio.send(clientChannelContext, packet);
	}
	
	/**发送消息,如果发送失败放在本地缓存，等待连接成功后发送
	 * @param clientChannelContext
	 * @param client
	 * @throws Exception
	 */
	public static boolean sendMessage(ClientChannelContext clientChannelContext,OneMessage message) throws Exception {
		Command packet = new Command();
		packet.setReqType(RequestType.MESSAGE);
		//连接已经关闭
		if(clientChannelContext==null || clientChannelContext.isClosed) {
			System.out.println("连接已经关闭--"+clientChannelContext);
			tioClient.reconnect(clientChannelContext, 5000);
			if(clientChannelContext.isClosed) {
				SendCache.addRertyMsgs(message.get_id(), message);
				return false;
			}
		}
		if(message!=null) {
			packet.setBody(Json.toJson(message).getBytes(Command.CHARSET));
		}
		boolean ok=Tio.send(clientChannelContext, packet);
		if(!ok) {
			SendCache.addRertyMsgs(message.get_id(), message);
		}
		return ok;
	}
}
