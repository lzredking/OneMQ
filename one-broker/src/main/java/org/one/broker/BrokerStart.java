/**
 * 
 */
package org.one.broker;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.util.HashSet;
import java.util.Set;

import org.one.remote.client.RemotingClientStarter;
import org.one.remote.cmd.Command;
import org.one.remote.common.OneBroker;
import org.one.remote.common.enums.RequestType;
import org.one.remote.common.enums.ServerRole;
import org.one.remote.server.RemotingServerListener;
import org.one.remote.server.RemotingServerStarter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tio.client.ClientChannelContext;
import org.tio.client.TioClient;
import org.tio.core.Node;
import org.tio.core.Tio;
import org.tio.server.ServerGroupStat;
import org.tio.server.TioServer;
import org.tio.server.intf.ServerAioHandler;
import org.tio.server.intf.ServerAioListener;
import org.tio.utils.json.Json;

import com.one.store.BrokerStore;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;

/**
 * @author yangkunguo
 *
 */
public class BrokerStart {

	private static TioClient client;
	private static OneBroker broker;
	private static Set<ClientChannelContext> registChannels=new HashSet<>();
	
	private static BrokerStore brokerStore;
	
	private static Logger log= LoggerFactory.getLogger(BrokerStart.class);
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
//			brokerStore = new BrokerStore();
//			brokerStore.start();
			
			ServerAioHandler aioHandler = new BrokerServerHandler(brokerStore);
			ServerAioListener aioListener = new RemotingServerListener();
			//Broker Server
			Node node=new Node("127.0.0.1", 6656);
			TioServer server=RemotingServerStarter.start(aioHandler,aioListener,node);
			//连接Register
			broker=new OneBroker("group1","def-master", ServerRole.MASTER.getValue(), "127.0.0.1:6656",1);
			Node serverNode=new Node("127.0.0.1", 6666);
			
			TioClient client=RemotingClientStarter.start(new BrokerClientHandler(broker),
					new BrokerClientAioListener(heartbeat()));

			ClientChannelContext registChannel=client.connect(serverNode);
			registChannels.add(registChannel);
			registBroker(registChannels);
//			ServerGroupStat stat=new ServerGroupStat();
			//日志配置
			LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
	        JoranConfigurator configurator = new JoranConfigurator();
	        configurator.setContext(lc);
	        lc.reset();
//	        configurator.doConfigure(namesrvConfig.getRocketmqHome() + "/conf/logback_namesrv.xml");
	        configurator.doConfigure(BrokerStart.class.getResource("/").getPath()+"logback_broker.xml");
	        
//			stat=server.getServerGroupContext().getTioClusterConfig();
	        
	        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
				@Override
				public void run() {
					if(brokerStore!=null)
						brokerStore.stop();
				}
	        }));
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("注册服务启动失败。。。");
			System.exit(1);
			if(brokerStore!=null)
				brokerStore.stop();
		} 
	}

	private static void registBroker(Set<ClientChannelContext> registChannels) throws UnsupportedEncodingException {
		Command packet = new Command();
		packet.setReqType(RequestType.BROKER);
		//System.out.println(ServerRole.MASTER);
		
		String json=Json.toJson(broker);
		log.info(json);
		packet.setBody(json.getBytes(Command.CHARSET));
		for(ClientChannelContext channel: registChannels)
			Tio.send(channel, packet);
	}

	/**向注册服务器汇报
	 * @return
	 * @throws UnsupportedEncodingException
	 */
	private static Command heartbeat() throws UnsupportedEncodingException {
		Command packet = new Command();
		packet.setReqType(RequestType.HEART_BEAT);
		String json=Json.toJson(broker);
		packet.setBody(json.getBytes(Command.CHARSET));
		return packet;
	}
	
	public static OneBroker getBroker() {
		return broker;
	}

	public static Set<ClientChannelContext> getRegistChannels() {
		return registChannels;
	}

	public static TioClient getClient() {
		return client;
	}

	public static BrokerStore getBrokerStore() {
		return brokerStore;
	}
}
