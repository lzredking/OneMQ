/**
 * 
 */
package org.one.register;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.one.remote.cmd.Command;
import org.one.remote.common.OneBroker;
import org.one.remote.common.enums.RequestType;
import org.one.remote.server.RemotingServerListener;
import org.one.remote.server.RemotingServerStarter;
import org.slf4j.LoggerFactory;
import org.tio.core.ChannelContext;
import org.tio.core.Node;
import org.tio.core.Tio;
import org.tio.server.TioServer;
import org.tio.server.intf.ServerAioHandler;
import org.tio.server.intf.ServerAioListener;

import com.alibaba.fastjson.JSONArray;
import com.one.store.RegisterStore;
import com.one.store.enums.RegisterType;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;

/**
 * @author yangkunguo
 *
 */
public class RegisterStart {

	private static RegisterConf conf;
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		//Load服务启动配置
		start(0);
//		start(7777);
	}

	private static void start(int port) {
		try {
			ServerAioHandler aioHandler = new RegisterServerHandler();
			ServerAioListener aioListener = new RemotingServerListener();
			if(port<=0)port=6666;
			Node node=new Node("127.0.0.1", port);
			TioServer server=RemotingServerStarter.start(aioHandler,aioListener,node);
			conf=new RegisterConf();
			conf.setLoad(true);
			//
			loadBroker();
			//检查Broker下线
			checkBroker();
			
			LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
			JoranConfigurator configurator = new JoranConfigurator();
			configurator.setContext(lc);
			lc.reset();
			try {
				configurator.doConfigure(RemotingServerStarter.class.getResource("/").getPath()+"logback_namesrv.xml");
			} catch (JoranException e) {
				e.printStackTrace();
			}
			
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("注册服务启动失败。。。");
			System.exit(2);
		}
		
	}
	private static void loadBroker() throws IOException {
		if(conf!=null && conf.isLoad()) {
			String json=RegisterStore.readStore(RegisterType.broker.name());
			if(StringUtils.isNotBlank(json)) {
				List<OneBroker> brokers=JSONArray.parseArray(json, OneBroker.class);
				for(OneBroker broker:brokers) {
					ClientInfoList.addBroker(broker,null);
					if(StringUtils.isNotBlank(broker.getQueueName())) {
			    		OneQueueManager.addCacheQueues(broker.getQueueName(), broker);
			    	}
				}
			}
		}
	}
	private static void checkBroker() {
		new Thread(new Runnable() {
			
			@Override
			public void run() {
				System.out.println(".....");
				while(true) {
					try {
						Thread.sleep(1000*10);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					
					Map<String, ChannelContext> chans=ClientInfoList.getBrokerChannels();
					Collection<OneBroker> brokers=ClientInfoList.getBrokers();
					System.out.println("check borker...size..."+brokers.size());
					long time=System.currentTimeMillis();
					for(OneBroker broker:brokers) {
						System.out.println("check borker..."+broker.toString());
						if(time>(broker.getLastTime()+1000*30)) {
							
							if(chans.get(broker.getBrokerName())!=null 
									&& chans.get(broker.getBrokerName()).isClosed) {
								System.out.println("删除下线Broker...");
								ClientInfoList.removeBroker(broker);
								continue;
							}
							Command command = new Command();
							command.setReqType(RequestType.REGISTER);
							try {
								command.setBody("are you ok?".getBytes(Command.CHARSET));
							} catch (UnsupportedEncodingException e) {
								e.printStackTrace();
							}
							Tio.send(chans.get(broker.getBrokerName()), command);
						}
						
//						if(time>(broker.getLastTime()+1000*90)) {
//							//删除下线Broker
//							if(chans.get(broker.getBrokerName())!=null 
//									&& chans.get(broker.getBrokerName()).isClosed) {
//								System.out.println("删除下线Broker...");
//								ClientInfoList.removeBroker(broker);
//							}
//							//转移Broker里的消息
//						}
					}
				}
				
				
			}
		}).start();
	}


	public RegisterConf getConf() {
		return conf;
	}

	public void setConf(RegisterConf conf) {
		this.conf = conf;
	}
}
