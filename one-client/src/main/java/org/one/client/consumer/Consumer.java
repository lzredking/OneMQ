/**
 * 
 */
package org.one.client.consumer;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.StringUtils;
import org.one.client.CheckBroker;
import org.one.client.ClientInfo;
import org.one.client.ClientUtil;
import org.one.remote.client.RemotingClientStarter;
import org.one.remote.cmd.OneConsumer;
import org.one.remote.cmd.OneMessage;
import org.one.remote.common.OneBroker;
import org.one.remote.common.OneClient;
import org.one.remote.common.OneMQException;
import org.one.remote.common.enums.RequestType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tio.client.ClientChannelContext;
import org.tio.client.intf.ClientAioListener;
import org.tio.core.ChannelContext;
import org.tio.core.Node;

/**
 * @author yangkunguo
 *
 */
public class Consumer {

	private static Logger log = LoggerFactory.getLogger(Consumer.class);
	
//	private String ip;
//	private int port;
	
	private String topic;
	
	private String clientName;
	
	private Integer readSize;//一次消费数量，默认为1
	
	private ConsumerMsgListener messageListener;
	
	private OneClient client;
	
	private String registerServer;//onemq.regster.server=127.0.0.1:6666,127.0.0.1:6667
	
	public Consumer(String clientName) {
		this.clientName=clientName;
	}
	
	public Consumer(String clientName,String topic) {
		this.clientName=clientName;
		this.topic=topic;
	}

	/**添加消息监听
	 * @param messageListener
	 * @return
	 */
	public Consumer messageListener(ConsumerMsgListener messageListener) {
		this.messageListener=messageListener;
		return this;
	}
	/**
	 * 启动消费端
	 */
	public void start() {
		try {
			//从配置文件读 onemq.regster.server=127.0.0.1:6666,127.0.0.1:6667
			String ip="127.0.0.1";
			int port=6666;
			if(StringUtils.isNotBlank(registerServer)) {
				String[] servers=registerServer.split(",");
				for(String server :servers) {
					if(StringUtils.isNotBlank(server)) {
						ip=server.split(":")[0];
						port=Integer.valueOf(server.split(":")[1]);
						Node registNode=new Node(ip,port);
						
						ClientChannelContext regChannel = RemotingClientStarter.start(new ConsumerClientHandler(),
								registNode);
						ClientInfo.addRegChannel(regChannel);
					}
				}
			}else {
				throw new OneMQException(-1, "请配置 registerServer,如[onemq.regster.server=127.0.0.1:6666,127.0.0.1:6667]");
			}
			
			ClientAioListener listener=new ConsumerClientAioListener(messageListener,this);
			ClientInfo.setListener(listener);
			
			//
			registerConsumer();
			
			new CheckBroker(this).start(RequestType.CONSUMMER);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	/**重连服务
	 * @throws Exception
	 */
	public void registerConsumer() throws Exception {
		client=new OneClient();
		client.setClientName(clientName);
		client.setClientUrl("127.0.0.1:6634");//本机配置
		if(StringUtils.isBlank(topic)) {
			throw new OneMQException(-1, "topic do not null");
		}
		client.setQueueName(topic);//有了队列名才能找到Broker
		client.setTags(null);
		//register
		for(ChannelContext cc:ClientInfo.getRegChannels()) {
			boolean ok=RemotingClientStarter.send(cc, client, RequestType.CONSUMMER);
			System.out.println("registering...."+ok);
			if(!ok) {
				throw new OneMQException("注册中心访问失败......");
			}
		}
		//connct Broker
		consumerMessage(ClientInfo.getListener(),topic,client);
	}
	private int sleepTime=1000;
	/**消息消费
	 * @param msg
	 * @return true 成功 false 失败
	 * @throws Exception
	 */
	private void consumerMessage(ClientAioListener listener,String topic,OneClient client) throws Exception {
		List<ChannelContext> brokerChannels=getConsumerBrokers(listener);
		int i=0;
		while(brokerChannels.isEmpty()) {
			//等待连接成功后返回Broker列表
			try {
				Thread.sleep(sleepTime);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			brokerChannels.clear();
			brokerChannels=getConsumerBrokers(listener);
			i++;
			
			if(i==6) {
				//如果得不到列表，再请求一次
				for(ChannelContext cc:ClientInfo.getRegChannels()) {
					boolean ok=RemotingClientStarter.send(cc, client, RequestType.CONSUMMER);
					System.out.println("retry registering...."+ok);
				}
				sleepTime=(int) (sleepTime*1.5);
				if(sleepTime>(1000*60)) {
					sleepTime=1000*30;
				}
				i=0;
			}
			
		}
		System.out.println(i);
		

	}
	
	
	
	
	/**创建Consumer与Broker的连接
	 * @param listener
	 * @return
	 */
	private synchronized List<ChannelContext> getConsumerBrokers(ClientAioListener listener) {
		List<ChannelContext> channels=ClientInfo.getBrokerChannels();
		Set<OneBroker> brokers=ClientInfo.getConsumerBrokers(topic);
		if(brokers==null) {
			return channels;
		}
		log.warn("getConsumerBrokers----"+brokers);
		System.out.println("getConsumerBrokers----"+brokers);
		
		if(!ClientInfo.getBrokerChannels().isEmpty()) {
			//重用连接
			System.out.println("重用连接。。。");
			for(ChannelContext channel:ClientInfo.getBrokerChannels()) {
				try {
					OneConsumer oneConsumer=new OneConsumer(topic);
					oneConsumer.setReadSize(getReadSize());
					boolean ok=ClientUtil.send(channel,oneConsumer);
					System.out.println(ok);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			return channels;
		}
		//创建新连接
		for(OneBroker broker:brokers) {
			String ip=broker.getBrokerUrl().split(":")[0];
			int port=Integer.parseInt(broker.getBrokerUrl().split(":")[1]);
			
			Node brokerNode=new Node(ip,port);
    		ClientChannelContext brokerChannel;
			try {
				System.out.println("创建连接。。。");
				brokerChannel = RemotingClientStarter.start(new ConsumerClientHandler(), 
						listener, brokerNode);
				ClientInfo.addBrokerChannel(broker.getBrokerName(),brokerChannel);
			} catch (Exception e) {
				e.printStackTrace();
			}
//    		log.info(broker.getBrokerName()+" = "+brokerChannel.toString());
			channels.add((ClientChannelContext) ClientInfo.getBrokerChannels(broker.getBrokerName()));
		}
		return channels;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public static void main(String []s) {
		AtomicLong num=new AtomicLong();
		Consumer producer=new Consumer("Consumer-1","topic-1");
		producer.setReadSize(50);
		producer.setRegisterServer("127.0.0.1:6666");
		producer.messageListener(new ConsumerMsgListener() {
			//
			@Override
			public Boolean onMessage(ChannelContext channelContext,List<OneMessage> msgs) {
				if(msgs!=null) {
//					System.out.println(channelContext.toString());
					num.addAndGet(msgs.size());
					System.out.println(msgs.size()+".........消息总数....."+num.get());
					return true;
				}
				return false;
			}
		}).start();
		
	}

	public OneClient getClient() {
		return client;
	}

	public Integer getReadSize() {
		return readSize;
	}

	public void setReadSize(Integer readSize) {
		this.readSize = readSize;
	}

	@Override
	public String toString() {
		return "Consumer [topic=" + topic + ", clientName=" + clientName
				+ ", readSize=" + readSize + "]";
	}

	public String getRegisterServer() {
		return registerServer;
	}

	public void setRegisterServer(String registerServer) {
		this.registerServer = registerServer;
	}

	
}
