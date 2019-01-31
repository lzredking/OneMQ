/**
 * 
 */
package org.one.client.producer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.StringUtils;
import org.one.client.CheckBroker;
import org.one.client.ClientInfo;
import org.one.client.ClientUtil;
import org.one.remote.client.RemotingClientStarter;
import org.one.remote.cmd.OneMessage;
import org.one.remote.common.OneBroker;
import org.one.remote.common.OneClient;
import org.one.remote.common.OneMQException;
import org.one.remote.common.broker.MsgInfo;
import org.one.remote.common.enums.RequestType;
import org.one.remote.producer.SendCache;
import org.tio.client.ClientChannelContext;
import org.tio.core.ChannelContext;
import org.tio.core.Node;

/**
 * @author yangkunguo
 *
 */
public class Producer {

	private String ip;
	private int port;
	
	private String topic;
	
	private String clientName;//不可重复
	
	private String registerServer;
	
	private MsgConfirmListener msgConfirmListener;//消息发送确认
	
	private MsgTanscationListener msgTanscationListener;//消息消费确认
	
//	private boolean isTanscation;//是否需要事务消息
	
	public Producer(String clientName) {
		this.clientName=clientName;
	}
	
	public Producer(String clientName,String topic) {
		this.clientName=clientName;
		this.topic=topic;
	}

	public void start() {
		//从配置文件读 onemq.regster.server=127.0.0.1:6666,127.0.0.1:6667
		
		init();
		
		registerProducer();
		
		new CheckBroker(this).start(RequestType.PRODUCER);
	}
	void init() {
		String ip="127.0.0.1";
		int port=6666;
		if(StringUtils.isNotBlank(registerServer)) {
			String[] servers=registerServer.split(",");
			for(String server :servers) {
				if(StringUtils.isNotBlank(server)) {
					ip=server.split(":")[0];
					port=Integer.valueOf(server.split(":")[1]);
					Node registNode=new Node(ip,port);
					
					try {
						ClientChannelContext regChannel = RemotingClientStarter.start(new ProducerClientHandler(this),
								new ProducerClientAioListener(this),
								registNode);
						ClientInfo.addRegChannel(regChannel);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		}else {
			throw new OneMQException(-1, "请配置 registerServer,如[onemq.regster.server=127.0.0.1:6666,127.0.0.1:6667]");
		}
	}
	/**
	 * 断线后重试
	 */
	public void registerProducer() {
		try {
			
			OneClient client=new OneClient();
			client.setClientName(clientName);
			client.setClientUrl("127.0.0.1:6633");//本机配置
			client.setQueueName(this.topic);
			client.setTags(null);
			
			
			//register
			for(ChannelContext cc:ClientInfo.getRegChannels()) {
				boolean ok=RemotingClientStarter.send(cc, client, RequestType.PRODUCER);
				System.out.println("registering...."+ok);
				if(!ok) {
					throw new OneMQException("注册中心访问失败......");
				}
			}
			Thread.sleep(1000);
			
			getBrokerChannels();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public boolean sendMessage(Object data) throws Exception{
		return sendMessage( data,null);
	}
	/**消息发送
	 * @param msg
	 * @return true 成功 false 失败
	 * @throws Exception
	 */
	public boolean sendMessage(Object data,String id) throws Exception {
		List<ChannelContext> brokerChannels=new ArrayList<>();
		int i=0;
		brokerChannels=getBrokerChannels();
		while(brokerChannels.isEmpty()) {
			if(i==100) {
				registerProducer();
				return false;//无Broker列表
			}
			Thread.sleep(1000);
			brokerChannels=getBrokerChannels();
			i++;
			
		}
//		System.out.println(i);
		int next=0;
		if(brokerChannels.size()>1) {
			Random rd=new Random();
			next=rd.nextInt(brokerChannels.size()-1);
		}
		//选择发给谁
		ClientChannelContext channel=(ClientChannelContext) brokerChannels.get(next);
//		if(ClientInfo.getBrokers().isEmpty()) {
//			registerProkucer();
//		}
		OneMessage msg=null;
		if(id!=null) {
			msg=new OneMessage(id, topic, data);
		}else {
			msg=new OneMessage(this.topic, data);
		}
		if(this.msgTanscationListener!=null) {
			msg.setTranscation(1);
		}
		
		if(msgConfirmListener!=null) {
			msg.setConfirm(1);
//			SendCache.addRertyMsgs(msg.get_id(), msg);
////			handlerFailedMsg(channel);
//			return RemotingClientStarter.sendMessage(channel, msg);
		}else {
		}
		return RemotingClientStarter.sendMessage(channel, msg);
	}
	
	/**
	 * @return
	 */
	private List<ChannelContext> getBrokerChannels() {
		List<ChannelContext> brokerChannels=new ArrayList<>();
		Map<String, OneBroker> map=ClientInfo.getBrokers();
		
		for(OneBroker broker:map.values()) {
//			System.out.println(broker.toString());
			if(broker.isMaster()) {
				brokerChannels.add(ClientInfo.getBrokerChannels(broker.getBrokerName()));
			}
		}
		return brokerChannels;
	}
	
	/**处理发送失败消息
	 * @param channelContext
	 */
	public void handlerFailedMsg(ChannelContext channelContext) {
		List<String> olds=new ArrayList<>();
		Set<Entry<String, OneMessage>> ents=SendCache.getRertyMsgs().entrySet();
		System.out.println(SendCache.getSize()+" ==没发送消息== "+ents.size());
		
		for(Entry<String, OneMessage> ent:ents) {
//			System.out.println(ent.getValue());
			boolean ok;
			try {
				ok = RemotingClientStarter.sendMessage((ClientChannelContext) channelContext, ent.getValue());
				if(ok) {
					olds.add(ent.getKey());
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		System.out.println("send failed msg size: "+SendCache.getRertyMsgs().size());
		for(String id:olds) {
			SendCache.removeRertyMsgs(id);
		}
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

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}


	public String getRegisterServer() {
		return registerServer;
	}

	public void setRegisterServer(String registerServer) {
		this.registerServer = registerServer;
	}

	public MsgConfirmListener getMsgConfirmListener() {
		return msgConfirmListener;
	}

	/**如果实现接口，标志此消息需要接受确认
	 * @param msgConfirmListener
	 * @return
	 */
	public Producer setMsgConfirmListener(MsgConfirmListener msgConfirmListener) {
		this.msgConfirmListener = msgConfirmListener;
		return this;
	}

	public MsgTanscationListener getMsgTanscationListener() {
		return msgTanscationListener;
	}

	/**如果实现接口，标志此消息需要事务
	 * @param msgTanscationListener
	 * @return
	 */
	public Producer setMsgTanscationListener(MsgTanscationListener msgTanscationListener) {
		this.msgTanscationListener = msgTanscationListener;
		return this;
	}

	private static AtomicLong sendNum= new AtomicLong(0);
	private static AtomicLong sendOK= new AtomicLong(0);
	private static AtomicLong resNum= new AtomicLong(0);
	/**测试
	 * @param s
	 */
	public static void main(String []s) {
		Producer producer=new Producer("producer-1");
		producer.setTopic("topic-1");
		producer.setRegisterServer("127.0.0.1:6666");
//		producer.setTanscation(true);
		producer.setMsgConfirmListener(new MsgConfirmListener() {
			
			@Override
			public void confirm(String id) {
				// 发送成功的消息
				sendOK.incrementAndGet();
//				System.out.println(id);
				System.out.println("确认发送成功："+SendCache.getSendMsgs().size());
			}
		});
		producer.setMsgTanscationListener(new MsgTanscationListener() {
			
			@Override
			public void tanscation(List<MsgInfo> id) {
				// 消费成功
				resNum.addAndGet(id.size());
//				System.out.println(id);
//				System.out.println(resNum.get()+" 确认消费成功："+id.size());
				System.out.println("发送成功数："+sendOK.get()+"--"+sendNum.get()+" 没收到事务消息数："+(sendNum.get()-resNum.get()));
			}
		});
		producer.start();
		
		Thread th=new Thread(new Runnable() {
			
			@Override
			public void run() {
				int c=0;
				while(true) {
					long s=System.currentTimeMillis();
					try {
						for(int i=0;i<100;i++) {
							
							long num=sendNum.addAndGet(1);
							
							boolean ok=producer.sendMessage("test");
						}
						System.out.println(sendNum.get()+"--消息发送完毕：");
						System.out.println("没收到事务消息数--"+(sendNum.get()-resNum.get()));
						long e=System.currentTimeMillis();
						System.out.println("时间："+(e-s));
						if((sendNum.get())>10000000) {
//							Thread.sleep(1000*60);
//						}else
//						if(c==5)
							break;}
							Thread.sleep(1000*1);
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					c++;
				}
				
			}
		});
		th.start();
	}
}
