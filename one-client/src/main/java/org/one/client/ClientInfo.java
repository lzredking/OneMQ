/**
 * 
 */
package org.one.client;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.one.remote.common.OneBroker;
import org.tio.client.ClientChannelContext;
import org.tio.client.intf.ClientAioListener;
import org.tio.core.ChannelContext;

/**
 * @author yangkunguo
 *
 */
public class ClientInfo {

	//注册---共用
	private static Set<ChannelContext> regChannels = new HashSet<>(); 
	
	//Broker通道,brokerName+Channel
	private static Map<String,ChannelContext> brokerChannels = new HashMap<>(10); 
	//Broker通道,brokerName+Channel
	private static Map<String,List<ChannelContext>> tanscationChannels = new HashMap<>(10); 
	//所有在线Broker,brokerName+OneBroker
	private static Map<String,OneBroker> brokers=new HashMap<>(10);
	//消费队列,队列名+Broker列表
	private static Map<String, Set<OneBroker>> consumerBrokers = new HashMap<>(100);
	//注册消息消费监听
	private static ClientAioListener listener = null;
		
	private ClientInfo() {}
	
	public static Map<String,OneBroker> getBrokers() {
		return brokers;
	}

	public static void setBrokers(List<OneBroker> brokers) {
		for(OneBroker broker:brokers) {
			ClientInfo.brokers.put(broker.getBrokerName(), broker);
		}
	}

	/**注册服务
	 * @return
	 */
	public static List<ChannelContext> getRegChannels() {
		List<ChannelContext> chans=new ArrayList<>();
		if(!regChannels.isEmpty()) {
			chans.addAll(regChannels);
		}
		return chans;
	}

	/**添加注册服务
	 * @param regChannel
	 */
	public static void addRegChannel(ChannelContext regChannel) {
		ClientInfo.regChannels.add(regChannel);
	}

	/**返回所有Broker信息
	 * @return
	 */
	public static List<ChannelContext> getBrokerChannels() {
		List<ChannelContext> cha=new ArrayList<>();
		cha.addAll(brokerChannels.values());
		return cha;
	}
	
	public static Map<String, ChannelContext> getBrokerChannelsMap() {
		return brokerChannels;
	}

	public static ChannelContext getBrokerChannels(String brokerName) {
		return  ClientInfo.brokerChannels.get(brokerName);
	}
	
	public static ChannelContext removeBrokerChannels(String brokerName) {
		return  ClientInfo.brokerChannels.get(brokerName);
	}
	
	/**添加BrokerChannel信息
	 * @param brokerName
	 * @param brokerChannel
	 */
	public static void addBrokerChannel(String brokerName,ClientChannelContext brokerChannel) {
		ClientInfo.brokerChannels.put(brokerName, brokerChannel);
	}

	public static Set<OneBroker> getConsumerBrokers(String topic) {
		Set<OneBroker> brokers=ClientInfo.consumerBrokers.get(topic);
		if(brokers==null) {
			brokers=new HashSet<>(10);
		}
		return brokers;
	}
	
	public static Map<String, Set<OneBroker>>  getConsumerBrokers() {
		return consumerBrokers;
	}

	/**消费Broker信息
	 * @param consumerBroker
	 */
	public static void addConsumerBroker(OneBroker consumerBroker) {
		Set<OneBroker> brokers=getConsumerBrokers(consumerBroker.getQueueName());
		
		brokers.add(consumerBroker);
		ClientInfo.consumerBrokers.put(consumerBroker.getQueueName(), brokers);
		ClientInfo.brokers.put(consumerBroker.getBrokerName(), consumerBroker);
	}
	
	/**删除消费都使用队列
	 * @param consumerBroker
	 */
	public static void removeConsumerBroker(OneBroker consumerBroker) {
		Set<OneBroker> brokers=getConsumerBrokers(consumerBroker.getQueueName());
		brokers.remove(consumerBroker);
		ClientInfo.consumerBrokers.put(consumerBroker.getQueueName(), brokers);
	}

	/**删除已经掉线的Broker和连接通道
	 * @param broker
	 */
	public static void removeBroker(OneBroker broker) {
		brokers.remove(broker.getBrokerName());
		brokerChannels.remove(broker.getBrokerName());
		tanscationChannels.remove(broker.getBrokerName());
		removeConsumerBroker(broker);
	}

	public static ClientAioListener getListener() {
		return listener;
	}

	public static void setListener(ClientAioListener listener) {
		ClientInfo.listener = listener;
	}

	public static Map<String, List<ChannelContext>> getTanscationChannels() {
		return tanscationChannels;
	}

	
	/**
	 * @param brokerName
	 * @param brokerChannel
	 */
	public static void addTanscationChannels(String brokerName,ClientChannelContext brokerChannel) {
		List<ChannelContext> channes=ClientInfo.tanscationChannels.get(brokerName);
		if(channes==null) {
			channes=new ArrayList<>();
		}
		channes.add(brokerChannel);
		ClientInfo.tanscationChannels.put(brokerName, channes);
	}
	
	
}
