/**
 * 
 */
package org.one.register;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.one.remote.common.OneBroker;
import org.one.remote.common.OneClient;
import org.tio.core.ChannelContext;

import com.one.store.RegisterStore;
import com.one.store.enums.RegisterType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;

/**
 * @author yangkunguo
 *
 */
public class ClientInfoList {

	/**
	 * Broker列表 
	 */
	private static Map<String,OneBroker> brokers=new HashMap<>();
	
	/**
	 * Broker分组列表
	 */
	private static Map<String,List<OneBroker>> groupBrokers=new HashMap<>();
	
	/**
	 * Broker通道列表
	 */
	private static Map<String,ChannelContext> brokerChannels=new HashMap<>();
	
	/**
	 * 生产者列表
	 */
	private static Map<String,OneClient> producers=new HashMap<>();
	
	/**
	 * 消费者列表
	 */
	private static Map<String,OneClient> consumers=new HashMap<>();
	

	/**返回所有Broker
	 * @return
	 */
	public static Collection<OneBroker> getBrokers() {
		return brokers.values();
	}
	
	public static Collection<OneBroker> getMasterBrokers() {
		Collection<OneBroker> list=new ArrayList<>();
		for(OneBroker broker:brokers.values()) {
			if(broker.isMaster()) {
				list.add(broker);
			}
		}
		return list;
	}

	/**记录Broker
	 * @param broker
	 * @param channel
	 */
	public static void addBroker(OneBroker broker,ChannelContext channel) {
		if(StringUtils.isBlank(broker.getGroupName()) || StringUtils.isBlank(broker.getBrokerName())) {
			return;
		}
		ClientInfoList.brokers.put(broker.getBrokerName(), broker);
		brokerChannels.put(broker.getBrokerName(), channel);
		List<OneBroker> bros=groupBrokers.get(broker.getGroupName());
		if(bros==null) {
			bros=new ArrayList<>();
		}
		if(bros.size()<3) {
			bros.add(broker);
			
			ClientInfoList.groupBrokers.put(broker.getGroupName(), bros);
		}
		
	}
	/**删除掉线的Broker
	 * @param broker
	 */
	public static void removeBroker(OneBroker broker) {
		ClientInfoList.brokers.remove(broker.getBrokerName());
		brokerChannels.remove(broker.getBrokerName());
		List<OneBroker> bros=groupBrokers.get(broker.getGroupName());
		for(OneBroker br:bros) {
			
			if(br==null || br.getBrokerName().equals(broker.getBrokerName())) {
				bros.remove(br);
				break;
			}
		}
		//写入文件
		RegisterStore.removeRegisterInfo(RegisterType.broker.name(), broker);
	}

	public static List<OneClient> getProducers() {
		List<OneClient> oc=new ArrayList<>();
		oc.addAll(producers.values());
		return oc;
	}

	public static void addProducers(OneClient producer) {
		ClientInfoList.producers.put(producer.getClientName(),producer);
	}

	public static List<OneClient> getConsumers() {
		List<OneClient> oc=new ArrayList<>();
		oc.addAll(consumers.values());
		return oc;
	}

	public static void addConsumers(OneClient consumer) {
		ClientInfoList.consumers.put(consumer.getClientName(),consumer);
	}

	public static Map<String, ChannelContext> getBrokerChannels() {
		return brokerChannels;
	}

	public static Map<String, List<OneBroker>> getGroupBrokers() {
		return groupBrokers;
	}

}
