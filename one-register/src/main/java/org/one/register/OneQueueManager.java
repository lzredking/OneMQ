/**
 * 
 */
package org.one.register;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.one.remote.common.OneBroker;

/**队列存放记录
 * @author yangkunguo
 *
 */
public class OneQueueManager {


	/**
	 * 队列名，《brokerName,Broker》
	 */
	private static Map<String, Map<String,OneBroker>> cacheQueues = new HashMap<>(100);

	/**
	 * @param queueName
	 * @return
	 */
	public static Collection<OneBroker> getCacheQueues(String queueName) {
		return cacheQueues.get(queueName)==null?new ArrayList<>():cacheQueues.get(queueName).values();
	}
	
	public static Collection<Map<String, OneBroker>> getCacheQueues() {
		return cacheQueues.values();
	}
	
	

	/**
	 * @param queueName
	 * @param broker
	 */
	public static void addCacheQueues(String queueName, OneBroker broker) {
		Map<String,OneBroker> brokers=OneQueueManager.cacheQueues.get(queueName);
		if(brokers==null)
			brokers = new HashMap<>();
		
		if(broker!=null) {
			brokers.put(broker.getBrokerName(),broker);
		}
		OneQueueManager.cacheQueues.put(queueName, brokers);
	}
	


}
