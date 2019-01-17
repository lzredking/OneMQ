/**
 * 
 */
package org.one.register;

import java.io.UnsupportedEncodingException;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.one.remote.cmd.Command;
import org.one.remote.common.OneBroker;
import org.one.remote.common.OneClient;
import org.one.remote.common.enums.RequestType;
import org.one.remote.server.RemotingServerHandler;
import org.tio.core.ChannelContext;
import org.tio.core.Tio;
import org.tio.core.intf.Packet;
import org.tio.utils.date.DateUtils;
import org.tio.utils.json.Json;

import com.one.store.RegisterStore;
import com.one.store.enums.RegisterType;

/**
 * @author yangkunguo
 *
 */
public class RegisterServerHandler extends RemotingServerHandler{

	@Override
	public void handler(Packet packet, ChannelContext channelContext) throws Exception {
		Command command = (Command) packet;
		
        byte[] body = command.getBody();
        byte reqtype = command.getReqType();
        try {
        	if (body != null) {
        		String str = new String(body, Command.CHARSET);
        		System.out.println("收到消息：" +str + DateUtils.formatDateTime(new Date()));
        		//消息Broker注册
        		if(reqtype==RequestType.BROKER) {
        			System.out.println("收到Broker注册消息。。。");
        			registBroker(channelContext,str);
        		}
        		//消息生产注册,
        		else if(reqtype==RequestType.PRODUCER) {
        			System.out.println("收到生产者注册消息。。。");
        			registProducer(channelContext, str);
        		}
        		//消息消费注册,
        		else if(reqtype == RequestType.CONSUMMER) {
        			System.out.println("收到消费者注册消息。。。");
        			registConsummer(channelContext,str);
        		}
        		//Broker心跳
        		else if(reqtype == RequestType.HEART_BEAT) {
//            	System.out.println(channelContext.getClientNode()+"---"+Json.toJson(command));
        			OneBroker broker=Json.toBean(str, OneBroker.class);
        			broker.setLastTime(System.currentTimeMillis());
        			ClientInfoList.addBroker(broker,channelContext);
        			if(StringUtils.isBlank(broker.getGroupName()) || StringUtils.isBlank(broker.getBrokerName())) {
        				RegisterStore.addRegisterInfo(RegisterType.broker.name(), broker);
        				//记录队列和Broker
//        		    	registTopic2Brokers(broker);
        			}
        		}
        		//返回消息
//        		Command resppacket = new Command();
//        		resppacket.setReqType(RequestType.REGISTER);
//        		resppacket.setBody(("注册成功，你的信息是:" + str).getBytes(Command.CHARSET));
//        		Tio.send(channelContext, resppacket);
        	}
        	
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
        
	
	/**注册Broker,同组一个Master,一个Slave。最多两个Slave.
	 * @param channelContext
	 * @param json
	 * @throws UnsupportedEncodingException
	 */
	private void registBroker(ChannelContext channelContext,String json) throws UnsupportedEncodingException {
		OneBroker broker=Json.toBean(json, OneBroker.class);
    	broker.setLastTime(System.currentTimeMillis());
    	//默认第一个为Master
//    	if(ClientInfoList.getGroupBrokers().isEmpty()) {
//    		broker.setRole(ServerRole.MASTER.getValue());
//    	}else {
//    		broker.setRole(ServerRole.SLAVE.getValue());
//    	}
    	//超过3个不再注册
    	if(!ClientInfoList.getGroupBrokers().isEmpty()
    			&& ClientInfoList.getGroupBrokers().get(broker.getGroupName())!=null
    			&& ClientInfoList.getGroupBrokers().get(broker.getGroupName()).size()>=3) {
    		List<OneBroker> brokers=ClientInfoList.getGroupBrokers().get(broker.getGroupName());
    		boolean same=false;
    		for(OneBroker bk:brokers) {
    			if(bk.getBrokerName().equals(broker.getBrokerName())) {
    				same=true;
    			}
    		}
    		if(!same) {
    			Command resppacket = new Command();
    			resppacket.setReqType(RequestType.MESSAGE);
    			resppacket.setBody(("注册失败，此组Broker已经有3个。你的信息是:" + json).getBytes(Command.CHARSET));
    			System.out.println(Tio.send(channelContext, resppacket));
    		}
    	}
    	ClientInfoList.addBroker(broker,channelContext);
    	//写入文件
    	RegisterStore.addRegisterInfo(RegisterType.broker.name(), broker);
    	
    	System.out.println(broker.toString()+"--[Brokers: "+ClientInfoList.getBrokers().size());
    	//记录队列和Broker
    	registTopic2Brokers(broker);
    	
	}
	
	/**返回所有Master Broker
	 * @param channelContext
	 * @param json
	 * @throws UnsupportedEncodingException
	 */
	private void registProducer(ChannelContext channelContext,String json) throws UnsupportedEncodingException{
		OneClient client=Json.toBean(json, OneClient.class);
    	ClientInfoList.addProducers(client);
    	RegisterStore.addRegisterInfo(RegisterType.producer.name(), client);
    	
    	System.out.println(client.toString()+"broker size: "+ClientInfoList.getBrokers().size());
    	//返回消息存放Broker的列表
    	Command resppacket = new Command();
    	resppacket.setReqType(RequestType.PRODUCER);
    	resppacket.setBody((Json.toJson(ClientInfoList.getMasterBrokers())).getBytes(Command.CHARSET));
    	Tio.send(channelContext, resppacket);
    	
	}
	
	/**返回存放指定队列的Broker
	 * @param channelContext
	 * @param json
	 * @throws UnsupportedEncodingException
	 */
	private void registConsummer(ChannelContext channelContext,String json) throws UnsupportedEncodingException{
		OneClient client=Json.toBean(json, OneClient.class);
    	ClientInfoList.addConsumers(client);
    	RegisterStore.addRegisterInfo(RegisterType.consumer.name(), client);
    	
    	//返回Topic存放Broker的列表
    	Command resppacket = new Command();
    	resppacket.setReqType(RequestType.CONSUMMER);
    	Collection<OneBroker> brokers=OneQueueManager.getCacheQueues(client.getQueueName());
    	System.out.println(client.getQueueName()+" -- brokers : "+brokers.size());
    	if(!brokers.isEmpty()) {
    		resppacket.setBody((Json.toJson(brokers)).getBytes(Command.CHARSET));
    		Tio.send(channelContext, resppacket);
    	}else {
    		System.out.println(Json.toJson(OneQueueManager.getCacheQueues()));
    		System.out.println("没有找到对应的服务："+client.getQueueName());
    	}
    	
	}
	
	/**记录队列和Broker关系
	 * @param broker
	 */
	private void registTopic2Brokers(OneBroker broker){
		if(StringUtils.isNotBlank(broker.getQueueName())) {
			String[] topics=broker.getQueueName().split(",");
			for(String name:topics) {
				OneQueueManager.addCacheQueues(name, broker);
			}
    		//写入文件
    		RegisterStore.addRegisterInfo(RegisterType.queue.name(), broker);
    	}
	}
}
