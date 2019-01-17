/**
 * 
 */
package org.one.client.consumer;

import java.util.List;

import org.one.client.ClientInfo;
import org.one.remote.client.RemotingClientHandler;
import org.one.remote.cmd.Command;
import org.one.remote.common.OneBroker;
import org.one.remote.common.enums.RequestType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tio.core.ChannelContext;
import org.tio.core.intf.Packet;

import com.alibaba.fastjson.JSON;

/**
 * @author yangkunguo
 *
 */
public class ConsumerClientHandler extends RemotingClientHandler{

	private static final Logger log = LoggerFactory.getLogger(ConsumerClientHandler.class);
	
	@Override
	public void handler(Packet packet, ChannelContext channelContext) throws Exception {
		Command cmd=(Command) packet;
		byte[] body = cmd.getBody();
		if (body != null) {
            String json = new String(body, Command.CHARSET);
            log.info(cmd.getReqType()+" 消息："+json);
            //注册中心返回队列所在的Broker
            if(cmd.getReqType() == RequestType.CONSUMMER) {
            	log.info(cmd.getReqType()+" 注册中心消息："+json);
            	List<OneBroker> brokers=JSON.parseArray(json, OneBroker.class);
            	//连接Broker
            	for(OneBroker broker:brokers) {
            		ClientInfo.addConsumerBroker(broker);
            		System.out.println(broker.getQueueName()+"---"+ClientInfo.getConsumerBrokers(broker.getQueueName()));
            		
//            		String ip=broker.getBrokerUrl().split(":")[0];
//        			int port=Integer.parseInt(broker.getBrokerUrl().split(":")[1]);
//        			Node brokerNode=new Node(ip,port);
//            		ClientChannelContext brokerChannel = RemotingClientStarter.start(new ConsumerClientHandler(), 
//            				new ConsumerClientAioListener(null), brokerNode);
//            		log.info(broker.getBrokerName()+" = "+brokerChannel.toString());
//            		ClientInfo.addBrokerChannel(broker.getBrokerName(),brokerChannel);
            	}
            }
            
            
		}
	}

}
