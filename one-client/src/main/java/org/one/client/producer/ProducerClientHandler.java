/**
 * 
 */
package org.one.client.producer;

import java.util.List;

import org.one.client.ClientInfo;
import org.one.client.ClientUtil;
import org.one.remote.client.RemotingClientHandler;
import org.one.remote.client.RemotingClientStarter;
import org.one.remote.cmd.Command;
import org.one.remote.cmd.OneConsumer;
import org.one.remote.cmd.OneMessage;
import org.one.remote.common.OneBroker;
import org.one.remote.common.enums.RequestType;
import org.tio.client.ClientChannelContext;
import org.tio.client.intf.ClientAioListener;
import org.tio.core.ChannelContext;
import org.tio.core.Node;
import org.tio.core.intf.Packet;
import org.tio.utils.json.Json;

import com.alibaba.fastjson.JSON;

/**
 * @author yangkunguo
 *
 */
public class ProducerClientHandler extends RemotingClientHandler{

	private Producer producer;
	
	public ProducerClientHandler(Producer producer) {
		this.producer=producer;
	}
	@Override
	public void handler(Packet packet, ChannelContext channelContext) throws Exception {
		Command cmd=(Command) packet;
		byte[] body = cmd.getBody();
		if (body != null) {
            String json = new String(body, Command.CHARSET);
            System.out.println(json);
            //注册服务返回Broker列表
            if(cmd.getReqType().equals(RequestType.PRODUCER)) {
//            	List<OneBroker> brokers=Json.toBean(json, tt)
            	List<OneBroker> brokers=JSON.parseArray(json, OneBroker.class);
            	ClientInfo.setBrokers(brokers);
            	
            	if(!ClientInfo.getBrokerChannels().isEmpty()) {
        			//重用连接
        			System.out.println("重用连接。。。");
        			for(ChannelContext channel:ClientInfo.getBrokerChannels()) {
        				try {
        					OneMessage msg=new OneMessage();
        					boolean ok=ClientUtil.sendNullMessage(channel,msg);
        					System.out.println(ok);
        				} catch (Exception e) {
        					e.printStackTrace();
        				}
        			}
        			return ;
        		}
            	//连接Broker
            	for(OneBroker broker:brokers) {
            		ChannelContext oldbrokerChannel=ClientInfo.getBrokerChannels(broker.getBrokerName());
            		if(oldbrokerChannel==null || oldbrokerChannel.getClientNode().getIp().equals("$UNKNOWN")) {
            			
            			String ip=broker.getBrokerUrl().split(":")[0];
            			int port=Integer.parseInt(broker.getBrokerUrl().split(":")[1]);
            			Node brokerNode=new Node(ip,port);
            			
            			ClientAioListener listener=new TanscationClientAioListener(producer);
            			//发送
            			ClientChannelContext brokerChannel = RemotingClientStarter.start(new ProducerClientHandler(producer),
            					new ProducerClientAioListener(producer)
//            					listener
            					,brokerNode);
            			ClientInfo.addBrokerChannel(broker.getBrokerName(),brokerChannel);
            			System.out.println(brokerChannel);
            			
            			//接收
            			System.out.println(ClientInfo.getTanscationChannels().get(broker.getBrokerName()));
            			if(ClientInfo.getTanscationChannels().get(broker.getBrokerName())==null 
            					|| ClientInfo.getTanscationChannels().get(broker.getBrokerName()).size()<=3) {
            				
            				ClientChannelContext brokerChannel2=RemotingClientStarter.start(new ProducerClientHandler(producer),
            						listener
            						,brokerNode);
            				ClientInfo.addTanscationChannels(broker.getBrokerName(), brokerChannel2);
          			
            				ClientChannelContext brokerChannel3=RemotingClientStarter.start(new ProducerClientHandler(producer),
            						listener
            						,brokerNode);
            				ClientInfo.addTanscationChannels(broker.getBrokerName(), brokerChannel3);
            				System.out.println(brokerChannel2);
            			}
            		}else if(oldbrokerChannel.isClosed){
//            			
            		}
            	}
            }
		}
	}

}
