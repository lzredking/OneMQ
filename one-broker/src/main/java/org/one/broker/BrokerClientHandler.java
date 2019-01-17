/**
 * 
 */
package org.one.broker;

import org.one.broker.message.QueneInfo;
import org.one.remote.client.RemotingClientHandler;
import org.one.remote.cmd.Command;
import org.one.remote.common.OneBroker;
import org.one.remote.common.enums.RequestType;
import org.tio.core.ChannelContext;
import org.tio.core.Tio;
import org.tio.core.intf.Packet;
import org.tio.utils.json.Json;

/**
 * @author yangkunguo
 *
 */
public class BrokerClientHandler extends RemotingClientHandler{

	private OneBroker broker;
	
	/**创建心跳包
	 * @param heartbeatPacket
	 */
	public BrokerClientHandler(OneBroker broker) {
//		super.setHeartbeatPacket(heartbeatPacket);
		this.broker=broker;
	}
	@Override
	public void handler(Packet packet, ChannelContext channelContext) throws Exception {
		Command command = (Command) packet;
        byte[] body = command.getBody();
        if (body != null) {
//            String str = new String(body, Command.CHARSET);
//            System.out.println("收到消息：" + str);
            //检查是否存活
            if(command.getReqType() == RequestType.REGISTER) {
            	Command resppacket = new Command();
            	resppacket.setReqType(RequestType.HEART_BEAT);
            	StringBuilder sb=new StringBuilder();
            	for(String topic:QueneInfo.getTopicChannels().keySet()) {
            		sb.append(topic).append(",");
            	}
            	broker.setQueueName(sb.toString());
            	String json=Json.toJson(broker);
            	resppacket.setBody(json.getBytes(Command.CHARSET));
            	Tio.send(channelContext, resppacket);
            }
            //
        }
		
	}

	
}
