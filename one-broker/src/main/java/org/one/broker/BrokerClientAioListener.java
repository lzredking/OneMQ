/**
 * 
 */
package org.one.broker;

import org.one.remote.cmd.Command;
import org.tio.client.intf.ClientAioListener;
import org.tio.core.ChannelContext;
import org.tio.core.Tio;
import org.tio.core.intf.Packet;

/**
 * @author yangkunguo
 *
 */
public class BrokerClientAioListener implements ClientAioListener{

	private Command heartbeatPacket;
//	
	public BrokerClientAioListener(Command heartbeatPacket) {
		this.heartbeatPacket=heartbeatPacket;
	}
	@Override
	public void onAfterConnected(ChannelContext channelContext, boolean isConnected, boolean isReconnect)
			throws Exception {
		System.out.println("服务连接成功状态 :"+ isConnected);
		//多个Register时
		if(isConnected) {
			Tio.send(channelContext, heartbeatPacket);
		}
	}

	@Override
	public void onAfterDecoded(ChannelContext channelContext, Packet packet, int packetSize) throws Exception {
		// TODO Auto-generated method stub
//		System.out.println("onAfterDecoded :"+ channelContext.toString());
//		Command cmd=(Command) packet;
//		System.out.println(Json.toJson(cmd));
	}

	@Override
	public void onAfterReceivedBytes(ChannelContext channelContext, int receivedBytes) throws Exception {
		// TODO Auto-generated method stub
//		System.out.println("onAfterReceivedBytes :"+ channelContext.toString());
	}

	@Override
	public void onAfterSent(ChannelContext channelContext, Packet packet, boolean isSentSuccess) throws Exception {
		// TODO Auto-generated method stub
//		System.out.println("onAfterSent :"+ channelContext.toString());
//		Command cmd=(Command) packet;
//		System.out.println(Json.toJson(cmd));
	}

	@Override
	public void onAfterHandled(ChannelContext channelContext, Packet packet, long cost) throws Exception {
//		System.out.println("收到返回消息---onAfterHandled :"+ channelContext.toString());
//		Command cmd=(Command) packet;
//		if(cmd.getReqType()==RequestType.COMSUMER_MSG) {
//			//返回消息给消费者
//			if(consumerMsgListener!=null) {
//				if(cmd.getBody()!=null) {
//					String json = new String(cmd.getBody(), Command.CHARSET);
//					System.out.println(".............."+json);
//				}else {
//					
//				}
//			}else {
//				throw new OneMQException("consumerMsgListener do not null,please new ConsumerMsgListener()");
//			}
//		}
//		System.out.println("本次处理消息耗时，单位：毫秒   : "+cost);
	}

	/* 连接关闭前触发本方法
	 */
	@Override
	public void onBeforeClose(ChannelContext channelContext, Throwable throwable, String remark, boolean isRemove)
			throws Exception {
		// TODO Auto-generated method stub
		System.out.println("onAfterHandled :"+ channelContext.toString());
		Tio.send(channelContext, heartbeatPacket);
	}


	
	
	
}
