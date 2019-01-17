/**
 * 
 */
package org.one.client.producer;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.one.client.ClientInfo;
import org.one.client.ClientUtil;
import org.one.remote.cmd.Command;
import org.one.remote.cmd.OneMessage;
import org.one.remote.common.broker.MsgInfo;
import org.one.remote.common.enums.RequestType;
import org.one.remote.producer.SendCache;
import org.tio.client.intf.ClientAioListener;
import org.tio.core.ChannelContext;
import org.tio.core.intf.Packet;

import com.alibaba.fastjson.JSONArray;

/**事务注册监听
 * @author yangkunguo
 *
 */
public class TanscationClientAioListener implements ClientAioListener{

	
	private Producer producer;
	
	private MsgConfirmListener msgConfirmListener;
	
	private MsgTanscationListener msgTanscationListener;
	
	private AtomicLong retryNum= new AtomicLong();
	
	public TanscationClientAioListener(Producer producer) {
//		this.msgConfirmListener=msgConfirmListener;
		this.producer=producer;
	}
	@Override
	public void onAfterConnected(ChannelContext channelContext, boolean isConnected, boolean isReconnect)
			throws Exception {
		System.out.println(channelContext.toString()+" 服务连接成功状态 :"+ isConnected);
		for(ChannelContext cc:ClientInfo.getRegChannels()) {
			if(!cc.getServerNode().toString().equals(channelContext.getServerNode().toString())) {
				if(isConnected && producer!=null) {
					System.out.println(producer+" 服务连接成功状态 :"+ producer.getTopic());
//					ClientUtil.send(channelContext,new OneConsumer(producer.getTopic()));
					//处理积压消息
					producer.handlerFailedMsg(channelContext);
					//Tascation
					reqTanscationMsg( channelContext) ;
					
				}else {
					retryNum.incrementAndGet();
					//10次连接失败，则重新注册
					if(retryNum.get()>10) {
						producer.registerProducer();
						retryNum.getAndSet(0);
						Thread.sleep(1000*20);
					}
				}
			}
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
		Command cmd=(Command) packet;
//		System.out.println(cmd.getReqType()+"收到返回消息---onAfterHandled :"+ channelContext.toString());
		if(cmd.getReqType()==RequestType.MSG_CONFIRM) {
			//返回消息给消费者
			msgConfirmListener=producer.getMsgConfirmListener();
			if(msgConfirmListener!=null) {
				if(cmd.getBody()!=null) {
					String id = new String(cmd.getBody(), Command.CHARSET);
					msgConfirmListener.confirm(id);
					SendCache.getSendMsgs().remove(id);
//					SendCache.removeRertyMsgs(id);
				}
			}
		}
		if(cmd.getReqType()==RequestType.MSG_TANSCATION) {
			//返回消息给消费者
			msgTanscationListener=producer.getMsgTanscationListener();
			if(msgTanscationListener!=null) {
				if(cmd.getBody()!=null) {
					String json = new String(cmd.getBody(), Command.CHARSET);
//					System.out.println(json);
					List<MsgInfo> ids=JSONArray.parseArray(json, MsgInfo.class);
					if(ids.size()>0)
					msgTanscationListener.tanscation(ids);
				}
			}
		}
		//继续请求
		reqTanscationMsg( channelContext) ;
		producer.handlerFailedMsg(channelContext);
//		System.out.println("本次处理消息耗时，单位：毫秒   : "+cost);
	}

	/* 连接关闭前触发本方法
	 */
	@Override
	public void onBeforeClose(ChannelContext channelContext, Throwable throwable, String remark, boolean isRemove)
			throws Exception {
		
		System.out.println("此连接正在关闭 :"+ channelContext.toString());
		Thread.sleep(1000*10);
		producer.registerProducer();
	}
	
	private void reqTanscationMsg(ChannelContext channelContext) {
		if(producer.getMsgTanscationListener()!=null) {
			OneMessage msg=new OneMessage(producer.getTopic(), null);
			ClientUtil.sendTansctionMessage(channelContext, msg);
		}
	}
}
