/**
 * 
 */
package org.one.broker.message.tanscation;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

import org.one.broker.BrokerServerHandler;
import org.one.broker.BrokerStart;
import org.one.broker.message.MsgTanscationInfo;
import org.one.remote.cmd.Command;
import org.one.remote.common.broker.MsgInfo;
import org.one.remote.common.enums.RequestType;
import org.tio.client.TioClient;
import org.tio.core.ChannelContext;
import org.tio.core.Tio;
import org.tio.utils.json.Json;

/**
 * @author yangkunguo
 *
 */
public class TanscationMessage {

	private static ReentrantLock lock=new ReentrantLock();
	
	/**
	 * @param channelContext
	 * @param id
	 * @throws UnsupportedEncodingException
	 */
	private static void tanscationMsg(ChannelContext channelContext,final Set<MsgInfo> ids)  {
//		new Thread(new Runnable() {
//			
//			@Override
//			public void run() {
//				
//				
//			}
//		}).start();
		//异常消息临时存放
		if(channelContext==null || 
				channelContext.getClientNode().getIp().equals("$UNKNOWN")
				|| channelContext.isClosed) {
			
				MsgTanscationInfo.addTansMsgIds(ids);
			
//			System.out.println(id);
			return;
		}
		
		Command resppacket = new Command();
		resppacket.setReqType(RequestType.MSG_TANSCATION);
		try {
			String json=Json.toJson(ids);
			resppacket.setBody(json.getBytes(Command.CHARSET));
		} catch (UnsupportedEncodingException e1) {
			e1.printStackTrace();
		}
		boolean ok=Tio.send(channelContext, resppacket);
		if(!ok) {
			TioClient client=BrokerStart.getClient();
			ChannelContext channel=null;
			try {//2次发送
				channel=client.connect(channelContext.getClientNode());
				
				boolean ok2=Tio.send(channel, resppacket);
				if(!ok2) {
					MsgTanscationInfo.addTansMsgIds(ids);
				}else {
					MsgTanscationInfo.removeTansMsgIds(ids);
				}
				System.out.println("channel close reconnect....."+ok2);
			} catch (Exception e) {
				e.printStackTrace();
			}finally {
				if(channel!=null) {
					channel.setClosed(true);
				}
			}
		}else {
			MsgTanscationInfo.removeTansMsgIds(ids);
//			System.out.println(ok+"--remove id: "+id);
		}
//		System.out.println(ok+"--tanscationMsg  MsgId size: "+MsgTanscationInfo.getMsgSize());
	}
	
	/**
	 * @param channelContext
	 * @param topic
	 */
	public void confirmTanscation(ChannelContext channelContext,String topic) {
		
		lock.lock();
		try {
			Set<MsgInfo> ids=MsgTanscationInfo.getTansMsgIds(topic, 50);
//			System.out.println(MsgTanscationInfo.getTansMsgSize(topic)+"  msgs.size()="+ids.size());
//			if(ids.size()<10) {
//				Thread.sleep(500);
//				ids=MsgTanscationInfo.getTansMsgIds(topic, 50);
//			}
			
//			List<MsgInfo> ids=new ArrayList<>(50);
//			for(MsgInfo id:msgs) {
//				ids.add(id);
////			System.out.println(id);
////			new TanscationMessage().tanscationMsg(channelContext , id);
//			}
			if(ids.size()>0) {
				tanscationMsg(channelContext , ids);
			}
		}catch (Exception e) {
			e.printStackTrace();
		} finally {
			lock.unlock();
		}
	}
}
