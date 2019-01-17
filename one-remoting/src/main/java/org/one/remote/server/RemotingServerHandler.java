/**
 * 
 */
package org.one.remote.server;

import java.nio.ByteBuffer;

import org.one.remote.cmd.Command;
import org.tio.core.ChannelContext;
import org.tio.core.GroupContext;
import org.tio.core.Tio;
import org.tio.core.exception.AioDecodeException;
import org.tio.core.intf.Packet;
import org.tio.server.intf.ServerAioHandler;

/**
 * @author yangkunguo
 *
 */
public class RemotingServerHandler implements ServerAioHandler{

	@Override
	public Packet decode(ByteBuffer buffer, int limit, int position, int readableLength, ChannelContext channelContext) throws AioDecodeException {
		//提醒：buffer的开始位置并不一定是0，应用需要从buffer.position()开始读取数据
        //收到的数据组不了业务包，则返回null以告诉框架数据不够
        if (readableLength < Command.HEADER_LENGHT) {
            return null;
        }
        //先读get
        byte type = buffer.get();
        //读取消息体的长度
        int bodyLength = buffer.getInt();
        //数据不正确，则抛出AioDecodeException异常
        if (bodyLength < 0) {
            throw new AioDecodeException("bodyLength [" + bodyLength + "] is not right, remote:" + channelContext.getClientNode());
        }
        //计算本次需要的数据长度
        int neededLength = Command.HEADER_LENGHT + bodyLength;
        //收到的数据是否足够组包
        int isDataEnough = readableLength - neededLength;
        // 不够消息体长度(剩下的buffe组不了消息体)
        if (isDataEnough < 0) {
            return null;
        } else {//组包成功
        	Command packet = new Command();
        	packet.setReqType(type);
            if (bodyLength > 0) {
                byte[] dst = new byte[bodyLength];
                buffer.get(dst);
                packet.setBody(dst);
            }
            
            return packet;
        }
	}

	@Override
	public ByteBuffer encode(Packet packet, GroupContext groupContext, ChannelContext channelContext) {
		Command cmd = (Command) packet;
		byte[] body = cmd.getBody();
		int bodyLen = 0;
		if (body != null) {
			bodyLen = body.length;
		}

		//总长度是消息头的长度+消息体的长度
		int allLen = Command.HEADER_LENGHT + bodyLen;

		ByteBuffer buffer = ByteBuffer.allocate(allLen);
		buffer.order(groupContext.getByteOrder());

		//写入消息类型
		buffer.put(cmd.getReqType());
		//写入消息体长度
		buffer.putInt(bodyLen);

		//写入消息体
		if (body != null) {
			buffer.put(body);
		}
		
		
        return buffer;
	}

	@Override
	public void handler(Packet packet, ChannelContext channelContext) throws Exception {
		Command command = (Command) packet;
        byte[] body = command.getBody();
        if (body != null) {
            String str = new String(body, Command.CHARSET);
            System.out.println("收到消息：" + str);
            //消息进入队列并存盘
            
            
            //返回消息
            Command resppacket = new Command();
            resppacket.setBody(("收到了你的消息，你的消息是:" + str).getBytes(Command.CHARSET));
            Tio.send(channelContext, resppacket);
        }
		
	}

}
