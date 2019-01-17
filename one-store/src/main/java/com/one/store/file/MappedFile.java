/**
 * 
 */
package com.one.store.file;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.charset.Charset;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.atomic.AtomicInteger;

import org.one.remote.cmd.OneMessage;
import org.tio.utils.json.Json;

import com.one.store.common.MsgPosition;

/**
 * @author yangkunguo
 *
 */
public class MappedFile extends ReferenceResource{

	private FileChannel fileChannel=null;
	private MappedByteBuffer mappedByteBuffer=null;
	private File file;
	protected final AtomicInteger wrotePosition = new AtomicInteger(0);
	private final AtomicInteger flushedPosition = new AtomicInteger(0);
	 
	protected ByteBuffer writeBuffer = null;
	private int fileSize;
	
	
	public MappedFile(File file,int fileSize) {
		super();
		this.file = file;
		this.fileSize=fileSize;
		init();
	}

	private void init() {
		try {
			fileChannel=new RandomAccessFile(file, "rw").getChannel();
			mappedByteBuffer=fileChannel.map(MapMode.READ_WRITE, 0, file.length());
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public boolean appand(OneMessage message) {
		long currentPos = this.wrotePosition.get();
		String msg=enCode(message)+"\n";
		byte[] data=msg.getBytes(Charset.forName("UTF-8"));
		
		 if ((currentPos + data.length) <= this.fileSize) {
	            try {
	                this.fileChannel.position(currentPos);
	                this.fileChannel.write(ByteBuffer.wrap(data));
	                
	            } catch (Throwable e) {
//	                log.error("Error occurred when append message to mappedFile.", e);
	            	e.printStackTrace();
	            }
	            this.wrotePosition.addAndGet(data.length);
	            return true;
	        }
		return false;
	}
	
	public OneMessage read(MsgPosition position) {
		try {
			
			int readPosition = this.wrotePosition.get();//position.getPosition().intValue();
			int pos=position.getPosition().intValue();
			if (pos < readPosition && pos >= 0) {
				if (this.hold()) {
					ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
					byteBuffer.position(pos);
					int size = readPosition - pos;
					ByteBuffer byteBufferNew = byteBuffer.slice();
					byteBufferNew.limit(size);
					byte[] bytes = new byte[position.getLength()];
					ByteBuffer byteBuffer2=byteBufferNew.get(bytes);
					
//					byte[] data=byteBuffer2.array();
					return deCode(new String(bytes));
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	public String enCode(OneMessage msg) {
		return Json.toJson(msg);
	}
	
	public OneMessage deCode(String json) {
		return Json.toBean(json, OneMessage.class);
	}

	public int getReadPosition() {
        return this.writeBuffer == null ? this.wrotePosition.get() : this.wrotePosition.get();
    }
	
	public Integer getWrotePosition() {
		return wrotePosition.get();
	}
	
	/**
     * @return The current flushed position
     */
    public int flush(final int flushLeastPages) {
//        if (this.isAbleToFlush(flushLeastPages)) {
            if (this.hold()) {
                int value = getReadPosition();

                try {
                    //We only append data to fileChannel or mappedByteBuffer, never both.
                    if (writeBuffer != null || this.fileChannel.position() != 0) {
                        this.fileChannel.force(false);
                    } else {
                        this.mappedByteBuffer.force();
                    }
                } catch (Throwable e) {
//                    log.error("Error occurred when force data to disk.", e);
                }

                this.flushedPosition.set(value);
                this.release();
            } else {
//                log.warn("in flush, hold failed, flush offset = " + this.flushedPosition.get());
                this.flushedPosition.set(getReadPosition());
            }
//        }
        return this.flushedPosition.get();
    }
//    private boolean isAbleToFlush(final int flushLeastPages) {
//        int flush = this.flushedPosition.get();
//        int write = getReadPosition();
//
//        if (this.isFull()) {
//            return true;
//        }
//
//        if (flushLeastPages > 0) {
//            return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE)) >= flushLeastPages;
//        }
//
//        return write > flush;
//    }

	public static void clean(final ByteBuffer buffer) {
        if (buffer == null || !buffer.isDirect() || buffer.capacity() == 0)
            return;
        invoke(invoke(viewed(buffer), "cleaner"), "clean");
    }

    private static Object invoke(final Object target, final String methodName, final Class<?>... args) {
        return AccessController.doPrivileged(new PrivilegedAction<Object>() {
            public Object run() {
                try {
                    Method method = method(target, methodName, args);
                    method.setAccessible(true);
                    return method.invoke(target);
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
        });
    }

    private static Method method(Object target, String methodName, Class<?>[] args)
        throws NoSuchMethodException {
        try {
            return target.getClass().getMethod(methodName, args);
        } catch (NoSuchMethodException e) {
            return target.getClass().getDeclaredMethod(methodName, args);
        }
    }
    private static ByteBuffer viewed(ByteBuffer buffer) {
        String methodName = "viewedBuffer";

        Method[] methods = buffer.getClass().getMethods();
        for (int i = 0; i < methods.length; i++) {
            if (methods[i].getName().equals("attachment")) {
                methodName = "attachment";
                break;
            }
        }

        ByteBuffer viewedBuffer = (ByteBuffer) invoke(buffer, methodName);
        if (viewedBuffer == null)
            return buffer;
        else
            return viewed(viewedBuffer);
    }

	@Override
	public boolean cleanup(long currentRef) {
		 if (this.isAvailable()) {
//	            log.error("this file[REF:" + currentRef + "] " + this.fileName
//	                + " have not shutdown, stop unmapping.");
	            return false;
	        }

	        if (this.isCleanupOver()) {
//	            log.error("this file[REF:" + currentRef + "] " + this.fileName
//	                + " have cleanup, do not do it again.");
	            return true;
	        }

	        clean(this.mappedByteBuffer);
//	        TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(this.fileSize * (-1));
//	        TOTAL_MAPPED_FILES.decrementAndGet();
//	        log.info("unmap file[REF:" + currentRef + "] " + this.fileName + " OK");
	        return true;
	}
}
