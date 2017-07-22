package com.erchashu.thrift;

import java.nio.ByteBuffer;

import org.apache.thrift.TByteArrayOutputStream;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;

/**
 * 调用缓冲分帧传输器。
 */
public class CallFramedTransport extends TTransport {
    protected CallReadyNotifier callNotifier = null;
    protected ByteBuffer bufferToRead;
    protected int timeout;  // read timeout, milliseconds

    protected static final int DEFAULT_MAX_LENGTH = 16384000;

    private int maxLength;
    private final TByteArrayOutputStream writeBuffer = new TByteArrayOutputStream(1024);
    private TMemoryInputTransport readBuffer = new TMemoryInputTransport(new byte[0]);

    public static class Factory extends TTransportFactory {
        protected int maxLength;
        protected int timeout; 

        public Factory() {
            this.maxLength = CallFramedTransport.DEFAULT_MAX_LENGTH;
            this.timeout = 0;
        }

        public Factory(int maxLength) {
            this.maxLength = maxLength;
            this.timeout = 0;
        }
        
        public Factory timeout(int milliseconds) {
            this.timeout = milliseconds;
            return this;
        }

        @Override
        public TTransport getTransport(TTransport base) {
            return new CallFramedTransport(timeout, maxLength);
        }
    }

    public CallFramedTransport() {
        this.timeout = 0;
        this.maxLength = CallFramedTransport.DEFAULT_MAX_LENGTH;
    }

    public CallFramedTransport(int timeout) {
        this.timeout = timeout;
        this.maxLength = CallFramedTransport.DEFAULT_MAX_LENGTH;
    }

    public CallFramedTransport(int timeout, int maxLength) {
        this.timeout = timeout;
        this.maxLength = maxLength;
    }
    
    public void setCallReadyNotifier(CallReadyNotifier notifier) {
        callNotifier = notifier;
    }

    /**
     * 追加 一个已经读到的 Buffer 并唤醒阻塞读线程。
     * @param buf
     */
    public void putBuffer(ByteBuffer buf) {
        synchronized(this) {
            if (bufferToRead == null) {
                bufferToRead = buf;
                this.notifyAll();
            }
        }
    }
    
    public void open() throws TTransportException {
    }

    public boolean isOpen() {
        return false;
    }

    public void close() {
    }

    public int read(byte[] buf, int off, int len) throws TTransportException {
        if (readBuffer != null) {
            int got = readBuffer.read(buf, off, len);
            if (got > 0) {
                return got;
            }
        }

        // Read another frame of data
        readFrame();

        return readBuffer.read(buf, off, len);
    }

    @Override
    public byte[] getBuffer() {
        return readBuffer.getBuffer();
    }

    @Override
    public int getBufferPosition() {
        return readBuffer.getBufferPosition();
    }

    @Override
    public int getBytesRemainingInBuffer() {
        return readBuffer.getBytesRemainingInBuffer();
    }

    @Override
    public void consumeBuffer(int len) {
        readBuffer.consumeBuffer(len);
    }

    private void readFrame() throws TTransportException {
        synchronized(this) {
            if (bufferToRead == null) {
                try {
                    //long start = System.currentTimeMillis();
                    this.wait(timeout);
                    //if (System.currentTimeMillis() - start > this.timeout) {
                    //    throw new TTransportException(TTransportException.TIMED_OUT,
                    //            "Timeout while waiting reply frame!");
                    //}
                } catch (InterruptedException e) {
                }
            }
        }
        
        if (bufferToRead != null) {
            bufferToRead.flip();
            int size = bufferToRead.getInt();
            
            if (size < 0) {
                close();
                throw new TTransportException(TTransportException.CORRUPTED_DATA,
                        "Read a negative frame size (" + size + ")!");
            }

            if (size > maxLength) {
                close();
                throw new TTransportException(TTransportException.CORRUPTED_DATA,
                        "Frame size (" + size + ") larger than max length (" + maxLength + ")!");
            }

            // 已由底层解析过帧， 必定满足 size == bufferToRead.cap() - 4
            byte[] buff = new byte[bufferToRead.remaining()];
            bufferToRead.get(buff);
            bufferToRead = null; // 已转移，尽快腾空给底层
            readBuffer.reset(buff);
        }
    }

    public void write(byte[] buf, int off, int len) throws TTransportException {
        writeBuffer.write(buf, off, len);
    }

    @Override
    public void flush() throws TTransportException {
        int len = writeBuffer.len();
        ByteBuffer buff = ByteBuffer.allocate(len + 4);
        buff.putInt(len);
        buff.put(writeBuffer.get(), 0, len);
        buff.flip();
        writeBuffer.reset();
        callNotifier.requestCall(buff);
    }

}