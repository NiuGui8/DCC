package com.erchashu.thrift;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.thrift.TByteArrayOutputStream;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 双向调用客户端服务。
 */
public class DuplexClientService extends Thread implements CallReadyNotifier {
    private static final Logger LOGGER = LoggerFactory.getLogger(DuplexClientService.class.getName());

    // 统一收发处理
    protected boolean isServing = false;
    protected FrameReadState readState;
    protected boolean isWriting;
    protected boolean isAwaitingWrite;
    protected long MAX_READ_BUFFER_BYTES; // 读缓冲的最大内存空间（字节数）
    protected ByteBuffer buffer_; // 读缓冲
    protected ByteBuffer writingBuffer = null; // 正在写入的缓冲
    // 客户端的待写入缓冲队列
    protected BlockingQueue<ByteBuffer> writingQueue = new ArrayBlockingQueue<ByteBuffer>(8);

    // 客户端调用链使用的传输器
    protected CallFramedTransport clientTransport;

    // 客户端提供的 RPC 服务的处理链
    protected TTransportFactory transportFactory = null;
    protected TProtocolFactory protocolFactory = null;
    protected TProcessor processor;
    protected TByteArrayOutputStream response_;
    protected TMemoryInputTransport frameTrans_;
    protected TTransport inTrans_;
    protected TTransport outTrans_;
    protected TProtocol inProt_;
    protected TProtocol outProt_;

    // 客户端 Socket 连接到服务器的参数
    protected Selector selector;
    protected SelectionKey selectionKey_;
    protected SocketChannel socketChannel;
    protected String host;
    protected int port;
    protected int socketTimeout; // Socket timeout - read timeout on the socket
    protected int connectTimeout; // Connection timeout

    /**
     * Frame 读状态机.
     */
    protected enum FrameReadState {
        /// 帧头读取中（从底层 Transport 中读出）
        READING_HEADER,
        /// 帧内数据读取中，尚未读完整帧数据
        READING_DATA,
        /// 已读完整帧数据，即将进行RPC调用
        READ_COMPLETE
    }

    /**
     * 使用指定的客户端传输器，构建 DuplexClientService 对象。
     * 
     * @param clientTransport 客户端传输器。
     */
    public DuplexClientService(CallFramedTransport clientTransport) {
        this.clientTransport = clientTransport;
        this.clientTransport.setCallReadyNotifier(this);

        MAX_READ_BUFFER_BYTES = Integer.MAX_VALUE;
    }

    public void setMaxReadBufferBytes(int maxBytes) {
        MAX_READ_BUFFER_BYTES = maxBytes;
    }

    static public class Args {
        public Args(TProcessor processor) {
            this.processor = processor;
        }

        public Args transportFactory(TTransportFactory factory) {
            transportFactory = factory;
            return this;
        }

        public Args protocolFactory(TProtocolFactory factory) {
            protocolFactory = factory;
            return this;
        }

        TProcessor processor = null;
        TTransportFactory transportFactory = null;
        TProtocolFactory protocolFactory = null;
    }

    public void setServiceArgs(Args args) {
        this.processor = args.processor;
        if (args.transportFactory == null) {
            this.transportFactory = new TTransportFactory();
        } else {
            this.transportFactory = args.transportFactory;
        }
        if (args.protocolFactory == null) {
            this.protocolFactory = new TBinaryProtocol.Factory();
        } else {
            this.protocolFactory = args.protocolFactory;
        }
    }

    /**
     * 设置服务器连接参数.
     *
     * @param host 服务器主机名/IP地址。
     * @param port 服务端口号。
     */
    public void setServer(String host, int port) {
        setServer(host, port, 0);
    }

    /**
     * 设置服务器连接参数.
     *
     * @param host 服务器主机名/IP地址。
     * @param port 服务端口号。
     * @param timeout Socket 超时和连接超时时间。
     */
    public void setServer(String host, int port, int timeout) {
        setServer(host, port, timeout, timeout);
    }

    /**
     * 设置服务器连接参数.
     *
     * @param host 服务器主机名/IP地址。
     * @param port 服务端口号。
     * @param timeout Socket 超时时间。
     * @param connectTimeout 连接超时时间。
     */
    public void setServer(String host, int port, int socketTimeout, int connectTimeout) {
        this.host = host;
        this.port = port;
        this.socketTimeout = socketTimeout;
        this.connectTimeout = connectTimeout;
    }

    public boolean isConnected() {
        if (socketChannel == null) {
            return false;
        }
        return socketChannel.isConnected();
    }

    /**
     * 连接到服务器.
     * 
     * @throws TTransportException
     */
    public void connect() throws TTransportException {
        if (isConnected()) {
            throw new TTransportException(TTransportException.ALREADY_OPEN, "Socket already connected.");
        }

        if (host == null || host.length() == 0) {
            throw new TTransportException(TTransportException.NOT_OPEN, "Cannot open null host.");
        }
        if (port <= 0 || port > 65535) {
            throw new TTransportException(TTransportException.NOT_OPEN, "Invalid port " + port);
        }

        try {
            socketChannel = SocketChannel.open(new InetSocketAddress(host, port));
            socketChannel.configureBlocking(false);
            socketChannel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
            socketChannel.setOption(StandardSocketOptions.SO_LINGER, 0);
            socketChannel.setOption(StandardSocketOptions.TCP_NODELAY, true);
            selector = Selector.open();
            selectionKey_ = socketChannel.register(selector, SelectionKey.OP_READ);

            // 建立反向服务的 RPC 处理链
            frameTrans_ = new TMemoryInputTransport();
            response_ = new TByteArrayOutputStream();
            inTrans_ = transportFactory.getTransport(frameTrans_);
            outTrans_ = transportFactory.getTransport(new TIOStreamTransport(response_));
            inProt_ = protocolFactory.getProtocol(inTrans_);
            outProt_ = protocolFactory.getProtocol(outTrans_);

            isWriting = false;
            isAwaitingWrite = false;
            prepareRead();
        } catch (IOException iox) {
            closeConnection();
            throw new TTransportException(TTransportException.NOT_OPEN, iox);
        }
    }

    /**
     * 断开与服务器的连接.
     */
    public void disconnect() {
        isServing = false;

        try {
            join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void waitForShutdown() {
    }

    @Override
    public void start() {
        isServing = true;
        super.start();
    }

    // 服务线程函数
    public void run() {
        LOGGER.info("Duplex client thread started");
        if (!isConnected()) {
            return;
        }

        while (isServing) {
            try {
                selector.select();
                changeSelectInterests();
                Iterator<SelectionKey> selectedKeys = selector.selectedKeys().iterator();
                while (isServing && selectedKeys.hasNext()) {
                    SelectionKey key = selectedKeys.next();
                    selectedKeys.remove();

                    // SelectionKey 出现异常（如 key被取消、SocketChannel被关闭等）
                    if (!key.isValid()) {
                        isServing = false;
                        break;
                    }

                    if (key.isReadable()) {
                        isServing = handleRead(); // 失败表示连接异常
                        break;
                    } else if (key.isWritable()) {
                        isServing = handleWrite(); // 失败表示连接异常
                    } else {
                        LOGGER.warn("Unexpected state in select! " + key.interestOps());
                    }
                }
            } catch (IOException e) {
                LOGGER.warn("Got an IOException while selecting!", e);
            }
        }

        closeConnection();
        LOGGER.info("Duplex client thread exit");
    }

    protected void prepareRead() {
        buffer_ = ByteBuffer.allocate(4);
        readState = FrameReadState.READING_HEADER;
    }

    protected boolean internalRead() {
        try {
            if (socketChannel.read(buffer_) < 0) {
                return false;
            }
            return true;
        } catch (IOException e) {
            LOGGER.warn("Got an IOException in internalRead!", e);
            return false;
        }
    }

    protected boolean handleRead() {
        if (!isServing) {
            return false;
        }

        // 读帧头
        byte frameType = 0;
        if (readState == FrameReadState.READING_HEADER) {
            // 尝试读取帧头
            if (!internalRead()) {
                return false;
            }

            // 帧头部已读完，可准备读剩下的帧内容
            if (buffer_.remaining() == 0) {
                // 解析帧类型及帧大小
                frameType = buffer_.get(0);
                int frameSize = buffer_.getInt(0) & 0x00FFFFFF; // Big-Endian
                // 错误的帧长度
                if (frameSize <= 0) {
                    LOGGER.error("Read an invalid frame size of " + frameSize + ". The frame maybe broken.");
                    return false;
                }
                if (frameSize > MAX_READ_BUFFER_BYTES) {
                    LOGGER.error("Read a frame size of " + frameSize
                            + ", which is bigger than the maximum allowable buffer size.");
                    return false;
                }

                // 分配整帧缓冲
                buffer_ = ByteBuffer.allocate(frameSize + 4);
                buffer_.putInt(frameSize);

                readState = FrameReadState.READING_DATA;
            } else {
                // 未读完帧头，继续等待更多数据
                return true;
            }
        }

        // 读帧数据
        if (readState == FrameReadState.READING_DATA) {
            if (!internalRead()) {
                return false;
            }

            // 帧数据已读完，分派处理
            if (buffer_.remaining() == 0) {
                switch (frameType) {
                case FrameType.THRIFT_CALL:
                    // 读到服务端的一个反向调用帧
                    // 直接在 Selector 线程中处理 RPC 请求
                    frameTrans_.reset(buffer_.array());
                    response_.reset();

                    try {
                        processor.process(inProt_, outProt_);
                        // 准备发送回应包给对端
                        if (response_.len() == 0) {
                            // oneway 调用，不需要返回
                        } else {
                            try {
                                // 准备好回应帧，并压到发送队列，注册 OP_WRITE 事件，等待该事件触发后才发送
                                ByteBuffer buf = ByteBuffer.wrap(response_.get(), 0, response_.len());
                                buf.put(0, (byte) FrameType.THRIFT_REPLY);
                                writingQueue.put(buf);

                                isAwaitingWrite = true;
                                changeSelectInterests();
                            } catch (InterruptedException e) {
                                LOGGER.warn("Interrupted while adding response buffer!", e);
                            }
                        }
                    } catch (TException te) {
                        LOGGER.warn("Exception while invoking!", te);
                        return false;
                    } catch (Throwable t) {
                        LOGGER.error("Unexpected throwable while invoking!", t);
                        return false;
                    }
                    break;

                case FrameType.THRIFT_REPLY:
                    // 读到客户端的一个回应帧，交由 clientTransport 处理，然后重新开始读帧
                    clientTransport.putBuffer(buffer_);
                    break;

                default:
                    // 未知的帧，直接抛弃，重新开始读帧
                    LOGGER.warn("Received unknown frame, type:{}", frameType);
                    break;
                }
                prepareRead();
            }

            return true;
        }

        // if we fall through to this point, then the state must be invalid.
        LOGGER.error("Read was called but state is invalid (" + readState + ")");
        return false;
    }

    /**
     * 发送写缓冲队列中的所有数据。
     * 
     * @return 发送过程是否出错。
     */
    protected boolean handleWrite() {
        boolean ret = false;
        if (isServing && isWriting) {
            while (true) {
                if (writingBuffer == null || writingBuffer.remaining() == 0) {
                    writingBuffer = writingQueue.poll();
                }

                if (writingBuffer == null || writingBuffer.remaining() == 0) {
                    // 队列中所有数据都已写完，切换回只读状态
                    selectionKey_.interestOps(SelectionKey.OP_READ);
                    isWriting = false;
                    ret = true;
                    break;
                } else {
                    try {
                        int len = socketChannel.write(writingBuffer);
                        if (len == 0) { // 底层发送缓冲已满，本次触发无再写
                            ret = true;
                            break;
                        } else if (len < 0) {   // 写出错
                            break;
                        }
                    } catch (IOException e) {
                        LOGGER.warn("Got an IOException during write!", e);
                        break;
                    }
                }
            }
        } else {
            LOGGER.error("Write was called unexpected. isServing={}, isWriting={}", isServing, isWriting);
        }

        return ret;
    }

    protected void closeConnection() {
        try {
            // 关闭 socket
            if (socketChannel != null) {
                socketChannel.close();
                socketChannel = null;
            }
            if (selector != null) {
                selector.close();
                selector = null;
            }
        } catch (IOException iox) {
            LOGGER.warn("Could not close socket.", iox);
        }
    }

    @Override
    public void requestCall(ByteBuffer buffer) throws TTransportException {
        if (isServing) {
            if (buffer != null && (buffer.remaining() > 0)) {
                try {
                    buffer.put(0, (byte) FrameType.THRIFT_CALL);
                    writingQueue.put(buffer);

                    // 唤醒 Selector 线程注册 WRITE 事件
                    isAwaitingWrite = true;
                    selector.wakeup();

                } catch (InterruptedException e) {
                    LOGGER.warn("Interrupted while adding requestCall buffer!", e);
                }
            }
        } else {
            // 连接已关闭，抛出异常让上层处理
            throw new TTransportException(TTransportException.NOT_OPEN, "Conection has been closed.");
        }
    }

    public void changeSelectInterests() {
        if (isAwaitingWrite) {
            selectionKey_.interestOps(SelectionKey.OP_WRITE | SelectionKey.OP_READ);
            isWriting = true;
            isAwaitingWrite = false;
        }
    }
}