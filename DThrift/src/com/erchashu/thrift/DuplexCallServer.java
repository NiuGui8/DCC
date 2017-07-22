/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.erchashu.thrift;

import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.spi.SelectorProvider;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.thrift.TException;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.AbstractNonblockingServer;
import org.apache.thrift.server.ServerContext;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TNonblockingSocket;
import org.apache.thrift.transport.TNonblockingTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 双向调用服务器. 基于 org.apache.thrift.server.TThreadedSelectorServer 修改。
 * 传输前时，本服务器将对原帧头格式（TFramedTransport的帧头定义为32位帧长）进行变换，
 * 把原帧头的4字节拆分为：FrameType(8bit) + FrameLength(24bit) ，这意味着一帧最多只能传输 16MB。
 * 
 * 帧类型定义请参考： {@link com.erchashu.thrift.FrameType} 。
 * 
 * A Half-Sync/Half-Async server with a separate pool of threads to handle
 * non-blocking I/O. Accepts are handled on a single thread, and a configurable
 * number of nonblocking selector threads manage reading and writing of client
 * connections. A synchronous worker thread pool handles processing of requests.
 * 
 * Performs better than TNonblockingServer/THsHaServer in multi-core
 * environments when the the bottleneck is CPU on the single selector thread
 * handling I/O. In addition, because the accept handling is decoupled from
 * reads/writes and invocation, the server has better ability to handle back-
 * pressure from new connections (e.g. stop accepting when busy).
 * 
 * Like TNonblockingServer, it relies on the use of TFramedTransport.
 */
public class DuplexCallServer extends AbstractNonblockingServer {
    private static final Logger LOGGER = LoggerFactory.getLogger(DuplexCallServer.class.getName());

    public static class Args extends AbstractNonblockingServerArgs<Args> {

        /**
         * The number of threads for selecting on already-accepted connections
         */
        public int selectorThreads = 2;
        /**
         * The size of the executor service (if none is specified) that will
         * handle invocations. This may be set to 0, in which case invocations
         * will be handled directly on the selector threads (as is in
         * TNonblockingServer)
         */
        private int workerThreads = 5;
        /** Time to wait for server to stop gracefully */
        private int stopTimeoutVal = 60;
        private TimeUnit stopTimeoutUnit = TimeUnit.SECONDS;
        /** The ExecutorService for handling dispatched requests */
        private ExecutorService executorService = null;
        /**
         * The size of the blocking queue per selector thread for passing
         * accepted connections to the selector thread
         */
        private int acceptQueueSizePerThread = 4;

        /**
         * Determines the strategy for handling new accepted connections.
         */
        public static enum AcceptPolicy {
            /**
             * Require accepted connection registration to be handled by the
             * executor. If the worker pool is saturated, further accepts will
             * be closed immediately. Slightly increases latency due to an extra
             * scheduling.
             */
            FAIR_ACCEPT,
            /**
             * Handle the accepts as fast as possible, disregarding the status
             * of the executor service.
             */
            FAST_ACCEPT
        }

        private AcceptPolicy acceptPolicy = AcceptPolicy.FAST_ACCEPT;

        public Args(TNonblockingServerTransport transport) {
            super(transport);
        }

        public Args selectorThreads(int i) {
            selectorThreads = i;
            return this;
        }

        public int getSelectorThreads() {
            return selectorThreads;
        }

        public Args workerThreads(int i) {
            workerThreads = i;
            return this;
        }

        public int getWorkerThreads() {
            return workerThreads;
        }

        public int getStopTimeoutVal() {
            return stopTimeoutVal;
        }

        public Args stopTimeoutVal(int stopTimeoutVal) {
            this.stopTimeoutVal = stopTimeoutVal;
            return this;
        }

        public TimeUnit getStopTimeoutUnit() {
            return stopTimeoutUnit;
        }

        public Args stopTimeoutUnit(TimeUnit stopTimeoutUnit) {
            this.stopTimeoutUnit = stopTimeoutUnit;
            return this;
        }

        public ExecutorService getExecutorService() {
            return executorService;
        }

        public Args executorService(ExecutorService executorService) {
            this.executorService = executorService;
            return this;
        }

        public int getAcceptQueueSizePerThread() {
            return acceptQueueSizePerThread;
        }

        public Args acceptQueueSizePerThread(int acceptQueueSizePerThread) {
            this.acceptQueueSizePerThread = acceptQueueSizePerThread;
            return this;
        }

        public AcceptPolicy getAcceptPolicy() {
            return acceptPolicy;
        }

        public Args acceptPolicy(AcceptPolicy acceptPolicy) {
            this.acceptPolicy = acceptPolicy;
            return this;
        }

        public void validate() {
            if (selectorThreads <= 0) {
                throw new IllegalArgumentException("selectorThreads must be positive.");
            }
            if (workerThreads < 0) {
                throw new IllegalArgumentException("workerThreads must be non-negative.");
            }
            if (acceptQueueSizePerThread <= 0) {
                throw new IllegalArgumentException("acceptQueueSizePerThread must be positive.");
            }
        }
    }

    // The thread handling all accepts
    private AcceptThread acceptThread;

    // Threads handling events on client transports
    private final Set<SelectorThread> selectorThreads = new HashSet<SelectorThread>();

    // This wraps all the functionality of queueing and thread pool management
    // for the passing of Invocations from the selector thread(s) to the workers
    // (if any).
    private final ExecutorService invoker;

    private final Args args;

    // 反向调用相关属性
    protected ClientFactory clientFactory = null;
    protected TProtocolFactory clientProtocolFactory = null;
    protected TTransportFactory clientTransportFactory = null;
    protected ThreadingClientManager threadingClientManager = null;
    protected int clientCallTimeout = 0;   // 反向调用超时，毫秒

    /**
     * 允许分配给一个客户端的读缓冲的最大内存空间（字节数） . 如果无此限制，服务器可能抛出内存不足的异常。
     */
    protected final long MAX_READ_BUFFER_BYTES;

    /**
     * 当前分配给读缓冲的字节数.
     */
    protected final AtomicLong readBufferBytesAllocated = new AtomicLong(0);

    /**
     * Create the server with the specified Args configuration
     */
    public DuplexCallServer(Args args) {
        super(args);
        args.validate();
        invoker = args.executorService == null ? createDefaultExecutor(args) : args.executorService;
        this.args = args;

        MAX_READ_BUFFER_BYTES = args.maxReadBufferBytes;
        this.clientTransportFactory = new TTransportFactory();
        this.threadingClientManager = ThreadingClientManager.getInstance();
    }

    public void setClientFactory(ClientFactory clientFactory) {
        this.clientFactory = clientFactory;
    }

    public void setClientProtocolFactory(TProtocolFactory clientProtocolFactory) {
        this.clientProtocolFactory = clientProtocolFactory;
    }

    public void setClientTransportFactory(TTransportFactory clientTransportFactory) {
        this.clientTransportFactory = clientTransportFactory;
    }
    
    /**
     * 设置反向调用的超时时间. 如果超时仍无返回，将抛出 TTransportException(TIMED_OUT) 异常。
     * @param milliseconds 超时毫秒数。0 表示一直等待。
     */
    public void setClientCallTimeout(int milliseconds) {
        this.clientCallTimeout = milliseconds;
    }

    /**
     * 获取反向调用的超时时间.
     */
    public int  getClientCallTimeout() {
        return this.clientCallTimeout;
    }

    /**
     * Start the accept and selector threads running to deal with clients.
     * 
     * @return true if everything went ok, false if we couldn't start for some
     *         reason.
     */
    @Override
    protected boolean startThreads() {
        try {
            for (int i = 0; i < args.selectorThreads; ++i) {
                selectorThreads.add(new SelectorThread(args.acceptQueueSizePerThread));
            }
            acceptThread = new AcceptThread((TNonblockingServerTransport) serverTransport_,
                    createSelectorThreadLoadBalancer(selectorThreads));
            for (SelectorThread thread : selectorThreads) {
                thread.start();
            }
            acceptThread.start();
            return true;
        } catch (IOException e) {
            LOGGER.error("Failed to start threads!", e);
            return false;
        }
    }

    /**
     * Joins the accept and selector threads and shuts down the executor
     * service.
     */
    @Override
    protected void waitForShutdown() {
        try {
            joinThreads();
        } catch (InterruptedException e) {
            // Non-graceful shutdown occurred
            LOGGER.error("Interrupted while joining threads!", e);
        }
        gracefullyShutdownInvokerPool();
    }

    protected void joinThreads() throws InterruptedException {
        // wait until the io threads exit
        acceptThread.join();
        for (SelectorThread thread : selectorThreads) {
            thread.join();
        }
    }

    /**
     * Stop serving and shut everything down.
     */
    @Override
    public void stop() {
        stopped_ = true;

        // Stop queuing connect attempts asap
        stopListening();

        if (acceptThread != null) {
            acceptThread.wakeupSelector();
        }
        if (selectorThreads != null) {
            for (SelectorThread thread : selectorThreads) {
                if (thread != null)
                    thread.wakeupSelector();
            }
        }
    }

    protected void gracefullyShutdownInvokerPool() {
        // try to gracefully shut down the executor service
        invoker.shutdown();

        // Loop until awaitTermination finally does return without a interrupted
        // exception. If we don't do this, then we'll shut down prematurely.
        // We want to let the executorService clear it's task queue, closing
        // client sockets appropriately.
        long timeoutMS = args.stopTimeoutUnit.toMillis(args.stopTimeoutVal);
        long now = System.currentTimeMillis();
        while (timeoutMS >= 0) {
            try {
                invoker.awaitTermination(timeoutMS, TimeUnit.MILLISECONDS);
                break;
            } catch (InterruptedException ix) {
                long newnow = System.currentTimeMillis();
                timeoutMS -= (newnow - now);
                now = newnow;
            }
        }
    }

    /**
     * We override the standard invoke method here to queue the invocation for
     * invoker service instead of immediately invoking. If there is no thread
     * pool, handle the invocation inline on this thread
     */
    @Override
    protected boolean requestInvoke(FrameBuffer frameBuffer) {
        Runnable invocation = getRunnable(frameBuffer);
        if (invoker != null) {
            try {
                invoker.execute(invocation);
                return true;
            } catch (RejectedExecutionException rx) {
                LOGGER.warn("ExecutorService rejected execution!", rx);
                return false;
            }
        } else {
            // Invoke on the caller's thread
            invocation.run();
            return true;
        }
    }

    protected Runnable getRunnable(FrameBuffer frameBuffer) {
        return new Invocation(frameBuffer);
    }

    /**
     * Helper to create the invoker if one is not specified
     */
    protected static ExecutorService createDefaultExecutor(Args options) {
        return (options.workerThreads > 0) ? Executors.newFixedThreadPool(options.workerThreads) : null;
    }

    private static BlockingQueue<TNonblockingTransport> createDefaultAcceptQueue(int queueSize) {
        if (queueSize == 0) {
            // Unbounded queue
            return new LinkedBlockingQueue<TNonblockingTransport>();
        }
        return new ArrayBlockingQueue<TNonblockingTransport>(queueSize);
    }

    /**
     * An Invocation represents a method call that is prepared to execute, given
     * an idle worker thread. It contains the input and output protocols the
     * thread's processor should use to perform the usual Thrift invocation.
     */
    class Invocation implements Runnable {
        private final FrameBuffer frameBuffer;

        public Invocation(final FrameBuffer frameBuffer) {
            this.frameBuffer = frameBuffer;
        }

        public void run() {
            frameBuffer.invoke();
        }
    }

    /**
     * The thread that selects on the server transport (listen socket) and
     * accepts new connections to hand off to the IO selector threads
     */
    protected class AcceptThread extends Thread {

        // The listen socket to accept on
        private final TNonblockingServerTransport serverTransport;
        private final Selector acceptSelector;

        private final SelectorThreadLoadBalancer threadChooser;

        /**
         * Set up the AcceptThead
         * 
         * @throws IOException
         */
        public AcceptThread(TNonblockingServerTransport serverTransport, SelectorThreadLoadBalancer threadChooser)
                throws IOException {
            this.serverTransport = serverTransport;
            this.threadChooser = threadChooser;
            this.acceptSelector = SelectorProvider.provider().openSelector();
            this.serverTransport.registerSelector(acceptSelector);
        }

        /**
         * The work loop. Selects on the server transport and accepts. If there
         * was a server transport that had blocking accepts, and returned on
         * blocking client transports, that should be used instead
         */
        public void run() {
            try {
                if (eventHandler_ != null) {
                    eventHandler_.preServe();
                }

                while (!stopped_) {
                    select();
                }
            } catch (Throwable t) {
                LOGGER.error("run() on AcceptThread exiting due to uncaught error", t);
            } finally {
                try {
                    acceptSelector.close();
                } catch (IOException e) {
                    LOGGER.error("Got an IOException while closing accept selector!", e);
                }
                // This will wake up the selector threads
                DuplexCallServer.this.stop();
            }
        }

        /**
         * If the selector is blocked, wake it up.
         */
        public void wakeupSelector() {
            acceptSelector.wakeup();
        }

        /**
         * Select and process IO events appropriately: If there are connections
         * to be accepted, accept them.
         */
        private void select() {
            try {
                // wait for connect events.
                acceptSelector.select();

                // process the io events we received
                Iterator<SelectionKey> selectedKeys = acceptSelector.selectedKeys().iterator();
                while (!stopped_ && selectedKeys.hasNext()) {
                    SelectionKey key = selectedKeys.next();
                    selectedKeys.remove();

                    // skip if not valid
                    if (!key.isValid()) {
                        continue;
                    }

                    if (key.isAcceptable()) {
                        handleAccept();
                    } else {
                        LOGGER.warn("Unexpected state in select! " + key.interestOps());
                    }
                }
            } catch (IOException e) {
                LOGGER.warn("Got an IOException while selecting!", e);
            }
        }

        /**
         * Accept a new connection.
         */
        private void handleAccept() {
            final TNonblockingTransport client = doAccept();
            if (client != null) {
                // Pass this connection to a selector thread
                final SelectorThread targetThread = threadChooser.nextThread();

                if (args.acceptPolicy == Args.AcceptPolicy.FAST_ACCEPT || invoker == null) {
                    doAddAccept(targetThread, client);
                } else {
                    // FAIR_ACCEPT
                    try {
                        invoker.submit(new Runnable() {
                            public void run() {
                                doAddAccept(targetThread, client);
                            }
                        });
                    } catch (RejectedExecutionException rx) {
                        LOGGER.warn("ExecutorService rejected accept registration!", rx);
                        // close immediately
                        client.close();
                    }
                }
            }
        }

        private TNonblockingTransport doAccept() {
            try {
                return (TNonblockingTransport) serverTransport.accept();
            } catch (TTransportException tte) {
                // something went wrong accepting.
                LOGGER.warn("Exception trying to accept!", tte);
                return null;
            }
        }

        private void doAddAccept(SelectorThread thread, TNonblockingTransport client) {
            if (!thread.addAcceptedConnection(client)) {
                client.close();
            }
        }
    } // AcceptThread

    /**
     * The SelectorThread(s) will be doing all the selecting on accepted active
     * connections.
     */
    protected class SelectorThread extends AbstractSelectThread {

        // Accepted connections added by the accept thread.
        private final BlockingQueue<TNonblockingTransport> acceptedQueue;

        /**
         * Set up the SelectorThread with an unbounded queue for incoming
         * accepts.
         * 
         * @throws IOException
         *             if a selector cannot be created
         */
        public SelectorThread() throws IOException {
            this(new LinkedBlockingQueue<TNonblockingTransport>());
        }

        /**
         * Set up the SelectorThread with an bounded queue for incoming accepts.
         * 
         * @throws IOException
         *             if a selector cannot be created
         */
        public SelectorThread(int maxPendingAccepts) throws IOException {
            this(createDefaultAcceptQueue(maxPendingAccepts));
        }

        /**
         * Set up the SelectorThread with a specified queue for connections.
         * 
         * @param acceptedQueue
         *            The BlockingQueue implementation for holding incoming
         *            accepted connections.
         * @throws IOException
         *             if a selector cannot be created.
         */
        public SelectorThread(BlockingQueue<TNonblockingTransport> acceptedQueue) throws IOException {
            this.acceptedQueue = acceptedQueue;
        }

        /**
         * Hands off an accepted connection to be handled by this thread. This
         * method will block if the queue for new connections is at capacity.
         * 
         * @param accepted
         *            The connection that has been accepted.
         * @return true if the connection has been successfully added.
         */
        public boolean addAcceptedConnection(TNonblockingTransport accepted) {
            try {
                acceptedQueue.put(accepted);
            } catch (InterruptedException e) {
                LOGGER.warn("Interrupted while adding accepted connection!", e);
                return false;
            }
            selector.wakeup();
            return true;
        }

        /**
         * The work loop. Handles selecting (read/write IO), dispatching, and
         * managing the selection preferences of all existing connections.
         */
        public void run() {
            try {
                while (!stopped_) {
                    select();
                    processAcceptedConnections();
                    processInterestChanges();
                }
                for (SelectionKey selectionKey : selector.keys()) {
                    cleanupSelectionKey(selectionKey);
                }
            } catch (Throwable t) {
                LOGGER.error("run() on SelectorThread exiting due to uncaught error", t);
            } finally {
                try {
                    selector.close();
                } catch (IOException e) {
                    LOGGER.error("Got an IOException while closing selector!", e);
                }
                // This will wake up the accept thread and the other selector
                // threads
                DuplexCallServer.this.stop();
            }
        }

        /**
         * Select and process IO events appropriately: If there are existing
         * connections with data waiting to be read, read it, buffering until a
         * whole frame has been read. If there are any pending responses, buffer
         * them until their target client is available, and then send the data.
         */
        private void select() {
            try {
                // wait for io events.
                selector.select();

                // process the io events we received
                Iterator<SelectionKey> selectedKeys = selector.selectedKeys().iterator();
                while (!stopped_ && selectedKeys.hasNext()) {
                    SelectionKey key = selectedKeys.next();
                    selectedKeys.remove();

                    // skip if not valid
                    if (!key.isValid()) {
                        cleanupSelectionKey(key);
                        continue;
                    }

                    if (key.isReadable()) {
                        // deal with reads
                        handleRead(key);
                    } else if (key.isWritable()) {
                        // deal with writes
                        handleWrite(key);
                    } else {
                        LOGGER.warn("Unexpected state in select! " + key.interestOps());
                    }
                }
            } catch (IOException e) {
                LOGGER.warn("Got an IOException while selecting!", e);
            }
        }

        private void processAcceptedConnections() {
            // Register accepted connections
            while (!stopped_) {
                TNonblockingTransport accepted = acceptedQueue.poll();
                if (accepted == null) {
                    break;
                }
                registerAccepted(accepted);
            }
        }

        protected FrameBuffer createFrameBuffer(final TNonblockingTransport trans, final SelectionKey selectionKey,
                final AbstractSelectThread selectThread) {
            return new DuplexFrameBuffer(trans, selectionKey, selectThread);
        }

        private void registerAccepted(TNonblockingTransport accepted) {
            SelectionKey clientKey = null;
            try {
                clientKey = accepted.registerSelector(selector, SelectionKey.OP_READ);

                threadingClientManager.attachSocket(((TNonblockingSocket)accepted).getSocketChannel().socket());
                FrameBuffer frameBuffer = createFrameBuffer(accepted, clientKey, SelectorThread.this);
                threadingClientManager.detach();

                clientKey.attach(frameBuffer);
            } catch (IOException e) {
                LOGGER.warn("Failed to register accepted connection to selector!", e);
                if (clientKey != null) {
                    cleanupSelectionKey(clientKey);
                }
                accepted.close();
            }
        }
    } // SelectorThread

    /**
     * Creates a SelectorThreadLoadBalancer to be used by the accept thread for
     * assigning newly accepted connections across the threads.
     */
    protected SelectorThreadLoadBalancer createSelectorThreadLoadBalancer(
            Collection<? extends SelectorThread> threads) {
        return new SelectorThreadLoadBalancer(threads);
    }

    /**
     * A round robin load balancer for choosing selector threads for new
     * connections.
     */
    protected static class SelectorThreadLoadBalancer {
        private final Collection<? extends SelectorThread> threads;
        private Iterator<? extends SelectorThread> nextThreadIterator;

        public <T extends SelectorThread> SelectorThreadLoadBalancer(Collection<T> threads) {
            if (threads.isEmpty()) {
                throw new IllegalArgumentException("At least one selector thread is required");
            }
            this.threads = Collections.unmodifiableList(new ArrayList<T>(threads));
            nextThreadIterator = this.threads.iterator();
        }

        public SelectorThread nextThread() {
            // Choose a selector thread (round robin)
            if (!nextThreadIterator.hasNext()) {
                nextThreadIterator = threads.iterator();
            }
            return nextThreadIterator.next();
        }
    }

    /**
     * FrameBuffer 读状态机.
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
     * 双向帧缓冲.
     * 
     * 实现时，不应影响原来的 FrameBufferState 的状态变迁，以免影响原来的 RPC 服务调用。
     * 本类只是额外增加了反向调用的功能，数据在刚读到时即转移到反向调用帧传输器 ReversedCallFrameTransport 中。
     * 因为需要处理两路数据，OP_READ 事件需要一直打开，而不是原来的 RPC服务调用完成才打开。 而 OP_WRITE
     * 事件在需要时（requestCall() 以及 responseReady()）打开， 且处理 WRITE
     * 事件时，需要检查两个缓冲是否有数据要写。
     */
    public class DuplexFrameBuffer extends FrameBuffer implements CallReadyNotifier {
        private final Logger LOGGER = LoggerFactory.getLogger(getClass().getName());
        // 客户端的反向调用 Stub
        protected TServiceClient client;
        protected Socket socket;
        // 客户端的反向调用的分帧传输器
        protected CallFramedTransport callFramedTransport;
        // 客户端的待写入缓冲队列
        protected BlockingQueue<ByteBuffer> writingQueue = new ArrayBlockingQueue<ByteBuffer>(8);
        // RPC 请求缓冲队列
        protected BlockingQueue<ByteBuffer> invokingQueue = new ArrayBlockingQueue<ByteBuffer>(8);
        
        // 正在写入的缓冲
        protected ByteBuffer writingBuffer = null;
        protected FrameReadState readState = FrameReadState.READING_HEADER;
        protected boolean isWriting;
        protected boolean isAwaitingWrite;
        protected boolean isAwaitingClose;
        protected boolean isActive;

        public DuplexFrameBuffer(final TNonblockingTransport trans, final SelectionKey selectionKey,
                final AbstractSelectThread selectThread) {
            super(trans, selectionKey, selectThread);

            callFramedTransport = new CallFramedTransport(clientCallTimeout);
            callFramedTransport.setCallReadyNotifier(this);
            client = clientFactory.getClient(
                    clientProtocolFactory.getProtocol(clientTransportFactory.getTransport(callFramedTransport)));

            socket = ((TNonblockingSocket)trans).getSocketChannel().socket();
            isActive = true;
            isWriting = false;
            isAwaitingWrite = false;
            isAwaitingClose = false;
        }

        public ServerContext getContext() {
            return context_;
        }

        public TServiceClient getClient() {
            return client;
        }

        public Socket getSocket() {
            return socket;
        }

        @Override
        public void requestCall(ByteBuffer buffer) throws TTransportException {
            if (isActive) {
                if (buffer != null && (buffer.remaining() > 0)) {
                    try {
                        buffer.put(0, (byte) FrameType.THRIFT_CALL);
                        writingQueue.put(buffer);

                        // 唤醒 Selector 线程注册 WRITE 事件
                        isAwaitingWrite = true;
                        requestSelectInterestChange();

                    } catch (InterruptedException e) {
                        LOGGER.warn("Interrupted while adding request buffer to writingQueue!", e);
                    }
                }
            } else {
                // 连接已关闭，抛出异常让上层处理
                throw new TTransportException(TTransportException.NOT_OPEN, "Conection has been closed.");
            }
        }

        /**
         * Give this FrameBuffer a chance to read. The selector loop should have
         * received a read event for this FrameBuffer.
         * 
         * @return true if the connection should live on, false if it should be
         *         closed
         */
        @Override
        public boolean read() {
            if (!isActive) {
                return false;
            }

            byte frameType = 0;
            if (readState == FrameReadState.READING_HEADER) {
                // 尝试读取帧头
                if (!internalRead()) {
                    return false;
                }

                // if the frame size has been read completely, then prepare to
                // read the actual frame.
                if (buffer_.remaining() == 0) {
                    // pull out the frame size as an integer.
                    frameType = buffer_.get(0);
                    int frameSize = buffer_.getInt(0) & 0x00FFFFFF; // Big-Endian
                    if (frameSize <= 0) {
                        LOGGER.error("Read an invalid frame size of " + frameSize
                                + ". Are you using TFramedTransport on the client side?");
                        return false;
                    }

                    // if this frame will always be too large for this server,
                    // log the error and close the connection.
                    if (frameSize > MAX_READ_BUFFER_BYTES) {
                        LOGGER.error("Read a frame size of " + frameSize
                                + ", which is bigger than the maximum allowable buffer size for ALL connections.");
                        return false;
                    }

                    // if this frame will push us over the memory limit, then
                    // return. with luck, more memory will free up the next
                    // time around.
                    if (readBufferBytesAllocated.get() + frameSize > MAX_READ_BUFFER_BYTES) {
                        return true;
                    }

                    // increment the amount of memory allocated to read buffers
                    readBufferBytesAllocated.addAndGet(frameSize + 4);

                    // reallocate the readbuffer as a frame-sized buffer
                    buffer_ = ByteBuffer.allocate(frameSize + 4);
                    buffer_.putInt(frameSize);

                    readState = FrameReadState.READING_DATA;
                } else {
                    // this skips the check of READING_DATA state below, since
                    // we can't possibly go on to that state if there's data
                    // left to be read at this one.
                    return true;
                }
            }

            // it is possible to fall through from the READING_HEADER
            // section to READING_DATA if there's already some frame
            // data available once READING_FRAME_SIZE is complete.
            if (readState == FrameReadState.READING_DATA) {
                if (!internalRead()) {
                    return false;
                }
                
                // since we're already in the select loop here for sure, we can
                // just modify our selection key directly.
                if (buffer_.remaining() == 0) {
                    switch (frameType) {
                    case FrameType.THRIFT_CALL:
                        // 读到客户端的一个调用帧，压入执行队列，等待执行
                        try {
                            invokingQueue.put(buffer_);
                        } catch (InterruptedException e) {
                            LOGGER.warn("Interrupted while adding buffer to invokingQueue!", e);
                        }
                        break;

                    case FrameType.THRIFT_REPLY:
                        // 读到客户端的一个反向回应帧，交由 callBufferFramedTransport
                        // 处理，然后重新开始读帧
                        callFramedTransport.putBuffer(buffer_);
                        break;

                    default:
                        // 未知的帧，直接抛弃，重新开始读帧
                        LOGGER.warn("Received unknown frame, type:{}", frameType);
                        break;
                    }
                    
                    // 重新开始读帧
                    prepareRead();
                }

                return true;
            }

            // if we fall through to this point, then the state must be invalid.
            LOGGER.error("Read was called but state is invalid (" + readState + ")");
            return false;
        }

        /**
         * Give this FrameBuffer a chance to write its output to the final
         * client.
         */
        @Override
        public boolean write() {
            boolean ret = false;
            if (isActive && isWriting) {
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
                            int len = trans_.write(writingBuffer);
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
                LOGGER.error("Write was called unexpected. isActive={}, isWriting={}", isActive, isWriting);
            }

            return ret;
        }

        /**
         * Give this FrameBuffer a chance to set its interest to write, once
         * data has come in.
         */
        @Override
        public void changeSelectInterests() {
            if (isAwaitingWrite) {
                try {
                    // add the OP_WRITE interest
                    selectionKey_.interestOps(SelectionKey.OP_WRITE | SelectionKey.OP_READ);
                } catch (CancelledKeyException e) {
                    LOGGER.warn("Write request while SelectionKey has been canceled", e);
                }
                isWriting = true;
                isAwaitingWrite = false;
            }
            if (isAwaitingClose) {
                close();
                selectionKey_.cancel();
                isAwaitingClose = false;
            }
        }

        /**
         * Shut the connection down.
         */
        @Override
        public void close() {
            // 通知应用层
            if (eventHandler_ != null) {
                eventHandler_.deleteContext(context_, inProt_, outProt_);
            }
            
            // if we're being closed due to an error, we might have allocated a
            // buffer that we need to subtract for our memory accounting.
            if (readState == FrameReadState.READING_DATA || readState == FrameReadState.READ_COMPLETE
                    || isAwaitingClose) {
                readBufferBytesAllocated.addAndGet(-buffer_.array().length);
            }
            
            // 关闭 SOCKET
            trans_.close();
            
            // 状态恢复
            isActive = false;
            isAwaitingWrite = false;
            isAwaitingClose = false;
        }

        /**
         * Check if this FrameBuffer has a full frame read.
         */
        @Override
        public boolean isFrameFullyRead() {
            return !invokingQueue.isEmpty();
        }

        /**
         * After the processor has processed the invocation, whatever thread is
         * managing invocations should call this method on this FrameBuffer so
         * we know it's time to start trying to write again. Also, if it turns
         * out that there actually isn't any data in the response buffer, we'll
         * skip trying to write and instead go back to reading.
         */
        @Override
        public void responseReady() {
            if (response_.len() == 0) {
                // this was probably an oneway method
            } else {
                try {
                    ByteBuffer buf = ByteBuffer.wrap(response_.get(), 0, response_.len());
                    buf.put(0, (byte) FrameType.THRIFT_REPLY);
                    writingQueue.put(buf);

                    // 唤醒 Selector 线程注册 WRITE 事件
                    isAwaitingWrite = true;
                    requestSelectInterestChange();
                } catch (InterruptedException e) {
                    LOGGER.warn("Interrupted while adding response buffer!", e);
                }
            }
        }

        /**
         * Actually invoke the method signified by this FrameBuffer.
         */
        @Override
        public void invoke() {
            ByteBuffer buf = invokingQueue.poll();
            if (buf != null) {
                frameTrans_.reset(buf.array());
                response_.reset();

                try {
                    // if (eventHandler_ != null) {
                    // eventHandler_.processContext(context_, inTrans_, outTrans_);
                    // }
                    threadingClientManager.attachFrameBuffer(this);
                    processorFactory_.getProcessor(inTrans_).process(inProt_, outProt_);
                    threadingClientManager.detach();
                    responseReady();
                    return;
                } catch (TException te) {
                    LOGGER.warn("Exception while invoking!", te);
                } catch (Throwable t) {
                    LOGGER.error("Unexpected throwable while invoking!", t);
                }
                // This will only be reached when there is a throwable.
                isAwaitingClose = true;
                requestSelectInterestChange();
            }
        }

        /**
         * Perform a read into buffer.
         * 
         * @return true if the read succeeded, false if there was an error or
         *         the connection closed.
         */
        protected boolean internalRead() {
            try {
                if (trans_.read(buffer_) < 0) {
                    return false;
                }
                return true;
            } catch (IOException e) {
                LOGGER.warn("Got an IOException in internalRead!", e);
                return false;
            }
        }

        /**
         * We're done writing, so reset our interest ops and change state
         * accordingly.
         */
        protected void prepareRead() {
            // the read buffer is definitely no longer in use, so we will
            // decrement
            // our read buffer count. we do this here as well as in close
            // because
            // we'd like to free this read memory up as quickly as possible for
            // other clients.
            readBufferBytesAllocated.addAndGet(-buffer_.array().length + 4);

            // get ready for another go-around
            buffer_ = ByteBuffer.allocate(4);
            readState = FrameReadState.READING_HEADER;
        }

        /**
         * When this FrameBuffer needs to change its select interests and
         * execution might not be in its select thread, then this method will
         * make sure the interest change gets done when the select thread wakes
         * back up. When the current thread is this FrameBuffer's select thread,
         * then it just does the interest change immediately.
         */
        @Override
        protected void requestSelectInterestChange() {
            if (Thread.currentThread() == this.selectThread_) {
                changeSelectInterests();
            } else {
                this.selectThread_.requestSelectInterestChange(this);
            }
        }
    }
}
