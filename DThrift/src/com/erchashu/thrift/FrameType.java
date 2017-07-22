package com.erchashu.thrift;


/**
 * 帧型定义. 
 * 传输帧帧头格式为：FrameType(8bit) + FrameLength(24bit) 。
 */
public class FrameType {
    /**
     * Thrift 协议调用帧，即由客户端主动发出的帧，帧格式应符合 Thrift 规范；
     */
    public static final int THRIFT_CALL = 0x01; 
    /**
     * Thrift 协议回应帧，即由服务器被动响应发出的帧，帧格式应符合 Thrift 规范；
     */
    public static final int THRIFT_REPLY = 0x02;
    /**
     * 回声帧，用于心跳机制；
     */
    public static final int ECHO = 0x03;
    
    private int value;

    public FrameType(int value) {
        this.value = value;
    }
    
    public int value() {
        return this.value;
    }
}
