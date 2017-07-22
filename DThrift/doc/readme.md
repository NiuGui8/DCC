# 项目说明

本项目是双向Thrift调用库（com.erchashu.thrift）的示例应用和测试项目，以仪器连接接口作为案例，提供了仪器信息上传和 echo() 测试服务。本项目包括客户端和服务端程序。

双向Thrift调用库是一个开源的基于 Thrift RPC 的双向调用库，支持在一个 TCP 连接上进行双向 RPC 调用，即实现了：

- 服务器向客户端提供了 RPC 服务（常规）
- 客户端向服务器提供了反向 RPC 服务，让服务器能主动调用客户端的服务

本项目带的 bat 脚本（bin目录下）用于方便运行测试程序。客户端和服务端的程序都支持命令行参数选项，运行时可使用 -help 查看支持的选项。