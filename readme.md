
see com.example.streaming.response.body.error.handling.race.condition.StreamingResponseBodyErrorHandlingRaceConditionTest

Generated via starter.spring.to on 2024-08-27.

Expected: Error handling is called for
```
com.example.streaming.response.body.error.handling.race.condition.StreamingResponseBodyErrorHandlingRaceConditionTest$SomethingWentWrongWhileStreamingException: org.springframework.web.context.request.async.AsyncRequestNotUsableException: ServletOutputStream failed to write: java.io.IOException: Broken pipe
at com.example.streaming.response.body.error.handling.race.condition.StreamingResponseBodyErrorHandlingRaceConditionTest$TestController.lambda$fails$0(StreamingResponseBodyErrorHandlingRaceConditionTest.java:128) ~[test/:na]
at org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBodyReturnValueHandler$StreamingResponseBodyTask.call(StreamingResponseBodyReturnValueHandler.java:110) ~[spring-webmvc-6.2.0-M7.jar:6.2.0-M7]
at org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBodyReturnValueHandler$StreamingResponseBodyTask.call(StreamingResponseBodyReturnValueHandler.java:97) ~[spring-webmvc-6.2.0-M7.jar:6.2.0-M7]
at org.springframework.web.context.request.async.WebAsyncManager.lambda$startCallableProcessing$4(WebAsyncManager.java:368) ~[spring-web-6.2.0-M7.jar:6.2.0-M7]
at java.base/java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:572) ~[na:na]
at java.base/java.util.concurrent.FutureTask.run(FutureTask.java:317) ~[na:na]
at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1144) ~[na:na]
at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:642) ~[na:na]
at java.base/java.lang.Thread.run(Thread.java:1583) ~[na:na]
Caused by: org.springframework.web.context.request.async.AsyncRequestNotUsableException: ServletOutputStream failed to write: java.io.IOException: Broken pipe
at org.springframework.web.context.request.async.StandardServletAsyncWebRequest$LifecycleHttpServletResponse.handleIOException(StandardServletAsyncWebRequest.java:323) ~[spring-web-6.2.0-M7.jar:6.2.0-M7]
at org.springframework.web.context.request.async.StandardServletAsyncWebRequest$LifecycleServletOutputStream.write(StandardServletAsyncWebRequest.java:381) ~[spring-web-6.2.0-M7.jar:6.2.0-M7]
at java.base/java.io.OutputStream.write(OutputStream.java:124) ~[na:na]
at com.example.streaming.response.body.error.handling.race.condition.StreamingResponseBodyErrorHandlingRaceConditionTest$TestController.lambda$fails$0(StreamingResponseBodyErrorHandlingRaceConditionTest.java:121) ~[test/:na]
... 8 common frames omitted
Caused by: org.apache.catalina.connector.ClientAbortException: java.io.IOException: Broken pipe
at org.apache.catalina.connector.OutputBuffer.realWriteBytes(OutputBuffer.java:341) ~[tomcat-embed-core-10.1.28.jar:10.1.28]
at org.apache.catalina.connector.OutputBuffer.appendByteArray(OutputBuffer.java:746) ~[tomcat-embed-core-10.1.28.jar:10.1.28]
at org.apache.catalina.connector.OutputBuffer.append(OutputBuffer.java:667) ~[tomcat-embed-core-10.1.28.jar:10.1.28]
at org.apache.catalina.connector.OutputBuffer.writeBytes(OutputBuffer.java:376) ~[tomcat-embed-core-10.1.28.jar:10.1.28]
at org.apache.catalina.connector.OutputBuffer.write(OutputBuffer.java:354) ~[tomcat-embed-core-10.1.28.jar:10.1.28]
at org.apache.catalina.connector.CoyoteOutputStream.write(CoyoteOutputStream.java:103) ~[tomcat-embed-core-10.1.28.jar:10.1.28]
at org.springframework.web.context.request.async.StandardServletAsyncWebRequest$LifecycleServletOutputStream.write(StandardServletAsyncWebRequest.java:378) ~[spring-web-6.2.0-M7.jar:6.2.0-M7]
... 10 common frames omitted
Caused by: java.io.IOException: Broken pipe
at java.base/sun.nio.ch.SocketDispatcher.write0(Native Method) ~[na:na]
at java.base/sun.nio.ch.SocketDispatcher.write(SocketDispatcher.java:62) ~[na:na]
at java.base/sun.nio.ch.IOUtil.writeFromNativeBuffer(IOUtil.java:137) ~[na:na]
at java.base/sun.nio.ch.IOUtil.write(IOUtil.java:102) ~[na:na]
at java.base/sun.nio.ch.IOUtil.write(IOUtil.java:58) ~[na:na]
at java.base/sun.nio.ch.SocketChannelImpl.write(SocketChannelImpl.java:542) ~[na:na]
at org.apache.tomcat.util.net.NioChannel.write(NioChannel.java:122) ~[tomcat-embed-core-10.1.28.jar:10.1.28]
at org.apache.tomcat.util.net.NioEndpoint$NioSocketWrapper.doWrite(NioEndpoint.java:1378) ~[tomcat-embed-core-10.1.28.jar:10.1.28]
at org.apache.tomcat.util.net.SocketWrapperBase.doWrite(SocketWrapperBase.java:764) ~[tomcat-embed-core-10.1.28.jar:10.1.28]
at org.apache.tomcat.util.net.SocketWrapperBase.writeBlocking(SocketWrapperBase.java:589) ~[tomcat-embed-core-10.1.28.jar:10.1.28]
at org.apache.tomcat.util.net.SocketWrapperBase.write(SocketWrapperBase.java:533) ~[tomcat-embed-core-10.1.28.jar:10.1.28]
at org.apache.coyote.http11.Http11OutputBuffer$SocketOutputBuffer.doWrite(Http11OutputBuffer.java:548) ~[tomcat-embed-core-10.1.28.jar:10.1.28]
at org.apache.coyote.http11.filters.ChunkedOutputFilter.doWrite(ChunkedOutputFilter.java:111) ~[tomcat-embed-core-10.1.28.jar:10.1.28]
at org.apache.coyote.http11.Http11OutputBuffer.doWrite(Http11OutputBuffer.java:193) ~[tomcat-embed-core-10.1.28.jar:10.1.28]
at org.apache.coyote.Response.doWrite(Response.java:633) ~[tomcat-embed-core-10.1.28.jar:10.1.28]
at org.apache.catalina.connector.OutputBuffer.realWriteBytes(OutputBuffer.java:329) ~[tomcat-embed-core-10.1.28.jar:10.1.28]
... 16 common frames omitted
```

Not expected: Error handling is called for
```
java.io.IOException: Broken pipe
	at java.base/sun.nio.ch.SocketDispatcher.write0(Native Method) ~[na:na]
	at java.base/sun.nio.ch.SocketDispatcher.write(SocketDispatcher.java:62) ~[na:na]
	at java.base/sun.nio.ch.IOUtil.writeFromNativeBuffer(IOUtil.java:137) ~[na:na]
	at java.base/sun.nio.ch.IOUtil.write(IOUtil.java:102) ~[na:na]
	at java.base/sun.nio.ch.IOUtil.write(IOUtil.java:58) ~[na:na]
	at java.base/sun.nio.ch.SocketChannelImpl.write(SocketChannelImpl.java:542) ~[na:na]
	at org.apache.tomcat.util.net.NioChannel.write(NioChannel.java:122) ~[tomcat-embed-core-10.1.28.jar:10.1.28]
	at org.apache.tomcat.util.net.NioEndpoint$NioSocketWrapper.doWrite(NioEndpoint.java:1378) ~[tomcat-embed-core-10.1.28.jar:10.1.28]
	at org.apache.tomcat.util.net.SocketWrapperBase.doWrite(SocketWrapperBase.java:764) ~[tomcat-embed-core-10.1.28.jar:10.1.28]
	at org.apache.tomcat.util.net.SocketWrapperBase.writeBlocking(SocketWrapperBase.java:589) ~[tomcat-embed-core-10.1.28.jar:10.1.28]
	at org.apache.tomcat.util.net.SocketWrapperBase.write(SocketWrapperBase.java:533) ~[tomcat-embed-core-10.1.28.jar:10.1.28]
	at org.apache.coyote.http11.Http11OutputBuffer$SocketOutputBuffer.doWrite(Http11OutputBuffer.java:548) ~[tomcat-embed-core-10.1.28.jar:10.1.28]
	at org.apache.coyote.http11.filters.ChunkedOutputFilter.doWrite(ChunkedOutputFilter.java:111) ~[tomcat-embed-core-10.1.28.jar:10.1.28]
	at org.apache.coyote.http11.Http11OutputBuffer.doWrite(Http11OutputBuffer.java:193) ~[tomcat-embed-core-10.1.28.jar:10.1.28]
	at org.apache.coyote.Response.doWrite(Response.java:633) ~[tomcat-embed-core-10.1.28.jar:10.1.28]
	at org.apache.catalina.connector.OutputBuffer.realWriteBytes(OutputBuffer.java:329) ~[tomcat-embed-core-10.1.28.jar:10.1.28]
	at org.apache.catalina.connector.OutputBuffer.appendByteArray(OutputBuffer.java:746) ~[tomcat-embed-core-10.1.28.jar:10.1.28]
	at org.apache.catalina.connector.OutputBuffer.append(OutputBuffer.java:667) ~[tomcat-embed-core-10.1.28.jar:10.1.28]
	at org.apache.catalina.connector.OutputBuffer.writeBytes(OutputBuffer.java:376) ~[tomcat-embed-core-10.1.28.jar:10.1.28]
	at org.apache.catalina.connector.OutputBuffer.write(OutputBuffer.java:354) ~[tomcat-embed-core-10.1.28.jar:10.1.28]
	at org.apache.catalina.connector.CoyoteOutputStream.write(CoyoteOutputStream.java:103) ~[tomcat-embed-core-10.1.28.jar:10.1.28]
	at org.springframework.web.context.request.async.StandardServletAsyncWebRequest$LifecycleServletOutputStream.write(StandardServletAsyncWebRequest.java:378) ~[spring-web-6.2.0-M7.jar:6.2.0-M7]
	at java.base/java.io.OutputStream.write(OutputStream.java:124) ~[na:na]
	at com.example.streaming.response.body.error.handling.race.condition.StreamingResponseBodyErrorHandlingRaceConditionTest$TestController.lambda$fails$0(StreamingResponseBodyErrorHandlingRaceConditionTest.java:121) ~[test/:na]
	at org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBodyReturnValueHandler$StreamingResponseBodyTask.call(StreamingResponseBodyReturnValueHandler.java:110) ~[spring-webmvc-6.2.0-M7.jar:6.2.0-M7]
	at org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBodyReturnValueHandler$StreamingResponseBodyTask.call(StreamingResponseBodyReturnValueHandler.java:97) ~[spring-webmvc-6.2.0-M7.jar:6.2.0-M7]
	at org.springframework.web.context.request.async.WebAsyncManager.lambda$startCallableProcessing$4(WebAsyncManager.java:368) ~[spring-web-6.2.0-M7.jar:6.2.0-M7]
	at java.base/java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:572) ~[na:na]
	at java.base/java.util.concurrent.FutureTask.run(FutureTask.java:317) ~[na:na]
	at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1144) ~[na:na]
	at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:642) ~[na:na]
	at java.base/java.lang.Thread.run(Thread.java:1583) ~[na:na]
```
