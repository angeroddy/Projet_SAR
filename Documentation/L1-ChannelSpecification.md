
# Overview: Broker / Channel

Brokers are named intermediaries that enable tasks to establish channels. Each broker has a unique name. Each channel is a communication channel between two end points, that is, a point-to-point stream of bytes. A channel is full-duplex, that is, each end point can be used to read or write bytes. A channel, obtained from an operation 'accept' or 'connect', is connected. A connected channel is FIFO and lossless. A connected channel can be disconnected at any time, by either sides.

The typical use of a channel is by two tasks to establish a full-duplex communication. However, there is no ownership between channel and tasks, any task may read or write in any channel it has a reference to. The following rules apply though:

- It is thread-safe for two tasks communicating through a channel to read or write concurrently, each at their respective end point of the channel. 
- Locally, at one end point, two tasks, one reading and the other writing, operating concurrently is safe also. Important: 
  - a blocked read operation does not prevent write operations to complete  
  - a blocked write operation does not prevent read operations to complete
- Concurrent read operations or concurrent write operations, on the same end point, by different tasks, are not safe since they do not have a clear semantic given the byte-stream nature of channels.  

# Connecting

A channel is established, in a fully connected state, when a request to 'connect' matches a request to 'accept'. When connecting, the given name is the one of the remote broker, the given port is the one of an accept on that remote broker. 

There is no precedence between connect and accept, this is a symmetrical rendez-vous: the first operation waits for the second one. Both operations are therefore blocking calls, blocking until the rendez-vous happens, both returning a fully connected full-duplex channel.

When connecting, we may want to distinguish between two cases: 
- there is no pending accept that corresponds yet 
- there is no broker with the given name 

When the named broker does not exist, the connect returns null. But if the remote broker is found, the connect blocks until there is a matching accept so that a connected channel can be constructed and returned. 

Note: we could consider introducing a timeout here, limiting the wait for the rendez-vous to happen.

# Writing

Signature: 
```java
  write(byte[] bytes,int offset,int length)int
```

When writing, the given byte array contains the bytes to write from the given offset and for the given length. The range [offset,offset+length[ must be within the array boundaries, without wrapping around at either ends. If it is not the case, an illegal-argument exception is thrown.

The method "write" returns the number of bytes actually written, that number may not be zero or negative. If zero would be return, the write operation blocks instead until it can make some progress.

**Nota Bene:** a channel is a stream, so although the write operation does take a range of bytes to write from an array of bytes, the semantics is one that writes one byte at a time in the stream.

The method "write" blocks if there is no room to write any byte, but does not have to write all the given bytes. The rationale is to avoid spinning when an application tries to send a certain number of bytes and the stream can make no progress. Here is an example:

  void send(byte[] bytes) {
    int remaining = bytes.length;
    int offset = 0;
    while (remaining!=0 && !channel.disconnected()) {
      int n = channel.write(bytes,offset,remaining);
      offset += n;
      remaining -= n;
    }
  }

Invoking a write operation on a disconnected channel throws an illegal-state exception. By disconnected, we mean that invoking the method "disconnected" would return 'true'.

A write operation that is blocked will unblock if the channel becomes disconnected, returning the number of written bytes; it is the only case the method 'write' may return zero.

# Reading

Signature: 

```java 
  read(byte[] bytes,int offset,int length)int
```

When reading, the given byte array will be used to store the bytes read from the channel, starting at the given offset. The given length provides the maximum number of bytes to read. The range [offset,offset+length[ must be within the array boundaries, without wrapping around at either ends. If this is not the case, an illegal-argument exception is thrown.

The method "read" will return the number of bytes actually read, that number may not be zero or negative. If zero would be returned, the method "read" blocks instead, until some bytes become available. The rationale is that a loop trying to read a given length, looping over until all the needed bytes are read will not induce an active polling. Here is an example:

  void receive(byte[] bytes)  {
    int remaining = bytes.length;
    int offset = 0;
    while (remaining!=0 && !channel.disconnected()) {
      int n = channel.read(bytes,offset,remaining);
      offset += n;
      remaining -= n;
    }
  }

Invoking a read operation on a disconnected channel throws an illegal-state exception. By disconnected, we mean that invoking the method "disconnected" would return 'true'.

A read operation that is blocked will unblock if the channel becomes disconnected, returning the number of available bytes in the given byte array; it is the only case the method 'read' may return zero.

# Disconnecting

A channel can be disconnected at any time, from either side. So this requires an asynchronous protocol to disconnect a channel. The effect of disconnecting a channel must be specified for both ends, the one that called the method "disconnect" as well as the other end. **Nota Bene:** both ends may call the method "disconnect" concurrently and the protocol to disconnect the channel **must** still work.

In the following, we will talk about the local side versus remote side, the local side being the end point where the method "disconnect" has been called.

It is important to state that since we have not asserted a strict ownership model between tasks and channels, it is possible that a channel be disconnected
by one task while some operations, called on other tasks, are blocked locally. These blocked operations must be interrupted, when appropriate.

The local rule is simple, once the method "disconnect" has been called on a channel, it is illegal to invoke the methods "read" or "write". If this happens, an illegal-state exception will be thrown. It is always legal to invoke the method "disconnected" to check the status of the channel. It is also legal to invoke the method "disconnect" multiple times, only the first invocation does something, others are simply ignored. 

The remote case is more complex to grasp, that is, when a channel is disconnected at one end, how should that disconnection be perceived at the other end? Remember: the local side is the end point when the method 'disconnect' was invoked, the remote side is the other end point. 

The main issue when disconnecting a channel is that there may be bytes still in transit while a channel is half-connected. Bytes in transit were written by the local side before it disconnected the channel. The channel must guarantee that these bytes can be read by the remote side before the channel appears disconnected to that remote side. During that half-connected window of time, write operations will succeed on the remote side, dropping the given bytes but indicating that they were written.

This behavior may seem counter-intuitive at first, but it is the only one that is consistent and it is in fact the easiest one on developers using channels. First, allowing to read the last bytes in transit is mandatory since it is likely that a communication will end by writing some bytes and then disconnecting.

Second, dropping written bytes may seem wrong but it is just leveraging an unavoidable truth: the last bytes written before a channel is disconnected may never reach the other side. It may be because of a network failure or a process failure. It may be also because the remote side disconnected the channel before reading all bytes.

# Brokers and Multi-tasking

Let's discuss now the relationship between tasks, brokers, and channels. Since both the operations 'connect' and 'accept' are blocking operations, we can state the following:

- A task cannot connect to the same name and port concurrently, but multiple tasks can. 
- Only one task may accept on a given port on a given broker. 
- Different tasks on different brokers may accept on the same port number. 
- Multiple tasks may accept on different ports on the same broker.

We know that each task is related to a broker, by its constructor. But a broker may be aliased across tasks, running in the same Java Runtime Environment (JRE), meaning that a broker can be used by multiple tasks. In other words, brokers may be shared between tasks, so brokers must be thread-safe.

Channels only need to be partially thread-safe:

- Multiple concurrent invocations of the method 'write' on one channel by multiple tasks is not supported.
- Multiple concurrent invocations of the method 'read' on one channel by multiple tasks is not supported.
- Two tasks may invoke concurrently the method 'read' and 'write' on the same channel.

Therefore, tasks that cooperate on one end point of a channel, invoking the methods 'read' and 'write' concurrently, must synchronize themselves according to the above rules. 

Classical cooperation patterns are the following:

- one task owns one end point of a channel, doing both the read and write operations.
- two tasks share one end point of a channel, one task reading and the other writing.
- one task does the reading and other tasks are synchronized to do writes.







