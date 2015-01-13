/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package nioechoserevr;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;


class Server implements Closeable{
    
    ServerSocketChannel serverSocketChannel;
    ByteBuffer tempBuffer = ByteBuffer.allocate(10);
    
    Set<SocketChannel> allChannels = new HashSet<>();
    
    Selector selector;
    
    private final int port = 1234;
    private final int sleepTime = 2000;  //ms
    public Server(){
        try{
            selector = Selector.open();
            serverSocketChannel = ServerSocketChannel.open();
            serverSocketChannel.bind(new InetSocketAddress(port));
            serverSocketChannel.configureBlocking(false);
            serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
            
        }catch(IOException e){
            System.err.println("Can't create server socket channel: " + e);
            System.exit(1);
        }
        System.out.println("Server initialized");
    }

    @Override
    public void close() throws IOException {
        serverSocketChannel.close();
    }
    
    public void loop() throws IOException, InterruptedException{
        System.out.println("Begin loop");
        while(true){
            int number = selector.select();
            
            if(number == 0){
                Thread.sleep(2000);
                System.out.println("No new connections, sleeps");
                continue;
            }
            
            Set<SelectionKey> keys = selector.selectedKeys();
//            System.out.println(keys.size());

            
            for(SelectionKey key: keys){        
                if(key.isAcceptable()){
                    System.out.println("Socket is acceptable, get socket and register on read");
                    SocketChannel socketChannel = serverSocketChannel.accept();
                    socketChannel.configureBlocking(false);
                    
                    socketChannel.register(selector, SelectionKey.OP_READ);
                    allChannels.add(socketChannel);
                }
                else if(key.isReadable()){
                    System.out.println("Socket is readable: read it: "+key);
                    SocketChannel channel = (SocketChannel) key.channel();
                    
                    ByteBuffer buf = ByteBuffer.allocate(10);
                    
                    if(channel.read(buf) == -1){
                        allChannels.remove(channel);
                        key.cancel();
                        continue;
                    };
                    
                    //если нет других каналов - делаем простое эхо себе
                    if(allChannels.size() == 1){
                        buf.flip();
                        channel.write(buf);
                    }else{
                        for(SocketChannel anotherChannel: allChannels){

                            if(!anotherChannel.equals(channel)){
                                System.out.println("Write to another channel: "+anotherChannel);
                                buf.flip();
                                anotherChannel.write(buf);
                            }
                        }
                    }
                    
                }
            }
            keys.clear();
        }
    }
    
}


/**
 *
 * @author stas
 */
public class NioEchoSerevr {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws InterruptedException{
        // TODO code application logic here
        
        try(Server server = new Server()){
            server.loop();
        }catch(IOException e){
            System.err.println("Unexpected exception: " + e);
            System.exit(4);
        }
        
    }   
    
}
