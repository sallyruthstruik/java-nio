/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package aggregator;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;
/**
 *
 * @author Станислав
 */
abstract class Opener implements Iterable<String>, Closeable{
}

class SimpleOpener extends Opener{
    
    BufferedReader reader;
    String name;
    
    class SimpleOpenerIterator implements Iterator<String>{
        String line;
        String next_line;
        
        SimpleOpenerIterator(){
            try{
                reader.mark(0);
                next_line = reader.readLine();
            }catch(IOException e){
                System.err.println(e);
                System.exit(1);
            }    
        }
        
        @Override
        public boolean hasNext() {
            return (next_line != null);
        }

        @Override
        public String next(){
            try{
                line = next_line;
                next_line = reader.readLine();
            }catch(IOException e){
                System.err.println(e);
                System.exit(1);
            }
            
            return line;
        }
        
    }
    
    @Override
    public Iterator<String> iterator(){
        return new SimpleOpenerIterator();
    }
    
    public SimpleOpener(String name){
                
        this.name = name;
        try{
            reader = new BufferedReader(
                    new FileReader(
                            new File(name)
                    )
            );
        }catch(FileNotFoundException e){
            System.err.println("No file "+name+" found");
            System.exit(1);
            
        }
    }
    
    public BufferedReader getReader(){
        return reader;
    }
    
    public void close(){
        try{
            reader.close();
        }catch(IOException e){
            System.err.println("Can't close file "+name+" getted exception "+e);
            System.exit(1);
        }
    }
}

class MemoryMappedOpener extends Opener{
    
    MappedByteBuffer buf;
    
    long SIZE;
    
    public MemoryMappedOpener(String name){
        try{
            File file = new File(name);
            SIZE = file.length();
            buf = new RandomAccessFile(file, "r")
                    .getChannel()
                    .map(FileChannel.MapMode.READ_ONLY, 0, SIZE);
        }catch(FileNotFoundException e){
            System.err.println("No file "+name+" found");
            System.exit(1);
        }catch(IOException e){
            System.err.println("Can't map file: "+e);
        }
    }
    
    class MemoryMappedOpenerIterator implements Iterator<String>{
        
        String line, nextline;
        int cur;
        
        public MemoryMappedOpenerIterator() {
            nextline = readOneLine();
        }
        
        final String readOneLine(){
            char symbol;
            StringBuilder lineBuilder = new StringBuilder();
            while(buf.hasRemaining()){
                symbol = (char)buf.get();
                
                if(symbol=='\n' || symbol == '\r'){
                    if(lineBuilder.length() > 0)
                        break;
                    else
                        continue;
                }
                lineBuilder.append(symbol);
            }
            if(lineBuilder.length() > 0)
                return lineBuilder.toString();
            else
                return null;
        }
        
        @Override
        public boolean hasNext() {
            return nextline != null;
        }

        @Override
        public String next() {
            line = nextline;
            nextline = readOneLine();
            return line;
        }
        
    }
    
    @Override
    public Iterator<String> iterator() {
        return new MemoryMappedOpenerIterator();
    }

    @Override
    public void close(){
    }
    
    
}