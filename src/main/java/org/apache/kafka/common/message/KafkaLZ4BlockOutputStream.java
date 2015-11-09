/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.common.message;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.kafka.common.utils.Utils;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.xxhash.XXHash32;
import net.jpountz.xxhash.XXHashFactory;

/**
 * A partial implementation of the v1.4.1 LZ4 Frame format.
 * 
 * @see <a href="https://docs.google.com/document/d/1Tdxmn5_2e5p1y4PtXkatLndWVb0R8QARJFe6JI4Keuo/edit">LZ4 Framing Format Spec</a>
   * 写入头文件
   * 4个字节存储magic
   * 1个字节存储FLG
   * 1个字节存储BD
   * 1个字节存储以上校验和
   * 
 */
public final class KafkaLZ4BlockOutputStream extends FilterOutputStream {

  public static final int MAGIC = 0x184D2204;
  public static final int LZ4_MAX_HEADER_LENGTH = 19;
  public static final int LZ4_FRAME_INCOMPRESSIBLE_MASK = 0x80000000;//表示使用非压缩的数据,因为压缩后比压缩前数据流还大,则没必要压缩
  
  public static final String CLOSED_STREAM = "The stream is already closed";
  
  public static final int BLOCKSIZE_64KB = 4;
  public static final int BLOCKSIZE_256KB = 5;
  public static final int BLOCKSIZE_1MB = 6;
  public static final int BLOCKSIZE_4MB = 7;
  
  private final LZ4Compressor compressor;//压缩计算器
  private final XXHash32 checksum;//校验和计算器
  private final FLG flg;//校验和属性信息
  private final BD bd;//数据块属性信息
  private final byte[] buffer;//原始文件缓冲区,可以缓冲一个数据块
  private final byte[] compressedBuffer;//压缩文件缓冲区,可以缓冲一个数据块
  private final int maxBlockSize;//每个数据块最多存储字节数
  private int bufferOffset;//当前在buffer原始文件缓冲区的位置
  private boolean finished;//true表示全部压缩完成

  /**
   * Create a new {@link OutputStream} that will compress data using the LZ4 algorithm.
   * 创建一个使用LZ4算法去压缩数据的输出流
   * @param out The output stream to compress
   * @param blockSize Default: 4. The block size used during compression. 4=64kb, 5=256kb, 6=1mb, 7=4mb. All other values will generate an exception 多少字节作为一个数据块
   * @param blockChecksum Default: false. When true, a XXHash32 checksum is computed and appended to the stream for every block of data,true表示要进行校验和处理,并且每一个校验和要写入每一个数据块的最后
   * @throws IOException
   */
  public KafkaLZ4BlockOutputStream(OutputStream out, int blockSize, boolean blockChecksum) throws IOException {
    super(out);
    compressor = LZ4Factory.fastestInstance().fastCompressor();
    checksum = XXHashFactory.fastestInstance().hash32();
    bd = new BD(blockSize);
    flg = new FLG(blockChecksum);
    bufferOffset = 0;
    maxBlockSize = bd.getBlockMaximumSize();
    buffer = new byte[maxBlockSize];
    compressedBuffer = new byte[compressor.maxCompressedLength(maxBlockSize)];
    finished = false;
    writeHeader();
  }
  
  /**
   * Create a new {@link OutputStream} that will compress data using the LZ4 algorithm.
   *  
   * @param out The stream to compress
   * @param blockSize Default: 4. The block size used during compression. 4=64kb, 5=256kb, 6=1mb, 7=4mb. All other values will generate an exception
   * @throws IOException
   */
  public KafkaLZ4BlockOutputStream(OutputStream out, int blockSize) throws IOException {
    this(out, blockSize, false);
  }
  
  /**
   * Create a new {@link OutputStream} that will compress data using the LZ4 algorithm.
   * 
   * @param out The output stream to compress
   * @throws IOException
   */
  public KafkaLZ4BlockOutputStream(OutputStream out) throws IOException {
    this(out, BLOCKSIZE_64KB);
  }

  /**
   * Writes the magic number and frame descriptor to the underlying {@link OutputStream}.
   *  
   * @throws IOException
   * 写入头文件
   * 4个字节存储magic
   * 1个字节存储FLG
   * 1个字节存储BD
   * 1个字节存储以上校验和
   */
  private void writeHeader() throws IOException {
    Utils.writeUnsignedIntLE(buffer, 0, MAGIC);//写入4个字节的magic信息
    bufferOffset = 4;//移动缓冲区位置
    buffer[bufferOffset++] = flg.toByte();//添加校验和属性
    buffer[bufferOffset++] = bd.toByte();//添加数据块属性
    // TODO write uncompressed content size, update flg.validate()
    // TODO write dictionary id, update flg.validate()
    // compute checksum on all descriptor fields 
    int hash = (checksum.hash(buffer, 0, bufferOffset, 0) >> 8) & 0xFF;
    buffer[bufferOffset++] = (byte) hash;//对以上属性进行校验和校验存储
    // write out frame descriptor
    out.write(buffer, 0, bufferOffset);//将缓冲区的数据写入输出流中
    bufferOffset = 0;//重置缓冲区位置为0
  }
  
  /**
   * Compresses buffered data, optionally computes an XXHash32 checksum, and writes
   * the result to the underlying {@link OutputStream}.
   * 压缩缓冲区数据,计算校验和,将结果写入输出流
   * @throws IOException
   * 该方法表示要将缓冲区内的数据写入到输出流中,因为表示该缓冲区已经写完一个数据块了
   */
  private void writeBlock() throws IOException {
    if (bufferOffset == 0) {//缓冲区位置为0,说明没有数据要写入
      return;
    }
    
    //将buffer内的数据压缩到compressedBuffer缓冲区内,返回压缩后的字节长度
    int compressedLength = compressor.compress(buffer, 0, bufferOffset, compressedBuffer, 0);
    byte[] bufferToWrite = compressedBuffer;//bufferToWrite表示最终要写入输出流的字节数组,默认是压缩后的数据缓冲区
    int compressMethod = 0;//0表示使用压缩的数据,LZ4_FRAME_INCOMPRESSIBLE_MASK表示使用原始压缩前数据
    
    // Store block uncompressed if compressed length is greater (incompressible) 数据压缩后的数据比压缩前数据还大,则不进行压缩,没有压缩的必要了
    if (compressedLength >= bufferOffset) {
      bufferToWrite = buffer;//切换为buffer缓冲区
      compressedLength = bufferOffset;//长度
      compressMethod = LZ4_FRAME_INCOMPRESSIBLE_MASK;//切换输出内容是原始数据
    }

    // Write content
    Utils.writeUnsignedIntLE(out, compressedLength | compressMethod);//将存储字节长度以及数据源方式写入输出流
    out.write(bufferToWrite, 0, compressedLength);//将内容写入输出流
    
    // Calculate and write block checksum
    if (flg.isBlockChecksumSet()) {//如果需要校验和,则计算该数据块的校验和,写入输出流中
      int hash = checksum.hash(bufferToWrite, 0, compressedLength, 0);
      Utils.writeUnsignedIntLE(out, hash);
    }
    bufferOffset = 0;
  }
  
  /**
   * Similar to the {@link #writeBlock()} method.  Writes a 0-length block 
   * (without block checksum) to signal the end of the block stream.
   * 
   * @throws IOException
   * 写入0到输出流中,表示已经完成输出,设置finished为true
   */
  private void writeEndMark() throws IOException {
    Utils.writeUnsignedIntLE(out, 0);
    // TODO implement content checksum, update flg.validate()
    finished = true;
  }

  //向原始文件输出流中写入一个字节
  @Override
  public void write(int b) throws IOException {
    ensureNotFinished();
    if (bufferOffset == maxBlockSize) {
      writeBlock();
    }
    buffer[bufferOffset++] = (byte) b;
  }

  //向原始文件输出流中写入字节数组
  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    net.jpountz.util.Utils.checkRange(b, off, len);
    ensureNotFinished();
     
    int bufferRemainingLength = maxBlockSize - bufferOffset;//该缓冲区内还可以写入多少个字节
    // while b will fill the buffer
    while (len > bufferRemainingLength) {//不断的写入数据到缓冲区内,缓冲区满了,会触发压手和写入输出流流程
      // fill remaining space in buffer 
      System.arraycopy(b, off, buffer, bufferOffset, bufferRemainingLength);
      bufferOffset = maxBlockSize;
      writeBlock();
      // compute new offset and length
      off += bufferRemainingLength;
      len -= bufferRemainingLength;
      bufferRemainingLength = maxBlockSize;
    }
    
    System.arraycopy(b, off, buffer, bufferOffset, len);
    bufferOffset += len;
  }

  //每次一个数据块写入完成,则进行一次flush操作
  @Override
  public void flush() throws IOException {
    if (!finished) {
        writeBlock();
    }
    if (out != null) {
      out.flush();
    }
  }

  /**
   * A simple state check to ensure the stream is still open.
   * 确保没有完成
   */
  private void ensureNotFinished() {
    if (finished) {
      throw new IllegalStateException(CLOSED_STREAM);
    }
  }

  @Override
  public void close() throws IOException {
    if (!finished) {
      writeEndMark();
      flush();
      finished = true;
    }
    if (out != null) {
      out.close();
      out = null;
    }
  }

  /**
   * 是否需要校验和信息对象
   */
  public static class FLG {

	  //该对象的属性其实都是boolean类型的,只是为了方便写成了int类型,很多默认都是0,即不支持
    private static final int VERSION = 1;
    
    private final int presetDictionary;
    private final int reserved1;
    private final int contentChecksum;
    private final int contentSize;
    private final int blockChecksum;//1表示需要校验和,0表示不需要校验和
    private final int blockIndependence;
    private final int version;
    
    public FLG() {
      this(false);
    }
    
    public FLG(boolean blockChecksum) {
      this(0, 0, 0, 0, blockChecksum ? 1 : 0, 1, VERSION);
    }
    
    private FLG(int presetDictionary, int reserved1, int contentChecksum, 
        int contentSize, int blockChecksum, int blockIndependence, int version) {
      this.presetDictionary = presetDictionary;
      this.reserved1 = reserved1;
      this.contentChecksum = contentChecksum;
      this.contentSize = contentSize;
      this.blockChecksum = blockChecksum;
      this.blockIndependence = blockIndependence;
      this.version = version;
      validate();
    }
    
    //使用byte转换成FLG对象
    public static FLG fromByte(byte flg) {
      int presetDictionary =  (flg >>> 0) & 1;
      int reserved1 =         (flg >>> 1) & 1;
      int contentChecksum =   (flg >>> 2) & 1;
      int contentSize =       (flg >>> 3) & 1;
      int blockChecksum =     (flg >>> 4) & 1;
      int blockIndependence = (flg >>> 5) & 1;
      int version =           (flg >>> 6) & 3;
      
      return new FLG(presetDictionary, reserved1, contentChecksum, 
          contentSize, blockChecksum, blockIndependence, version);
    }
    
    //为方便存储,将FLG对象转换成一个byte对象
    public byte toByte() {
      return (byte) (
            ((presetDictionary   & 1) << 0)
          | ((reserved1          & 1) << 1)
          | ((contentChecksum    & 1) << 2)
          | ((contentSize        & 1) << 3)
          | ((blockChecksum      & 1) << 4)
          | ((blockIndependence  & 1) << 5)
          | ((version            & 3) << 6) );
    }
    
    //校验,默认都是0,表示不支持
    private void validate() {
      if (presetDictionary != 0) {
        throw new RuntimeException("Preset dictionary is unsupported");
      }
      if (reserved1 != 0) {
        throw new RuntimeException("Reserved1 field must be 0");
      }
      if (contentChecksum != 0) {
        throw new RuntimeException("Content checksum is unsupported");
      }
      if (contentSize != 0) {
        throw new RuntimeException("Content size is unsupported");
      }
      if (blockIndependence != 1) {
        throw new RuntimeException("Dependent block stream is unsupported");
      }
      if (version != VERSION) {
        throw new RuntimeException(String.format("Version %d is unsupported", version));
      }
    }
    
    //true,表示支持
    public boolean isPresetDictionarySet() {
      return presetDictionary == 1;
    }

    //true,表示支持
    public boolean isContentChecksumSet() {
      return contentChecksum == 1;
    }
    
    //true,表示支持
    public boolean isContentSizeSet() {
      return contentSize == 1;
    }
    
    //true,表示支持,需要校验和
    public boolean isBlockChecksumSet() {
      return blockChecksum == 1;
    }
    
    //true,表示支持
    public boolean isBlockIndependenceSet() {
      return blockIndependence == 1;
    }
    
    public int getVersion() {
      return version;
    }
  }
  
  /**
   * 表示一个输出的数据块大小
   */
  public static class BD {
    
    private final int reserved2;//预保留字段,该值目前就是为0
    private final int blockSizeValue;//数据块大小,该值被定义为4 5 6 7
    private final int reserved3;//预保留字段,该值目前就是为0
    
    public BD() {
      this(0, BLOCKSIZE_64KB, 0);
    }
    
    public BD(int blockSizeValue) {
      this(0, blockSizeValue, 0);
    }
    
    private BD(int reserved2, int blockSizeValue, int reserved3) {
      this.reserved2 = reserved2;
      this.blockSizeValue = blockSizeValue;
      this.reserved3 = reserved3;
      validate();
    }
    
    //将一个byte转换成BD对象
    public static BD fromByte(byte bd) {
      int reserved2 =        (bd >>> 0) & 15;
      int blockMaximumSize = (bd >>> 4) & 7;
      int reserved3 =        (bd >>> 7) & 1;
      
      return new BD(reserved2, blockMaximumSize, reserved3);
    }
    
    //校验
    private void validate() {
      if (reserved2 != 0) {
        throw new RuntimeException("Reserved2 field must be 0");
      }
      if (blockSizeValue < 4 || blockSizeValue > 7) {
        throw new RuntimeException("Block size value must be between 4 and 7");
      }
      if (reserved3 != 0) {
        throw new RuntimeException("Reserved3 field must be 0");
      }
    }
    
    // 2^(2n+8) 该数据块占用大小,1 << ((2 * blockSizeValue) 表示数据块占用大小,8表示两个预先保留位中两个int占用字节数
    public int getBlockMaximumSize() {
      return (1 << ((2 * blockSizeValue) + 8));
    }
    
    //将该BD对象转换成一个byte,方便存储
    public byte toByte() {
      return (byte) (
            ((reserved2       & 15) << 0)
          | ((blockSizeValue  & 7) << 4)
          | ((reserved3       & 1) << 7) );
    }
  }
  
}
