/*
 * Copyright (c) 2018 by Andrew Charneski.
 *
 * The author licenses this file to you under the
 * Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy
 * of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.simiacryptus.aws;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import com.esotericsoftware.kryonet.Client;
import com.esotericsoftware.kryonet.Connection;
import com.esotericsoftware.kryonet.Listener;
import com.esotericsoftware.kryonet.Server;
import com.esotericsoftware.kryonet.rmi.ObjectSpace;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.CloseShieldOutputStream;
import org.objenesis.strategy.StdInstantiatorStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static com.simiacryptus.aws.EC2Util.*;

public class Tendril {
  
  private static final Logger logger = LoggerFactory.getLogger(EC2Util.class);
  
  public static void main(String... args) {
    try {
      Server server = new Server();
      configure(server.getKryo());
      ObjectSpace.registerClasses(server.getKryo());
      ObjectSpace objectSpace = new ObjectSpace();
      objectSpace.register(1318, new LocalTendrilNode());
      server.addListener(new Listener() {
        @Override
        public void connected(final Connection connection) {
          objectSpace.addConnection(connection);
          super.connected(connection);
        }
        
        @Override
        public void disconnected(final Connection connection) {
          objectSpace.removeConnection(connection);
          super.disconnected(connection);
        }
      });
      server.start();
      server.bind(new InetSocketAddress("127.0.0.1", 1318), null);
    } catch (Throwable e) {
      e.printStackTrace();
    }
  }
  
  @Nullable
  public static TendrilNode startRemoteJvm(final EC2Node node, final String javaOpts, final String programArguments, final String libPrefix, final String keyspace, final String bucket, final int localControlPort, final Predicate<String> classpathFilter) {
    String localClasspath = System.getProperty("java.class.path");
    logger.info("Java Local Classpath: " + localClasspath);
    String remoteClasspath = stageRemoteClasspath(node, libPrefix, keyspace, bucket, classpathFilter, localClasspath, true);
    logger.info("Java Remote Classpath: " + remoteClasspath);
    String commandLine = String.format("java %s -cp %s %s %s", javaOpts, remoteClasspath, Tendril.class.getCanonicalName(), programArguments);
    logger.info("Java Command Line: " + commandLine);
    EC2Util.Process process = execAsync(node.getConnection(), commandLine, new CloseShieldOutputStream(System.out));
    return getControl(localControlPort);
  }
  
  public static TendrilNode getControl(final int localControlPort) {return getControl(localControlPort, 3);}
  
  public static TendrilNode getControl(final int localControlPort, final int retries) {
    try {
      Client client = new Client();
      configure(client.getKryo());
      client.start();
      client.connect(30000, "127.0.0.1", localControlPort, -1);
      return ObjectSpace.getRemoteObject(client, 1318, TendrilNode.class);
    } catch (Throwable e) {
      if (retries > 0) {
        sleep(5000);
        return getControl(localControlPort, retries - 1);
      }
      throw new RuntimeException(e);
    }
  }
  
  public static void configure(final Kryo kryo) {
    kryo.register(TendrilNode.class);
    kryo.register(LocalTendrilNode.class);
    kryo.setRegistrationRequired(false);
    kryo.setInstantiatorStrategy(new StdInstantiatorStrategy());
    kryo.setDefaultSerializer(JavaSerializer.class);
    kryo.setAutoReset(true);
  }
  
  @Nonnull
  public static String stageRemoteClasspath(final EC2Node node, final String libPrefix, final String keyspace, final String bucket, final Predicate<String> classpathFilter, final String localClasspath, final boolean parallel) {
    logger.info(String.format("Mkdir %s: %s", libPrefix, node.exec("mkdir -p " + libPrefix)));
    Stream<String> stream = Arrays.stream(localClasspath.split(File.pathSeparator)).filter(classpathFilter);
    if (parallel) stream = stream.parallel();
    return stream.map(entryPath -> {
      return stageClasspathEntry(node, libPrefix, keyspace, bucket, entryPath);
    }).reduce((a, b) -> a + ":" + b).get();
  }
  
  @Nonnull
  public static String stageClasspathEntry(final EC2Node node, final String libPrefix, final String keyspace, final String bucket, final String entryPath) {
    logger.info(String.format("Processing %s", entryPath));
    final File entryFile = new File(entryPath);
    try {
      if (entryFile.isFile()) {
        String remote = libPrefix + hash(entryFile) + ".jar";
        logger.info(String.format("Uploading %s to %s", entryPath, remote));
        try {
          stage(node, keyspace, bucket, entryFile, remote);
        } catch (Throwable e) {
          logger.warn(String.format("Error staging %s to %s", entryFile, remote), e);
        }
        return remote;
      }
      else {
        File tempJar = toJar(entryFile);
        String remote = libPrefix + hash(tempJar) + ".jar";
        logger.info(String.format("Uploading %s to %s", tempJar, remote));
        try {
          stage(node, keyspace, bucket, tempJar, remote);
        } catch (Throwable e) {
          logger.warn(String.format("Error staging %s to %s", entryFile, remote), e);
        }
        return remote;
      }
    } catch (Throwable e) {
      throw new RuntimeException(e);
    }
  }
  
  public static boolean shouldTransfer(final String file) {
    if (file.contains("jre")) return false;
    return !file.contains("jdk");
  }
  
  public static void stage(final EC2Node node, final String keyspace, final String bucket, final File entryFile, final String remote) {stage(node, keyspace, bucket, entryFile, remote, 3);}
  
  public static void stage(final EC2Node node, final String keyspace, final String bucket, final File entryFile, final String remote, final int retries) {
    try {
      if (null == bucket || bucket.isEmpty()) {
        node.scp(entryFile, remote);
      }
      else {
        node.stage(entryFile, remote, bucket, keyspace);
      }
    } catch (Throwable e) {
      if (retries > 0) {
        logger.debug("Retrying " + remote, e);
        sleep(10000);
        stage(node, keyspace, bucket, entryFile, remote, retries - 1);
      }
      else {
        throw new RuntimeException(e);
      }
    }
  }
  
  @Nonnull
  public static File toJar(final File entry) throws IOException {
    File tempJar = new File(UUID.randomUUID() + ".jar");
    logger.info(String.format("Archiving %s to %s", entry, tempJar));
    try (ZipOutputStream zip = new ZipOutputStream(new FileOutputStream(tempJar))) {
      for (final File file : Arrays.stream(entry.listFiles()).sorted().collect(Collectors.toList())) {
        write(zip, "", file);
      }
    }
    tempJar.deleteOnExit();
    return tempJar;
  }
  
  private static void write(final ZipOutputStream zip, final String base, final File entry) throws IOException {
    if (entry.isFile()) {
      zip.putNextEntry(new ZipEntry(base + entry.getName()));
      IOUtils.copy(new FileInputStream(entry), zip);
    }
    else {
      for (final File file : Arrays.stream(entry.listFiles()).sorted().collect(Collectors.toList())) {
        write(zip, base + entry.getName() + "/", file);
      }
    }
    
  }
  
  public static String hash(final File classpath) throws NoSuchAlgorithmException, IOException {
    MessageDigest digest = MessageDigest.getInstance("SHA-1");
    InputStream fis = new FileInputStream(classpath);
    int n = 0;
    byte[] buffer = new byte[8192];
    while (n != -1) {
      n = fis.read(buffer);
      if (n > 0) {
        digest.update(buffer, 0, n);
      }
    }
    return new String(Hex.encodeHex(digest.digest()));
  }
  
  public interface SerializableCallable<T> extends Callable<T>, Serializable {
  }
  
  public interface TendrilNode {
    boolean isAlive();
    
    long time();
  
    <T> T run(SerializableCallable<T> task) throws Exception;
  }
  
  public static class LocalTendrilNode implements TendrilNode {
    @Override
    public boolean isAlive() {
      return true;
    }
    
    @Override
    public long time() {
      return System.currentTimeMillis();
    }
    
    @Override
    public <T> T run(final SerializableCallable<T> task) throws Exception {
      return task.call();
    }
  }
}
