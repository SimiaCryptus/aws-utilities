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

import com.amazonaws.services.s3.AmazonS3;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.serializers.ClosureSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import com.esotericsoftware.kryonet.Client;
import com.esotericsoftware.kryonet.Connection;
import com.esotericsoftware.kryonet.Listener;
import com.esotericsoftware.kryonet.Server;
import com.esotericsoftware.kryonet.rmi.ObjectSpace;
import com.simiacryptus.util.test.SysOutInterceptor;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.CloseShieldOutputStream;
import org.jetbrains.annotations.NotNull;
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
import java.io.PrintStream;
import java.io.Serializable;
import java.lang.invoke.SerializedLambda;
import java.net.InetSocketAddress;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
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
      if (Boolean.parseBoolean(System.getProperty("SHUTDOWN_ON_EXIT", "true")))
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
          String command = "sudo shutdown -h 0";
          System.err.println("Terminating system via command: " + command);
          try {
            int i = Runtime.getRuntime().exec(command).waitFor();
            System.err.printf("Result %s for %s%n", i, command);
          } catch (IOException e) {
            e.printStackTrace();
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }));
      Server server = new Server();
      configure(server.getKryo());
      ObjectSpace.registerClasses(server.getKryo());
      ObjectSpace objectSpace = new ObjectSpace();
      objectSpace.register(1318, new TendrilLinkImpl());
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
  public static TendrilControl startRemoteJvm(final EC2Node node, final int localControlPort, final String javaOpts, final String programArguments, final String libPrefix, final String keyspace, final Predicate<String> classpathFilter, final AmazonS3 s3, final String bucket) {
    String localClasspath = System.getProperty("java.class.path");
    logger.info("Java Local Classpath: " + localClasspath);
    String remoteClasspath = stageRemoteClasspath(node, localClasspath, classpathFilter, libPrefix, true, s3, bucket, keyspace);
    logger.info("Java Remote Classpath: " + remoteClasspath);
    String commandLine = String.format("java %s -cp %s %s %s", javaOpts, remoteClasspath, Tendril.class.getCanonicalName(), programArguments);
    logger.info("Java Command Line: " + commandLine);
    execAsync(node.getConnection(), commandLine, new CloseShieldOutputStream(System.out));
    return new TendrilControl(getControl(localControlPort));
  }
  
  
  public static TendrilLink getControl(final int localControlPort) {return getControl(localControlPort, 3);}
  
  public static TendrilLink getControl(final int localControlPort, final int retries) {
    try {
      Client client = new Client();
      configure(client.getKryo());
      client.start();
      client.connect(5000, "127.0.0.1", localControlPort, -1);
      return ObjectSpace.getRemoteObject(client, 1318, TendrilLink.class);
    } catch (Throwable e) {
      if (retries > 0) {
        sleep(5000);
        return getControl(localControlPort, retries - 1);
      }
      throw new RuntimeException(e);
    }
  }
  
  public static void configure(final Kryo kryo) {
    kryo.setRegistrationRequired(false);
    kryo.setInstantiatorStrategy(new Kryo.DefaultInstantiatorStrategy(new StdInstantiatorStrategy()));
    kryo.register(Object[].class);
    kryo.register(java.lang.Class.class);
    kryo.register(SerializedLambda.class);
    com.esotericsoftware.kryonet.rmi.ObjectSpace.registerClasses(kryo);
    kryo.register(ClosureSerializer.Closure.class, new ClosureSerializer());
    kryo.register(SerializableCallable.class, new JavaSerializer());
    kryo.register(TendrilLink.class);
  }
  
  @Nonnull
  public static String stageRemoteClasspath(final EC2Node node, final String localClasspath, final Predicate<String> classpathFilter, final String libPrefix, final boolean parallel, final AmazonS3 s3, final String bucket, final String keyspace) {
    logger.info(String.format("Mkdir %s: %s", libPrefix, node.exec("mkdir -p " + libPrefix)));
    Stream<String> stream = Arrays.stream(localClasspath.split(File.pathSeparator)).filter(classpathFilter);
    PrintStream out = SysOutInterceptor.INSTANCE.currentHandler();
    if (parallel) stream = stream.parallel();
    return stream.map(entryPath -> {
      PrintStream prev = SysOutInterceptor.INSTANCE.setCurrentHandler(out);
      String classpathEntry = stageClasspathEntry(node, libPrefix, entryPath, s3, bucket, keyspace);
      SysOutInterceptor.INSTANCE.setCurrentHandler(prev);
      return classpathEntry;
    }).reduce((a, b) -> a + ":" + b).get();
  }
  
  @Nonnull
  public static String stageClasspathEntry(final EC2Node node, final String libPrefix, final String entryPath, final AmazonS3 s3, final String bucket, final String keyspace) {
    logger.info(String.format("Processing %s", entryPath));
    final File entryFile = new File(entryPath);
    try {
      if (entryFile.isFile()) {
        String remote = libPrefix + hash(entryFile) + ".jar";
        logger.info(String.format("Uploading %s to %s", entryPath, remote));
        try {
          stage(node, entryFile, remote, s3, bucket, keyspace);
        } catch (Throwable e) {
          logger.warn(String.format("Error staging %s to %s", entryFile, remote), e);
        }
        return remote;
      }
      else {
        File tempJar = toJar(entryFile);
        try {
          String remote = libPrefix + hash(tempJar) + ".jar";
          logger.info(String.format("Uploading %s to %s", tempJar, remote));
          try {
            stage(node, tempJar, remote, s3, bucket, keyspace);
          } catch (Throwable e) {
            throw new RuntimeException(String.format("Error staging %s to %s", entryFile, remote), e);
          }
          return remote;
        } finally {
          tempJar.delete();
        }
      }
    } catch (Throwable e) {
      throw new RuntimeException(e);
    }
  }
  
  public static boolean defaultClasspathFilter(final String file) {
    if (file.replace('\\', '/').contains("/jre/")) return false;
    return !file.replace('\\', '/').contains("/jdk/");
  }
  
  public static void stage(final EC2Node node, final File entryFile, final String remote, final AmazonS3 s3, final String bucket, final String keyspace) {stage(node, entryFile, remote, 10, s3, bucket, keyspace);}
  
  public static void stage(final EC2Node node, final File entryFile, final String remote, final int retries, final AmazonS3 s3, final String bucket, final String keyspace) {
    try {
      if (null == bucket || bucket.isEmpty()) {
        node.scp(entryFile, remote);
      }
      else {
        node.stage(entryFile, remote, bucket, keyspace, s3);
      }
    } catch (Throwable e) {
      if (retries > 0) {
        logger.info("Retrying " + remote, e);
        sleep(5000);
        stage(node, entryFile, remote, retries - 1, s3, bucket, keyspace);
      }
      else {
        throw new RuntimeException(e);
      }
    }
  }
  
  @Nonnull
  public static File toJar(final File entry) throws IOException {
    File tempJar = File.createTempFile(UUID.randomUUID().toString(), ".jar").getAbsoluteFile();
    logger.info(String.format("Archiving %s to %s", entry, tempJar));
    try (ZipOutputStream zip = new ZipOutputStream(new FileOutputStream(tempJar))) {
      for (final File file : Arrays.stream(entry.listFiles()).sorted().collect(Collectors.toList())) {
        write(zip, "", file);
      }
    } catch (Throwable e) {
      if (tempJar.exists()) tempJar.delete();
      throw new RuntimeException(e);
    }
    return tempJar;
  }
  
  private static void write(final ZipOutputStream zip, final String base, final File entry) throws IOException {
    if (entry.isFile()) {
      zip.putNextEntry(new ZipEntry(base + entry.getName()));
      try (FileInputStream input = new FileInputStream(entry)) {
        IOUtils.copy(input, zip);
      }
    }
    else {
      for (final File file : Arrays.stream(entry.listFiles()).sorted().collect(Collectors.toList())) {
        write(zip, base + entry.getName() + "/", file);
      }
    }
    
  }
  
  public static String hash(final File classpath) throws NoSuchAlgorithmException, IOException {
    MessageDigest digest = MessageDigest.getInstance("MD5");
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
  
  public static TendrilControl startRemoteJvm(final EC2Node node, final JvmConfig jvmConfig, final int localControlPort, final Predicate<String> shouldTransfer, final AmazonS3 s3, final String bucket) {
    return startRemoteJvm(node, localControlPort, jvmConfig.javaOpts, jvmConfig.programArguments, jvmConfig.libPrefix, jvmConfig.keyspace, shouldTransfer, s3, bucket);
  }
  
  public interface SerializableRunnable extends Runnable, Serializable {
  }
  
  public interface SerializableCallable<T> extends Callable<T>, Serializable {
  }
  
  public interface TendrilLink {
    boolean isAlive();
  
    void exit();
    
    long time();
  
    <T> T run(SerializableCallable<T> task) throws Exception;
  }
  
  public static class TendrilControl implements Executor, AutoCloseable {
    
    private final TendrilLink inner;
    
    public TendrilControl(final TendrilLink inner) {this.inner = inner;}
    
    public long time() {
      return inner.time();
    }
    
    public <T> T run(final SerializableCallable<T> task) throws Exception {
      assert inner.isAlive();
      return inner.run(task);
    }
    
    public void start(SerializableRunnable task) {
      assert inner.isAlive();
      try {
        inner.run(() -> {
          new Thread(() -> {
            try {
              task.run();
            } catch (Exception e) {
              logger.warn("Task Error", e);
            }
          }).start();
          return null;
        });
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    
    @Override
    public void execute(final @NotNull Runnable command) {
      start(() -> command.run());
    }
    
    @Override
    public void close() {
      logger.info("Closing " + this);
      inner.exit();
    }
  }
  
  protected static class TendrilLinkImpl implements TendrilLink {
    @Override
    public boolean isAlive() {
      return true;
    }
  
    @Override
    public void exit() {
      exit(0, 1000);
    }
  
    public void exit(final int status, final int wait) {
      logger.warn(String.format("Exiting with code %d in %d", status, wait));
      new Thread(() -> {
        try {
          Thread.sleep(wait);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        System.exit(status);
      }).start();
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
  
  public static class JvmConfig extends NodeConfig {
    public String javaOpts;
    public String programArguments;
    public String libPrefix;
    public String keyspace;
    
    public JvmConfig(final String imageId, final String instanceType, final String username) {
      super(imageId, instanceType, username);
      javaOpts = "";
      programArguments = "";
      libPrefix = "lib/";
      keyspace = "";
    }
  }
}
