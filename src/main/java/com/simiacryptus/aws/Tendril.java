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
import com.esotericsoftware.kryo.serializers.OptionalSerializers;
import com.esotericsoftware.kryonet.Client;
import com.esotericsoftware.kryonet.Connection;
import com.esotericsoftware.kryonet.KryoSerialization;
import com.esotericsoftware.kryonet.Listener;
import com.esotericsoftware.kryonet.Server;
import com.esotericsoftware.kryonet.rmi.ObjectSpace;
import com.simiacryptus.util.test.SysOutInterceptor;
import com.twitter.chill.KryoInstantiator;
import com.twitter.chill.java.Java8ClosureRegistrar;
import com.twitter.chill.java.UnmodifiableCollectionSerializer;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static com.simiacryptus.aws.EC2Util.*;

/**
 * The type Tendril.
 */
public class Tendril {
  
  private static final Logger logger = LoggerFactory.getLogger(EC2Util.class);
  
  /**
   * The entry point of application.
   *
   * @param args the input arguments
   */
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
      Server server = new Server(16384, 2048, new KryoSerialization(getKryo()));
      ObjectSpace.registerClasses(server.getKryo());
      ObjectSpace objectSpace = new ObjectSpace();
      TendrilLinkImpl tendrilLink = new TendrilLinkImpl();
      Executors.newScheduledThreadPool(1).schedule(() -> {
        if (!tendrilLink.contacted) {
          logger.warn("Server has not been contacted yet. Exiting.");
          System.exit(1);
        }
      }, 5, TimeUnit.MINUTES);
      objectSpace.register(1318, tendrilLink);
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
  
  
  /**
   * Start remote jvm tendril control.
   *
   * @param node             the node
   * @param localControlPort the local control port
   * @param javaOpts         the java opts
   * @param programArguments the program arguments
   * @param libPrefix        the lib prefix
   * @param keyspace         the keyspace
   * @param classpathFilter  the classpath filter
   * @param s3               the s 3
   * @param bucket           the bucket
   * @return the tendril control
   */
  @Nullable
  public static TendrilControl startRemoteJvm(final EC2Node node, final int localControlPort, final String javaOpts, final String programArguments, final String libPrefix, final String keyspace, final Predicate<String> classpathFilter, final AmazonS3 s3, final String bucket) {
    String localClasspath = System.getProperty("java.class.path");
    logger.info("Java Local Classpath: " + localClasspath);
    String remoteClasspath = stageRemoteClasspath(node, localClasspath, classpathFilter, libPrefix, true, s3, bucket, keyspace);
    logger.info("Java Remote Classpath: " + remoteClasspath);
    String commandLine = String.format("nohup java %s -cp %s %s %s", javaOpts, remoteClasspath, Tendril.class.getCanonicalName(), programArguments);
    logger.info("Java Command Line: " + commandLine);
    execAsync(node.getConnection(), commandLine, new CloseShieldOutputStream(System.out));
    return new TendrilControl(getControl(localControlPort));
  }
  
  
  /**
   * Gets control.
   *
   * @param localControlPort the local control port
   * @return the control
   */
  public static TendrilLink getControl(final int localControlPort) {return getControl(localControlPort, 10);}
  
  /**
   * Gets control.
   *
   * @param localControlPort the local control port
   * @param retries          the retries
   * @return the control
   */
  public static TendrilLink getControl(final int localControlPort, final int retries) {
    try {
      Client client = new Client(8192, 2048, new KryoSerialization(getKryo()));
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
  
  /**
   * Gets kryo.
   *
   * @return the kryo
   */
  public static Kryo getKryo() {
    final Kryo kryo = new KryoInstantiator().setRegistrationRequired(false).setReferences(true).newKryo();
    kryo.setRegistrationRequired(false);
    kryo.setInstantiatorStrategy(new StdInstantiatorStrategy());
    kryo.register(Object[].class);
    kryo.register(Class.class);
    kryo.register(SerializedLambda.class);
    kryo.register(ClosureSerializer.Closure.class, new ClosureSerializer());
    ObjectSpace.registerClasses(kryo);
    new Java8ClosureRegistrar().apply(kryo);
    UnmodifiableCollectionSerializer.registrar().apply(kryo);
    OptionalSerializers.addDefaultSerializers(kryo);
    kryo.register(SerializableCallable.class, new JavaSerializer());
    kryo.register(SerializableConsumer.class, new JavaSerializer());
    kryo.register(Serializable.class, new JavaSerializer());
    kryo.register(TendrilLink.class);
    return kryo;
  }
  
  /**
   * Stage remote classpath string.
   *
   * @param node            the node
   * @param localClasspath  the local classpath
   * @param classpathFilter the classpath filter
   * @param libPrefix       the lib prefix
   * @param parallel        the parallel
   * @param s3              the s 3
   * @param bucket          the bucket
   * @param keyspace        the keyspace
   * @return the string
   */
  @Nonnull
  public static String stageRemoteClasspath(final EC2Node node, final String localClasspath, final Predicate<String> classpathFilter, final String libPrefix, final boolean parallel, final AmazonS3 s3, final String bucket, final String keyspace) {
    logger.info(String.format("Mkdir %s: %s", libPrefix, node.exec("mkdir -p " + libPrefix)));
    Stream<String> stream = Arrays.stream(localClasspath.split(File.pathSeparator)).filter(classpathFilter);
    PrintStream out = SysOutInterceptor.INSTANCE.currentHandler();
    if (parallel) stream = stream.parallel();
    return stream.flatMap(entryPath -> {
      PrintStream prev = SysOutInterceptor.INSTANCE.setCurrentHandler(out);
      List<String> classpathEntry = stageClasspathEntry(node, libPrefix, entryPath, s3, bucket, keyspace);
      SysOutInterceptor.INSTANCE.setCurrentHandler(prev);
      return classpathEntry.stream();
    }).reduce((a, b) -> a + ":" + b).get();
  }
  
  /**
   * Stage classpath entry string.
   *
   * @param node      the node
   * @param libPrefix the lib prefix
   * @param entryPath the entry path
   * @param s3        the s 3
   * @param bucket    the bucket
   * @param keyspace  the keyspace
   * @return the string
   */
  @Nonnull
  public static List<String> stageClasspathEntry(final EC2Node node, final String libPrefix, final String entryPath, final AmazonS3 s3, final String bucket, final String keyspace) {
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
        return Arrays.asList(remote);
      }
      else {
        ArrayList<String> list = new ArrayList<>();
        if (entryFile.getName().equals("classes") && entryFile.getParentFile().getName().equals("target")) {
          list.add(addDir(node, libPrefix, s3, bucket, keyspace, new File(new File(new File(entryFile.getParentFile().getParentFile(), "src"), "main"), "java")));
        }
        if (entryFile.getName().equals("test-classes") && entryFile.getParentFile().getName().equals("target")) {
          list.add(addDir(node, libPrefix, s3, bucket, keyspace, new File(new File(new File(entryFile.getParentFile().getParentFile(), "src"), "test"), "java")));
        }
        list.add(addDir(node, libPrefix, s3, bucket, keyspace, entryFile));
        return list;
      }
    } catch (Throwable e) {
      throw new RuntimeException(e);
    }
  }
  
  @Nonnull
  public static String addDir(final EC2Node node, final String libPrefix, final AmazonS3 s3, final String bucket, final String keyspace, final File entryFile) throws IOException, NoSuchAlgorithmException {
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
  
  /**
   * Default classpath filter boolean.
   *
   * @param file the file
   * @return the boolean
   */
  public static boolean defaultClasspathFilter(final String file) {
    if (file.replace('\\', '/').contains("/jre/")) return false;
    return !file.replace('\\', '/').contains("/jdk/");
  }
  
  /**
   * Stage.
   *
   * @param node      the node
   * @param entryFile the entry file
   * @param remote    the remote
   * @param s3        the s 3
   * @param bucket    the bucket
   * @param keyspace  the keyspace
   */
  public static void stage(final EC2Node node, final File entryFile, final String remote, final AmazonS3 s3, final String bucket, final String keyspace) {stage(node, entryFile, remote, 10, s3, bucket, keyspace);}
  
  /**
   * Stage.
   *
   * @param node      the node
   * @param entryFile the entry file
   * @param remote    the remote
   * @param retries   the retries
   * @param s3        the s 3
   * @param bucket    the bucket
   * @param keyspace  the keyspace
   */
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
  
  /**
   * To jar file.
   *
   * @param entry the entry
   * @return the file
   * @throws IOException the io exception
   */
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
  
  /**
   * Hash string.
   *
   * @param classpath the classpath
   * @return the string
   * @throws NoSuchAlgorithmException the no such algorithm exception
   * @throws IOException              the io exception
   */
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
  
  /**
   * Start remote jvm tendril control.
   *
   * @param node             the node
   * @param jvmConfig        the jvm config
   * @param localControlPort the local control port
   * @param shouldTransfer   the should transfer
   * @param s3               the s 3
   * @param bucket           the bucket
   * @return the tendril control
   */
  public static TendrilControl startRemoteJvm(final EC2Node node, final JvmConfig jvmConfig, final int localControlPort, final Predicate<String> shouldTransfer, final AmazonS3 s3, final String bucket) {
    return startRemoteJvm(node, localControlPort, jvmConfig.javaOpts, jvmConfig.programArguments, jvmConfig.libPrefix, jvmConfig.keyspace, shouldTransfer, s3, bucket);
  }
  
  /**
   * The interface Serializable runnable.
   */
  public interface SerializableRunnable extends Runnable, Serializable {
  }
  
  /**
   * The interface Serializable callable.
   *
   * @param <T> the type parameter
   */
  public interface SerializableCallable<T> extends Callable<T>, Serializable {
  }
  
  /**
   * The interface Serializable consumer.
   *
   * @param <T> the type parameter
   */
  public interface SerializableConsumer<T> extends Consumer<T>, Serializable {
  }
  
  /**
   * The interface Tendril link.
   */
  public interface TendrilLink {
    /**
     * Is alive boolean.
     *
     * @return the boolean
     */
    boolean isAlive();
  
    /**
     * Exit.
     */
    void exit();
  
    /**
     * Time long.
     *
     * @return the long
     */
    long time();
  
    /**
     * Run t.
     *
     * @param <T>  the type parameter
     * @param task the task
     * @return the t
     * @throws Exception the exception
     */
    <T> T run(SerializableCallable<T> task) throws Exception;
  }
  
  /**
   * The type Tendril control.
   */
  public static class TendrilControl implements Executor, AutoCloseable {
    
    private final TendrilLink inner;
  
    /**
     * Instantiates a new Tendril control.
     *
     * @param inner the inner
     */
    public TendrilControl(final TendrilLink inner) {this.inner = inner;}
  
    /**
     * Time long.
     *
     * @return the long
     */
    public long time() {
      return inner.time();
    }
  
    /**
     * Run t.
     *
     * @param <T>  the type parameter
     * @param task the task
     * @return the t
     * @throws Exception the exception
     */
    public <T> T run(final SerializableCallable<T> task) throws Exception {
      assert inner.isAlive();
      return inner.run(task);
    }
  
    /**
     * Start.
     *
     * @param task the task
     */
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
  
  /**
   * The type Tendril link.
   */
  protected static class TendrilLinkImpl implements TendrilLink {
    /**
     * The Contacted.
     */
    public boolean contacted = false;
  
    @Override
    public boolean isAlive() {
      contacted = true;
      return true;
    }
    
    @Override
    public void exit() {
      contacted = true;
      exit(0, 1000);
    }
  
    /**
     * Exit.
     *
     * @param status the status
     * @param wait   the wait
     */
    public void exit(final int status, final int wait) {
      contacted = true;
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
      contacted = true;
      return System.currentTimeMillis();
    }
    
    @Override
    public <T> T run(final SerializableCallable<T> task) throws Exception {
      contacted = true;
      return task.call();
    }
  }
  
  /**
   * The type Jvm config.
   */
  public static class JvmConfig extends NodeConfig {
    /**
     * The Java opts.
     */
    public String javaOpts;
    /**
     * The Program arguments.
     */
    public String programArguments;
    /**
     * The Lib prefix.
     */
    public String libPrefix;
    /**
     * The Keyspace.
     */
    public String keyspace;
  
    /**
     * Instantiates a new Jvm config.
     *
     * @param imageId      the image id
     * @param instanceType the instance type
     * @param username     the username
     */
    public JvmConfig(final String imageId, final String instanceType, final String username) {
      super(imageId, instanceType, username);
      javaOpts = "";
      programArguments = "";
      libPrefix = "lib/";
      keyspace = "";
    }
  }
}
