/*
 * Copyright (c) 2019 by Andrew Charneski.
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
import com.esotericsoftware.kryonet.*;
import com.esotericsoftware.kryonet.rmi.ObjectSpace;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.simiacryptus.lang.SerializableCallable;
import com.simiacryptus.lang.SerializableConsumer;
import com.simiacryptus.util.test.SysOutInterceptor;
import com.twitter.chill.KryoInstantiator;
import com.twitter.chill.java.Java8ClosureRegistrar;
import com.twitter.chill.java.UnmodifiableCollectionSerializer;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.output.CloseShieldOutputStream;
import org.objenesis.strategy.StdInstantiatorStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.Serializable;
import java.lang.invoke.SerializedLambda;
import java.net.InetSocketAddress;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static com.simiacryptus.aws.EC2Util.*;

public class Tendril {

  private static final Logger logger = LoggerFactory.getLogger(Tendril.class);
  private static final int BUFFER_SIZE = 8 * 1024 * 1024;

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
      Server server = new Server(BUFFER_SIZE, BUFFER_SIZE, new KryoSerialization(getKryo()));
      ObjectSpace.registerClasses(server.getKryo());
      ObjectSpace objectSpace = new ObjectSpace();
      TendrilLinkImpl tendrilLink = new TendrilLinkImpl();
      Executors.newScheduledThreadPool(1, new ThreadFactoryBuilder().setDaemon(true).build()).schedule(() -> {
        if (!tendrilLink.contacted) {
          logger.warn("Server has not been contacted yet. Exiting.", new RuntimeException("Stack Trace"));
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
      server.bind(new InetSocketAddress("127.0.0.1", Integer.parseInt(System.getProperty("controlPort", "1318"))), null);
    } catch (Throwable e) {
      e.printStackTrace();
    }
  }


  @Nonnull
  public static TendrilControl startRemoteJvm(
      final EC2Node node,
      final int localControlPort,
      final String javaOpts,
      final String programArguments,
      final String libPrefix,
      final String keyspace,
      final Predicate<String> classpathFilter,
      final AmazonS3 s3,
      final HashMap<String, String> env,
      final String[] bucket
  ) {
    String localClasspath = System.getProperty("java.class.path");
    Arrays.stream(new File(".").listFiles()).filter(x -> x.getName().endsWith(".json")).forEach(file -> {
      logger.info("Deploy " + file.getAbsoluteFile());
      node.scp(file, file.getName());
    });
    String remoteClasspath = stageRemoteClasspath(node, localClasspath, classpathFilter, libPrefix, true, s3, bucket[0], keyspace);
    String commandLine = String.format(
        "nohup java %s -cp %s %s %s",
        javaOpts,
        remoteClasspath,
        Tendril.class.getCanonicalName(),
        programArguments
    );
    logger.info("Java Local Classpath: " + localClasspath);
    logger.info("Java Remote Classpath: " + remoteClasspath);
    logger.info("Java Environment: " + env.entrySet().stream().map(e -> e.getKey() + " = " + e.getValue()).reduce((a, b) -> a + "; " + b).orElse(""));
    logger.info("Java Command Line: " + commandLine);
    execAsync(node.getConnection(), commandLine, new CloseShieldOutputStream(System.out), env);
    return new TendrilControl(getControl(localControlPort));
  }

  @Nonnull
  public static TendrilControl startLocalJvm(
      final int controlPort,
      final String javaOpts,
      final HashMap<String, String> env,
      File workingDir
  ) {
    final String programArguments = "";
    File javaBin = new File(new File(System.getProperty("java.home")), "bin");
    String javaExePath = Arrays.stream(javaBin.listFiles()).filter(x -> {
      String name = x.getName();
      String[] split = name.split("\\.");
      return split[0].equals("java") && (name.endsWith("exe") || name.equals("java"));
    }).findFirst().get().getAbsolutePath();
    try {
      ArrayList<String> cmd = new ArrayList<>(Arrays.asList(
          javaExePath
      ));
      Arrays.stream(javaOpts.split(" ")).forEach(cmd::add);
      String classpath = Arrays.stream(System.getProperty("java.class.path").split(File.pathSeparator))
          .map(path -> workingDir.toURI().relativize(new File(path).toURI()).getPath())
          .reduce((a, b) -> a + File.pathSeparator + b).get();
      cmd.addAll(Arrays.asList(
          "-cp", classpath,
          //ClasspathUtil.summarizeLocalClasspath().getAbsolutePath(),
          "-DcontrolPort=" + controlPort,
          Tendril.class.getCanonicalName()
      ));
      Arrays.stream(programArguments.split(" ")).forEach(cmd::add);
      logger.info("Java Environment: " + env.entrySet().stream().map(e -> e.getKey() + " = " + e.getValue()).reduce((a, b) -> a + "; " + b).orElse(""));
      logger.info(String.format("Java Command Line (from %s): %s", workingDir.getAbsolutePath(), cmd.stream().reduce((a, b) -> a + " " + b).get()));
      ProcessBuilder processBuilder = new ProcessBuilder().command(cmd).directory(workingDir).inheritIO();
      processBuilder.environment().putAll(env);
      processBuilder.start();
      return new TendrilControl(getControl(controlPort));
    } catch (IOException e) {
      throw new RuntimeException("Error running child jvm", e);
    }
  }


  public static TendrilLink getControl(final int localControlPort) {
    return getControl(localControlPort, 10, 300);
  }

  public static TendrilLink getControl(int localControlPort, int retries, int timeoutSeconds) {
    try {
      Client client = new Client(BUFFER_SIZE, BUFFER_SIZE, new KryoSerialization(getKryo()));
      client.start();
      client.setTimeout((int) TimeUnit.SECONDS.toMillis(timeoutSeconds));
      client.setKeepAliveTCP((int) TimeUnit.SECONDS.toMillis(15));
      new Thread(() -> {
        try {
          while (!Thread.interrupted()) {
            try {
              client.update(10);
            } catch (IOException e) {
              e.printStackTrace();
              Thread.sleep(1000);
            }
          }
        } catch (Throwable throwable) {
          throwable.printStackTrace();
        }
      }).start();
      client.connect((int) TimeUnit.SECONDS.toMillis(90), "127.0.0.1", localControlPort, -1);
      TendrilLink remoteObject = ObjectSpace.getRemoteObject(client, 1318, TendrilLink.class);
      if (!remoteObject.isAlive()) throw new RuntimeException("Not Alive");
      return remoteObject;
    } catch (Throwable e) {
      if (retries > 0) {
        sleep(5000);
        return getControl(localControlPort, retries - 1, timeoutSeconds);
      }
      throw new RuntimeException(e);
    }
  }

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

  @Nonnull
  public static String stageRemoteClasspath(final EC2Node node, final String localClasspath, final Predicate<String> classpathFilter, final String libPrefix, final boolean parallel, final AmazonS3 s3, final String bucket, final String keyspace) {
    logger.info(String.format("Mkdir %s: %s", libPrefix, node.exec("mkdir -p " + libPrefix)));
    ExecutorService executorService = Executors.newFixedThreadPool(4);
    PrintStream out = SysOutInterceptor.INSTANCE.currentHandler();
    try {
      Stream<String> stream = Arrays.stream(localClasspath.split(File.pathSeparator)).filter(classpathFilter);
      if(null != bucket && !bucket.isEmpty()) stream = stream.parallel();
      return stream.map(entryPath -> {
        return executorService.submit(() -> {
          PrintStream prev = SysOutInterceptor.INSTANCE.setCurrentHandler(out);
          List<String> classpathEntry = stageClasspathEntry(node, libPrefix, entryPath, s3, bucket, keyspace);
          SysOutInterceptor.INSTANCE.setCurrentHandler(prev);
          return classpathEntry.stream().reduce((a, b) -> a + ":" + b).get();
        });
      }).map(x -> {
        try {
          return (String) ((Future) x).get();
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        } catch (ExecutionException e) {
          throw new RuntimeException(e);
        }
      }).reduce((a, b) -> a + ":" + b).get();
    } finally {
      executorService.shutdown();
    }
  }

  @Nonnull
  public static List<String> stageClasspathEntry(final EC2Node node, final String libPrefix, final String entryPath, final AmazonS3 s3, final String bucket, final String keyspace) {
    final File entryFile = new File(entryPath);
    try {
      if (entryFile.isFile()) {
        String remote = libPrefix + ClasspathUtil.hash(entryFile) + ".jar";
        logger.info(String.format("Staging %s via %s", entryPath, remote));
        try {
          stage(node, entryFile, remote, s3, bucket, keyspace);
        } catch (Throwable e) {
          throw new IOException(String.format("Error staging %s to %s/%s", entryFile, bucket, remote), e);
          //logger.warn(String.format("Error staging %s to %s/%s", entryFile, bucket, remote), e);
        }
        return Arrays.asList(remote);
      } else {
        logger.info(String.format("Processing %s", entryPath));
        ArrayList<String> list = new ArrayList<>();
        if (entryFile.getName().equals("classes") && entryFile.getParentFile().getName().equals("target")) {
          File javaSrc = new File(new File(new File(entryFile.getParentFile().getParentFile(), "src"), "main"), "java");
          if (javaSrc.exists()) list.add(addDir(node, libPrefix, s3, bucket, keyspace, javaSrc));
          File scalaSrc = new File(new File(new File(entryFile.getParentFile().getParentFile(), "src"), "main"), "scala");
          if (scalaSrc.exists()) list.add(addDir(node, libPrefix, s3, bucket, keyspace, scalaSrc));
        }
        if (entryFile.getName().equals("test-classes") && entryFile.getParentFile().getName().equals("target")) {
          File javaSrc = new File(new File(new File(entryFile.getParentFile().getParentFile(), "src"), "test"), "java");
          if (javaSrc.exists()) list.add(addDir(node, libPrefix, s3, bucket, keyspace, javaSrc));
          File scalaSrc = new File(new File(new File(entryFile.getParentFile().getParentFile(), "src"), "test"), "scala");
          if (scalaSrc.exists()) list.add(addDir(node, libPrefix, s3, bucket, keyspace, scalaSrc));
        }
        list.add(addDir(node, libPrefix, s3, bucket, keyspace, entryFile));
        return list;
      }
    } catch (Throwable e) {
      throw e instanceof RuntimeException ? (RuntimeException) e : new RuntimeException(e);
    }
  }

  @Nonnull
  public static String addDir(final EC2Node node, final String libPrefix, final AmazonS3 s3, final String bucket, final String keyspace, final File entryFile) throws IOException, NoSuchAlgorithmException {
    File tempJar = ClasspathUtil.toJar(entryFile);
    try {
      String remote = libPrefix + ClasspathUtil.hash(tempJar) + ".jar";
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

  public static boolean defaultClasspathFilter(final String file) {
    if (file.replace('\\', '/').contains("/jre/")) return false;
    return !file.replace('\\', '/').contains("/jdk/");
  }

  public static void stage(final EC2Node node, final File entryFile, final String remote, final AmazonS3 s3, final String bucket, final String keyspace) {
    stage(node, entryFile, remote, 10, s3, bucket, keyspace);
  }

  public static void stage(final EC2Node node, final File entryFile, final String remote, final int retries, final AmazonS3 s3, final String bucket, final String keyspace) {
    try {
      if (null == bucket || bucket.isEmpty()) {
        node.scp(entryFile, remote);
      } else {
        node.stage(entryFile, remote, bucket, keyspace, s3);
      }
    } catch (Throwable e) {
      if (retries > 0) {
        logger.debug("Retrying " + remote, e);
        sleep((int) (Math.random() * 15000));
        stage(node, entryFile, remote, retries - 1, s3, bucket, keyspace);
      } else {
        throw new RuntimeException(e);
      }
    }
  }

  public static void stage(final File entryFile, final String remote, final int retries) {
    try {
      FileUtils.copyFile(entryFile, new File(remote));
    } catch (Throwable e) {
      if (retries > 0) {
        logger.info("Retrying " + remote);
        sleep(5000);
        stage(entryFile, remote, retries - 1);
      } else {
        throw new RuntimeException(e);
      }
    }
  }

  @Nonnull
  public static TendrilControl startRemoteJvm(
      final EC2Node node,
      final JvmConfig jvmConfig,
      final int localControlPort,
      final Predicate<String> shouldTransfer,
      final AmazonS3 s3,
      final HashMap<String, String> env,
      final String... bucket
  ) {
    return startRemoteJvm(
        node,
        localControlPort,
        jvmConfig.javaOpts,
        jvmConfig.programArguments,
        jvmConfig.libPrefix,
        jvmConfig.keyspace,
        shouldTransfer,
        s3,
        env,
        bucket
    );
  }

  @Nonnull
  public static TendrilControl startLocalJvm(
      final JvmConfig jvmConfig,
      final int localControlPort,
      final HashMap<String, String> env
  ) {
    return startLocalJvm(
        localControlPort,
        jvmConfig.javaOpts,
        env, new File(".")
    );
  }

  public interface TendrilLink {
    boolean isAlive();

    void exit();

    long time();

    <T> T run(SerializableCallable<T> task) throws Exception;
  }

  protected static class TendrilLinkImpl implements TendrilLink {
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

    public void exit(final int status, final int wait) {
      contacted = true;
      logger.warn(String.format("Exiting with eval %d in %d", status, wait), new RuntimeException("Stack Trace"));
      new Thread(() -> {
        try {
          Thread.sleep(wait);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        logger.warn(String.format("Exiting with eval %d in %d", status, wait), new RuntimeException("Stack Trace"));
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
