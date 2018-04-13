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

import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.AuthorizeSecurityGroupIngressRequest;
import com.amazonaws.services.ec2.model.CreateKeyPairRequest;
import com.amazonaws.services.ec2.model.CreateSecurityGroupRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeSecurityGroupsRequest;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.IpPermission;
import com.amazonaws.services.ec2.model.IpRange;
import com.amazonaws.services.ec2.model.KeyPair;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.amazonaws.services.ec2.model.TerminateInstancesResult;
import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.simiacryptus.util.FastRandom;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

public class EC2Util {
  
  private static final Logger logger = LoggerFactory.getLogger(EC2Util.class);
  private static final Charset charset = Charset.forName("UTF-8");
  private static volatile KeyPair keyPair = null;
  
  public static void main(String[] a) {
    AmazonEC2 ec2 = AmazonEC2ClientBuilder.standard().withRegion(Regions.US_EAST_1).build();
    String imageId = "ami-346b9c49";
    String instanceType = "t1.micro";
    String username = "ubuntu";
    try {
      String consoleLog = run(ec2, imageId, instanceType, username, session -> {
        try {
          File file = new File("test.txt");
          FileUtils.write(file, "Hello World", charset);
          scp(session, file, file.getName());
          String exec = new EC2Util().exec(session, "cat test.txt");
          shell(session);
          return exec;
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }, 22, 80);
      logger.info(String.format("Result: %s", consoleLog));
    } finally {
      if (Thread.interrupted()) logger.info("Interupted");
      try {
        Thread.sleep(15000);
      } catch (InterruptedException e) {
      } finally {
        System.exit(0);
      }
    }
  }
  
  @Nonnull
  public static String scp(final Session session, final File file, final String remote) {
    try {
      ChannelExec channel = (ChannelExec) session.openChannel("exec");
      channel.setCommand(String.format("scp -t %s", remote));
      ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
      channel.connect();
      OutputStream outputStream = channel.getOutputStream();
      IOUtils.write(String.format("C0644 %d %s\n", file.length(), remote), outputStream, charset);
      IOUtils.copy(new FileInputStream(file), outputStream);
      outputStream.close();
      int exitStatus = channel.getExitStatus();
      if (0 != exitStatus) {
        throw new AssertionError("Exit Status: " + exitStatus);
      }
      return new String(outBuffer.toByteArray(), charset);
    } catch (IOException | JSchException e) {
      throw new RuntimeException(e);
    }
  }
  
  @Nullable
  public static void shell(final Session session) {
    try {
      Channel channel = session.openChannel("shell");
      channel.setOutputStream(System.out);
      channel.setExtOutputStream(System.err);
      channel.setInputStream(System.in);
      channel.connect();
      while (!channel.isClosed()) {
        Thread.sleep(100);
      }
      int exitStatus = channel.getExitStatus();
      if (0 != exitStatus) {
        throw new AssertionError("Exit Status: " + exitStatus);
      }
    } catch (InterruptedException | JSchException e) {
      throw new RuntimeException(e);
    }
  }
  
  public static <T> T run(final AmazonEC2 ec2, final String imageId, final String instanceType, final String username, Function<Session, T> task, final int... ports) {
    return start(ec2, imageId, instanceType, username, ports).runAndTerminate(task);
  }
  
  @Nonnull
  public static EC2Node start(final AmazonEC2 ec2, final String imageId, final String instanceType, final String username, final int... ports) {
    return start(ec2, imageId, instanceType, username, getKeyPair(ec2), newSecurityGroup(ec2, ports));
  }
  
  @Nullable
  public static EC2Node start(final AmazonEC2 ec2, final String imageId, final String instanceType, final String username, final KeyPair keyPair, final String groupId) {
    Instance instance = start(ec2, imageId, instanceType, groupId, keyPair);
    try {
      return new EC2Node(ec2, connect(keyPair, username, instance), instance.getInstanceId());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return null;
    }
  }
  
  @Nonnull
  public static KeyPair getKeyPair(final AmazonEC2 ec2) {
    if (null == keyPair) {
      synchronized (EC2Util.class) {
        if (null == keyPair) {
          keyPair = ec2.createKeyPair(new CreateKeyPairRequest().withKeyName("key_" + randomHex())).getKeyPair();
          try {
            FileUtils.write(new File(keyPair.getKeyName() + ".pem"), keyPair.getKeyMaterial(), "UTF-8");
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      }
    }
    return keyPair;
  }
  
  public static String newSecurityGroup(final AmazonEC2 ec2, int... ports) {
    String groupName = "sg_" + randomHex();
    String groupId = ec2.createSecurityGroup(new CreateSecurityGroupRequest()
      .withGroupName(groupName)
      .withDescription("Created by " + EC2Util.class.getCanonicalName())).getGroupId();
    while (!exists(ec2, groupId)) {
      logger.info("Awaiting security group creation...");
      sleep(10000);
    }
    ec2.authorizeSecurityGroupIngress(new AuthorizeSecurityGroupIngressRequest()
      .withGroupId(groupId)
      .withIpPermissions(Arrays.stream(ports).mapToObj(port -> getTcpPermission(port)).toArray(i -> new IpPermission[i])));
    return groupId;
  }
  
  public static void sleep(final int millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
  
  @Nonnull
  public static Session connect(final KeyPair keyPair, final String username, final Instance ec2instance) throws InterruptedException {
    long timeout = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(10);
    while (true) {
      String state = ec2instance.getState().getName();
      if (!state.equals("running")) throw new RuntimeException("Illegal State: " + state);
      if (System.currentTimeMillis() > timeout) throw new RuntimeException("Timeout");
      try {
        JSch jSch = new JSch();
        jSch.addIdentity(username, keyPair.getKeyMaterial().getBytes(charset), keyPair.getKeyFingerprint().getBytes(charset), null);
        Session session = jSch.getSession(username, ec2instance.getPublicIpAddress());
        session.setConfig("StrictHostKeyChecking", "no");
        session.connect((int) TimeUnit.SECONDS.toMillis(15));
        return session;
      } catch (JSchException e) {
        logger.info("Awaiting instance connection", e);
        Thread.sleep(10000);
      }
    }
  }
  
  public static Instance start(final AmazonEC2 ec2, final String ami, final String instanceType, final String groupId, final KeyPair keyPair) {
    final AtomicReference<Instance> instance = new AtomicReference<>();
    instance.set(ec2.runInstances(new RunInstancesRequest()
      .withImageId(ami)
      .withInstanceType(instanceType)
      .withMinCount(1)
      .withMaxCount(1)
      .withKeyName(keyPair.getKeyName())
      .withSecurityGroupIds(groupId)
    ).getReservation().getInstances().get(0));
    while (instance.get().getState().getName().equals("pending")) {
      logger.info("Awaiting instance startup...");
      sleep(10000);
      instance.set(getInstance(ec2, instance.get()));
    }
    return instance.get();
  }
  
  public static boolean exists(final AmazonEC2 ec2, final String groupId) {
    try {
      return ec2.describeSecurityGroups(new DescribeSecurityGroupsRequest().withGroupIds(groupId)).getSecurityGroups().size() == 1;
    } catch (Throwable e) {
      return false;
    }
  }
  
  public static Instance getInstance(final AmazonEC2 ec2, final Instance instance) {
    return ec2.describeInstances(new DescribeInstancesRequest().withInstanceIds(instance.getInstanceId())).getReservations().get(0).getInstances().get(0);
  }
  
  @Nonnull
  public static String randomHex() {
    return Long.toHexString(FastRandom.INSTANCE.next()).substring(0, 5);
  }
  
  public static IpPermission getTcpPermission(final int port) {
    return new IpPermission()
      .withIpv4Ranges(Arrays.asList(new IpRange().withCidrIp("0.0.0.0/0")))
      .withIpProtocol("tcp")
      .withFromPort(port)
      .withToPort(port);
  }
  
  @Nonnull
  public String exec(final Session session, final String script) {
    try {
      ChannelExec channel = (ChannelExec) session.openChannel("exec");
      channel.setCommand(script);
      ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
      channel.setOutputStream(outBuffer);
      channel.setExtOutputStream(System.err);
      channel.connect();
      while (!(channel.isClosed() || channel.isEOF())) {
        Thread.sleep(100);
      }
      String output = new String(outBuffer.toByteArray(), charset);
      int exitStatus = channel.getExitStatus();
      if (0 != exitStatus) {
        logger.info(String.format("Exit Status: %d; Output:\n%s", exitStatus, output));
        throw new AssertionError("Exit Status: " + exitStatus);
      }
      return output;
    } catch (InterruptedException | JSchException e) {
      throw new RuntimeException(e);
    }
  }
  
  public static class EC2Node implements AutoCloseable {
    private final AmazonEC2 ec2;
    private final Session connection;
    private final String instanceId;
    
    public EC2Node(final AmazonEC2 ec2, final Session connection, final String instanceId) {
      this.ec2 = ec2;
      this.connection = connection;
      this.instanceId = instanceId;
    }
    
    public <T> T runAndTerminate(final Function<Session, T> task) {
      try {
        return task.apply(getConnection());
      } finally {
        terminate();
      }
    }
    
    public TerminateInstancesResult terminate() {
      return ec2.terminateInstances(new TerminateInstancesRequest().withInstanceIds(getInstanceId()));
    }
    
    public Session getConnection() {
      return connection;
    }
    
    public String getInstanceId() {
      return instanceId;
    }
    
    @Override
    public void close() {
      terminate();
    }
    
    public void shell() {
      EC2Util.shell(getConnection());
    }
  }
}
