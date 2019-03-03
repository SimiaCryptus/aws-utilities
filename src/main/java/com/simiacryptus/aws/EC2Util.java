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

import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.model.*;
import com.amazonaws.services.ec2.model.KeyPair;
import com.amazonaws.services.identitymanagement.AmazonIdentityManagement;
import com.amazonaws.services.identitymanagement.AmazonIdentityManagementClientBuilder;
import com.amazonaws.services.identitymanagement.model.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.util.StringInputStream;
import com.jcraft.jsch.*;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.CloseShieldOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.*;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * The type Ec 2 util.
 */
public class EC2Util {

  private static final Logger logger = LoggerFactory.getLogger(EC2Util.class);
  private static final Charset charset = Charset.forName("UTF-8");
  private static final Random random = new Random();
  public static final Regions REGION = Regions.fromName(System.getProperty("AWS_REGION", Regions.US_WEST_2.getName()));
  private static volatile KeyPair keyPair = null;

  /**
   * Stage.
   *
   * @param session        the session
   * @param file           the file
   * @param remote         the remote
   * @param bucket         the bucket
   * @param cacheNamespace the cacheLocal namespace
   * @param s3             the s 3
   */
  public static void stage(final Session session, final File file, final String remote, final String bucket, final String cacheNamespace, final AmazonS3 s3) {
    String key = cacheNamespace + remote;
    if (!s3.doesObjectExist(bucket, key)) {
      logger.info(String.format("Pushing to s3: %s/%s <= %s", bucket, key, file));
      s3.putObject(new PutObjectRequest(bucket, key, file));
    }
    logger.debug(String.format("Pulling from s3: %s/%s", bucket, key));
    exec(session, String.format("aws s3api get-object --bucket %s --key %s %s", bucket, key, remote));
  }

  public static String publicHostname() {
    try {
      return IOUtils.toString(new URI("http://169.254.169.254/latest/meta-data/public-hostname"), "UTF-8");
    } catch (Throwable e) {
      try {
        return InetAddress.getLocalHost().getHostName();
      } catch (UnknownHostException e1) {
        throw new RuntimeException(e1);
      }
    }
  }

  /**
   * Scp string.
   *
   * @param session the session
   * @param file    the file
   * @param remote  the remote
   * @return the string
   */
  @Nonnull
  public static String scp(final Session session, final File file, final String remote) {
    return scp(session, file, remote, 2);
  }

  /**
   * Scp string.
   *
   * @param session the session
   * @param file    the file
   * @param remote  the remote
   * @param retries the retries
   * @return the string
   */
  @Nonnull
  public static String scp(final Session session, final File file, final String remote, final int retries) {
    try {
      assert file.exists();
      ChannelExec channel = (ChannelExec) session.openChannel("exec");
      channel.setCommand(String.format("scp -t %s", remote));
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      channel.setOutputStream(out);
      channel.setExtOutputStream(new CloseShieldOutputStream(System.err));
      String header = String.format("C0644 %d %s\n", file.length(), Arrays.stream(remote.split("/")).reduce((a, b) -> b).get());
      channel.setInputStream(Arrays.asList(
          new StringInputStream(header),
          new FileInputStream(file),
          new ByteArrayInputStream(new byte[]{0})
      ).stream().reduce((a, b) -> new SequenceInputStream(a, b)).get());
      channel.connect();
      join((Channel) channel);
      int exitStatus = channel.getExitStatus();
      if (0 != exitStatus) {
        String msg = String.format("Error Exit Code %d while copying %s to %s; log: %s", exitStatus, file, remote, new String(out.toByteArray(), charset));
        logger.warn(msg);
        if (retries > 0) return scp(session, file, remote, retries - 1);
        throw new RuntimeException(msg);
      }
      return new String(out.toByteArray(), charset);
    } catch (Throwable e) {
      if (retries > 0) return scp(session, file, remote, retries - 1);
      throw new RuntimeException(e);
    }
  }

  /**
   * Shell int.
   *
   * @param session the session
   * @return the int
   */
  @Nullable
  public static int shell(final Session session) {
    try {
      Channel channel = session.openChannel("shell");
      channel.setOutputStream(new CloseShieldOutputStream(System.out));
      channel.setExtOutputStream(new CloseShieldOutputStream(System.err));
      channel.setInputStream(System.in);
      channel.connect();
      join(channel);
      int exitStatus = channel.getExitStatus();
      if (0 != exitStatus) {
        logger.warn("Shell Exit Status: " + exitStatus);
      }
      return exitStatus;
    } catch (InterruptedException | JSchException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Join.
   *
   * @param channel the channel
   * @throws InterruptedException the interrupted exception
   */
  public static void join(final Channel channel) throws InterruptedException {
    while (!channel.isClosed()) {
      Thread.sleep(100);
    }
  }

  /**
   * Run t.
   *
   * @param <T>              the type parameter
   * @param ec2              the ec 2
   * @param imageId          the image id
   * @param instanceType     the instance type
   * @param username         the username
   * @param task             the task
   * @param bucket           the bucket
   * @param localControlPort the local control port
   * @param ports            the ports
   * @return the t
   */
  public static <T> T run(final AmazonEC2 ec2, final String imageId, final String instanceType, final String username, Function<Session, T> task, final String bucket, final int localControlPort, final int... ports) {
    return start(ec2, imageId, instanceType, username, AmazonIdentityManagementClientBuilder.defaultClient(), bucket, localControlPort, ports).runAndTerminate(task);
  }

  /**
   * Start ec 2 node.
   *
   * @param ec2              the ec 2
   * @param imageId          the image id
   * @param instanceType     the instance type
   * @param username         the username
   * @param iam              the iam
   * @param bucket           the bucket
   * @param localControlPort the local control port
   * @param ports            the ports
   * @return the ec 2 node
   */
  @Nonnull
  public static EC2Node start(final AmazonEC2 ec2, final String imageId, final String instanceType, final String username, final AmazonIdentityManagement iam, final String bucket, final int localControlPort, final int... ports) {
    return start(ec2, imageId, instanceType, username, newIamRole(iam, ("{\n" +
        "  \"Version\": \"2012-10-17\",\n" +
        "  \"Statement\": [\n" +
        "    {\n" +
        "      \"Action\": \"s3:*\",\n" +
        "      \"Effect\": \"Allow\",\n" +
        "      \"Resource\": \"arn:aws:s3:::BUCKET*\"\n" +
        "    }\n" +
        "  ]\n" +
        "}").replaceAll("BUCKET", bucket)), localControlPort, ports);
  }

  /**
   * Start ec 2 node.
   *
   * @param ec2              the ec 2
   * @param imageId          the image id
   * @param instanceType     the instance type
   * @param username         the username
   * @param iam              the iam
   * @param localControlPort the local control port
   * @param ports            the ports
   * @return the ec 2 node
   */
  @Nonnull
  public static EC2Node start(final AmazonEC2 ec2, final String imageId, final String instanceType, final String username, final InstanceProfile iam, final int localControlPort, final int... ports) {
    return start(ec2, imageId, instanceType, username, getKeyPair(ec2), newSecurityGroup(ec2, ports), iam, localControlPort);
  }

  /**
   * Start ec 2 node.
   *
   * @param ec2              the ec 2
   * @param imageId          the image id
   * @param instanceType     the instance type
   * @param username         the username
   * @param keyPair          the key pair
   * @param groupId          the group id
   * @param instanceProfile  the instance profile
   * @param localControlPort the local control port
   * @return the ec 2 node
   */
  @Nullable
  public static EC2Node start(final AmazonEC2 ec2, final String imageId, final String instanceType, final String username, final KeyPair keyPair, final String groupId, final InstanceProfile instanceProfile, final int localControlPort) {
    Instance instance = start(ec2, imageId, instanceType, groupId, keyPair, instanceProfile);
    try {
      return new EC2Node(ec2, connect(keyPair, username, instance, localControlPort), instance.getInstanceId());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return null;
    }
  }

  /**
   * Start ec 2 node.
   *
   * @param ec2          the ec 2
   * @param imageId      the image id
   * @param instanceType the instance type
   * @param username     the username
   * @param keyPair      the key pair
   * @param groupId      the group id
   * @param iam          the iam
   * @param bucket       the bucket
   * @return the ec 2 node
   */
  @Nullable
  public static EC2Node start(final AmazonEC2 ec2, final String imageId, final String instanceType, final String username, final KeyPair keyPair, final String groupId, final AmazonIdentityManagement iam, final String bucket) {
    return start(ec2, imageId, instanceType, username, keyPair, groupId, newIamRole(iam, ("{\n" +
        "  \"Version\": \"2012-10-17\",\n" +
        "  \"Statement\": [\n" +
        "    {\n" +
        "      \"Action\": \"s3:*\",\n" +
        "      \"Effect\": \"Allow\",\n" +
        "      \"Resource\": \"arn:aws:s3:::BUCKET*\"\n" +
        "    }\n" +
        "  ]\n" +
        "}").replaceAll("BUCKET", bucket)), 1319);
  }

  /**
   * Gets key pair.
   *
   * @param ec2 the ec 2
   * @return the key pair
   */
  @Nonnull
  public static KeyPair getKeyPair(final AmazonEC2 ec2) {
    if (null == keyPair) {
      synchronized (EC2Util.class) {
        if (null == keyPair) {
          KeyPair key = Arrays.stream(new File(".").listFiles()).filter(x -> x.getName().endsWith(".pem")).map(pem -> {
            String[] split = pem.getName().split("\\.");
            return split.length > 0 ? split[0] : "";
          }).filter(x -> !x.isEmpty()).map(keyName -> loadKey(ec2, keyName)).filter(x -> x != null).findFirst().orElse(null);
          if (null != key) {
            keyPair = key;
          } else {
            String keyName = "key_" + randomHex();
            logger.info("Creating Key Pair: " + keyName);
            keyPair = ec2.createKeyPair(new CreateKeyPairRequest().withKeyName(keyName)).getKeyPair();
            try {
              FileUtils.write(new File(keyPair.getKeyName() + ".pem"), keyPair.getKeyMaterial(), "UTF-8");
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
        }
      }
    }
    return keyPair;
  }

  /**
   * Load key key pair.
   *
   * @param ec2      the ec 2
   * @param keyNames the key names
   * @return the key pair
   */
  @Nullable
  public static KeyPair loadKey(final AmazonEC2 ec2, final String keyNames) {
    try {
      KeyPairInfo localKeyPair = ec2.describeKeyPairs(new DescribeKeyPairsRequest().withKeyNames(keyNames)).getKeyPairs().get(0);
      String pemData = FileUtils.readFileToString(new File(localKeyPair.getKeyName() + ".pem"), charset);
      logger.info("Loaded Key Pair: " + localKeyPair);
      return new KeyPair().withKeyName(localKeyPair.getKeyName()).withKeyFingerprint(localKeyPair.getKeyFingerprint()).withKeyMaterial(pemData);
    } catch (Throwable e) {
      logger.warn("Error loading local keys");
      return null;
    }
  }

  /**
   * New iam role instance profile.
   *
   * @param iam            the iam
   * @param policyDocument the policy document
   * @return the instance profile
   */
  @Nonnull
  public static InstanceProfile newIamRole(final AmazonIdentityManagement iam, final String policyDocument) {
    String initialDocument = "{\n" +
        "               \"Version\" : \"2012-10-17\",\n" +
        "               \"Statement\": [ {\n" +
        "                  \"Effect\": \"Allow\",\n" +
        "                  \"Principal\": {\n" +
        "                     \"Service\": [ \"ec2.amazonaws.com\" ]\n" +
        "                  },\n" +
        "                  \"Action\": [ \"sts:AssumeRole\" ]\n" +
        "               } ]\n" +
        "            }";
    Role role = iam.createRole(new CreateRoleRequest()
        .withRoleName("role_" + randomHex())
        .withAssumeRolePolicyDocument(initialDocument)
    ).getRole();
    while (!getRole(iam, role.getRoleName()).isPresent()) sleep(10000);
    Policy policy = iam.createPolicy(new CreatePolicyRequest()
        .withPolicyDocument(policyDocument.replaceAll("ROLEARN", role.getArn()))
        .withPolicyName("policy-" + randomHex())
    ).getPolicy();
    iam.attachRolePolicy(new AttachRolePolicyRequest().withPolicyArn(policy.getArn()).withRoleName(role.getRoleName()));
    InstanceProfile instanceProfile = iam.createInstanceProfile(new CreateInstanceProfileRequest().withInstanceProfileName("runpol-" + randomHex())).getInstanceProfile();
    iam.addRoleToInstanceProfile(new AddRoleToInstanceProfileRequest()
        .withInstanceProfileName(instanceProfile.getInstanceProfileName())
        .withRoleName(role.getRoleName())
    );
    return instanceProfile;
  }

  /**
   * Gets role.
   *
   * @param iam      the iam
   * @param roleName the role name
   * @return the role
   */
  public static Optional<GetRoleResult> getRole(final AmazonIdentityManagement iam, final String roleName) {
    try {
      return Optional.of(iam.getRole(new GetRoleRequest().withRoleName(roleName)));
    } catch (Throwable e) {
      return Optional.empty();
    }
  }

  /**
   * New security group string.
   *
   * @param ec2   the ec 2
   * @param ports the ports
   * @return the string
   */
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
        .withIpPermissions(Stream.concat(
            Arrays.stream(ports).mapToObj(port -> getTcpPermission(port)),
            Stream.of(new IpPermission()
                .withUserIdGroupPairs(new UserIdGroupPair().withGroupId(groupId))
                .withIpProtocol("tcp")
                .withFromPort(0)
                .withToPort(0xFFFF))
        ).toArray(i -> new IpPermission[i])));
    return groupId;
  }

  /**
   * Connect session.
   *
   * @param keyPair          the key pair
   * @param username         the username
   * @param ec2instance      the ec 2 instance
   * @param localControlPort the local control port
   * @return the session
   * @throws InterruptedException the interrupted exception
   */
  @Nonnull
  public static Session connect(final KeyPair keyPair, final String username, final Instance ec2instance, final int localControlPort) throws InterruptedException {
    long timeout = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(10);
    while (true) {
      String state = ec2instance.getState().getName();
      if (!state.equals("running")) throw new RuntimeException("Illegal State: " + state);
      if (System.currentTimeMillis() > timeout) throw new RuntimeException("Timeout");
      Session session = null;
      try {
        JSch jSch = new JSch();
        logger.info(String.format("Connecting to %s with key %s", ec2instance.getPublicIpAddress(), keyPair.getKeyFingerprint()));
        jSch.addIdentity(username, keyPair.getKeyMaterial().getBytes(charset), keyPair.getKeyFingerprint().getBytes(charset), null);
        session = jSch.getSession(username, ec2instance.getPublicIpAddress());
        session.setConfig("StrictHostKeyChecking", "no");
        if (0 < localControlPort) session.setPortForwardingL(localControlPort, "127.0.0.1", 1318);
        session.connect((int) TimeUnit.SECONDS.toMillis(15));
        return session;
      } catch (JSchException e) {
        logger.info("Awaiting instance connection: " + e.getMessage());
        if (null != session) {
          try {
            if (0 < localControlPort) session.delPortForwardingL(localControlPort);
          } catch (JSchException e1) {
            logger.debug("Error cleaning up", e1);
          }
          session.disconnect();
          session = null;
        }
        Thread.sleep(10000);
      }
    }
  }

  /**
   * Start instance.
   *
   * @param ec2          the ec 2
   * @param ami          the ami
   * @param instanceType the instance type
   * @param groupId      the group id
   * @param keyPair      the key pair
   * @param iam          the iam
   * @param bucket       the bucket
   * @return the instance
   */
  public static Instance start(final AmazonEC2 ec2, final String ami, final String instanceType, final String groupId, final KeyPair keyPair, final AmazonIdentityManagement iam, final String bucket) {
    return start(ec2, ami, instanceType, groupId, keyPair, newIamRole(iam, ("{\n" +
        "  \"Version\": \"2012-10-17\",\n" +
        "  \"Statement\": [\n" +
        "    {\n" +
        "      \"Action\": \"s3:*\",\n" +
        "      \"Effect\": \"Allow\",\n" +
        "      \"Resource\": \"arn:aws:s3:::BUCKET*\"\n" +
        "    }\n" +
        "  ]\n" +
        "}").replaceAll("BUCKET", bucket)));
  }

  /**
   * Sleep.
   *
   * @param millis the millis
   */
  public static void sleep(final int millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      logger.info("Interuptted");
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Exists boolean.
   *
   * @param ec2     the ec 2
   * @param groupId the group id
   * @return the boolean
   */
  public static boolean exists(final AmazonEC2 ec2, final String groupId) {
    try {
      return ec2.describeSecurityGroups(new DescribeSecurityGroupsRequest().withGroupIds(groupId)).getSecurityGroups().size() == 1;
    } catch (Throwable e) {
      return false;
    }
  }

  /**
   * Gets instance.
   *
   * @param ec2      the ec 2
   * @param instance the instance
   * @return the instance
   */
  public static Instance getInstance(final AmazonEC2 ec2, final Instance instance) {
    return ec2.describeInstances(new DescribeInstancesRequest().withInstanceIds(instance.getInstanceId())).getReservations().get(0).getInstances().get(0);
  }

  /**
   * Start instance.
   *
   * @param ec2             the ec 2
   * @param ami             the ami
   * @param instanceType    the instance type
   * @param groupId         the group id
   * @param keyPair         the key pair
   * @param instanceProfile the instance profile
   * @return the instance
   */
  public static Instance start(final AmazonEC2 ec2, final String ami, final String instanceType, final String groupId, final KeyPair keyPair, final InstanceProfile instanceProfile) {
    final AtomicReference<Instance> instance = new AtomicReference<>();

    instance.set(ec2.runInstances(new RunInstancesRequest()
            .withImageId(ami)
            .withInstanceType(instanceType)
            .withIamInstanceProfile(new IamInstanceProfileSpecification()
                    .withArn(instanceProfile.getArn())
//        .withName(role.getInstanceProfileName())
            )
            .withInstanceInitiatedShutdownBehavior(ShutdownBehavior.Terminate)
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

  /**
   * Random hex string.
   *
   * @return the string
   */
  @Nonnull
  public static String randomHex() {
    return Long.toHexString(random.nextLong()).substring(0, 5);
  }

  /**
   * Gets tcp permission.
   *
   * @param port the port
   * @return the tcp permission
   */
  public static IpPermission getTcpPermission(final int port) {
    return new IpPermission()
        .withIpv4Ranges(Arrays.asList(new IpRange().withCidrIp("0.0.0.0/0")))
        .withIpProtocol("tcp")
        .withFromPort(port)
        .withToPort(port);
  }

  /**
   * Exec string.
   *
   * @param session the session
   * @param script  the script
   * @return the string
   */
  @Nonnull
  public static String exec(final Session session, final String script) {
    try {
      logger.debug("Executing: " + script);
      Process process = execAsync(session, script, new HashMap<String, String>());
      join(process.getChannel());
      String output = new String(process.getOutBuffer().toByteArray(), charset);
      int exitStatus = process.getChannel().getExitStatus();
      if (0 != exitStatus) {
        logger.info(String.format("Exit Status: %d; Output:\n%s", exitStatus, output));
        throw new AssertionError("Exit Status: " + exitStatus);
      }
      return output;
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Exec async process.
   *
   * @param session the session
   * @param script  the script
   * @param env
   * @return the process
   */
  @Nonnull
  public static Process execAsync(final Session session, final String script, HashMap<String, String> env) {
    return execAsync(session, script, new ByteArrayOutputStream(), env);
  }

  /**
   * Exec async process.
   *
   * @param session   the session
   * @param script    the script
   * @param outBuffer the out buffer
   * @param env
   * @return the process
   */
  @Nonnull
  public static Process execAsync(final Session session, final String script, final OutputStream outBuffer, HashMap<String, String> env) {
    try {
      return new Process(session, script, outBuffer, env);
    } catch (JSchException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Join.
   *
   * @param channel the channel
   * @throws InterruptedException the interrupted exception
   */
  public static void join(final ChannelExec channel) throws InterruptedException {
    while (!(channel.isClosed() || channel.isEOF())) {
      Thread.sleep(100);
    }
  }

  /**
   * Start ec 2 node.
   *
   * @param ec2              the ec 2
   * @param jvmConfig        the jvm config
   * @param serviceConfig    the service config
   * @param localControlPort the local control port
   * @return the ec 2 node
   */
  public static EC2Node start(final AmazonEC2 ec2, final NodeConfig jvmConfig, final ServiceConfig serviceConfig, final int localControlPort) {
    return start(ec2, jvmConfig.imageId, jvmConfig.instanceType, jvmConfig.username, serviceConfig.keyPair, serviceConfig.groupId, serviceConfig.instanceProfile, localControlPort);
  }

  /**
   * The type Ec 2 node.
   */
  public static class EC2Node implements AutoCloseable {
    private final AmazonEC2 ec2;
    private final Session connection;
    private final String instanceId;

    /**
     * Instantiates a new Ec 2 node.
     *
     * @param ec2        the ec 2
     * @param connection the connection
     * @param instanceId the instance id
     */
    public EC2Node(final AmazonEC2 ec2, final Session connection, final String instanceId) {
      this.ec2 = ec2;
      this.connection = connection;
      this.instanceId = instanceId;
    }

    /**
     * Start jvm tendril . tendril control.
     *
     * @param ec2              the ec 2
     * @param s3               the s 3
     * @param settings         the settings
     * @param localControlPort the local control port
     * @return the tendril . tendril control
     */
    public TendrilControl startJvm(
        final AmazonEC2 ec2,
        final AmazonS3 s3,
        final AwsTendrilNodeSettings settings,
        final int localControlPort
    ) {
      return Tendril.startRemoteJvm(this,
          settings.newJvmConfig(),
          localControlPort,
          Tendril::defaultClasspathFilter,
          s3,
          settings.getServiceConfig(ec2).bucket,
          new HashMap<String, String>()
      );
    }

    /**
     * Run and terminate t.
     *
     * @param <T>  the type parameter
     * @param task the task
     * @return the t
     */
    public <T> T runAndTerminate(final Function<Session, T> task) {
      try {
        return task.apply(getConnection());
      } finally {
        terminate();
      }
    }

    /**
     * Terminate terminate instances result.
     *
     * @return the terminate instances result
     */
    public TerminateInstancesResult terminate() {
      logger.info("Terminating " + getInstanceId());
      return ec2.terminateInstances(new TerminateInstancesRequest().withInstanceIds(getInstanceId()));
    }

    /**
     * Gets connection.
     *
     * @return the connection
     */
    public Session getConnection() {
      return connection;
    }

    /**
     * Gets instance id.
     *
     * @return the instance id
     */
    public String getInstanceId() {
      return instanceId;
    }

    @Override
    public void close() {
      terminate();
    }

    /**
     * Shell int.
     *
     * @return the int
     */
    public int shell() {
      return EC2Util.shell(getConnection());
    }

    /**
     * Scp string.
     *
     * @param local  the local
     * @param remote the remote
     * @return the string
     */
    public String scp(final File local, final String remote) {
      return EC2Util.scp(getConnection(), local, remote);
    }

    /**
     * Exec string.
     *
     * @param command the command
     * @return the string
     */
    public String exec(final String command) {
      return EC2Util.exec(getConnection(), command);
    }

    /**
     * Exec async process.
     *
     * @param command the command
     * @return the process
     */
    public Process execAsync(final String command) {
      return EC2Util.execAsync(getConnection(), command, new HashMap<String, String>());
    }

    /**
     * Stage.
     *
     * @param entryFile the entry file
     * @param remote    the remote
     * @param bucket    the bucket
     * @param keyspace  the keyspace
     * @param s3        the s 3
     */
    public void stage(final File entryFile, final String remote, final String bucket, final String keyspace, final AmazonS3 s3) {
      EC2Util.stage(getConnection(), entryFile, remote, bucket, keyspace, s3);
    }

    /**
     * Gets status.
     *
     * @return the status
     */
    public Instance getStatus() {
      return ec2.describeInstances(new DescribeInstancesRequest().withInstanceIds(getInstanceId())).getReservations().get(0).getInstances().get(0);
    }
  }

  /**
   * The type Process.
   */
  public static class Process {
    private final ChannelExec channel;
    private final OutputStream outBuffer;

    /**
     * Instantiates a new Process.
     *
     * @param session   the session
     * @param script    the script
     * @param outBuffer the out buffer
     * @param env
     * @throws JSchException the j sch exception
     */
    public Process(final Session session, final String script, final OutputStream outBuffer, HashMap<String, String> env) throws JSchException {
      channel = (ChannelExec) session.openChannel("exec");
      channel.setCommand(script);
      this.outBuffer = outBuffer;
      channel.setOutputStream(this.outBuffer);
      env.forEach((k, v) -> channel.setEnv(k, v));
      channel.setExtOutputStream(new CloseShieldOutputStream(System.err));
      channel.connect();
    }

    /**
     * Gets channel.
     *
     * @return the channel
     */
    public ChannelExec getChannel() {
      return channel;
    }

    /**
     * Gets out buffer.
     *
     * @return the out buffer
     */
    public ByteArrayOutputStream getOutBuffer() {
      return (ByteArrayOutputStream) outBuffer;
    }

    /**
     * Join string.
     *
     * @return the string
     */
    public String join() {
      try {
        EC2Util.join(channel);
        return getOutput();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    /**
     * Gets output.
     *
     * @return the output
     */
    @Nonnull
    public String getOutput() {
      return new String(getOutBuffer().toByteArray(), charset);
    }
  }

  /**
   * The type Service config.
   */
  public static class ServiceConfig {
    /**
     * The Bucket.
     */
    public String bucket;
    /**
     * The Instance profile.
     */
    public InstanceProfile instanceProfile;
    /**
     * The Group id.
     */
    public String groupId;
    /**
     * The Key pair.
     */
    public KeyPair keyPair;

    /**
     * Instantiates a new Service config.
     *
     * @param ec2     the ec 2
     * @param bucket  the bucket
     * @param roleArn the role arn
     */
    public ServiceConfig(final AmazonEC2 ec2, final String bucket, final String roleArn) {
      this(
          ec2,
          bucket,
          roleArn,
          EC2Util.newSecurityGroup(ec2, 22, 1080, 4040, 8080)
      );
    }

    /**
     * Instantiates a new Service config.
     *
     * @param ec2     the ec 2
     * @param bucket  the bucket
     * @param roleArn the role arn
     * @param groupId the group id
     */
    public ServiceConfig(final AmazonEC2 ec2, final String bucket, final String roleArn, final String groupId) {
      this(ec2, bucket, groupId, new InstanceProfile().withArn(roleArn));
    }

    /**
     * Instantiates a new Service config.
     *
     * @param ec2             the ec 2
     * @param bucket          the bucket
     * @param groupId         the group id
     * @param instanceProfile the instance profile
     */
    public ServiceConfig(final AmazonEC2 ec2, final String bucket, final String groupId, final InstanceProfile instanceProfile) {
      this(bucket, groupId, instanceProfile, EC2Util.getKeyPair(ec2));
    }

    /**
     * Instantiates a new Service config.
     *
     * @param bucket          the bucket
     * @param groupId         the group id
     * @param instanceProfile the instance profile
     * @param keyPair         the key pair
     */
    public ServiceConfig(final String bucket, final String groupId, final InstanceProfile instanceProfile, final KeyPair keyPair) {
      this.bucket = bucket;
      this.groupId = groupId;
      this.instanceProfile = instanceProfile;
      this.keyPair = keyPair;
    }
  }

  /**
   * The type Node config.
   */
  public static class NodeConfig {
    /**
     * The Image id.
     */
    public String imageId;
    /**
     * The Instance type.
     */
    public String instanceType;
    /**
     * The Username.
     */
    public String username;

    /**
     * Instantiates a new Node config.
     *
     * @param imageId      the image id
     * @param instanceType the instance type
     * @param username     the username
     */
    public NodeConfig(final String imageId, final String instanceType, final String username) {
      this.imageId = imageId;
      this.instanceType = instanceType;
      this.username = username;
    }
  }

}
