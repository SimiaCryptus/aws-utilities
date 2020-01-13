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

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.model.KeyPair;
import com.amazonaws.services.ec2.model.*;
import com.amazonaws.services.identitymanagement.AmazonIdentityManagement;
import com.amazonaws.services.identitymanagement.AmazonIdentityManagementClientBuilder;
import com.amazonaws.services.identitymanagement.model.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.util.StringInputStream;
import com.jcraft.jsch.*;
import com.simiacryptus.ref.lang.RefAware;
import com.simiacryptus.ref.lang.RefIgnore;
import com.simiacryptus.ref.lang.RefUtil;
import com.simiacryptus.ref.wrappers.*;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.CloseShieldOutputStream;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.*;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

public class EC2Util {

  public static final Regions REGION = Regions
      .fromName(com.simiacryptus.ref.wrappers.RefSystem.getProperty("AWS_REGION", getCurrentRegion()));
  private static final Logger logger = LoggerFactory.getLogger(EC2Util.class);
  private static final Charset charset = Charset.forName("UTF-8");
  private static final Random random = new Random();
  private static volatile KeyPair keyPair = null;

  private static String getCurrentRegion() {
    try {
      Region currentRegion = Regions.getCurrentRegion();
      if (null == currentRegion)
        return Regions.US_EAST_1.getName();
      return currentRegion.getName();
    } catch (Throwable e) {
      return Regions.US_EAST_1.getName();
    }
  }

  public static void stage(final Session session, final File file, final String remote, final String bucket,
      final String cacheNamespace, final AmazonS3 s3) {
    String key = cacheNamespace + remote;
    if (!s3.doesObjectExist(bucket, key)) {
      logger.info(RefString.format("Pushing to s3: %s/%s <= %s", bucket, key, file));
      s3.putObject(new PutObjectRequest(bucket, key, file));
    }
    logger.debug(RefString.format("Pulling from s3: %s/%s", bucket, key));
    exec(session, RefString.format("aws s3api get-object --bucket %s --key %s %s", bucket, key, remote));
  }

  public static String publicHostname() {
    try {
      return getInstanceMetadata("public-hostname");
    } catch (Throwable e) {
      try {
        return InetAddress.getLocalHost().getHostName();
      } catch (UnknownHostException e1) {
        throw new RuntimeException(e1);
      }
    }
  }

  public static String instanceId() {
    try {
      return getInstanceMetadata("instance-id");
    } catch (Throwable e) {
      return "";
    }
  }

  public static String getInstanceMetadata(String key) throws IOException {
    try {
      return IOUtils.toString(new URI("http://169.254.169.254/latest/meta-data/" + key), "UTF-8");
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  @Nonnull
  public static String scp(final Session session, final File file, final String remote) {
    return scp(session, file, remote, 2);
  }

  @Nonnull
  public static String scp(final Session session, final File file, final String remote, final int retries) {
    try {
      assert file.exists();
      ChannelExec channel = (ChannelExec) session.openChannel("exec");
      channel.setCommand(RefString.format("scp -t %s", remote));
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      channel.setOutputStream(out);
      channel.setExtOutputStream(new CloseShieldOutputStream(com.simiacryptus.ref.wrappers.RefSystem.err));
      String header = RefString.format("C0644 %d %s\n", file.length(),
          RefUtil.get(RefArrays.stream(remote.split("/")).reduce((a, b) -> b)));
      RefList<InputStream> temp_06_0003 = RefArrays.asList(new StringInputStream(header), new FileInputStream(file),
          new ByteArrayInputStream(new byte[] { 0 }));
      channel.setInputStream(RefUtil.get(temp_06_0003.stream().reduce((a, b) -> new SequenceInputStream(a, b))));
      if (null != temp_06_0003)
        temp_06_0003.freeRef();
      channel.connect();
      join((Channel) channel);
      int exitStatus = channel.getExitStatus();
      if (0 != exitStatus) {
        String msg = RefString.format("Error Exit Code %d while copying %s to %s; log: %s", exitStatus, file, remote,
            new String(out.toByteArray(), charset));
        logger.warn(msg);
        if (retries > 0)
          return scp(session, file, remote, retries - 1);
        throw new RuntimeException(msg);
      }
      return new String(out.toByteArray(), charset);
    } catch (Throwable e) {
      if (retries > 0)
        return scp(session, file, remote, retries - 1);
      throw new RuntimeException(e);
    }
  }

  @Nullable
  public static int shell(final Session session) {
    try {
      Channel channel = session.openChannel("shell");
      channel.setOutputStream(new CloseShieldOutputStream(com.simiacryptus.ref.wrappers.RefSystem.out));
      channel.setExtOutputStream(new CloseShieldOutputStream(com.simiacryptus.ref.wrappers.RefSystem.err));
      channel.setInputStream(com.simiacryptus.ref.wrappers.RefSystem.in);
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

  public static void join(final Channel channel) throws InterruptedException {
    while (!channel.isClosed()) {
      Thread.sleep(100);
    }
  }

  public static <T> T run(final AmazonEC2 ec2, final String imageId, final String instanceType, final String username,
      Function<Session, T> task, final String bucket, final int localControlPort, final int... ports) {
    return start(ec2, imageId, instanceType, username, AmazonIdentityManagementClientBuilder.defaultClient(), bucket,
        localControlPort, ports).runAndTerminate(task);
  }

  @Nonnull
  public static EC2Node start(final AmazonEC2 ec2, final String imageId, final String instanceType,
      final String username, final AmazonIdentityManagement iam, final String bucket, final int localControlPort,
      final int... ports) {
    return start(ec2, imageId, instanceType, username,
        newIamRole(iam, ("{\n" + "  \"Version\": \"2012-10-17\",\n" + "  \"Statement\": [\n" + "    "
            + bucketGrantStr(bucket) + "\n" + "  ]\n" + "}").replaceAll("BUCKET", bucket)),
        localControlPort, ports);
  }

  @Nonnull
  public static EC2Node start(final AmazonEC2 ec2, final String imageId, final String instanceType,
      final String username, final InstanceProfile iam, final int localControlPort, final int... ports) {
    return start(ec2, imageId, instanceType, username, getKeyPair(ec2), newSecurityGroup(ec2, ports), iam,
        localControlPort);
  }

  @Nullable
  public static EC2Node start(final AmazonEC2 ec2, final String imageId, final String instanceType,
      final String username, final KeyPair keyPair, final String groupId, final InstanceProfile instanceProfile,
      final int localControlPort) {
    Instance instance = start(ec2, imageId, instanceType, groupId, keyPair, instanceProfile);
    try {
      return new EC2Node(ec2, connect(keyPair, username, instance, localControlPort), instance.getInstanceId());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return null;
    }
  }

  @Nullable
  public static EC2Node start(final AmazonEC2 ec2, final String imageId, final String instanceType,
      final String username, final KeyPair keyPair, final String groupId, final AmazonIdentityManagement iam,
      final String bucket) {
    return start(ec2, imageId, instanceType, username, keyPair, groupId,
        newIamRole(iam, ("{\n" + "  \"Version\": \"2012-10-17\",\n" + "  \"Statement\": [\n" + "    "
            + bucketGrantStr(bucket) + "\n" + "  ]\n" + "}").replaceAll("BUCKET", bucket)),
        1319);
  }

  @Nonnull
  public static KeyPair getKeyPair(final AmazonEC2 ec2) {
    if (null == keyPair) {
      synchronized (EC2Util.class) {
        if (null == keyPair) {
          KeyPair key = RefArrays.stream(new File(".").listFiles()).filter(x -> x.getName().endsWith(".pem"))
              .map(pem -> {
                String[] split = pem.getName().split("\\.");
                return split.length > 0 ? split[0] : "";
              }).filter(x -> !x.isEmpty()).map(keyName -> loadKey(ec2, keyName)).filter(x -> x != null).findFirst()
              .orElse(null);
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

  @Nullable
  public static KeyPair loadKey(final AmazonEC2 ec2, final String keyNames) {
    try {
      KeyPairInfo localKeyPair = ec2.describeKeyPairs(new DescribeKeyPairsRequest().withKeyNames(keyNames))
          .getKeyPairs().get(0);
      String pemData = FileUtils.readFileToString(new File(localKeyPair.getKeyName() + ".pem"), charset);
      logger.info("Loaded Key Pair: " + localKeyPair);
      return new KeyPair().withKeyName(localKeyPair.getKeyName()).withKeyFingerprint(localKeyPair.getKeyFingerprint())
          .withKeyMaterial(pemData);
    } catch (Throwable e) {
      logger.warn("Error loading local keys");
      return null;
    }
  }

  @Nonnull
  public static InstanceProfile newIamRole(final AmazonIdentityManagement iam, final String policyDocument) {
    String initialDocument = "{\n" + "               \"Version\" : \"2012-10-17\",\n"
        + "               \"Statement\": [ {\n" + "                  \"Effect\": \"Allow\",\n"
        + "                  \"Principal\": {\n" + "                     \"Service\": [ \"ec2.amazonaws.com\" ]\n"
        + "                  },\n" + "                  \"Action\": [ \"sts:AssumeRole\" ]\n" + "               } ]\n"
        + "            }";
    String id = randomHex();
    Role role = iam
        .createRole(new CreateRoleRequest().withRoleName("role_" + id).withAssumeRolePolicyDocument(initialDocument))
        .getRole();
    while (!getRole(iam, role.getRoleName()).isPresent())
      sleep(10000);
    Policy policy = iam.createPolicy(new CreatePolicyRequest()
        .withPolicyDocument(policyDocument.replaceAll("ROLEARN", role.getArn())).withPolicyName("policy-" + id))
        .getPolicy();
    iam.attachRolePolicy(new AttachRolePolicyRequest().withPolicyArn(policy.getArn()).withRoleName(role.getRoleName()));
    InstanceProfile instanceProfile = iam
        .createInstanceProfile(new CreateInstanceProfileRequest().withInstanceProfileName("runpol-" + id))
        .getInstanceProfile();
    iam.addRoleToInstanceProfile(new AddRoleToInstanceProfileRequest()
        .withInstanceProfileName(instanceProfile.getInstanceProfileName()).withRoleName(role.getRoleName()));
    return instanceProfile;
  }

  public static Optional<GetRoleResult> getRole(final AmazonIdentityManagement iam, final String roleName) {
    try {
      return Optional.of(iam.getRole(new GetRoleRequest().withRoleName(roleName)));
    } catch (Throwable e) {
      return Optional.empty();
    }
  }

  public static String newSecurityGroup(final AmazonEC2 ec2, int... ports) {
    String groupName = "sg_" + randomHex();
    String groupId = ec2.createSecurityGroup(new CreateSecurityGroupRequest().withGroupName(groupName)
        .withDescription("Created by " + EC2Util.class.getCanonicalName())).getGroupId();
    while (!exists(ec2, groupId)) {
      logger.info("Awaiting security group creation...");
      sleep(10000);
    }
    ec2.authorizeSecurityGroupIngress(
        new AuthorizeSecurityGroupIngressRequest().withGroupId(groupId)
            .withIpPermissions(
                RefStream
                    .concat(RefArrays.stream(ports).mapToObj(port -> getTcpPermission(port)),
                        RefStream.of(new IpPermission().withUserIdGroupPairs(new UserIdGroupPair().withGroupId(groupId))
                            .withIpProtocol("tcp").withFromPort(0).withToPort(0xFFFF)))
                    .toArray(i -> new IpPermission[i])));
    return groupId;
  }

  @Nonnull
  public static Session connect(final KeyPair keyPair, final String username, final Instance ec2instance,
      final int localControlPort) throws InterruptedException {
    long timeout = com.simiacryptus.ref.wrappers.RefSystem.currentTimeMillis() + TimeUnit.MINUTES.toMillis(10);
    while (true) {
      String state = ec2instance.getState().getName();
      if (!state.equals("running"))
        throw new RuntimeException("Illegal State: " + state);
      if (com.simiacryptus.ref.wrappers.RefSystem.currentTimeMillis() > timeout)
        throw new RuntimeException("Timeout");
      Session session = null;
      try {
        JSch jSch = new JSch();
        logger.info(RefString.format("Connecting to %s with key %s", ec2instance.getPublicIpAddress(),
            keyPair.getKeyFingerprint()));
        jSch.addIdentity(username, keyPair.getKeyMaterial().getBytes(charset),
            keyPair.getKeyFingerprint().getBytes(charset), null);
        session = jSch.getSession(username, ec2instance.getPublicIpAddress());
        session.setConfig("StrictHostKeyChecking", "no");
        if (0 < localControlPort)
          session.setPortForwardingL(localControlPort, "127.0.0.1", 1318);
        session.connect((int) TimeUnit.SECONDS.toMillis(15));
        return session;
      } catch (JSchException e) {
        logger.info("Awaiting instance connection: " + e.getMessage());
        if (null != session) {
          try {
            if (0 < localControlPort)
              session.delPortForwardingL(localControlPort);
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

  public static Instance start(final AmazonEC2 ec2, final String ami, final String instanceType, final String groupId,
      final KeyPair keyPair, final AmazonIdentityManagement iam, final String... bucket) {
    return start(ec2, ami, instanceType, groupId, keyPair, newIamRole(iam, ("{\n" + "  \"Version\": \"2012-10-17\",\n"
        + "  \"Statement\": [\n" + "    " + bucketGrantStr(bucket) + "\n" + "  ]\n" + "}")));
  }

  @NotNull
  public static String bucketGrantStr(String... bucket) {
    return RefUtil.get(RefArrays
        .stream(bucket).map(b -> RefString.format("{\n" + "      \"Action\": \"s3:*\",\n"
            + "      \"Effect\": \"Allow\",\n" + "      \"Resource\": \"arn:aws:s3:::%s*\"\n" + "    }", b))
        .reduce((a, b) -> a + ", " + b));
  }

  public static void sleep(final int millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      logger.info("Interuptted");
      Thread.currentThread().interrupt();
    }
  }

  public static boolean exists(final AmazonEC2 ec2, final String groupId) {
    try {
      return ec2.describeSecurityGroups(new DescribeSecurityGroupsRequest().withGroupIds(groupId)).getSecurityGroups()
          .size() == 1;
    } catch (Throwable e) {
      return false;
    }
  }

  public static Instance getInstance(final AmazonEC2 ec2, final Instance instance) {
    return ec2.describeInstances(new DescribeInstancesRequest().withInstanceIds(instance.getInstanceId()))
        .getReservations().get(0).getInstances().get(0);
  }

  public static Instance start(final AmazonEC2 ec2, final String ami, final String instanceType, final String groupId,
      final KeyPair keyPair, final InstanceProfile instanceProfile) {
    final AtomicReference<Instance> instance = new AtomicReference<>();

    instance.set(ec2.runInstances(new RunInstancesRequest().withImageId(ami).withInstanceType(instanceType)
        .withIamInstanceProfile(new IamInstanceProfileSpecification().withArn(instanceProfile.getArn())
        //        .withName(role.getInstanceProfileName())
        ).withInstanceInitiatedShutdownBehavior(ShutdownBehavior.Terminate).withMinCount(1).withMaxCount(1)
        .withKeyName(keyPair.getKeyName()).withSecurityGroupIds(groupId)).getReservation().getInstances().get(0));
    while (instance.get().getState().getName().equals("pending")) {
      logger.info("Awaiting instance startup...");
      sleep(10000);
      instance.set(getInstance(ec2, instance.get()));
    }
    Instance info = instance.get();
    logger.info(RefString.format("Instance started: %s @ http://%s:1080/ - %s", info.getInstanceId(),
        info.getPublicDnsName(), info));
    return info;
  }

  @Nonnull
  public static String randomHex() {
    return Long.toHexString(random.nextLong()).substring(0, 5);
  }

  public static IpPermission getTcpPermission(final int port) {
    return new IpPermission().withIpv4Ranges(Arrays.asList(new IpRange().withCidrIp("0.0.0.0/0"))).withIpProtocol("tcp")
        .withFromPort(port).withToPort(port);
  }

  @Nonnull
  public static String exec(final Session session, final String script) {
    try {
      logger.debug("Executing: " + script);
      Process process = execAsync(session, script, new RefHashMap<String, String>());
      join(process.getChannel());
      String output = new String(process.getOutBuffer().toByteArray(), charset);
      int exitStatus = process.getChannel().getExitStatus();
      if (0 != exitStatus) {
        logger.info(RefString.format("Exit Status: %d; Output:\n%s", exitStatus, output));
        throw new AssertionError("Exit Status: " + exitStatus);
      }
      return output;
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Nonnull
  public static Process execAsync(final Session session, final String script, RefHashMap<String, String> env) {
    EC2Util.Process temp_06_0001 = execAsync(session, script, new ByteArrayOutputStream(), RefUtil.addRef(env));
    if (null != env)
      env.freeRef();
    return temp_06_0001;
  }

  @Nonnull
  public static Process execAsync(final Session session, final String script, final OutputStream outBuffer,
      RefHashMap<String, String> env) {
    try {
      EC2Util.Process temp_06_0002 = new Process(session, script, outBuffer, RefUtil.addRef(RefUtil.addRef(env)));
      if (null != env)
        env.freeRef();
      return temp_06_0002;
    } catch (JSchException e) {
      throw new RuntimeException(e);
    }
  }

  public static void join(final ChannelExec channel) throws InterruptedException {
    while (!(channel.isClosed() || channel.isEOF())) {
      Thread.sleep(100);
    }
  }

  public static EC2Node start(final AmazonEC2 ec2, final NodeConfig jvmConfig, final ServiceConfig serviceConfig,
      final int localControlPort) {
    return start(ec2, jvmConfig.imageId, jvmConfig.instanceType, jvmConfig.username, serviceConfig.keyPair,
        serviceConfig.groupId, serviceConfig.instanceProfile, localControlPort);
  }

  public static AwsTendrilNodeSettings setup(AmazonEC2 ec2, final AmazonIdentityManagement iam, final AmazonS3 s3,
      final String instanceType, final String imageId, final String username) {
    return setup(ec2, iam, s3.createBucket("data-" + randomHex()).getName(), instanceType, imageId, username);
  }

  public static AwsTendrilNodeSettings setup(AmazonEC2 ec2, final AmazonIdentityManagement iam, final String bucket,
      final String instanceType, final String imageId, final String username) {
    return AwsTendrilEnvSettings.setup(instanceType, imageId, username, newSecurityGroup(ec2, 22, 1080, 4040, 8080),
        newIamRole(iam, S3Util.defaultPolicy(bucket)).getArn(), bucket);
  }

  public static AwsTendrilEnvSettings setup(AmazonEC2 ec2, final AmazonIdentityManagement iam, final AmazonS3 s3) {
    return setup(ec2, iam, s3.createBucket("data-" + randomHex()).getName());
  }

  public static AwsTendrilEnvSettings setup(AmazonEC2 ec2, final AmazonIdentityManagement iam, final String bucket) {
    return AwsTendrilEnvSettings.setup(newSecurityGroup(ec2, 22, 1080, 4040, 8080),
        newIamRole(iam, S3Util.defaultPolicy(bucket)).getArn(), bucket);
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

    public Session getConnection() {
      return connection;
    }

    public String getInstanceId() {
      return instanceId;
    }

    public Instance getStatus() {
      return ec2.describeInstances(new DescribeInstancesRequest().withInstanceIds(getInstanceId())).getReservations()
          .get(0).getInstances().get(0);
    }

    public TendrilControl startJvm(final AmazonEC2 ec2, final AmazonS3 s3, final AwsTendrilNodeSettings settings,
        final int localControlPort) {
      return Tendril.startRemoteJvm(this, settings.newJvmConfig(), localControlPort, Tendril::defaultClasspathFilter,
          s3, new RefHashMap<String, String>(), settings.getServiceConfig(ec2).bucket);
    }

    public <T> T runAndTerminate(final Function<Session, T> task) {
      try {
        return task.apply(getConnection());
      } finally {
        terminate();
      }
    }

    public TerminateInstancesResult terminate() {
      logger.info("Terminating " + getInstanceId());
      return ec2.terminateInstances(new TerminateInstancesRequest().withInstanceIds(getInstanceId()));
    }

    @Override
    public void close() {
      terminate();
    }

    public int shell() {
      return EC2Util.shell(getConnection());
    }

    public String scp(final File local, final String remote) {
      return EC2Util.scp(getConnection(), local, remote);
    }

    public String exec(final String command) {
      return EC2Util.exec(getConnection(), command);
    }

    public Process execAsync(final String command) {
      return EC2Util.execAsync(getConnection(), command, new RefHashMap<String, String>());
    }

    public void stage(final File entryFile, final String remote, final String bucket, final String keyspace,
        final AmazonS3 s3) {
      EC2Util.stage(getConnection(), entryFile, remote, bucket, keyspace, s3);
    }
  }

  public static class Process {
    private final ChannelExec channel;
    private final OutputStream outBuffer;

    public Process(final Session session, final String script, final OutputStream outBuffer,
        RefHashMap<String, String> env) throws JSchException {
      channel = (ChannelExec) session.openChannel("exec");
      channel.setCommand(script);
      this.outBuffer = outBuffer;
      channel.setOutputStream(this.outBuffer);
      env.forEach((k, v) -> channel.setEnv(k, v));
      if (null != env)
        env.freeRef();
      channel.setExtOutputStream(new CloseShieldOutputStream(com.simiacryptus.ref.wrappers.RefSystem.err));
      channel.connect();
    }

    public ChannelExec getChannel() {
      return channel;
    }

    public ByteArrayOutputStream getOutBuffer() {
      return (ByteArrayOutputStream) outBuffer;
    }

    @Nonnull
    public String getOutput() {
      return new String(getOutBuffer().toByteArray(), charset);
    }

    public String join() {
      try {
        EC2Util.join(channel);
        return getOutput();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public static class ServiceConfig {
    public String[] bucket;
    public InstanceProfile instanceProfile;
    public String groupId;
    public KeyPair keyPair;

    public ServiceConfig(final AmazonEC2 ec2, final String roleArn, final String... bucket) {
      this(ec2, roleArn, EC2Util.newSecurityGroup(ec2, 22, 1080, 4040, 8080), bucket);
    }

    public ServiceConfig(final AmazonEC2 ec2, final String roleArn, final String groupId, final String... bucket) {
      this(ec2, groupId, new InstanceProfile().withArn(roleArn), bucket);
    }

    public ServiceConfig(final AmazonEC2 ec2, final String groupId, final InstanceProfile instanceProfile,
        final String... bucket) {
      this(groupId, instanceProfile, EC2Util.getKeyPair(ec2), bucket);
    }

    public ServiceConfig(final String groupId, final InstanceProfile instanceProfile, final KeyPair keyPair,
        final String... bucket) {
      this.bucket = bucket;
      this.groupId = groupId;
      this.instanceProfile = instanceProfile;
      this.keyPair = keyPair;
    }
  }

  public static class NodeConfig {
    public String imageId;
    public String instanceType;
    public String username;

    public NodeConfig(final String imageId, final String instanceType, final String username) {
      this.imageId = imageId;
      this.instanceType = instanceType;
      this.username = username;
    }
  }

}
