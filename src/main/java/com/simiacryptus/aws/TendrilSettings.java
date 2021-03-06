/*
 * Copyright (c) 2020 by Andrew Charneski.
 *
 * The author licenses this file to you under the
 * Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy
 * of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.simiacryptus.aws;

import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.util.EC2MetadataUtils;
import com.simiacryptus.lang.Settings;
import com.simiacryptus.lang.UncheckedSupplier;
import com.simiacryptus.notebook.NotebookOutput;
import com.simiacryptus.ref.lang.RefIgnore;
import com.simiacryptus.util.S3Uploader;
import com.simiacryptus.util.Util;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.simiacryptus.lang.Settings.get;

public class TendrilSettings implements Settings {
  public static final TendrilSettings INSTANCE = new TendrilSettings();
  public static final String KEY_BUCKET = "tendril.bucket";
  public static final String KEY_KEYSPACE = "tendril.keyspace";
  public static final String KEY_LOCALCP = "tendril.localcp";
  public static final String KEY_SESSION_ID = "tendril.sessionId";
  public final String keyspace = get(KEY_KEYSPACE, "lib/");
  public final String localcp = get(KEY_LOCALCP, "~/lib/");
  public final String session = get(KEY_SESSION_ID, UUID.randomUUID().toString());
  public String bucket = get(KEY_BUCKET, "simiacryptus");

  private TendrilSettings() {
  }

  @org.jetbrains.annotations.Nullable
  public UncheckedSupplier<?> getAppTask() {
    try {
      URI appHome = getS3AppHome();
      logger.info("Downloading App Task: " + appHome);
      byte[] download = S3Uploader.download(appHome);
      if (null != download) {
        logger.info("Downloaded: " + appHome);
        return Tendril.fromBytes(download);
      }
    } catch (Throwable e) {
      e.printStackTrace();
    }
    return null;
  }

  @NotNull
  @RefIgnore
  public String getDefines() {
    return toDefineCli(set(systemProperties()));
  }

  @NotNull
  public URI getS3AppHome() throws URISyntaxException {
    String sessionFile = session.replace('-', '_').replaceAll("_", "");
    return new URI(String.format("s3://%s/%s/apps/%s.kryo",
        bucket, keyspace, sessionFile)).normalize();
  }

  @NotNull
  public static HashMap<String, String> systemProperties() {
    HashMap<String, String> defines = new HashMap<>();
    System.getProperties().forEach((k, v) -> defines.put(k.toString(), v.toString()));
    return defines;
  }

  @NotNull
  @RefIgnore
  public static String toDefineCli(HashMap<String, String> defines) {
    return defines.entrySet().stream()
        .filter(x -> !x.getKey().startsWith("java."))
        .filter(x -> !x.getKey().startsWith("sun."))
        .filter(x -> !x.getKey().startsWith("file."))
        .filter(x -> !x.getKey().startsWith("os."))
        .filter(x -> !x.getKey().startsWith("awt."))
        .filter(x -> !x.getKey().startsWith("user."))
        .filter(x -> !x.getKey().endsWith(".separator"))
        .map(e -> "-D" + e.getKey() + "=" + e.getValue())
        .reduce((a, b) -> a + " " + b).get();
  }

  public <T> UncheckedSupplier<T> setAppTask(UncheckedSupplier<T> task) {
    try {
      URI path = getS3AppHome();
      S3Uploader.upload(Tendril.getBytes(task), path);
      logger.info("Wrote runnable instructions to " + path);
      return task;
    } catch (Exception e) {
      throw Util.throwException(e);
    }
  }

  public void howToRun(NotebookOutput log) {
    try {
      log.h1("How To Run");
      File localCpDir = new File(this.localcp);
      List<String> jars = Stream.of(System.getProperty("java.class.path").split(File.pathSeparator))
          .map(x -> new File(x))
          .map(x -> x.getAbsolutePath().startsWith(localCpDir.getAbsolutePath()) ? (localCpDir.toPath().relativize(x.toPath()).toString()) : (x.getName()))
          .collect(Collectors.toList());
//      String region = "us-east-1";
//      String ami = "ami-04f94a03f151839d7";
//      String type = "m5.large";
//      String securityGroup = "sg-0ec421448a2748215";
//      String keyName = "key_480a7";
//      String iamName = "runpol-f1abd";
      String region = "us-east-1";
      String ami = "ami-00000000000000000";
      String type = "xyz1.large";
      String securityGroup = "sg-00000000000000000";
      String keyName = "SSHKEY";
      String iamName = "IAM-ARN";
      String userdata = "#!/bin/bash\n" +
          "sudo -H -u ec2-user /bin/bash << UIS\n" +
          "\texport CP=\"\";\n" +
          "\tcd ~/;\n" +
          "\tfor jar in " + jars.stream().reduce((a, b) -> a + " " + b).orElse("") + "; \n" +
          "\tdo \n" +
          "\t  export FILE=\"" + this.localcp + "\\\\\\$jar\"; \n" +
          "\t  aws s3 cp \"" + new URI("s3://" + this.bucket + "/" + this.keyspace + "/").normalize().toString() + "\\\\\\$jar\" \\\\\\$FILE; \n" +
          "\t  export CP=\"\\\\\\$CP:\\\\\\$FILE\"; \n" +
          "\tdone\n" +
          "\tnohup java " + getDefines() + " -cp \\\\\\$CP " + S3TaskRunner.class.getName() + " &\n" +
          "UIS";
      try {
        region = EC2MetadataUtils.getEC2InstanceRegion();
        ami = EC2MetadataUtils.getAmiId();
        type = EC2MetadataUtils.getInstanceType();
        securityGroup = EC2MetadataUtils.getSecurityGroups().get(0);
        iamName = EC2MetadataUtils.getIAMInstanceProfileInfo().instanceProfileArn;
      } catch (Throwable e) {
        //e.printStackTrace();
      }
      try {
        String userData = EC2MetadataUtils.getUserData().trim();
        if (!userData.isEmpty()) userdata = userData + "\n";
      } catch (Throwable e) {
        //e.printStackTrace();
      }
      try {
        AmazonEC2ClientBuilder standard = AmazonEC2ClientBuilder.standard();
        standard.setRegion(region);
        DescribeInstancesResult result = standard.build().describeInstances(new DescribeInstancesRequest().withInstanceIds(EC2MetadataUtils.getInstanceId()));
        Instance instance = result.getReservations().stream().flatMap(x -> x.getInstances().stream()).findAny().get();
        keyName = instance.getKeyName();
      } catch (Throwable e) {
        //e.printStackTrace();
      }
      log.p("To run this notebook on AWS yourself, simply run this awc cli command or the equivalent. " +
          "Note you will need to configure and specify your own _security group_, _iam profile_, and _ssh key_.");
      log.out("\n\n" +
          "```bash\n" +
          "aws ec2 run-instances \\\n" +
          " --region " + region + " \\\n" +
          //" --associate-public-ip-address \\\n" +
          " --instance-initiated-shutdown-behavior terminate \\\n" +
          " --image-id " + ami + " \\\n" +
          " --instance-type " + type + " \\\n" +
          " --count 1 \\\n" +
          " --security-groups " + securityGroup + " \\\n" +
          " --iam-instance-profile Arn=\"" + iamName + "\" \\\n" +
          " --key-name " + keyName + " \\\n" +
          " --user-data \"$(cat <<- EOF \n" +
          "\t" + userdata.replaceAll("\n", "\n\t") +
          "\nEOF\n" +
          ")\"\n\n" +
          "```\n\n");
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  public HashMap<String, String> set(HashMap<String, String> defines) {
    defines.put(TendrilSettings.KEY_BUCKET, TendrilSettings.INSTANCE.bucket);
    defines.put(TendrilSettings.KEY_KEYSPACE, TendrilSettings.INSTANCE.keyspace);
    defines.put(TendrilSettings.KEY_LOCALCP, TendrilSettings.INSTANCE.localcp);
    defines.put(TendrilSettings.KEY_SESSION_ID, TendrilSettings.INSTANCE.session);
    return defines;
  }

}
