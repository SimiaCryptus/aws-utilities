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
  public static String bucket = get(KEY_BUCKET, "simiacryptus");
  public static final String keyspace = get(KEY_KEYSPACE, "lib/");
  public static final String localcp = get(KEY_LOCALCP, "~/lib/");
  public static final String session = get(KEY_SESSION_ID, UUID.randomUUID().toString());

  private TendrilSettings() {
  }

  @org.jetbrains.annotations.Nullable
  public UncheckedSupplier<?> getAppTask() {
    try {
      URI appHome = getS3AppHome();
      logger.info("Downloading App Task: " + appHome);
      byte[] download = S3Uploader.download(appHome);
      if(null != download) {
        logger.info("Downloaded: " + appHome);
        return Tendril.fromBytes(download);
      }
    } catch (Throwable e) {
      e.printStackTrace();
    }
    return null;
  }

  @NotNull
  public URI getS3AppHome() throws URISyntaxException {
    String sessionFile = session.replace('-', '_').replaceAll("_", "");
    return new URI(String.format("s3://%s/%s/apps/%s.kryo",
        bucket, keyspace, sessionFile)).normalize();
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
      // JAVA_ARGS
      String region = "us-east-1";
      String ami = "ami-04f94a03f151839d7";
      String type = "m5.large";
      String securityGroup = "sg-0ec421448a2748215";
      String keyName = "key_480a7";
      String iamName = "runpol-f1abd";
      log.p("\n\n" +
          "```bash\n" +
          "aws ec2 run-instances \\\n" +
          " --region " + region + " \\\n" +
          " --associate-public-ip-address \\\n" +
          " --image-id " + ami + " \\\n" +
          " --instance-type " + type + " \\\n" +
          " --count 1 \\\n" +
          " --security-group-ids " + securityGroup + " \\\n" +
          " --iam-instance-profile Name=\"" + iamName + "\" \\\n" +
          " --key-name " + keyName + " \\\n" +
          " --user-data \"$(cat <<- EOF \n" +
          "\t#!/bin/bash\n" +
          "\tsudo -H -u ec2-user /bin/bash << UIS\n" +
          "\t\texport CP=\"\";\n" +
          "\t\tcd ~/;\n" +
          "\t\tfor jar in " + jars.stream().reduce((a, b) -> a + " " + b).orElse("") + "; \n" +
          "\t\tdo \n" +
          "\t\t  export FILE=\"" + this.localcp + "\\\\\\$jar\"; \n" +
          "\t\t  aws s3 cp \"" + new URI("s3://" + this.bucket + "/" + this.keyspace + "/").normalize().toString() + "\\\\\\$jar\" \\\\\\$FILE; \n" +
          "\t\t  export CP=\"\\\\\\$CP:\\\\\\$FILE\"; \n" +
          "\t\tdone\n" +
          "\t\tjava " + getDefines() + " -cp \\\\\\$CP " + Tendril.class.getName() + " continue\n" +
          "\tUIS\n" +
          "EOF\n" +
          ")\"\n" +
          "```\n\n");
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  @NotNull
  @RefIgnore
  public String getDefines() {
    return toDefineCli(set(systemProperties()));
  }

  @NotNull
  public static HashMap<String, String> systemProperties() {
    HashMap<String, String> defines = new HashMap<>();
    System.getProperties().forEach((k, v) -> defines.put(k.toString(), v.toString()));
    return defines;
  }

  public HashMap<String, String> set(HashMap<String, String> defines) {
    defines.put(TendrilSettings.KEY_BUCKET, TendrilSettings.INSTANCE.bucket);
    defines.put(TendrilSettings.KEY_KEYSPACE, TendrilSettings.INSTANCE.keyspace);
    defines.put(TendrilSettings.KEY_LOCALCP, TendrilSettings.INSTANCE.localcp);
    defines.put(TendrilSettings.KEY_SESSION_ID, TendrilSettings.INSTANCE.session);
    return defines;
  }

  @NotNull
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

}
