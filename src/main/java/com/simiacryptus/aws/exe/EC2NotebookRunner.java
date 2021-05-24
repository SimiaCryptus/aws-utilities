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

package com.simiacryptus.aws.exe;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.identitymanagement.AmazonIdentityManagement;
import com.amazonaws.services.identitymanagement.AmazonIdentityManagementClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.simpleemail.AmazonSimpleEmailServiceClientBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.simiacryptus.aws.*;
import com.simiacryptus.lang.SerializableConsumer;
import com.simiacryptus.notebook.MarkdownNotebookOutput;
import com.simiacryptus.notebook.NotebookOutput;
import com.simiacryptus.ref.wrappers.RefSystem;
import com.simiacryptus.util.JsonUtil;
import com.simiacryptus.util.ReportingUtil;
import com.simiacryptus.util.S3Uploader;
import com.simiacryptus.util.Util;
import com.simiacryptus.util.test.SysOutInterceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.*;

public class EC2NotebookRunner {

  private static final Logger logger = LoggerFactory.getLogger(EC2NotebookRunner.class);
  @Nonnull
  public static String JAVA_OPTS = " -Xmx50g";

  static {
    SysOutInterceptor.INSTANCE.init();
  }

  private final String emailAddress = UserSettings.load().getEmailAddress();
  SerializableConsumer<NotebookOutput>[] fns;
  private String s3bucket = "";

  private EC2NotebookRunner(final SerializableConsumer<NotebookOutput>... fns) {
    this.fns = fns;
  }

  public static AmazonEC2 getEc2() {
    return AmazonEC2ClientBuilder.standard().withRegion(EC2Util.REGION).build();
  }

  public static AmazonIdentityManagement getIam() {
    return AmazonIdentityManagementClientBuilder.standard().withRegion(EC2Util.REGION).build();
  }

  public static AmazonS3 getS3(Regions region) {
    return AmazonS3ClientBuilder.standard().withRegion(region).build();
  }

  public static void test(@Nonnull SerializableConsumer<NotebookOutput>... reportTasks) {
    for (final SerializableConsumer<NotebookOutput> reportTask : reportTasks) {
      Tendril.getKryo().copy(reportTask);
    }
  }

  public static String getTestName(@Nonnull final SerializableConsumer<NotebookOutput> fn) {
    String name = fn.getClass().getCanonicalName();
    if (null == name || name.isEmpty())
      name = fn.getClass().getSimpleName();
    if (name.isEmpty())
      name = "index";
    return name;
  }

  public static void logFiles(@Nonnull final File f) {
    if (f.isDirectory()) {
      for (final File child : f.listFiles()) {
        logFiles(child);
      }
    } else {
      logger.info(String.format("File %s length %d", f.getAbsolutePath(), f.length()));
    }
  }

  public static void run(@Nonnull final SerializableConsumer<NotebookOutput> consumer, final String testName) {
    try {
      final File file = new File(String.format("report/%s_%s", testName, UUID.randomUUID().toString()));
      try (NotebookOutput log = new MarkdownNotebookOutput(file, true, file.getName(), UUID.randomUUID(), 1080)) {
        log.subreport("To Run Again On EC2", "run_ec2", sublog -> {
          TendrilSettings.INSTANCE.howToRun(sublog);
          return null;
        });
        consumer.accept(log);
        logger.info("Finished " + testName);
      } catch (Throwable e) {
        logger.warn("Error!", e);
      }
    } catch (Throwable e) {
      logger.warn("Error!", e);
    }
  }

  public static void launch(EC2NodeSettings machineType, String ami, String javaOpts, SerializableConsumer<NotebookOutput> task) {
    JAVA_OPTS = javaOpts;
    test(task);
    EC2NotebookRunner runner = new EC2NotebookRunner(task);
    final AwsTendrilNodeSettings nodeSettings = runner.initNodeSettings(machineType, ami);
    Tendril.JvmConfig jvmSettings = runner.initJvmSettings(nodeSettings);
    runner.launch(nodeSettings, jvmSettings);
  }

  public void launch(AwsTendrilNodeSettings nodeSettings, Tendril.JvmConfig jvmConfig) {
    s3bucket = nodeSettings.getBucket();
    EC2Util.EC2Node node = start(nodeSettings, jvmConfig);
    open(node);
    join(node);
  }

  public Tendril.JvmConfig initJvmSettings(AwsTendrilNodeSettings settings) {
    Tendril.JvmConfig jvmConfig = settings.newJvmConfig();
    jvmConfig.javaOpts += JAVA_OPTS;
    return jvmConfig;
  }

  public void open(EC2Util.EC2Node node) {
    try {
      assert node != null;
      ReportingUtil.browse(new URI(String.format("http://%s:1080/", node.getStatus().getPublicIpAddress())));
    } catch (@Nonnull IOException | URISyntaxException e) {
      logger.info("Error opening browser", e);
    }
  }

  public EC2Util.EC2Node start(AwsTendrilNodeSettings settings, Tendril.JvmConfig jvmConfig) {
    int localControlPort = new Random().nextInt(1024) + 1024;
    SESUtil.setup(AmazonSimpleEmailServiceClientBuilder.standard().withRegion(EC2Util.REGION).build(), emailAddress);
    EC2Util.EC2Node node = settings.startNode(getEc2(), localControlPort);
    try {
      assert node != null;
      TendrilControl tendrilControl = Tendril.startRemoteJvm(
          node,
          jvmConfig,
          localControlPort,
          file -> Tendril.defaultClasspathFilter(file),
          S3Uploader.buildClientForBucket(settings.getBucket()),
          new HashMap<String, String>(),
          settings.getBucket());
      tendrilControl.start(TendrilSettings.INSTANCE.setAppTask(() -> {
        this.nodeMain();
        return null;
      }));
      //tendrilControl.close();
    } catch (Throwable e) {
      assert node != null;
      node.close();
      throw Util.throwException(e);
    }
    return node;
  }

  public void join(EC2Util.EC2Node node) {
    while ("running".equals(node.getStatus().getState().getName()))
      try {
        Thread.sleep(30 * 1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
  }

  public AwsTendrilNodeSettings initNodeSettings(EC2NodeSettings node, String ami) {
    final AwsTendrilNodeSettings settings;
    try {
      Regions region = EC2Util.REGION;
      settings = JsonUtil.cache(new File("ec2-settings." + region.toString() + ".json"),
          AwsTendrilNodeSettings.class,
          () -> EC2Util.setup(getEc2(), getIam(), getS3(region), node.machineType, node.imageId, node.username));
      settings.imageId = ami;
      settings.instanceType = node.machineType;
      settings.username = node.username;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return settings;
  }

  void nodeMain() {
    try {
      for (final SerializableConsumer<NotebookOutput> fn : this.fns) {
        run(notificationWrapper(getTestName(fn), fn), getTestName(fn));
      }
    } finally {
      logger.warn("Exiting node worker", new RuntimeException("Stack Trace"));
      RefSystem.exit(0);
    }
  }

  @Nonnull
  private SerializableConsumer<NotebookOutput> notificationWrapper(final String testName,
                                                                   @Nonnull final SerializableConsumer<NotebookOutput> fn) {
    long startTime = RefSystem.currentTimeMillis();
    return log -> {
      log.setArchiveHome(URI.create("s3://" + s3bucket + "/reports/" + UUID.randomUUID() + "/"));
      log.onComplete(() -> {
        logFiles(log.getRoot());
        URI archiveHome = log.getArchiveHome();
        Map<File, URL> uploads;
        if (null != archiveHome) {
          uploads = S3Uploader.upload(S3Uploader.buildClientForBucket(archiveHome.getHost()), archiveHome, log.getRoot());
        } else {
          uploads = new HashMap<>();
        }
        EmailUtil.sendCompleteEmail(log, startTime, emailAddress, uploads, false);
      });
      try {
        sendStartEmail(testName, fn);
      } catch (@Nonnull IOException | URISyntaxException e) {
        throw Util.throwException(e);
      }
      fn.accept(log);
      log.setMetadata("status", "OK");
    };
  }

  private void sendStartEmail(final String testName, final SerializableConsumer<NotebookOutput> fn)
      throws IOException, URISyntaxException {
    String publicHostname = EC2Util.getPublicHostname();
    String instanceId = EC2Util.getInstanceId();
    String functionJson = new ObjectMapper().enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL)
        .enable(SerializationFeature.INDENT_OUTPUT).writer().writeValueAsString(fn);
    String html = "<html><body>" + "<p><a href=\"http://" + publicHostname
        + ":1080/\">The " + testName + " report can be monitored at " + publicHostname + "</a></p><hr/>"
        + "<p><a href=\"https://console.aws.amazon.com/ec2/v2/home?region=us-east-1#Instances:search=" + instanceId
        + "\">View instance " + instanceId + " on AWS Console</a></p><hr/>" + "<p>Script Definition:" + "<pre>"
        + functionJson + "</pre></p>" + "</body></html>";
    String txtBody = "Process Started at " + new Date();
    String subject = testName + " Starting";
    SESUtil.send(AmazonSimpleEmailServiceClientBuilder.defaultClient(), subject, emailAddress, txtBody, html);
  }

}
