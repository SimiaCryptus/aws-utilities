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
import com.simiacryptus.ref.lang.RefAware;
import com.simiacryptus.ref.lang.RefUtil;
import com.simiacryptus.ref.wrappers.RefHashMap;
import com.simiacryptus.ref.wrappers.RefMap;
import com.simiacryptus.ref.wrappers.RefStream;
import com.simiacryptus.ref.wrappers.RefString;
import com.simiacryptus.util.JsonUtil;
import com.simiacryptus.util.ReportingUtil;
import com.simiacryptus.util.Util;
import com.simiacryptus.util.test.SysOutInterceptor;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Date;
import java.util.Random;
import java.util.UUID;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class EC2NotebookRunner {

  private static final Logger logger = LoggerFactory.getLogger(EC2NotebookRunner.class);
  public static String JAVA_OPTS = " -Xmx50g";

  static {
    SysOutInterceptor.INSTANCE.init();
  }

  private final String emailAddress = UserSettings.load().emailAddress;
  SerializableConsumer<NotebookOutput>[] fns;
  private String s3bucket = "";
  private boolean emailFiles = false;

  public EC2NotebookRunner(final SerializableConsumer<NotebookOutput>... fns) {
    this.fns = fns;
  }

  public static AmazonEC2 getEc2() {
    return AmazonEC2ClientBuilder.standard().withRegion(EC2Util.REGION).build();
  }

  public static AmazonIdentityManagement getIam() {
    return AmazonIdentityManagementClientBuilder.standard().withRegion(EC2Util.REGION).build();
  }

  public static AmazonS3 getS3() {
    return AmazonS3ClientBuilder.standard().withRegion(EC2Util.REGION).build();
  }

  public boolean isEmailFiles() {
    return emailFiles;
  }

  public EC2NotebookRunner setEmailFiles(boolean emailFiles) {
    this.emailFiles = emailFiles;
    return this;
  }

  public static void run(final SerializableConsumer<NotebookOutput>... reportTasks) throws IOException {
    for (final SerializableConsumer<NotebookOutput> reportTask : reportTasks) {
      Tendril.getKryo().copy(reportTask);
    }
    String runnerName = EC2NotebookRunner.class.getSimpleName();
    File reportFile = new File("target/report/" + Util.dateStr("yyyyMMddHHmmss") + "/" + runnerName);
    try (NotebookOutput log = new MarkdownNotebookOutput(reportFile, true)) {
      new EC2NotebookRunner(reportTasks).launchNotebook(log);
    }
  }

  public static String getTestName(final SerializableConsumer<NotebookOutput> fn) {
    String name = fn.getClass().getCanonicalName();
    if (null == name || name.isEmpty())
      name = fn.getClass().getSimpleName();
    if (null == name || name.isEmpty())
      name = "index";
    return name;
  }

  public static void logFiles(final File f) {
    if (f.isDirectory()) {
      for (final File child : f.listFiles()) {
        logFiles(child);
      }
    } else {
      logger.info(RefString.format("File %s length %d", f.getAbsolutePath(), f.length()));
    }
  }

  public static void run(final SerializableConsumer<NotebookOutput> consumer, final String testName) {
    try {
      final File file = new File(RefString.format("report/%s_%s", testName, UUID.randomUUID().toString()));
      try (NotebookOutput log = new MarkdownNotebookOutput(file, 1080, true, file.getName(), UUID.randomUUID())) {
        consumer.accept(log);
        logger.info("Finished worker tiledTexturePaintingPhase");
      } catch (Throwable e) {
        logger.warn("Error!", e);
      }
    } catch (Throwable e) {
      logger.warn("Error!", e);
    }
  }

  public void launchNotebook(final NotebookOutput log) {
    AwsTendrilNodeSettings settings = log.eval(() -> {
      EC2NodeSettings nodeSettings = EC2NodeSettings.P3_2XL;

      return JsonUtil.cache(new File("ec2-settings." + EC2Util.REGION.toString() + ".json"),
          AwsTendrilNodeSettings.class, () -> EC2Util.setup(getEc2(), getIam(), getS3(), nodeSettings.machineType,
              nodeSettings.imageId, nodeSettings.username));
    });
    s3bucket = settings.bucket;
    settings.imageId = EC2NodeSettings.AMI_AMAZON_DEEP_LEARNING;
    settings.instanceType = EC2NodeSettings.P2_XL.machineType;
    settings.username = EC2NodeSettings.P2_XL.username;

    Tendril.JvmConfig jvmConfig = settings.newJvmConfig();
    jvmConfig.javaOpts += JAVA_OPTS;
    int localControlPort = new Random().nextInt(1024) + 1024;
    SESUtil.setup(AmazonSimpleEmailServiceClientBuilder.defaultClient(), emailAddress);
    EC2Util.EC2Node node = settings.startNode(getEc2(), localControlPort);
    try {
      log.run(() -> {
        TendrilControl tendrilControl = Tendril.startRemoteJvm(node, jvmConfig, localControlPort,
            Tendril::defaultClasspathFilter, getS3(), new RefHashMap<String, String>(),
            settings.getServiceConfig(getEc2()).bucket);
        tendrilControl.start(() -> {
          this.nodeMain();
          return null;
        });
      });
    } catch (Throwable e) {
      node.close();
      throw new RuntimeException(e);
    }
    try {
      ReportingUtil.browse(new URI(RefString.format("http://%s:1080/", node.getStatus().getPublicIpAddress())));
    } catch (IOException | URISyntaxException e) {
      logger.info("Error opening browser", e);
    }
    while ("running".equals(getStatus(log, node)))
      try {
        Thread.sleep(30 * 1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
  }

  public String getStatus(final NotebookOutput log, final EC2Util.EC2Node node) {
    try {
      return log.eval(() -> {
        return node.getStatus().getState().getName();
      });
    } catch (Throwable e) {
      return e.getMessage();
    }
  }

  void nodeMain() {
    try {
      for (final SerializableConsumer<NotebookOutput> fn : this.fns) {
        run(notificationWrapper(getTestName(fn), fn), getTestName(fn));
      }
    } finally {
      logger.warn("Exiting node worker", new RuntimeException("Stack Trace"));
      com.simiacryptus.ref.wrappers.RefSystem.exit(0);
    }
  }

  private SerializableConsumer<NotebookOutput> notificationWrapper(final String testName,
      final SerializableConsumer<NotebookOutput> fn) {
    long startTime = com.simiacryptus.ref.wrappers.RefSystem.currentTimeMillis();
    return log -> {
      log.setArchiveHome(URI.create("s3://" + s3bucket + "/reports/" + UUID.randomUUID() + "/"));
      log.onComplete(() -> {
        logFiles(log.getRoot());
        RefMap<File, URL> uploads = S3Util.upload(getS3(), log.getArchiveHome(), log.getRoot());
        sendCompleteEmail(testName, log.getRoot(), uploads == null ? null : uploads.addRef(), startTime);
        if (null != uploads)
          uploads.freeRef();
      });
      try {
        sendStartEmail(testName, fn);
      } catch (IOException | URISyntaxException e) {
        throw new RuntimeException(e);
      }
      fn.accept(log);
      log.setFrontMatterProperty("status", "OK");
    };
  }

  private void sendCompleteEmail(final String testName, final File workingDir, final RefMap<File, URL> uploads,
      final long startTime) {
    String html = null;
    try {
      html = FileUtils.readFileToString(new File(workingDir, testName + ".html"), "UTF-8");
    } catch (IOException e) {
      html = e.getMessage();
    }
    Pattern compile = Pattern.compile("\"([^\"]+?)\"");
    Matcher matcher = compile.matcher(html);
    int start = 0;
    String replacedHtml = "";
    while (matcher.find(start)) {
      replacedHtml += html.substring(start, matcher.start());
      String group = matcher.group(1);
      File imageFile = new File(workingDir, group).getAbsoluteFile();
      URL url = uploads.get(imageFile);
      if (null == url) {
        logger.info(RefString.format("No File Found for %s, reverting to %s", imageFile, group));
        replacedHtml += "\"" + group + "\"";
      } else {
        logger.info(RefString.format("Rewriting %s to %s at %s", group, imageFile, url));
        replacedHtml += "\"" + url + "\"";
      }
      start = matcher.end();
    }
    double durationMin = (com.simiacryptus.ref.wrappers.RefSystem.currentTimeMillis() - startTime) / (1000.0 * 60);
    String subject = RefString.format("%s Completed in %.3fmin", testName, durationMin);
    File zip = new File(workingDir, testName + ".zip");
    File pdf = new File(workingDir, testName + ".pdf");

    String append = "<hr/>"
        + RefUtil.get(RefStream.of(zip, pdf, new File(workingDir, testName + ".html"))
        .map(
            RefUtil.wrapInterface(
                (Function<File, String>) file -> String.format("<p><a href=\"%s\">%s</a></p>",
                    uploads.get(file.getAbsoluteFile()), file.getName()),
                uploads == null ? null : uploads.addRef()))
        .reduce((a, b) -> a + b));
    if (null != uploads)
      uploads.freeRef();
    String endTag = "</body>";
    if (replacedHtml.contains(endTag)) {
      replacedHtml.replace(endTag, append + endTag);
    } else {
      replacedHtml += append;
    }
    File[] attachments = isEmailFiles() ? new File[] { zip, pdf } : new File[] {};
    SESUtil.send(AmazonSimpleEmailServiceClientBuilder.defaultClient(), subject, emailAddress, replacedHtml,
        replacedHtml, attachments);
  }

  private void sendStartEmail(final String testName, final SerializableConsumer<NotebookOutput> fn)
      throws IOException, URISyntaxException {
    String publicHostname = IOUtils.toString(new URI("http://169.254.169.254/latest/meta-data/public-hostname"),
        "UTF-8");
    String instanceId = IOUtils.toString(new URI("http://169.254.169.254/latest/meta-data/instance-id"), "UTF-8");
    String functionJson = new ObjectMapper().enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL)
        .enable(SerializationFeature.INDENT_OUTPUT).writer().writeValueAsString(fn);
    //https://console.aws.amazon.com/ec2/v2/home?region=us-east-1#Instances:search=i-00651bdfd6121e199
    String html = "<html><body>" + "<p><a href=\"http://" + publicHostname
        + ":1080/\">The tiledTexturePaintingPhase can be monitored at " + publicHostname + "</a></p><hr/>"
        + "<p><a href=\"https://console.aws.amazon.com/ec2/v2/home?region=us-east-1#Instances:search=" + instanceId
        + "\">View instance " + instanceId + " on AWS Console</a></p><hr/>" + "<p>Script Definition:" + "<pre>"
        + functionJson + "</pre></p>" + "</body></html>";
    String txtBody = "Process Started at " + new Date();
    String subject = testName + " Starting";
    SESUtil.send(AmazonSimpleEmailServiceClientBuilder.defaultClient(), subject, emailAddress, txtBody, html);
  }
}
