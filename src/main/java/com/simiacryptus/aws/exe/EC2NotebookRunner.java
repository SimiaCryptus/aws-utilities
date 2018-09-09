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
import com.simiacryptus.aws.AwsTendrilNodeSettings;
import com.simiacryptus.aws.EC2Util;
import com.simiacryptus.aws.S3Util;
import com.simiacryptus.aws.SESUtil;
import com.simiacryptus.aws.Tendril;
import com.simiacryptus.util.Util;
import com.simiacryptus.util.io.JsonUtil;
import com.simiacryptus.util.io.MarkdownNotebookOutput;
import com.simiacryptus.util.io.NotebookOutput;
import com.simiacryptus.util.lang.CodeUtil;
import com.simiacryptus.util.lang.SerializableConsumer;
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
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * The type Ec 2 runner.
 */
public class EC2NotebookRunner {
  
  private static final Logger logger = LoggerFactory.getLogger(EC2NotebookRunner.class);
  public static String JAVA_OPTS = " -Xmx50g";
  
  static {
    SysOutInterceptor.INSTANCE.init();
  }
  
  private final String emailAddress = UserSettings.load().emailAddress;
  /**
   * The Fns.
   */
  SerializableConsumer<NotebookOutput>[] fns;
  private String s3bucket = "";
  private boolean emailFiles = false;
  
  /**
   * Instantiates a new Ec 2 runner.
   *
   * @param fns the fns
   */
  public EC2NotebookRunner(final SerializableConsumer<NotebookOutput>... fns) {
    this.fns = fns;
  }
  
  /**
   * Run.
   *
   * @param reportTasks the report tasks
   * @throws IOException the io exception
   */
  public static void run(final SerializableConsumer<NotebookOutput>... reportTasks) throws IOException {
    for (final SerializableConsumer<NotebookOutput> reportTask : reportTasks) {
      Tendril.getKryo().copy(reportTask);
    }
    String runnerName = EC2NotebookRunner.class.getSimpleName();
    File reportFile = new File("target/report/" + Util.dateStr("yyyyMMddHHmmss") + "/" + runnerName);
    try (NotebookOutput log = new MarkdownNotebookOutput(reportFile, Util.AUTO_BROWSE)) {
      new EC2NotebookRunner(reportTasks).launchNotebook(log);
    }
  }
  
  /**
   * Gets ec 2.
   *
   * @return the ec 2
   */
  public static AmazonEC2 getEc2() {
    return AmazonEC2ClientBuilder.standard().withRegion(Regions.US_EAST_1).build();
  }
  
  /**
   * Gets iam.
   *
   * @return the iam
   */
  public static AmazonIdentityManagement getIam() {
    return AmazonIdentityManagementClientBuilder.standard().withRegion(Regions.US_EAST_1).build();
  }
  
  /**
   * Gets s 3.
   *
   * @return the s 3
   */
  public static AmazonS3 getS3() {
    return AmazonS3ClientBuilder.standard().withRegion(Regions.US_WEST_2).build();
  }
  
  /**
   * Gets test name.
   *
   * @param fn the fn
   * @return the test name
   */
  public static String getTestName(final SerializableConsumer<NotebookOutput> fn) {
    String name = fn.getClass().getCanonicalName();
    if (null == name || name.isEmpty()) name = fn.getClass().getSimpleName();
    if (null == name || name.isEmpty()) name = "index";
    return name;
  }
  
  /**
   * Log files.
   *
   * @param f the f
   */
  public static void logFiles(final File f) {
    if (f.isDirectory()) {
      for (final File child : f.listFiles()) {
        logFiles(child);
      }
    }
    else {
      logger.info(String.format("File %s length %d", f.getAbsolutePath(), f.length()));
    }
  }
  
  /**
   * Run.
   *
   * @param consumer the consumer
   * @param testName the test name
   */
  public static void run(final SerializableConsumer<NotebookOutput> consumer, final String testName) {
    try {
      String dateStr = Util.dateStr("yyyyMMddHHmmss");
      try (NotebookOutput log = new MarkdownNotebookOutput(
        new File("report/" + dateStr + "/" + testName),
          1080, true
      ))
      {
        consumer.accept(log);
        logger.info("Finished worker tiledTexturePaintingPhase");
      } catch (Throwable e) {
        logger.warn("Error!", e);
      }
    } catch (Throwable e) {
      logger.warn("Error!", e);
    }
  }
  
  /**
   * Launch notebook.
   *
   * @param log the log
   */
  public void launchNotebook(final NotebookOutput log) {
    AwsTendrilNodeSettings settings = log.eval(() -> {
        EC2NodeSettings nodeSettings = EC2NodeSettings.P3_2XL;
      return JsonUtil.cache(new File("ec2-settings.json"), AwsTendrilNodeSettings.class,
                            () -> AwsTendrilNodeSettings.setup(
                              getEc2(),
                              getIam(),
                              getS3(),
                              nodeSettings.machineType,
                              nodeSettings.imageId,
                              nodeSettings.username
                            )
      );
    });
    s3bucket = settings.bucket;
    Tendril.JvmConfig jvmConfig = settings.jvmConfig();
    jvmConfig.javaOpts += JAVA_OPTS;
    jvmConfig.javaOpts += " -DGITBASE=\"" + CodeUtil.getGitBase() + "\"";
    int localControlPort = new Random().nextInt(1024) + 1024;
    SESUtil.setup(AmazonSimpleEmailServiceClientBuilder.defaultClient(), emailAddress);
    EC2Util.EC2Node node = settings.startNode(getEc2(), localControlPort);
    try {
      log.run(() -> {
        Tendril.TendrilControl tendrilControl = Tendril.startRemoteJvm(node,
                                                                       jvmConfig,
                                                                       localControlPort,
                                                                       Tendril::defaultClasspathFilter,
                                                                       getS3(),
                                                                       settings.getServiceConfig(getEc2()).bucket,
                                                                       new HashMap<String, String>()
        );
        tendrilControl.start(this::nodeMain);
      });
    } catch (Throwable e) {
      node.close();
      throw new RuntimeException(e);
    }
    try {
      Util.browse(new URI(String.format("http://%s:1080/", node.getStatus().getPublicIpAddress())));
    } catch (IOException | URISyntaxException e) {
      logger.info("Error opening browser", e);
    }
    while ("running".equals(getStatus(log, node))) try {
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
  
  private void nodeMain() {
    try {
      for (final SerializableConsumer<NotebookOutput> fn : this.fns) {
        run(notificationWrapper(getTestName(fn), fn), getTestName(fn));
      }
    } finally {
      logger.info("Exiting node worker");
      System.exit(0);
    }
  }
  
  private SerializableConsumer<NotebookOutput> notificationWrapper(
    final String testName,
    final SerializableConsumer<NotebookOutput> fn
  )
  {
    long startTime = System.currentTimeMillis();
    return log -> {
      log.setArchiveHome(URI.create("s3://" + s3bucket + "/reports/"));
      log.onComplete(() -> {
        logFiles(log.getRoot());
        Map<File, URL> uploads = S3Util.upload(getS3(), log.getArchiveHome(), log.getRoot());
        sendCompleteEmail(testName, log.getRoot(), uploads, startTime);
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
  
  private void sendCompleteEmail(final String testName, final File workingDir, final Map<File, URL> uploads, final long startTime) {
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
        logger.info(String.format("No File Found for %s, reverting to %s", imageFile, group));
        replacedHtml += "\"" + group + "\"";
      }
      else {
        logger.info(String.format("Rewriting %s to %s at %s", group, imageFile, url));
        replacedHtml += "\"" + url + "\"";
      }
      start = matcher.end();
    }
    double durationMin = (System.currentTimeMillis() - startTime) / (1000.0 * 60);
    String subject = String.format("%s Completed in %.3fmin", testName, durationMin);
    File zip = new File(workingDir, testName + ".zip");
    File pdf = new File(workingDir, testName + ".pdf");
    
    String append = "<hr/>" + Stream.of(zip, pdf, new File(workingDir, testName + ".html"))
                                .map(file -> String.format("<p><a href=\"%s\">%s</a></p>", uploads.get(file.getAbsoluteFile()), file.getName()))
                                .reduce((a, b) -> a + b).get();
    String endTag = "</body>";
    if (replacedHtml.contains(endTag)) {
      replacedHtml.replace(endTag, append + endTag);
    }
    else {
      replacedHtml += append;
    }
    File[] attachments = isEmailFiles() ? new File[]{zip, pdf} : new File[]{};
    SESUtil.send(AmazonSimpleEmailServiceClientBuilder.defaultClient(),
                 subject, emailAddress, replacedHtml, replacedHtml, attachments
    );
  }
  
  private void sendStartEmail(final String testName, final SerializableConsumer<NotebookOutput> fn) throws IOException, URISyntaxException {
    String publicHostname = IOUtils.toString(new URI("http://169.254.169.254/latest/meta-data/public-hostname"), "UTF-8");
    String instanceId = IOUtils.toString(new URI("http://169.254.169.254/latest/meta-data/instance-id"), "UTF-8");
    String functionJson = new ObjectMapper()
                            .enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL)
                            .enable(SerializationFeature.INDENT_OUTPUT).writer().writeValueAsString(fn);
    //https://console.aws.amazon.com/ec2/v2/home?region=us-east-1#Instances:search=i-00651bdfd6121e199
    String html = "<html><body>" +
                    "<p><a href=\"http://" + publicHostname + ":1080/\">The tiledTexturePaintingPhase can be monitored at " + publicHostname + "</a></p><hr/>" +
                    "<p><a href=\"https://console.aws.amazon.com/ec2/v2/home?region=us-east-1#Instances:search=" + instanceId + "\">View instance " + instanceId + " on AWS Console</a></p><hr/>" +
                    "<p>Script Definition:" +
                    "<pre>" + functionJson + "</pre></p>" +
                    "</body></html>";
    String txtBody = "Process Started at " + new Date();
    String subject = testName + " Starting";
    SESUtil.send(AmazonSimpleEmailServiceClientBuilder.defaultClient(),
                 subject, emailAddress, txtBody,
                 html
    );
  }
  
  public boolean isEmailFiles() {
    return emailFiles;
  }
  
  public EC2NotebookRunner setEmailFiles(boolean emailFiles) {
    this.emailFiles = emailFiles;
    return this;
  }
}
