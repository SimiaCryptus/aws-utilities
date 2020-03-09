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

import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.identitymanagement.AmazonIdentityManagement;
import com.amazonaws.services.identitymanagement.AmazonIdentityManagementClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.simpleemail.AmazonSimpleEmailServiceClientBuilder;
import com.simiacryptus.notebook.MarkdownNotebookOutput;
import com.simiacryptus.notebook.NotebookOutput;
import com.simiacryptus.ref.wrappers.RefString;
import com.simiacryptus.ref.wrappers.RefSystem;
import com.simiacryptus.util.JsonUtil;
import com.simiacryptus.util.Util;
import com.simiacryptus.util.test.SysOutInterceptor;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.awt.*;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.Date;
import java.util.Random;
import java.util.UUID;

public class RemoteNotebookDemo {

  static final Logger logger = LoggerFactory.getLogger(RemoteNotebookDemo.class);
  private static final String to = "acharneski+mindseye@gmail.com";
  private static final String gitBase = "https://github.com/SimiaCryptus/aws-utilities";
  private static final String default_bucket = "simiacryptus";
  private static final String default_instanceType = "t2.micro";
  private static final String default_imageId = "ami-330eab4c";
  private static final String default_username = "ec2-user";
  private static final String testName = "index";

  static {
    SysOutInterceptor.INSTANCE.init();
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

  public static void main(String... args) throws Exception {
    try (NotebookOutput log = new MarkdownNotebookOutput(
        new File("target/report/" + Util.dateStr("yyyyMMddHHmmss") + "/index"), true)) {
      new RemoteNotebookDemo().launcherNotebook(log);
    }
  }

  public void launcherNotebook(@Nonnull final NotebookOutput log) {
    AwsTendrilNodeSettings settings = log.eval(() -> {
      return JsonUtil.cache(new File("settings.json"), AwsTendrilNodeSettings.class, () -> EC2Util.setup(getEc2(),
          getIam(), default_bucket, default_instanceType, default_imageId, default_username));
    });
    int localControlPort = new Random().nextInt(1024) + 1024;
    EC2Util.EC2Node node = settings.startNode(getEc2(), localControlPort);
    log.run(() -> {
      assert node != null;
      TendrilControl tendrilControl = node.startJvm(getEc2(), getS3(), settings, localControlPort);
      tendrilControl.start(() -> {
        nodeMain();
        return null;
      });
    });
    try {
      assert node != null;
      Desktop.getDesktop().browse(new URI(RefString.format("http://%s:1080/", node.getStatus().getPublicIpAddress())));
    } catch (@Nonnull IOException | URISyntaxException e) {
      logger.info("Error opening browser", e);
    }
    for (int i = 0; i < 100; i++) {
      String state = log.eval(() -> {
        return node.getStatus().getState().getName();
      });
      if (!"running".equals(state))
        break;
      try {
        Thread.sleep(15 * 1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  public void nodeTaskNotebook(@Nonnull final NotebookOutput log) {
    logger.info("Running worker process");
    for (int i = 0; i < 10; i++) {
      logger.info("Running worker loop " + i);
      log.run(() -> {
        try {
          RefSystem.out.println(RefString.format("The time is now %s", new Date()));
          Thread.sleep(10000);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      });
      logger.info("Finished worker loop " + i);
    }
  }

  private void nodeMain() {
    try {
      final File file = new File(RefString.format("report/%s_%s", testName, UUID.randomUUID().toString()));
      try (MarkdownNotebookOutput log = new MarkdownNotebookOutput(file, 1080, true, file.getName(),
          UUID.randomUUID())) {
        log.setArchiveHome(URI.create("s3://" + default_bucket + "/reports/" + UUID.randomUUID() + "/"));
        log.onComplete(() -> {
          S3Util.upload(getS3(), log.getArchiveHome(), log.getRoot());
        });
        log.onComplete(() -> {
          String html = "";
          try {
            html = FileUtils.readFileToString(new File(log.getRoot(), testName + ".html"), Charset.defaultCharset());
          } catch (IOException e) {
            logger.warn("Error reading html", e);
          }
          SESUtil.send(AmazonSimpleEmailServiceClientBuilder.defaultClient(), "Demo Report", to, "Test Report", html,
              new File(log.getRoot(), testName + ".zip"), new File(log.getRoot(), testName + ".pdf"));
        });
        nodeTaskNotebook(log);
        logger.info("Finished worker process");
      } catch (IOException e) {
        logger.warn("Error!", e);
      }
    } catch (Throwable e) {
      logger.warn("Error!", e);
    } finally {
      logger.warn("Exiting node worker", new RuntimeException("Stack Trace"));
      RefSystem.exit(0);
    }
  }

}
