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
import com.amazonaws.services.identitymanagement.AmazonIdentityManagement;
import com.amazonaws.services.identitymanagement.AmazonIdentityManagementClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.simpleemail.AmazonSimpleEmailServiceClientBuilder;
import com.simiacryptus.util.Util;
import com.simiacryptus.util.io.JsonUtil;
import com.simiacryptus.util.io.MarkdownNotebookOutput;
import com.simiacryptus.util.test.SysOutInterceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.*;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Date;
import java.util.Random;

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
  
  public static void main(String... args) throws Exception {
    try (MarkdownNotebookOutput log = new MarkdownNotebookOutput(
      new File("target/report/" + Util.dateStr("yyyyMMddHHmmss") + "/index"),
      gitBase + "/tree/master/src/")) {
      new RemoteNotebookDemo().launcherNotebook(log);
    }
  }
  
  public static AmazonEC2 getEc2() {
    return AmazonEC2ClientBuilder.standard().withRegion(Regions.US_EAST_1).build();
  }
  
  public static AmazonIdentityManagement getIam() {
    return AmazonIdentityManagementClientBuilder.standard().withRegion(Regions.US_EAST_1).build();
  }
  
  public static AmazonS3 getS3() {
    return AmazonS3ClientBuilder.standard().withRegion(Regions.US_WEST_2).build();
  }
  
  public void launcherNotebook(final MarkdownNotebookOutput log) {
    AwsTendrilSettings settings = log.code(() -> {
      return JsonUtil.cache(new File("settings.json"), AwsTendrilSettings.class,
        () -> AwsTendrilSettings.setup(getEc2(), getIam(), default_bucket, default_instanceType, default_imageId, default_username));
    });
    int localControlPort = new Random().nextInt(1024) + 1024;
    EC2Util.EC2Node node = settings.startNode(getEc2(), localControlPort);
    log.code(() -> {
      Tendril.TendrilControl tendrilControl = node.startJvm(getEc2(), getS3(), settings, localControlPort);
      tendrilControl.start(this::nodeMain);
    });
    try {
      Desktop.getDesktop().browse(new URI(String.format("http://%s:1080/", node.getStatus().getPublicIpAddress())));
    } catch (IOException | URISyntaxException e) {
      throw new RuntimeException(e);
    }
    for (int i = 0; i < 100; i++) {
      String state = log.code(() -> {
        return node.getStatus().getState().getName();
      });
      if (!"running".equals(state)) break;
      try {
        Thread.sleep(15 * 1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }
  
  private void nodeMain() {
    try {
      String dateStr = Util.dateStr("yyyyMMddHHmmss");
      try (MarkdownNotebookOutput log = new MarkdownNotebookOutput(
        new File("report/" + dateStr + "/" + testName),
        gitBase + "/tree/master/src/", 1080)) {
        log.onComplete(workingDir -> {
          S3Util.upload(getS3(), default_bucket, "reports/", workingDir);
        });
        log.onComplete(workingDir -> {
          SESUtil.send(AmazonSimpleEmailServiceClientBuilder.defaultClient(),
            "Demo Report", to, "Test Report",
            new File(workingDir, testName + ".zip"),
            new File(workingDir, testName + ".pdf"));
        });
        nodeTaskNotebook(log);
        logger.info("Finished worker process");
      } catch (IOException e) {
        e.printStackTrace();
        logger.warn("Error!", e);
      }
    } catch (Throwable e) {
      e.printStackTrace();
      logger.warn("Error!", e);
    } finally {
      logger.info("Exiting node worker");
      System.exit(0);
    }
  }
  
  public void nodeTaskNotebook(final MarkdownNotebookOutput log) {
    logger.info("Running worker process");
    for (int i = 0; i < 10; i++) {
      logger.info("Running worker loop " + i);
      log.code(() -> {
        try {
          System.out.println(String.format("The time is now %s", new Date()));
          Thread.sleep(10000);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      });
      logger.info("Finished worker loop " + i);
    }
  }
  
}
