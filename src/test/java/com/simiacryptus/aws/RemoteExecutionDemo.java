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
import com.simiacryptus.util.Util;
import com.simiacryptus.util.io.JsonUtil;
import com.simiacryptus.util.io.MarkdownNotebookOutput;
import com.simiacryptus.util.io.NotebookOutput;
import com.simiacryptus.util.test.SysOutInterceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Date;
import java.util.Random;

/**
 * The type Remote execution demo.
 */
public class RemoteExecutionDemo {
  
  /**
   * The Logger.
   */
  static final Logger logger = LoggerFactory.getLogger(RemoteExecutionDemo.class);
  private static final String default_bucket = "simiacryptus";
  private static final String default_instanceType = "t2.micro";
  private static final String default_imageId = "ami-330eab4c";
  private static final String default_username = "ec2-user";
  private static final String gitBase = "https://github.com/SimiaCryptus/aws-utilities";
  
  static {
    SysOutInterceptor.INSTANCE.init();
  }
  
  /**
   * The entry point of application.
   *
   * @param args the input arguments
   * @throws Exception the exception
   */
  public static void main(String... args) throws Exception {
    try (NotebookOutput log = new MarkdownNotebookOutput(
      new File("target/report/" + Util.dateStr("yyyyMMddHHmmss") + "/index"),
      gitBase + "/tree/master/src/", Util.AUTO_BROWSE)) {
      new RemoteExecutionDemo().demo(log);
    }
  }
  
  /**
   * Demo.
   *
   * @param log the log
   */
  public void demo(final NotebookOutput log) {
    AmazonEC2 ec2 = AmazonEC2ClientBuilder.standard().withRegion(Regions.US_EAST_1).build();
    AmazonIdentityManagement iam = AmazonIdentityManagementClientBuilder.standard().withRegion(Regions.US_EAST_1).build();
    AmazonS3 s3 = AmazonS3ClientBuilder.standard().build();
  
    AwsTendrilNodeSettings settings = log.eval(() -> {
      return JsonUtil.cache(new File("settings.json"), AwsTendrilNodeSettings.class,
        () -> {
          return AwsTendrilNodeSettings.setup(ec2, iam, default_bucket, default_instanceType, default_imageId, default_username);
        });
    });
  
    log.eval(() -> {
      int localControlPort = new Random().nextInt(1024) + 1024;
      try (EC2Util.EC2Node node = settings.startNode(ec2, localControlPort)) {
        //node.shell();
        try (Tendril.TendrilControl remoteJvm = node.startJvm(ec2, s3, settings, localControlPort)) {
          return remoteJvm.run(() -> {
            String msg = String.format("Hello World! The time is %s", new Date());
            System.out.println("Returning Value: " + msg);
            return msg;
          });
        } catch (Exception e) {
          throw new RuntimeException(e);
        } finally {
          logger.info("Pausing to demonstrate automatic shutdown...");
          for (int i = 0; i < 10; i++) {
            String state = node.getStatus().getState().getName();
            logger.info("Current Machine State: " + state);
            if (!"running".equals(state)) break;
            try {
              Thread.sleep(15 * 1000);
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
          }
        }
      }
    });
    
  }
  
}
