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
import com.simiacryptus.notebook.MarkdownNotebookOutput;
import com.simiacryptus.notebook.NotebookOutput;
import com.simiacryptus.ref.wrappers.RefString;
import com.simiacryptus.ref.wrappers.RefSystem;
import com.simiacryptus.util.JsonUtil;
import com.simiacryptus.util.Util;
import com.simiacryptus.util.test.SysOutInterceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.File;
import java.util.Date;
import java.util.Random;

public class RemoteExecutionDemo {

  static final Logger logger = LoggerFactory.getLogger(RemoteExecutionDemo.class);
  private static final String default_bucket = "simiacryptus";
  private static final String default_instanceType = "t2.micro";
  private static final String default_imageId = "ami-330eab4c";
  private static final String default_username = "ec2-user";
  private static final String gitBase = "https://github.com/SimiaCryptus/aws-utilities";

  static {
    SysOutInterceptor.INSTANCE.init();
  }

  public static void main(String... args) throws Exception {
    try (NotebookOutput log = new MarkdownNotebookOutput(
        new File("target/report/" + Util.dateStr("yyyyMMddHHmmss") + "/index"), true)) {
      new RemoteExecutionDemo().demo(log);
    }
  }

  public void demo(@Nonnull final NotebookOutput log) {
    AmazonEC2 ec2 = AmazonEC2ClientBuilder.standard().withRegion(EC2Util.REGION).build();
    AmazonIdentityManagement iam = AmazonIdentityManagementClientBuilder.standard().withRegion(EC2Util.REGION).build();
    AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(EC2Util.REGION).build();

    AwsTendrilNodeSettings settings = log.eval(() -> {
      return JsonUtil.cache(new File("settings.json"), AwsTendrilNodeSettings.class, () -> {
        return EC2Util.setup(ec2, iam, default_bucket, default_instanceType, default_imageId, default_username);
      });
    });

    log.eval(() -> {
      int localControlPort = new Random().nextInt(1024) + 1024;
      try (EC2Util.EC2Node node = settings.startNode(ec2, localControlPort)) {
        //node.shell();
        assert node != null;
        try (TendrilControl remoteJvm = node.startJvm(ec2, s3, settings, localControlPort)) {
          return remoteJvm.eval(() -> {
            String msg = RefString.format("Hello World! The time is %s", new Date());
            RefSystem.out.println("Returning Value: " + msg);
            return msg;
          });
        } catch (Exception e) {
          throw new RuntimeException(e);
        } finally {
          logger.info("Pausing to demonstrate automatic shutdown...");
          for (int i = 0; i < 10; i++) {
            String state = node.getStatus().getState().getName();
            logger.info("Current Machine State: " + state);
            if (!"running".equals(state))
              break;
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
