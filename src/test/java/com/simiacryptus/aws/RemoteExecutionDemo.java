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
import com.simiacryptus.util.io.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Date;
import java.util.Random;

public class RemoteExecutionDemo {
  
  private static final Logger logger = LoggerFactory.getLogger(RemoteExecutionDemo.class);
  
  public static void main(String... args) throws Exception {
    AmazonEC2 ec2 = AmazonEC2ClientBuilder.standard().withRegion(Regions.US_EAST_1).build();
    AmazonIdentityManagement iam = AmazonIdentityManagementClientBuilder.standard().withRegion(Regions.US_EAST_1).build();
    AmazonS3 s3 = AmazonS3ClientBuilder.standard().build();
    
    String bucket = "simiacryptus";
    String instanceType = "t2.micro";
    String imageId = "ami-330eab4c";
    String username = "ec2-user";
    int localControlPort = new Random().nextInt(1024) + 1024;
    
    AwsTendrilSettings settings = JsonUtil.cache(new File("settings.json"), AwsTendrilSettings.class,
      () -> AwsTendrilSettings.setup(ec2, iam, bucket, instanceType, imageId, username));
    
    try (EC2Util.EC2Node node = settings.startNode(ec2, localControlPort)) {
      Tendril.TendrilControl remoteJvm = node.startJvm(ec2, s3, settings, localControlPort);
      logger.info("Remote Test: " + remoteJvm.run(() -> {
        String msg = String.format("Hello World! The time is %s", new Date());
        System.out.println("Returning Value: " + msg);
        return msg;
      }));
    }
    
  }
  
}
