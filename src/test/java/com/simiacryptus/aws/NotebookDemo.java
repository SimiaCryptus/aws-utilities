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
import com.amazonaws.services.ec2.model.KeyPair;
import com.amazonaws.services.identitymanagement.model.InstanceProfile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Random;

public class NotebookDemo {
  
  private static final Logger logger = LoggerFactory.getLogger(NotebookDemo.class);
  
  public static void main(String... args) throws Exception {
    AmazonEC2 ec2 = AmazonEC2ClientBuilder.standard().withRegion(Regions.US_EAST_1).build();
//    String imageId = "ami-1853ac65"; // Basic Ubuntu
//    String imageId = "ami-1853ac65";  // Basic AMZ Linux
//    String username = "ubuntu";
    InstanceProfile role = new InstanceProfile()
      .withInstanceProfileName("runpol-f2369")
      .withArn("arn:aws:iam::470240306861:instance-profile/runpol-f2369");
    String imageId = "ami-330eab4c";
    String instanceType = "t2.micro";
    String username = "ec2-user";
    KeyPair keyPair = EC2Util.getKeyPair(ec2);
    int localControlPort = new Random().nextInt(1024) + 1024;
    String groupId = EC2Util.newSecurityGroup(ec2, 22, 80);
    String javaOpts = "";
    String programArguments = "";
    String libPrefix = "lib/";
    String keyspace = "";
    String bucket = "simiacryptus";
    
    try (EC2Util.EC2Node node = EC2Util.start(ec2, imageId, instanceType, username, keyPair, groupId, role, localControlPort)) {
      //node.shell();
      Tendril.TendrilNode remoteJvm = Tendril.startRemoteJvm(node, javaOpts, programArguments, libPrefix, keyspace, bucket, localControlPort, Tendril::shouldTransfer);
      logger.info("Remote Test: " + remoteJvm.run(() -> {
        String msg = String.format("Hello World! The time is %s", new Date());
        System.out.println(msg);
        return msg;
      }));
    }
    
  }
  
}
