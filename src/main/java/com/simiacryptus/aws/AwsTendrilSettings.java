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

import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.identitymanagement.AmazonIdentityManagement;
import com.amazonaws.services.identitymanagement.model.InstanceProfile;
import com.simiacryptus.util.io.JsonUtil;

import javax.annotation.Nonnull;

import static com.simiacryptus.aws.EC2Util.sleep;

/**
 * The type Aws tendril settings.
 */
public final class AwsTendrilSettings {
  /**
   * The Security group.
   */
  public String securityGroup;
  /**
   * The Instance profile arn.
   */
  public String instanceProfileArn;
  /**
   * The Bucket.
   */
  public String bucket;
  /**
   * The Image id.
   */
  public String imageId;
  /**
   * The Instance type.
   */
  public String instanceType;
  /**
   * The Username.
   */
  public String username;
  
  /**
   * Instantiates a new Aws tendril settings.
   */
  protected AwsTendrilSettings() {
  }
  
  /**
   * Sets .
   *
   * @param ec2          the ec 2
   * @param iam          the iam
   * @param bucket       the bucket
   * @param instanceType the instance type
   * @param imageId      the image id
   * @param username     the username
   * @return the
   */
  public static AwsTendrilSettings setup(AmazonEC2 ec2, final AmazonIdentityManagement iam, final String bucket, final String instanceType, final String imageId, final String username) {
    AwsTendrilSettings self = new AwsTendrilSettings();
    self.securityGroup = EC2Util.newSecurityGroup(ec2, 22, 1080);
    self.bucket = bucket;
    self.instanceProfileArn = EC2Util.newIamRole(iam, ("{\n" +
      "  \"Version\": \"2012-10-17\",\n" +
      "  \"Statement\": [\n" +
      "    {\n" +
      "      \"Action\": \"s3:*\",\n" +
      "      \"Effect\": \"Allow\",\n" +
      "      \"Resource\": \"arn:aws:s3:::" + self.bucket + "*\"\n" +
      "    },\n" +
      "    {\n" +
      "      \"Action\": \"s3:ListBucket*\",\n" +
      "      \"Effect\": \"Allow\",\n" +
      "      \"Resource\": \"arn:aws:s3:::*\"\n" +
      "    }\n" +
      "  ]\n" +
      "}")).getArn();
    self.imageId = imageId;
    self.instanceType = instanceType;
    self.username = username;
    sleep(30000); // Pause for objects to init
    return self;
  }
  
  @Override
  public String toString() {
    return JsonUtil.toJson(this).toString();
  }
  
  /**
   * Start node ec 2 util . ec 2 node.
   *
   * @param ec2              the ec 2
   * @param localControlPort the local control port
   * @return the ec 2 util . ec 2 node
   */
  public EC2Util.EC2Node startNode(final AmazonEC2 ec2, final int localControlPort) {
    return EC2Util.start(ec2, jvmConfig(), getServiceConfig(ec2), localControlPort);
  }
  
  /**
   * Gets service config.
   *
   * @param ec2 the ec 2
   * @return the service config
   */
  @Nonnull
  public EC2Util.ServiceConfig getServiceConfig(final AmazonEC2 ec2) {
    return new EC2Util.ServiceConfig(ec2, this.bucket, this.securityGroup, new InstanceProfile().withArn(this.instanceProfileArn));
  }
  
  /**
   * Jvm config tendril . jvm config.
   *
   * @return the tendril . jvm config
   */
  @Nonnull
  public Tendril.JvmConfig jvmConfig() {
    return new Tendril.JvmConfig(this.imageId, this.instanceType, this.username);
  }
}
