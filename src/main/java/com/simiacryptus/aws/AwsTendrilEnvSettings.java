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
import com.amazonaws.services.s3.AmazonS3;
import com.simiacryptus.util.io.JsonUtil;

import javax.annotation.Nonnull;

import java.io.Serializable;

import static com.simiacryptus.aws.EC2Util.*;

public class AwsTendrilEnvSettings implements Serializable {
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
  
  public AwsTendrilEnvSettings(final String securityGroup, final String instanceProfileArn, final String bucket) {
    this.securityGroup = securityGroup;
    this.instanceProfileArn = instanceProfileArn;
    this.bucket = bucket;
  }
  
  public AwsTendrilEnvSettings() {
    this.securityGroup = "";
    this.instanceProfileArn = "";
    this.bucket = "";
  }
  
  /**
   * Sets .
   *
   * @param ec2          the ec 2
   * @param iam          the iam
   * @param s3           the s3
   * @param instanceType the instance type
   * @param imageId      the image id
   * @param username     the username
   * @return the
   */
  public static AwsTendrilNodeSettings setup(
    AmazonEC2 ec2,
    final AmazonIdentityManagement iam,
    final AmazonS3 s3,
    final String instanceType,
    final String imageId,
    final String username
  )
  {
    return setup(ec2, iam, s3.createBucket("data-" + randomHex()).getName(), instanceType, imageId, username);
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
  public static AwsTendrilNodeSettings setup(
    AmazonEC2 ec2,
    final AmazonIdentityManagement iam,
    final String bucket,
    final String instanceType,
    final String imageId,
    final String username
  )
  {
    return setup(
      bucket,
      instanceType,
      imageId,
      username,
      EC2Util.newSecurityGroup(ec2, 22, 1080, 4040, 8080),
      EC2Util.newIamRole(iam, defaultPolicy(bucket)).getArn()
    );
  }
  
  /**
   * Sets .
   *
   * @param ec2 the ec 2
   * @param iam the iam
   * @param s3  the s3
   * @return the
   */
  public static AwsTendrilEnvSettings setup(AmazonEC2 ec2, final AmazonIdentityManagement iam, final AmazonS3 s3) {
    return setup(ec2, iam, s3.createBucket("data-" + randomHex()).getName());
  }
  
  /**
   * Sets .
   *
   * @param ec2    the ec 2
   * @param iam    the iam
   * @param bucket the bucket
   * @return the
   */
  public static AwsTendrilEnvSettings setup(AmazonEC2 ec2, final AmazonIdentityManagement iam, final String bucket) {
    return setup(bucket, EC2Util.newSecurityGroup(ec2, 22, 1080, 4040, 8080), EC2Util.newIamRole(iam, defaultPolicy(bucket)).getArn());
  }
  
  /**
   * Default policy string.
   *
   * @param bucket the bucket
   * @return the string
   */
  @Nonnull
  public static String defaultPolicy(final String bucket) {
    return "{\n" +
             "  \"Version\": \"2012-10-17\",\n" +
             "  \"Statement\": [\n" +
             "    {\n" +
             "      \"Action\": \"s3:*\",\n" +
             "      \"Effect\": \"Allow\",\n" +
             "      \"Resource\": \"arn:aws:s3:::" + bucket + "*\"\n" +
             "    },\n" +
             "    {\n" +
             "      \"Action\": \"s3:ListBucket*\",\n" +
             "      \"Effect\": \"Allow\",\n" +
             "      \"Resource\": \"arn:aws:s3:::*\"\n" +
             "    },\n" +
             "    {\n" +
             "      \"Action\": [\"ses:SendEmail\",\"ses:SendRawEmail\"],\n" +
             "      \"Effect\": \"Allow\",\n" +
             "      \"Resource\": \"*\"\n" +
             "    }\n" +
             "  ]\n" +
             "}";
  }
  
  /**
   * Sets .
   *
   * @param bucket             the bucket
   * @param instanceType       the instance type
   * @param imageId            the image id
   * @param username           the username
   * @param securityGroup      the security group
   * @param instanceProfileArn the instance profile arn
   * @return the
   */
  public static AwsTendrilNodeSettings setup(
    final String bucket,
    final String instanceType,
    final String imageId,
    final String username,
    final String securityGroup,
    final String instanceProfileArn
  )
  {
    AwsTendrilNodeSettings self = new AwsTendrilNodeSettings();
    self.securityGroup = securityGroup;
    self.bucket = bucket;
    self.instanceProfileArn = instanceProfileArn;
    self.imageId = imageId;
    self.instanceType = instanceType;
    self.username = username;
    sleep(30000); // Pause for objects to init
    return self;
  }
  
  public static AwsTendrilEnvSettings setup(final String bucket, final String securityGroup, final String instanceProfileArn) {
    AwsTendrilEnvSettings self = new AwsTendrilEnvSettings();
    self.securityGroup = securityGroup;
    self.bucket = bucket;
    self.instanceProfileArn = instanceProfileArn;
    sleep(30000); // Pause for objects to init
    return self;
  }
  
  @Override
  public String toString() {
    return JsonUtil.toJson(this).toString();
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
}
