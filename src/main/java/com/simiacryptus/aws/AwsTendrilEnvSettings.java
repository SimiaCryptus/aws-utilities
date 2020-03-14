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
import com.amazonaws.services.identitymanagement.model.InstanceProfile;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.simiacryptus.util.JsonUtil;

import javax.annotation.Nonnull;
import java.io.Serializable;

import static com.simiacryptus.aws.EC2Util.sleep;

public class AwsTendrilEnvSettings implements Serializable {
  public String securityGroup;
  public String instanceProfileArn;
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

  @Nonnull
  public static AwsTendrilNodeSettings setup(final String instanceType, final String imageId, final String username,
                                             final String securityGroup, final String instanceProfileArn, final String bucket) {
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

  @Nonnull
  public static AwsTendrilEnvSettings setup(final String securityGroup, final String instanceProfileArn,
                                            final String bucket) {
    AwsTendrilEnvSettings self = new AwsTendrilEnvSettings();
    self.securityGroup = securityGroup;
    self.bucket = bucket;
    self.instanceProfileArn = instanceProfileArn;
    sleep(30000); // Pause for objects to init
    return self;
  }

  @Nonnull
  @Override
  public String toString() {
    return JsonUtil.toJson(this).toString();
  }

  @JsonIgnore
  public EC2Util.ServiceConfig getServiceConfig(@Nonnull final AmazonEC2 ec2) {
    return new EC2Util.ServiceConfig(ec2, this.securityGroup, new InstanceProfile().withArn(this.instanceProfileArn),
        this.bucket);
  }
}
