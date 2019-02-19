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

import javax.annotation.Nonnull;

/**
 * The type Aws tendril settings.
 */
public final class AwsTendrilNodeSettings extends AwsTendrilEnvSettings {
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
  protected AwsTendrilNodeSettings() {
    super();
  }

  public AwsTendrilNodeSettings(final String securityGroup, final String instanceProfileArn, final String bucket) {
    super(securityGroup, instanceProfileArn, bucket);
  }

  public AwsTendrilNodeSettings(AwsTendrilEnvSettings parent) {
    super(parent.securityGroup, parent.instanceProfileArn, parent.bucket);
  }

  /**
   * Jvm config tendril . jvm config.
   *
   * @return the tendril . jvm config
   */
  @Nonnull
  public Tendril.JvmConfig newJvmConfig() {
    return new Tendril.JvmConfig(this.imageId, this.instanceType, this.username);
  }

  /**
   * Start node ec 2 util . ec 2 node.
   *
   * @param ec2              the ec 2
   * @param localControlPort the local control port
   * @return the ec 2 util . ec 2 node
   */
  public EC2Util.EC2Node startNode(final AmazonEC2 ec2, final int localControlPort) {
    return EC2Util.start(ec2, newJvmConfig(), getServiceConfig(ec2), localControlPort);
  }

}
