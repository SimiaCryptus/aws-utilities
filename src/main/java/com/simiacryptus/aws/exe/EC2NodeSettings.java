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

package com.simiacryptus.aws.exe;

import com.simiacryptus.aws.EC2Util;
import com.simiacryptus.ref.wrappers.RefSystem;

import javax.annotation.Nonnull;

public class EC2NodeSettings {

  public static final String AMI_AMAZON_DEEP_LEARNING = RefSystem
      .getProperty("AMI_AMAZON_DEEP_LEARNING", AMI_AMAZON_DEEP_LEARNING());
  public static final String AMI_AMAZON_LINUX = RefSystem.getProperty("AMI_AMAZON_LINUX",
      AMI_AMAZON_LINUX());
  public static final String AMI_BUILD = RefSystem.getProperty("AMI_BUILD",
      AMI_BUILD());
  public static final EC2NodeSettings P3_2XL = new EC2NodeSettings("p3.2xlarge", AMI_AMAZON_DEEP_LEARNING, "ec2-user");
  public static final EC2NodeSettings P3_8XL = new EC2NodeSettings("p3.8xlarge", AMI_AMAZON_DEEP_LEARNING, "ec2-user");
  public static final EC2NodeSettings P2_XL = new EC2NodeSettings("p2.xlarge", AMI_AMAZON_DEEP_LEARNING, "ec2-user");
  public static final EC2NodeSettings P2_8XL = new EC2NodeSettings("p2.8xlarge", AMI_AMAZON_DEEP_LEARNING, "ec2-user");
  public static final EC2NodeSettings T2_L = new EC2NodeSettings("t2.large", AMI_AMAZON_LINUX, "ec2-user");
  public static final EC2NodeSettings T2_XL = new EC2NodeSettings("t2.xlarge", AMI_AMAZON_LINUX, "ec2-user");
  public static final EC2NodeSettings T2_2XL = new EC2NodeSettings("t2.2xlarge", AMI_AMAZON_LINUX, "ec2-user");
  public static final EC2NodeSettings M5_L = new EC2NodeSettings("m5.large", AMI_AMAZON_LINUX, "ec2-user");
  public static final EC2NodeSettings M5_XL = new EC2NodeSettings("m5.xlarge", AMI_AMAZON_LINUX, "ec2-user");
  public static final EC2NodeSettings M5_2XL = new EC2NodeSettings("m5.2xlarge", AMI_AMAZON_LINUX, "ec2-user");
  public static final EC2NodeSettings R5_L = new EC2NodeSettings("r5.large", AMI_AMAZON_LINUX, "ec2-user");
  public static final EC2NodeSettings R5_XL = new EC2NodeSettings("r5.xlarge", AMI_AMAZON_LINUX, "ec2-user");
  public static final EC2NodeSettings R5_2XL = new EC2NodeSettings("r5.2xlarge", AMI_AMAZON_LINUX, "ec2-user");
  public final String machineType;
  public final String imageId;
  public final String username;

  public EC2NodeSettings(final String ec2_type, final String ec2_ami, final String ec2_login) {
    username = ec2_login;
    imageId = ec2_ami;
    machineType = ec2_type;
  }

  @Nonnull
  private static String AMI_AMAZON_DEEP_LEARNING() {
    // https://aws.amazon.com/marketplace/server/configuration?productId=a782650d-12cb-4a82-a284-b88e8670a16a
    switch (EC2Util.REGION) {
      case US_EAST_1:
        return "ami-00f44084952227ef0";
      case US_WEST_2:
        return "ami-01a495658aa5f7930";
    }
    throw new IllegalArgumentException("EC2Util.REGION: " + EC2Util.REGION);
  }

  @Nonnull
  private static String AMI_BUILD() {
    switch (EC2Util.REGION) {
      case US_EAST_1:
        return "ami-06fb4a66a99d92c30";
    }
    throw new IllegalArgumentException("EC2Util.REGION: " + EC2Util.REGION);
  }

  @Nonnull
  private static String AMI_AMAZON_LINUX() {
    switch (EC2Util.REGION) {
      case US_EAST_1:
        return "ami-0622127045c41c9c7";
      case US_WEST_2:
        return "ami-03465eaeb7b2fe8ca";
    }
    throw new IllegalArgumentException("EC2Util.REGION: " + EC2Util.REGION);
  }

  @Nonnull
  public EC2NodeSettings withAMI(final String ec2_ami) {
    return new EC2NodeSettings(username, ec2_ami, machineType);
  }
}
