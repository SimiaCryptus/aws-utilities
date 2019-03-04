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

/**
 * The type Ec 2 settings.
 */
public class EC2NodeSettings {

  public static final String AMI_AMAZON_DEEP_LEARNING = System.getProperty("AMI_AMAZON_DEEP_LEARNING", AMI_AMAZON_DEEP_LEARNING());

  private static String AMI_AMAZON_DEEP_LEARNING() {
    switch(EC2Util.REGION) {
      case US_EAST_1:
        return "ami-02d5d2820a4b1293f"; //"ami-003c401895188b246";
      case US_WEST_2:
        return "ami-054cf807dc5790510"; //"ami-003c401895188b246";
    }
    throw new IllegalArgumentException("EC2Util.REGION: " + EC2Util.REGION);
  }
  private static String AMI_AMAZON_LINUX() {
    switch(EC2Util.REGION) {
      case US_EAST_1:
        return "ami-0622127045c41c9c7";
      case US_WEST_2:
        return "ami-03465eaeb7b2fe8ca";
    }
    throw new IllegalArgumentException("EC2Util.REGION: " + EC2Util.REGION);
  }

  public static final String AMI_AMAZON_LINUX = System.getProperty("AMI_AMAZON_LINUX", AMI_AMAZON_LINUX());

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

  /**
   * The constant EC2_TYPE.
   */
  public final String machineType;
  /**
   * The constant EC2_AMI.
   */
  public final String imageId;
  /**
   * The constant EC2_LOGIN.
   */
  public final String username;

  public EC2NodeSettings(final String ec2_type, final String ec2_ami, final String ec2_login) {
    username = ec2_login;
    imageId = ec2_ami;
    machineType = ec2_type;
  }

  public EC2NodeSettings withAMI(final String ec2_ami) {
    return new EC2NodeSettings(username, ec2_ami, machineType);
  }
}
