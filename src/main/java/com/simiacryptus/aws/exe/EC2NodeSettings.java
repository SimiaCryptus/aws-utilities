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

package com.simiacryptus.aws.exe;

/**
 * The type Ec 2 settings.
 */
public class EC2NodeSettings {

    public static final EC2NodeSettings P3_2XL = new EC2NodeSettings("p3.2xlarge", "ami-2fb71c52", "ec2-user");
    public static final EC2NodeSettings P2_XL = new EC2NodeSettings("p2.xlarge", "ami-2fb71c52", "ec2-user");
    public static final EC2NodeSettings T2_L = new EC2NodeSettings("t2.large", "ami-2fb71c52", "ec2-user");
    public static final EC2NodeSettings T2_XL = new EC2NodeSettings("t2.xlarge", "ami-2fb71c52", "ec2-user");
    public static final EC2NodeSettings T2_2XL = new EC2NodeSettings("t2.2xlarge", "ami-2fb71c52", "ec2-user");

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
}
