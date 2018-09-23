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

import com.simiacryptus.util.JsonUtil;

import java.io.File;
import java.io.IOException;
import java.util.Scanner;

/**
 * The type User settings.
 */
public class UserSettings {
  /**
   * The Email address.
   */
  public String emailAddress;

  private UserSettings() {

  }

  /**
   * Load user settings.
   *
   * @return the user settings
   */
  public static UserSettings load() {
    try {
      return JsonUtil.cache(new File("user-settings.json"), UserSettings.class, () -> {
        UserSettings userSettings = new UserSettings();
        Scanner scanner = new Scanner(System.in);
        System.out.print("Enter user email address: ");
        userSettings.emailAddress = scanner.nextLine();
        return userSettings;
      });
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
