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

import com.simiacryptus.util.JsonUtil;
import com.simiacryptus.util.Util;

import java.io.File;
import java.io.IOException;
import java.util.Scanner;

public class UserSettings {
  private String emailAddress;

  private UserSettings() {
  }

  public static UserSettings load() {
    try {
      return JsonUtil.cache(new File("user-settings.json"), UserSettings.class, () -> {
        UserSettings userSettings = new UserSettings();

        String email_env = System.getenv("EMAIL");
        if (email_env != null) {
          userSettings.setEmailAddress(email_env);
        } else {
          Scanner scanner = new Scanner(System.in);
          System.out.print("Enter user email address: ");
          userSettings.setEmailAddress(scanner.nextLine());
        }
        return userSettings;
      });
    } catch (IOException e) {
      throw Util.throwException(e);
    }
  }

  public String getEmailAddress() {
    String email_env = System.getenv("EMAIL");
    if (email_env != null) {
      return email_env;
    }
    return emailAddress;
  }

  public void setEmailAddress(String emailAddress) {
    this.emailAddress = emailAddress;
  }
}
