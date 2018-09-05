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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.PutObjectRequest;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

/**
 * The type S3 util.
 */
public class S3Util {
  public static Map<File, URL> upload(final AmazonS3 s3, final URI path, final File file) {
    try {
      HashMap<File, URL> map = new HashMap<>();
      if (!file.exists()) throw new RuntimeException(file.toString());
      if (file.isFile()) {
        if (path.getScheme().startsWith("s3")) {
          String key = path.getPath() + file.getName();
          s3.putObject(new PutObjectRequest(path.getHost(), key, file).withCannedAcl(CannedAccessControlList.PublicRead));
          map.put(file.getAbsoluteFile(), s3.getUrl(path.getHost(), key));
        } else {
          URI dest = path.resolve(file.getName());
          try {
            FileUtils.copyFile(file, new File(dest.getPath()));
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      } else {
        for (File subfile : file.listFiles()) {
          map.putAll(upload(s3, path.resolve(file.getName()), subfile));
        }
      }
      return map;
    } catch (Throwable e) {
      throw new RuntimeException("Error uploading " + file + " to " + path, e);
    }
  }
}
