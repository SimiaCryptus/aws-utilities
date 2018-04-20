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

import java.io.File;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

/**
 * The type S 3 util.
 */
public class S3Util {
  /**
   * Upload.
   *
   * @param s3        the s 3
   * @param bucket    the bucket
   * @param namespace the namespace
   * @param file      the file
   */
  public static Map<File, URL> upload(final AmazonS3 s3, final String bucket, final String namespace, final File file) {
    HashMap<File, URL> map = new HashMap<>();
    if (!file.exists()) throw new RuntimeException(file.toString());
    if (file.isFile()) {
      String key = namespace + file.getName();
      s3.putObject(new PutObjectRequest(bucket, key, file).withCannedAcl(CannedAccessControlList.PublicRead));
      map.put(file.getAbsoluteFile(), s3.getUrl(bucket, key));
    }
    else {
      for (File subfile : file.listFiles()) {
        map.putAll(upload(s3, bucket, namespace + file.getName() + "/", subfile));
      }
    }
    return map;
  }
}
