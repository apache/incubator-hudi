/*
 * Copyright (c) 2019 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.common.table.timeline.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.hadoop.fs.Path;

@JsonIgnoreProperties(ignoreUnknown = true)
public class FilePathDTO {

  @JsonProperty("uri")
  private String uri;

  public static FilePathDTO fromPath(Path path) {
    if (null == path) {
      return null;
    }
    FilePathDTO dto = new FilePathDTO();
    dto.uri = path.toUri().toString();
    return dto;
  }

  public static Path toPath(FilePathDTO dto) {
    if (null == dto) {
      return null;
    }

    try {
      return new Path(new URI(dto.uri));
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }
}
