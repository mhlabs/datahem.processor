package io.anemos.metastore.core.proto;

/*-
 * ========================LICENSE_START=================================
 * DataHem
 * %%
 * Copyright (C) 2018 - 2019 MatHem Sverige AB
 * %%
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License along with this program. If not, see <https://www.gnu.org/licenses/>.
 * =========================LICENSE_END==================================
 */

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.ExtensionRegistry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Convert {

  public static Map<String, DescriptorProtos.FileDescriptorProto> extractProtoMap(
      DescriptorProtos.FileDescriptorSet fileDescriptorSet) {
    HashMap<String, DescriptorProtos.FileDescriptorProto> map = new HashMap<>();
    fileDescriptorSet.getFileList().forEach(fdp -> map.put(fdp.getName(), fdp));
    return map;
  }

  private static Descriptors.FileDescriptor convertToFileDescriptorMap(
      String name,
      Map<String, DescriptorProtos.FileDescriptorProto> inMap,
      Map<String, Descriptors.FileDescriptor> outMap,
      ExtensionRegistry extensionRegistry) {
    if (outMap.containsKey(name)) {
      return outMap.get(name);
    }
    Descriptors.FileDescriptor fileDescriptor;
    if ("google/protobuf/descriptor.proto".equals(name)) {
      fileDescriptor = DescriptorProtos.getDescriptor();
    } else {
      DescriptorProtos.FileDescriptorProto fileDescriptorProto = inMap.get(name);
      List<Descriptors.FileDescriptor> dependencies = new ArrayList<>();
      if (fileDescriptorProto.getDependencyCount() > 0) {
        fileDescriptorProto
            .getDependencyList()
            .forEach(
                dependencyName ->
                    dependencies.add(
                        convertToFileDescriptorMap(
                            dependencyName, inMap, outMap, extensionRegistry)));
      }
      try {
        fileDescriptor =
            Descriptors.FileDescriptor.buildFrom(
                fileDescriptorProto, dependencies.toArray(new Descriptors.FileDescriptor[0]));

      } catch (Descriptors.DescriptorValidationException e) {
        throw new RuntimeException(e);
      }
    }
    outMap.put(name, fileDescriptor);
    return fileDescriptor;
  }

  public static Map<String, Descriptors.FileDescriptor> convertFileDescriptorSet(
      DescriptorProtos.FileDescriptorSet fileDescriptorSet) {
    Map<String, DescriptorProtos.FileDescriptorProto> inMap = extractProtoMap(fileDescriptorSet);
    Map<String, Descriptors.FileDescriptor> outMap = new HashMap<>();
    ExtensionRegistry registry = ExtensionRegistry.newInstance();
    inMap.forEach((k, v) -> convertToFileDescriptorMap(k, inMap, outMap, registry));

    return outMap;
  }

  // TODO Find way to do this dynamically
  public static Map<String, Descriptors.FileDescriptor> registerOptions(
      Map<String, Descriptors.FileDescriptor> fileDescriptorMap) {
    Map<String, DescriptorProtos.FileDescriptorProto> inMap = new HashMap<>();
    Map<String, Descriptors.FileDescriptor> outMap = new HashMap<>();

    fileDescriptorMap.forEach((name, fd) -> inMap.put(name, fd.toProto()));

    ExtensionRegistry registry = ExtensionRegistry.newInstance();
    inMap.forEach((k, v) -> convertToFileDescriptorMap(k, inMap, outMap, registry));

    outMap.forEach(
        (k, fileDescriptor) -> {
          Descriptors.FileDescriptor.internalUpdateFileDescriptor(fileDescriptor, registry);
        });
    return outMap;
  }
}
