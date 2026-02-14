# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Simple finder for LZ4 when no CMake config package is installed (e.g. Homebrew).

if(LZ4_INCLUDE_DIR AND LZ4_LIBRARY)
  set(LZ4_FOUND TRUE)
else()
  find_path(
    LZ4_INCLUDE_DIR lz4.h
    HINTS /opt/homebrew /usr/local
    PATH_SUFFIXES include)

  find_library(
    LZ4_LIBRARY
    NAMES lz4 liblz4
    HINTS /opt/homebrew /usr/local
    PATH_SUFFIXES lib)

  if(LZ4_INCLUDE_DIR AND LZ4_LIBRARY)
    set(LZ4_FOUND TRUE)
  else()
    set(LZ4_FOUND FALSE)
  endif()
endif()

if(LZ4_FOUND)
  set(LZ4_INCLUDE_DIRS ${LZ4_INCLUDE_DIR})
  set(LZ4_LIBRARIES ${LZ4_LIBRARY})
  if(NOT TARGET LZ4::lz4)
    add_library(LZ4::lz4 UNKNOWN IMPORTED)
    set_target_properties(
      LZ4::lz4
      PROPERTIES IMPORTED_LOCATION ${LZ4_LIBRARY}
                 INTERFACE_INCLUDE_DIRECTORIES ${LZ4_INCLUDE_DIR})
  endif()
  message(STATUS "Found LZ4: ${LZ4_LIBRARY}")
else()
  message(FATAL_ERROR "Could not find LZ4. Set LZ4_DIR or CMAKE_PREFIX_PATH.")
endif()

mark_as_advanced(LZ4_INCLUDE_DIR LZ4_LIBRARY)
