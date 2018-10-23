#=============================================================================
# Copyright 2018 BlazingDB, Inc.
# Contributors:
#     Eyal Rozenberg <eyalroz@blazingdb.com>
#
# This file may only be used under the terms of the Apache License, version
# 2.0. A copy of the license may be obtained at:
# 
#     http://www.apache.org/licenses/LICENSE-2.0
#
#=============================================================================

set(BASE_DIR_FOR_THIRD_PARTY_CODE "${CMAKE_SOURCE_DIR}/thirdparty")

ExternalProject_Add(cuda-api-wrappers_project                            # Name for custom target
	PREFIX          CMakeFiles/cuda-api-wrappers_project                 # Root dir for entire project
	TMP_DIR         CMakeFiles/cuda-api-wrappers_project/tmp             # Directory to store temporary files
	STAMP_DIR       CMakeFiles/cuda-api-wrappers_project/stamp           # Directory to store step timestamps
	GIT_REPOSITORY  git@github.com:eyalroz/cuda-api-wrappers.git         # URL of git repo
	GIT_TAG         8c6e49a6c6c5a604a656b00f15e118968d01476e             # Git branch name, commit id or tag
	UPDATE_COMMAND  ""                                                   # Source work-tree update command
	SOURCE_DIR      "${BASE_DIR_FOR_THIRD_PARTY_CODE}/cuda-api-wrappers" # Source dir to be used for build
	BUILD_IN_SOURCE 1                                                    # Use source dir (of external project) as its build dir
	INSTALL_COMMAND ""                                                   # Command to drive install after build
)
ExternalProject_Get_Property(cuda-api-wrappers_project SOURCE_DIR)
include_directories(${SOURCE_DIR}/src)
link_directories(${SOURCE_DIR}/lib)

# TODO:
#
# * Consider copying the include files somewhere else
