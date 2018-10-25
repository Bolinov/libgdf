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

ExternalProject_Add(gsl_project                                   # Name for custom target
	PREFIX          CMakeFiles/gsl_project                        # Root dir for entire project
	TMP_DIR         CMakeFiles/gsl_project/tmp                    # Directory to store temporary files
	STAMP_DIR       CMakeFiles/gsl_project/stamp                  # Directory to store step timestamps
	GIT_REPOSITORY  git@github.com:Microsoft/GSL.git              # URL of git repo
	GIT_TAG         1995e86d1ad70519465374fb4876c6ef7c9f8c61      # Git branch name, commit id or tag
	UPDATE_COMMAND  ""                                            # Source work-tree update command
	SOURCE_DIR      "${BASE_DIR_FOR_THIRD_PARTY_CODE}/gsl"        # Source dir to be used for build
	BUILD_IN_SOURCE 1                                             # Use source dir (of external project) as its build dir
	BUILD_COMMAND   ":"                                           # <- This will actually prevent building!
	INSTALL_COMMAND ""                                            # Command to drive install after build
)
ExternalProject_Get_Property(gsl_project SOURCE_DIR)
include_directories(${SOURCE_DIR}/src)
#link_directories(${SOURCE_DIR}/lib)

# TODO:
#
# * No need to actually build anything; can we disable building?
# * Consider copying the include files somewhere else
