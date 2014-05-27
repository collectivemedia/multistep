#-------------------------------------------------------------------------------
#
# Package multistep 
#
# General description 
# 
# Sergei Izrailev, 2014
#-------------------------------------------------------------------------------
# Copyright 2011-2014 Collective, Inc.
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
# http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#-------------------------------------------------------------------------------

# An important design decision is to hide all details from the end-user, so
# while the MultiStep class is used to store and check the state of the system,
# the API does not expose any of its methods directly. This may eventually change.
# The goal is to reduce the complexity to a bare minimum. 
#
# The current version is working with a singleton of the class.

#' Utilities for running, timing and logging multi-step processes from R.
#'
#' \tabular{ll}{
#' Package: \tab multistep\cr
#' Type: \tab Package\cr
#' Version: \tab 0.1\cr
#' Date: \tab 2014-05-26\cr
#' License: \tab Apache License, Version 2.0\cr
#' Author: \tab Sergei Izrailev, Jeremy Stanley\cr
#' Maintainer: \tab Sergei Izrailev <sizrailev@@collective.com>\cr
#' LazyLoad: \tab yes\cr
#' }
#' 
#' @name multistep
#' @docType package
#' @import cmrutils
#' @import R.utils
#' @import methods
NULL
