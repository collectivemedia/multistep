#-------------------------------------------------------------------------------
#
# Package multistep 
#
# tests for MultiStep class constructor
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

context("Test MultiStep class construction")

#-------------------------------------------------------------------------------

test_that("no default constructor with empty slots", 
{
   expect_error(new("MultiStep", config = list(), current = list()))    
   expect_error(new("MultiStep", current = list()))    
   expect_error(new("MultiStep", config = list()))    
   expect_error(new("MultiStep"))    
   expect_error(MultiStep(config = list(), current = list()))    
   expect_error(MultiStep(current = list()))    
   expect_error(MultiStep(config = list()))    
})

#-------------------------------------------------------------------------------

test_that("default constructor functional", 
{
   ms <- MultiStep()
   expect_true(length(ms@config) > 0 && length(ms@current) > 0)    
   expect_error(MultiStep(dsn = "gibbrish_that_couldn't possibly_exist"))    
   expect_error(MultiStep(stage.type = "some other garbage"))
   expect_true(ms@config$stage.type %in% c("FLEX", "FIXED"))
   # Add more error checking
})

#-------------------------------------------------------------------------------
