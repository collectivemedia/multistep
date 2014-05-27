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


if (0)
{
   
   # The overall process example.
   
   ##############################
   # INITIALIZATION
   # We are going to log in tables myjobs_run_ids, etc. using the FIXED stage type,
   # so we'll need to initialize the myjobs_stages tables with a set of fixed stages.
   # For this example, we'll have 3 stages: input, compute, and cleanup.
   # The run type is 'testproc'.
   
   # Initialize the system. 
   multistep.init(dsn = "NZS_ERNIE", prefix = "myjobs", 
         log.dir = "/data1/datasci/dslogs", stage.type = "FIXED")
   # Or
   multistep.init(dsn = "mysql_hs", prefix="hs", stage.type = "FIXED")
   
   # Install the tables
   install.db(verbose = TRUE)
   
   # Populate the stages table.
   stages <- ds.multistep.stages.empty.df(run.type = "testproc")
   stages <- ds.multistep.stages.add(stages, 
         stage.name = "input", 
         description.short = "Read and verify the inputs.", 
         description.long = "")
   stages <- ds.multistep.stages.add(stages, 
         stage.name = "compute", 
         description.short = "Perform the computation.", 
         description.long = "A potentially long description goes here.")
   stages <- ds.multistep.stages.add(stages, 
         stage.name = "cleanup", 
         description.short = "Perform backup, drop temp tables and remove temp files.", 
         description.long = "")
   
   ds.multistep.stages.register(stages)
   
   ##############################
   # Running the process
   
   # Initialize the system. 
   ds.multistep.init(dsn = "NZS_ERNIE", prefix = "myjobs", 
         log.dir = "/data1/datasci/dslogs", stage.type = "FIXED")
   
   # Create a new run id for this run.
   ds.multistep.set.run.id(run.id = cm.uid(), run.type = "testproc", save.args = TRUE)
   
   # First stage.
   ds.multistep.run.stage("input", { 
            ds.multistep.run("R",  "input",   "check_pg_integrity")   # Checks for 1+ row in tables required         
            ds.multistep.run("R",  "input",   "format_master")        # Formats the master data and pushes to NZ   *** OK
            ds.multistep.run("R",  "input",   "input_integrity")      # Check table integrity
         })
   
}
