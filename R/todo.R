#-------------------------------------------------------------------------------
#
# Package multistep 
#
# Assorted TODO lists
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

# TODO: parameterize max string lengths (step_name, stage_name, etc).

#-------------------------------------------------------------------------------


# Different processes can log their activities either in different sets of tables, 
# differentiated by the prefix, or in the same set of tables but using a different
# run_type in the run_ids table.

# Place logs in /data1/datasci/dslogs/<run.id>/<stage>/<step>.log or local dir
# if target dir (/data1/datasci/dslogs) is not specified.

# A stage will fail if any of the previous stages had errors.
# A stage will fail if it has been already attempted.

# LOGGING
# 3 levels: run, stage, step
# all substitution fields must be non-NA (special function?)
# depth-first traversal:
#   enter
#       if there's a template for this level (not NULL)
#          then construct a file name
#       if the constructed file name != current file name
#          then
#             truncate the file if overwrite == TRUE
#             sink to the constructed file name in append mode
#       else
#          keep using the previous file (or nothing)
#   exit
#       pop the sink stack
# TODO: implement single template logging (all fields must be non-NA). - not sure about this anymore
# TODO: fix initialization to reflect the above (no more logs.split)

# TODO: verify constraints 
#       - both stage names and stage ids are unique per run_id in run_stages
#       - step ids are unique per run_id in run_steps
#       - step names are unique per stage_name and run_id in run_steps
#       - both stage names and stage ids are unique per run_type in stages

# TODO: wrap the whole thing into another layer called 'run.multistep'
#       This is done to abstract the start/finish events for the whole process.

# TODO: implement logging from stage only
# TODO: check that the file name works b/c of lower case of the names in the list
# TODO: implement optional for stage and step => warning vs error event on failure
# TODO: implement working.dir 
#       replace fail on _any_ prior stage failing with stages on which current stage depends
# TODO* Throw an error if run.stage() calls are nested in FLEX mode


# TODO: finalize running R scripts as separate processes
#       Here, consider 2 scenarios. First is that the child script is aware
#       of the multistep framework. Second, is that it's a 3rd part script that 
#       cannot be modified and instrumented. The latter can generalize to running
#       an arbitrary shell script.
# TODO: add the ability to run scripts remotely via ssh

# TODO: implement passing data from parent to child process
#       three possibilities:
#       1. (R only) save data as a temp .RData file and pass the name to child
#       2. (R only) save data in the database using cm.nzsave.rdata
#       3. Pass everything via args
#       (3) is too verbose; (2) only adds complexity over (1) - need to clean up
#       the database; so stick with (1), which should also allow for passing 
#       user-defined data. 
#       *** Use (1) and pass the temp file name and the user file name in --args
# TODO: populate run_params with run options
# TODO: Do we need get/set run_id? could it be done as "if passed in args, then get, otherwise, set"?
# TODO: consider only using set.run.id in the outher function, where it is set
#       to a new run.id if not supplied on the command line, or executes a get
#       if provided via -runid
# TODO: save everything passed in --args to pass to the all scripts - perhaps in the init function
# TODO: pass step name and step id down to other steps
# TODO: pass the stage name and id to the steps
# TODO: implement passing environment to scripts: write out to temp file, have the separate process load it.
#       pass the temp file name as arg and load it in the init() function.


# TODO: instrument the start/finish events + error/warning
#       provide with a default implementation, which could simply be printing
#       and calling 'stop' or 'warning' on error.
#       pass copies of options and state, so that the caller can't modify them
# TODO: define ds.stop or another way to signal errors and change all stop() calls to that
# TODO: provide an implementation for notifications by email
#       figure out where and how to set up lists of email addresses
# TODO: provide an implementation of the top level events writing to dsproc_status.

# TODO: implement restart

# TODO: add some printing of all parameters
# TODO: document the logs templates, e.g. "LOGS.DIR/RUN.ID/STAGE.NAME/STEP.NAME.log" for steps

# Two modes, indicated by stage.type (set during initialization). 
# FIXED - The stages are fixed and created in the 'stages' table with a unique 
#         run_type and stage_ids. The stage ids indicate the order in which they  
#         are expected to run, however the order is not enforced, i.e., no error 
#         will be thrown if the code runs the stages in a different order. 
#         Initially, the stages are copied to the run_stages tables and the 'run'
#         flag is set to 1 for all stages. The process can then turn some of the
#         stages off by setting the 'run' flag to 0.
#
# FLEX  - The stages are flexible and are not registered in the 'stages' table.
#         This allows to create a multistep process and log the progress even 
#         if the stages are constantly changing or are ad hoc.
#         Stage id is assigned at the time when each stage is run. 

# TODO: for FLEX provide means to populate stage descriptions somehow

# TODO: add functionality to be able to make some stages optional. In this
#       case, an error in the stage will be flagged as a warning in the run_stages
#       table rather than an error, and the following stages will be executed.
#       The purpose is to flag leaves in the process with no downstream dependencies.

# TODO: add functionality to populate the 'stages' table. One possibility is 
#       to create a new record from an ad hoc process.

# TODO: R function calls without scripts. Has to be a function signature where 
#       the required parameters are whatever we pass to the step on command line.
#       so the use would be 
#       ao.run("Rfunc",  "input",   "myfunc", arg1 = val1, arg2 = val2)
#       myfunc <- function(stage.id, stage.name, stage.type, step.name, step.type, step.id = NULL, ...)
#       - with perhaps other required args, and the arg1, arg2 are passed as part of ...

# LATER:
# TODO: refactor to use S3 classes
# TODO: make into a separate package 
# TODO: resolve dependencies on cmrutils
#-------------------------------------------------------------------------------
