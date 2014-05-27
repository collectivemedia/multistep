#-------------------------------------------------------------------------------
#
# Package multistep 
#
# run.stage
# 
# Sergei Izrailev, Jeremy Stanley 2011-2014
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

run.stage <- function(
      stage.name      # Name of the stage
      , expr          # Set of commands to run
) {
   
   start.time <- Sys.time()
   
   if (nchar(stage.name) > 64) 
   {
      stop(cm.varsub.sql("Length of stage name SN (LEN) exceeds the maximum of 64 characters.", 
                  list(SN = stage.name, len = nchar(stage.name))))
   }
   
   if ( !is.null( ds.multistep.get.state.var("stage.id") ) )
   {
      stop("Nested stages are not supported.")
   }
   
   # Constants   
   dsn <- get.config.var("dsn")
   tab.stages <- get.config.var("tab.stages")
   tab.run.ids <- get.config.var("tab.run.ids")
   tab.run.stages <- get.config.var("tab.run.stages")
   tab.run.steps <- get.config.var("tab.run.steps")
   stage.type <- get.config.var("stage.type")
   run.id <- get.config.var("run.id")
   run.type <- get.config.var("run.type")
   temp.dir <- get.config.var("temp.dir")
   
   stage.name <- as.character(stage.name)
   
   # Constants for logging
   logs.dir <- get.config.var("logs.dir")
   logs.split <- get.config.var("logs.split")
   logs.file.template <- get.config.var("logs.file.template")
   logs.overwrite <- get.config.var("logs.overwrite")
   logs.user.group <- get.config.var("logs.user.group")
   logs.grp.permissions <- get.config.var("logs.grp.permissions")
   
   # Get the stage id and whether to run it.
   if (stage.type == "FIXED")
   {
      # Get the stage id from the stages table
      stage.id <- cm.nzquery("
                  select stage_id from TAB.STAGES
                  where run_type = RUN.TYPE and stage_name = STAGE.NAME"
            , list(TAB.STAGES = tab.stages, STAGE.NAME = stage.name, RUN.TYPE = run.type)
            , convert.names = TRUE
            , dsn = dsn)$stage.id
      
      # See if this stage id needs to be run. 
      do.run <- cm.nzquery("
                  select do_run from TAB.RUN.STAGES 
                  where run_id = RUN.ID and stage_id = STAGE.ID"
            , list(TAB.RUN.STAGES = tab.run.stages, STAGE.ID = stage.id, RUN.ID = run.id)
            , convert.names = TRUE
            , dsn = dsn)$do.run
      
      if (is.null(do.run) || is.na(do.run)) 
      {
         do.run <- 0
         cm.nzquery("
                     update TAB.RUN.STAGES set do_run = 0 
                     where run_id = RUN.ID and stage_id = STAGE.ID"
               , list(TAB.RUN.STAGES = tab.run.stages, STAGE.ID = stage.id, RUN.ID = run.id)
               , dsn = dsn)
      }      
   }
   else if (stage.type == "FLEX")
   {
      # Increment from the previous stage.
      stage.id <- 1 + cm.nzquery("
                  select max(stage_id) as stage_id from TAB.RUN.STAGES
                  where run_id = RUN.ID"
            , list(TAB.RUN.STAGES = tab.run.stages, RUN.ID = run.id)
            , convert.names = TRUE
            , dsn = dsn)$stage.id 
      if (is.na(stage.id)) stage.id <- 1 # first stage
      do.run <- 1
      
      # Create a new record
      cm.nzquery("
                  insert into TAB.RUN.STAGES (run_id, stage_id, stage_name, do_run)
                  values (RUN.ID, STAGE.ID, STAGE.NAME, 1)
                  ", list(
                  RUN.ID = run.id
                  , STAGE.ID = stage.id
                  , STAGE.NAME = tab.stages
                  , TAB.RUN.STAGES = tab.run.stages
            ), dsn = dsn)
   }
   else
   {
      stop(paste("Unknown stage type:", stage.type))
   }
   
   # Check to see if any prior stages had errors
   errors <- cm.nzquery("
               select error from TAB.RUN.STAGES where run_id = RUN.ID and attempted = 1 and stage_id < STAGE.ID"
         , list(
               RUN.ID = run.id
               , STAGE.ID = stage.id
               , TAB.RUN.STAGES = tab.run.stages
         ), dsn = dsn)$ERROR
   
   if (any(errors == 1)) stop(paste("Prior errors for run.id", run.id, "exist."))
   
   # Check to see if this stage has been run already
   attempted <- cm.nzquery("
               select attempted from TAB.RUN.STAGES where run_id = RUN.ID and stage_id = STAGE.ID"
         , list(
               RUN.ID = run.id
               , STAGE.ID = stage.id
               , TAB.RUN.STAGES = tab.run.stages
         ), dsn = dsn)$ATTEMPTED
   
   if (attempted == 1) stop(paste("This stage.id", stage.id, "has been already run for run.id", run.id))
   
   if (!run)
   {
      # Just update the record in run_stages
      cm.nzquery("
                  update TAB.RUN.STAGES set
                  start_time = START.TIME
                  , end_time = END.TIME
                  , elapsed = 0
                  where
                  run_id = RUN.ID 
                  and stage_id = STAGE.ID
                  ", list(
                  RUN.ID = run.id
                  , TAB.RUN.STAGES = tab.run.stages
                  , STAGE.ID = stage.id
                  , START.TIME = start.time
                  , END.TIME = Sys.time()
            ), dsn = dsn)
      return(0);
   }
   
   # Run the stage.
   
   
   # Start clock
   tic(stage.name)
   
   # Save the stage id
   set.state.var("stage.id", stage.id)
   
   # Execute the expression and identify if there was an error
   try <- tryCatch(
         {
            expr
            list(error = FALSE, warning = FALSE, message = NA)
         },
         error = function(e) 
         {
            list(error = TRUE, warning = FALSE, message = e$message)
         },
         warning = function(e)
         {
            list(error = FALSE, warning = TRUE, message = e$message)
         })
   
   # Clear the stage id
   set.state.var("stage.id", NA)
   
   # Grab end time
   end.time <- Sys.time()   
   timing <- toc(quiet = TRUE)
   elapsed <- timing$toc - timing$tic   
   
   # Construct data.frame
   df <- data.frame(
         run.id = as.character(run.id),
         stage.id = stage.id,
         stage.name = stage.name,
         do.run = 1,
         attempted = 1,
         error = as.integer(try$error),
         warning = as.integer(try$warning),
         start.time = as.character(start.time),
         end.time = as.character(end.time),
         elapsed = elapsed,
         message = substr(try$message, 1, 255),
         stringsAsFactors = FALSE)
   
   # Delete data from run_stages
   cm.nzquery("delete from TAB.RUN.STAGES where run_id = RUN.ID and stage_id = STAGE.ID"
         , list(RUN.ID = run.id, STAGE.ID = stage.id, TAB.RUN.STAGES = tab.run.stages)
         , dsn = dsn)
   
   # Store all in database table
   cm.nzinsert("run_stages", df, dsn = dsn, gen.stats = FALSE)
   
   # Throw error if one occurred
   if (try$error) {      
      stop(try$message)      
   }
}

#-------------------------------------------------------------------------------
