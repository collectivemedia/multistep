#-------------------------------------------------------------------------------
#
# Package multistep 
#
# run.process
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

# Function that checks to ensure run.id does not already exist, and then inserts it into run_ids
set.run.id <- function(run.id, run.type, save.args = TRUE) 
{
   # Constants
   dsn <- get.config.var("dsn")
   tab.stages <- get.config.var("tab.stages")
   tab.run.ids <- get.config.var("tab.run.stages")
   tab.run.stages <- get.config.var("tab.run.stages")
   stage.type <- get.config.var("stage.type")
   
   # Sanity checking for run.id
   if (length(run.id) > 1) stop("run.id is not a scalar")
   if (is.null(run.id) || is.na(run.id)) stop("run.id is NULL or NA")
   run.id <- as.int64(run.id)
   if (is.na(run.id)) stop("Can't convert run.id to int64")
   
   # Sanity checking for run.type
   if (length(run.type) > 1) stop("run.type is not a scalar")
   if (is.null(run.type) || is.na(run.type)) stop("run.type is NULL or NA")
   run.type <- as.character(run.type)
   if (is.na(run.type)) stop("Can't convert run.type to character")
   if (nchar(run.type) > 64) stop("run.type is too long (max = 64 characters)")
   
   # Check if run type exists for FIXED stage types.
   if (stage.type == "FIXED")
   {
      if (!cm.nzexists.bykey(tab.stages, list("run_type" = run.type), dsn = dsn) )
      {
         stop(paste("No run type '", run.type, "' for FIXED stage type in table '", 
                     tab.stages, "'", sep = ""))
      }
      # Check the stages table: both stage_name and stage_id must be unique for the run_type.
      cm.table.integrity(tab.stages, list("run_type" = run.type), pk.set = c("stage_name"), dsn = dsn)
      cm.table.integrity(tab.stages, list("run_type" = run.type), pk.set = c("stage_id"), dsn = dsn)
   }
   
   # Check to see if this run has already been created
   if (cm.nzexists.bykey(tab.run.ids, list("run_id" = run.id), dsn = dsn))
   {
      stop("This run has already been created, run_id = ", run.id)      
   }
   
   # Set options
   set.config.var("run.id", run.id) 
   set.config.var("run.type", run.type) 
   
   # Create a new record
   cm.nzinsert(tab.run.ids, 
         list( 
               "run_id" = as.character(run.id)   # This is necessary to ensure the run.id goes is an an int64
               , "run_type" = run.type
               , "stage_type" = stage.type
               , "owner_name" = System$getUsername()
               , "system_time" = as.character(Sys.time())
         ), dsn = dsn)
   
   # For FIXED stage type, populate the run_stages table.
   if (stage.type == "FIXED")
   {
      cm.nzquery("
                  insert into TAB.RUN.STAGES (run_id, stage_id, stage_name, run)
                  select
                  RUN.ID as run_id
                  , stage_id
                  , stage_name 
                  , 1 as run
                  from TAB.STAGES
                  where run_type = RUN.TYPE
                  ", list(
                  RUN.ID = run.id
                  , RUN.TYPE = run.type 
                  , TAB.RUN.STAGES = tab.run.stages
                  , TAB.STAGES = tab.stages)
            , dsn = dsn)
   }
   
   if (save.args)
   {
      # TODO: Save the arguments passed into R for passing further on.
   }
   return(run.id)
}

#-------------------------------------------------------------------------------

# TODO: get rid of DS.MULTISTEP.DEBUG

# run.id has to be passed on the command line with '--args -runid <run.id>'
# The user must call this function at the start of each individual step running 
# in a separate OS process.
get.run.id <- function() 
{   
   # Constants
   dsn <- get.config.var("dsn")
   tab.stages <- get.config.var("tab.stages")
   tab.run.ids <- get.config.var("tab.run.stages")
   
   # Extract all args as a named key=value list
   args <- tryCatch(commandArgs(asValues = TRUE, excludeReserved = TRUE, excludeEnvVars = TRUE),
      error = function(e) 
      {
         writeLines(e$message)
         list()
      })
   
   # Check command line arguments for run id
   if ("runid" %in% names(args)) 
   {
      # Pick it up off the command line arguments
      run.id <- as.int64(args[["runid"]])
   } 
   else 
   {     
      # Bail if not debugging
      if (!options()$DS.MULTISTEP.DEBUG) 
         stop("Must pass '-runid' into script arguments in command line or set options()$DS.MULTISTEP.DEBUG to TRUE")
      
      # Get most recent (max) run_id
      warning("Argument '-runid' was not set in script call; selecting most recent run_id") 
      run.id <- as.int64(cm.nzquery("select max(run_id)::varchar(64) as run_id from TAB.IDS"
                  , list(TAB.IDS = tab.run.ids)
                  , convert.names = TRUE
                  , dsn = dsn)$run.id)
   }
   
   # Check to ensure a valid run.id
   valid.run.id <- cm.nzexists.bykey("run_ids", list("run_id" = as.character(run.id))) # Have to use as.character to deal with int64 properly
   if (!valid.run.id) stop(paste("The run.id you entered does not exist in run_ids table:", run.id))
   
   # Check if the stage type matches the one set in the init() call.
   stage.type <- cm.nzquery("select stage_type from TAB.IDS where run_id = RUN.ID"
         , list(TAB.IDS = tab.run.ids, RUN.ID  = run.id)
         , convert.names = TRUE
         , dsn = dsn)$stage.type
   if (stage.type != get.config.var("stage.type"))
   {
      stop(cm.varsub.sql("Stage type TYPE for run_id RUN.ID doesn't match the current stage type CUR",
                  list(TYPE = stage.type, RUN.ID = run.id, CUR = get.config.var(stage.type))))
   }
   
   run.type <- cm.nzquery("select run_type from TAB.IDS where run_id = RUN.ID"
         , list(TAB.IDS = tab.run.ids, RUN.ID  = run.id)
         , convert.names = TRUE
         , dsn = dsn)$run.type
   
   # Set options
   set.config.var("run.id", run.id) 
   set.config.var("run.type", run.type) 
   
   # Return if needed locally
   run.id   
}

