#-------------------------------------------------------------------------------
#
# Package multistep 
#
# run.step
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

# Runs a step of this process
run.step <- function(
      step.type = c("R", "Rscript")     # only support running R (previous versions supported SQL or stored procedures, or bash scripts)
      , step.name           # the step in the process (corresponds to the .R file prefix or the netezza table)
      , expr = NULL         # for running Rfunc type (pure R code)
      , step.id = NULL      # if NULL, the step id is assigned to the previous step.id + 1
      , optional = FALSE    # if TRUE, then only throw a warning on failure (rather than an error)
) {
   
   # Start time
   start.time <- Sys.time()
   
   # Check names
   if (nchar(step.name) > 255) 
   {
      stop(cm.varsub.sql("Length of step name SN (LEN) exceeds the maximum of 255 characters.", 
                  list(SN = step.name, len = nchar(step.name))))
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
   
   # Constants for logging
   logs.dir <- get.config.var("logs.dir")
   logs.split <- get.config.var("logs.split")
   logs.file.template <- get.config.var("logs.file.template")
   logs.overwrite <- get.config.var("logs.overwrite")
   logs.user.group <- get.config.var("logs.user.group")
   logs.grp.permissions <- get.config.var("logs.grp.permissions")
   
   do.run <- 1 # should this be a parameter to the function? why?
   
   # Check step type
   step.type <- match.arg(step.type)
   
   # User
   user <- System$getUsername()
   
   # Get the stage and the parent step info, if any
   stage.id <- get.state.var("stage.id")
   if (is.null(stage.id)) stage.id = NA
   parent.step.id <- get.state.var("step.id")
   if (is.null(parent.step.id)) parent.step.id = NA
   parent.log.file <- get.state.var("log.file")
   if (is.null(parent.log.file)) parent.log.file = NA
   parent.working.dir <- get.state.var("working.dir")
   if (is.null(parent.working.dir)) parent.working.dir = NA
   
   # Assign step.id
   if (is.null(step.id))
   {
      step.id <- 1 + cm.nzquery("
         select max(step_id) as step_id from TAB.RUN.STEPS
         where run_id = RUN.ID"
         , list(TAB.RUN.STAGES = tab.run.stages, RUN.ID = run.id)
         , convert.names = TRUE
         , dsn = dsn)$step.id    
      if (is.na(step.id)) step.id <- 1 # first step
   }
   set.state.var("step.id", step.id)
   
   # Get stage name
   if (is.na(stage.id)) stage.name <- NA
   else
   {
      stage.name <- cm.nzquery("
         select stage_name from TAB.RUN.STAGES 
         where run_id = RUN.ID and stage_id = STAGE.ID"
         , list(TAB.RUN.STAGES = tab.run.stages, RUN.ID = run.id, STAGE.ID = stage.id)
         , convert.names = TRUE
         , dsn = dsn)$stage.name    
      if (is.na(stage.name)) stop(paste("Can't find a stage with id", stage.id, "in table", tab.run.stages))
   }
   
   # Figure out where we are logging. This has to be done after all options and state vars are set.
   logs.split <- get.config.var("logs.split") 
   logs.file.template <- get.config.var("logs.file.template") 
   log.file <- logs.file(logs.file.template)
   if (log.file != parent.log.file)
   {
      # A new file, so check that we can write to it
      filepath <- dirname(getAbsolutePath(log.file))
      filename <- basename(getAbsolutePath(log.file))
      if (!file.exists(filepath))
      {
         tryCatch(cm.mkdir(filepath, user.group = logs.user.group, permissions = logs.grp.permissions),
            error = function(e)
            {
               stop(paste("Can't create log file path '", filepath, "': ", e$message, sep = ""))
            })
      }
      # Try opening the log file for writing 
      # This creates the file if it doesn't exist; if overwriting, truncates the existing file.
      conn <- tryCatch(file(log.file, ifelse(logs.overwrite, "w", "a")),
         error = function(e)
         {
            stop(paste("Can't open log file '", log.file, "' for writing: ", e$message, sep = ""))
         })
      close(conn)
      set.state.var("log.file", log.file)
   }
   
   # Create a new record
   step.df <- data.frame(
      run.id = as.character(run.id),
      stage.id = stage.id,
      stage.name = stage.name,
      step.id = step.id,
      step.name = step.name,
      step.type = step.type,
      user.name = System$getUsername(),
      parent.step.id = parent.step.id,
      do.run = 1,
      start.time = as.character(start.time),
      stringsAsFactors = FALSE)
   
   # Delete data from run_steps
   cm.nzquery("delete from TAB.RUN.STEPS where run_id = RUN.ID and step_id = STEP.ID"
         , list(RUN.ID = run.id, STEP.ID = step.id, TAB.RUN.STEPS = tab.run.steps)
         , dsn = dsn)
   
   # Store all in database table
   cm.nzinsert(tab.run.steps, step.df, dsn = dsn, gen.stats = FALSE)
   
   # Start measuring time
   tic(step.name)
   
   # Type-dependent stuff
   if (type == "R")
   {
      # Set up logging
      if (split.logs)
      {
         # redirect output to file; always append since we truncated the file earlier
         sink(file = log.file, append = TRUE, type = "output")
      }
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
      # Restore output
      if (split.logs && is.na(parent.step.id))
      {
         # redirect only when there's no parent
         sink(file = NULL, type = "output")
      }
      
   }
   else if (type == "Rscript") 
   {
      
      # Get the command line arguments needed out of options
      mode <- tolower(options()$AO.MODE)
      gdoc.list <- options()$GDOC.LOOKUP  
      gdoc <- names(gdoc.list)[gdoc.list == options()$GDOCNAME]
      
      # Define and execute R
      cmd <- paste("cd ", stage, "; R -f ", step, ".R -runid ", run.id, " -mode ", mode, " -gdoc ", gdoc, " > ", step, ".log 2>&1", sep = "")
      
      # Try to run the R, throw meaningful error if fail
      try <- tryCatch(
            system(cmd, intern = TRUE), 
            warning = function(w) {
               
               body <- paste("In script: ", stage, "/", step, ".R\nFor run.id: ", run.id, "\n\nSee log file for details.", sep = "")
               if (!warn) body <- paste(body, "\n\nAborting.", sep = "")
               
               # Mail the error
               ao.mail(
                     to = options()$AO.EMAIL.LIST,
                     subject = if (!warn) "AO Error" else "AO Warn",
                     body = body,
                     attach.file.list = paste(stage, "/", step, ".log", sep = ""))
               
               if (!warn) stop(body)
               else warning(body)
               
            })
   }
   
   # Compute elapsed time
   timing <- toc(quiet = TRUE)
   elapsed <- timing$toc - timing$tic
   
   # Restore step id, if any
   set.state.var("step.id", parent.step.id)   
   set.state.var("log.file", parent.log.file)
   set.state.var("working.dir", parent.working.dir)
   
   # Progress list
   progress <- list(
         "run_id" = as.character(run.id), # deals with int64
         "run_start_time" = as.character(run.start.time),
         "user_name" = user,
         "stage" = stage,
         "step" = step,
         "step_type" = type,
         "elapsed" = elapsed)
   
   # Print progress
   df.prog <- subset(data.frame(progress), select = -user_name)
   row.names(df.prog) <- NULL
   print(df.prog)
   
   # Store all in database table
   cm.nzinsert("run_progress", data.frame(progress, stringsAsFactors = FALSE), dsn = getOption("NZDSN"), gen.stats = FALSE)
   
}

#-------------------------------------------------------------------------------
