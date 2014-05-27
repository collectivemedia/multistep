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

setClass("MultiStep", 
   representation = representation(config = "list", state = "list")
   , validity = function(object) {
      if (!(res = valid.config(object@config))) 
      {
         attr(res, "msg")
      } else if (!(res = valid.config(object@config))) 
      {
         attr(res, "msg")
      } 
      else 
      {
         TRUE
      }
   }
)

#-------------------------------------------------------------------------------

setMethod(
   f = "initialize", 
   signature = "MultiStep", 
   definition = function(.Object, config, state) 
   {
      if (missing(config)) stop("Error initializing class MultiStep: empty config.")
      if (missing(state)) stop("Error initializing class MultiStep: empty state.")
      # Assign slots
      .Object@config <- config
      .Object@state <- state
      # Check validity (not called automatically when initialize is defined)
      validObject(.Object)
      return(.Object)
   }
      
)

#-------------------------------------------------------------------------------

MultiStep <- function(
      dsn = NULL,                      # DSN to connect to the database. Can be NULL if no DB is used.
      prefix = NULL,                   # used to prefix database table names
      stage.type = c("FLEX", "FIXED"),
      dbtype = c("netezza", "mysql"),
      logs.dir = NULL,                 # root directory for logging; default is current directory
      logs.stage.template = NULL,      # when TRUE, splits the output of stages into files
      logs.step.template = NULL,       # when TRUE, splits the output of steps into files 
      logs.overwrite = FALSE,          # when TRUE, overwrites the file, otherwise appends
      logs.user.group = NULL,          # log directories are changed to this group
      logs.grp.permissions = "rx",     # group permissions on the log directories; use "rwx" for group-writable
      temp.dir = Sys.getenv("TMPDIR")  # temp directory for miscellaneous file exchange 
)
{
   # Check the stage type.
   stage.type <- match.arg(stage.type)
   
   # Check for non-scalars
   if (length(dsn) > 1) stop("ds.multistep.init: dsn must be a scalar")
   if (length(prefix) > 1) stop("ds.multistep.init: prefix must be a scalar")
   if (length(logs.dir) > 1) stop("ds.multistep.init: logs.dir must be a scalar")
   
   # Check that the prefix is a valid string to prepend to a table name. 32 characters is arbitrary.
   res <- grep("[^A-Za-z0-9_]", prefix)
   if (length(res) > 0) stop(paste("ds.multistep.init: prefix contains invalid characters:", prefix))
   if (!is.null(prefix) && nchar(prefix) > 32) 
   {
      stop(paste("ds.multistep.init: prefix is too long:", nchar(prefix), "characters. Max is 32."))
   }
      
   # Check that the directory is valid
   if (!is.null(logs.dir) && !isDirectory(logs.dir)) stop(paste("Invalid logging directory:", logs.dir))
   
   
   if (!is.null(dsn)) 
   {
      # Check that the dsn is valid.
      cm.db.check.dsn(dsn)
   
      # Check dbtype
      dbtype <- match.arg(dbtype)
      dbtype.default <- options()$CM.DB.DEFAULT.DBTYPE
      if (!is.null(dbtype.default))
      {
         if (dbtype.default != dbtype)
         {
            warning(paste("dbtype (", dbtype, ") differs from the default dbtype (", 
                        dbtype.default, ") set in option CM.DB.DEFAULT.DBTYPE .", sep = ""))
         }
      }
   } 
   else
   {
      stop("Database is not optional in this version. Please provide a DSN.")
      # TODO: if we have no DSN, we can only use FLEX model
   }
   
   # Check logs directory
   if (is.null(logs.dir) || is.na(logs.dir))
   {
      logs.dir <- getwd()
   }   
   
   # Check temp directory
   if (is.null(temp.dir) || is.na(temp.dir))
   {
      temp.dir <- getwd()
   }
   # Verify that it either exists and is writable, or we can create it 
   if (file.exists(temp.dir))
   {
      if (file.access(temp.dir, mode = 2) != 0) 
      {
         stop(cm.varsub.sql("Temp directory TEMP.DIR is not writable.", list(TEMP.DIR = temp.dir)))
      }
   }
   else
   {
      tryCatch(cm.mkdir(temp.dir, user.group = logs.user.group, permissions = logs.grp.permissions),
         error = function(e)
         {
            stop(paste("Can't create temp directory '", temp.dir, "': ", e$message, sep = ""))
         })
   }
   
   # Set the constant options
   config <- list(
        dsn = dsn
      , prefix = prefix
      , tab.run.ids = cm.literal(paste(c(prefix, "run_ids"), collapse = "_"))
      , tab.run.steps = cm.literal(paste(c(prefix, "run_steps"), collapse = "_"))
      , tab.run.stages = cm.literal(paste(c(prefix, "run_stages"), collapse = "_"))
      , tab.stages = cm.literal(paste(c(prefix, "stages"), collapse = "_"))
      , tab.run.params = cm.literal(paste(c(prefix, "run_params"), collapse = "_"))
      , stage.type = stage.type
      , logs.dir = logs.dir
      , logs.stage.template = logs.stage.template
      , logs.step.template = logs.step.template
      , logs.overwrite = logs.overwrite
      , logs.user.group = logs.user.group
      , logs.grp.permissions = logs.grp.permissions
      , temp.dir = temp.dir
   )
   
   # Initialize transient values
   state <- list(
        run.id          = NA
      , stage.id        = NA
      , step.id         = NA
      , log.file        = NA
      , working.dir     = NA
   )
   return(new("MultiStep", config = config, state = state))   
}

#-------------------------------------------------------------------------------

