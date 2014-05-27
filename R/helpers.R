#-------------------------------------------------------------------------------
#
# Package multistep 
#
# Helper functions
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

valid.config <- function(config)
{
   retval <- FALSE
   if (is.null(config) || is.na(config) || length(config) == 0) 
   {
      attribute(retval, "msg") <- "Empty config. Use MultiStep(...) to create MultiStep objects."
      return(retval)
   }
   if (!is.list(config)) 
   {
      attribute(retval, "msg") <- "config is not a list. Use MultiStep(...) to create MultiStep objects."
      return(retval)
   }
   required.config <- c("dsn", "prefix", "tab.run.ids", "tab.run.steps", "tab.run.stages", "tab.stages", "tab.run.params", "stage.type", "logs.dir", "logs.stage.template", "logs.step.template", "logs.overwrite", "logs.user.group", "logs.grp.permissions", "temp.dir")
   if (any(!(names(config) %in% required.config))) 
   {
      attr(retval, "msg") <- "Invalid config. Use MultiStep(...) to create MultiStep objects." 
      return(retval)
   }
   return(TRUE)
}

#-------------------------------------------------------------------------------

valid.state <- function(state)
{
   if (is.null(state) || is.na(state) || length(state) == 0) 
   {
      retval <- FALSE
      attribute(retval, "msg") <- "Empty state. Use MultiStep(...) to create MultiStep objects."
      return(retval)
   }
   required.current <- c("run.id", "stage.id", "step.id", "log.file", "working.dir")
   if (any(!(names(state) %in% required.current))) 
   {
      retval <- FALSE
      attr(retval, "msg") <- "Invalid state. Use MultiStep(...) to create MultiStep objects." 
      return(retval)
   }
   return(TRUE)
}

#-------------------------------------------------------------------------------

# Returns the singleton, if it exists and is of the right class.
get.multistep <- function()
{
   if (!exists(".multistep.obj", envir = baseenv())) 
   {
      stop("multistep not initialized. Forgot to call multistep.init?")
   }
   ms <- get(".multistep.obj", envir = baseenv())
   if (!inherits(ms, "MultiStep")) stop("multistep not initialized properly. Expecting class MultiStep.")
   if (!validObject(ms)) stop("Invalid MultiStep object")
   return(ms)   
}

#-------------------------------------------------------------------------------

# Sets the singleton.
set.multistep <- function(ms)
{
   if (!inherits(ms, "MultiStep")) stop("Expecting class MultiStep.")
   if (!validObject(ms)) stop("Invalid MultiStep object")
   assign(".multistep.obj", ms, envir=baseenv())
   invisible(ms)   
}

#-------------------------------------------------------------------------------

get.config <- function() 
{
   ms <- get.multistep()
   return( config(ms) )
}

#-------------------------------------------------------------------------------

set.config <- function(config) 
{
   ms <- get.multistep()
   config(ms) <- config
   set.multistep(ms)
   invisible(ms)
}

#-------------------------------------------------------------------------------

get.config.var <- function(var) 
{
   ms <- get.multistep()
   return( getConfigVar(ms, var) )
}

#-------------------------------------------------------------------------------

set.config.var <- function(var, val) 
{
   ms <- get.multistep()
   ms <- setConfigVar(ms, var, val)
   set.multistep(ms)
   invisible(ms)
}

#-------------------------------------------------------------------------------

get.state <- function() 
{
   ms <- get.multistep()
   return( state(ms) )
}

#-------------------------------------------------------------------------------

set.state <- function(state) 
{
   ms <- get.multistep()
   state(ms) <- state
   set.multistep(ms)
   invisible(ms)
}

#-------------------------------------------------------------------------------

get.state.var <- function(var) 
{
   ms <- get.multistep()
   return( getStateVar(ms, var) )
}

#-------------------------------------------------------------------------------

set.state.var <- function(var, val) 
{
   ms <- get.multistep()
   ms <- setStateVar(ms, var, val)
   set.multistep(ms)
   invisible(ms)
}

#-------------------------------------------------------------------------------

# Applies macros in the template and produces a log file name.
logs.file <- function(logs.file.template)
{
   ms <- get.multistep()
   config <- config(ms)
   state <- state(ms)
   lst <- c(opts, state)
   varsub <- lapply(lst, function(x) cm.literal(as.character(x)))
   names(varsub) <- toupper(names(lst))
   # Check if any of the substitutions are NULL or NA
   if ((res = cm.varsub.checknulls(logs.file.template, varsub)))
   {
      stop(paste("Substitution list in file template '", logs.file.template, 
                  "' contains variables that are NULL or NA: ", 
                  cm.quote.csv(attr(res, "var.nulls")), sep=""))   
   }
   # Run the substitutionss
   file.name <- cm.varsub.sql(logs.file.template, varsub)
   return( file.name )
}

#-------------------------------------------------------------------------------

# Unwind the sink (stop redirecting output to the previous sink).
logs.unsink <- function() 
{ 
   sink(file = NULL, type = "output") 
}

#-------------------------------------------------------------------------------

# Start redirecting output to the file. Applies group ownership permissions to the  
# logs directory, but only if it's new.
logs.sink <- function(log.file, logs.overwrite = get.config.var("logs.overwrite")) 
{
   if (is.null(log.file) || is.na(log.file) || !is.character(log.file))
   {
      stop(paste("Invalid log file '", log.file, "'", sep = ""))
   }
   filepath <- dirname(getAbsolutePath(log.file))
   filename <- basename(getAbsolutePath(log.file))
   if (!file.exists(filepath))
   {
      tryCatch(cm.mkdir(filepath, 
                        user.group  = get.config.var("logs.user.group"), 
                        permissions = get.config.var("logs.grp.permissions")),
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
   
   # redirect output to the file; always append since we truncated the file earlier
   # stderr is supposed to be tricky to catch, so catch it and write separately
   sink(file = log.file, append = TRUE, type = "output")
   
}

#-------------------------------------------------------------------------------
