#-------------------------------------------------------------------------------
#
# Package multistep 
#
# Utility functions to construct predefined stages.
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

# TODO: parametrize the constant string lengths (along with the table fields in DDL)

#-------------------------------------------------------------------------------

stages.empty.df <- function(run.type)
{
   if (nchar(run.type) > 64) 
   {
      stop(cm.varsub.sql("Length of run type RT (LEN) exceeds the maximum of 64 characters.", 
                  list(RT = run.type, len = nchar(run.type))))
   }
   df <- data.frame(run.type = character(), stage.id = integer(), stage.name = character(),
         description.short = character(), description.long = character(), 
         stringsAsFactors = FALSE) 
   attr(df, "run.type") <- run.type
   return(df)
}

#-------------------------------------------------------------------------------

stages.add <- function(df, stage.name, description.short, description.long = NA)
{
   # Error checking
   if (nchar(stage.name) > 64) 
   {
      stop(cm.varsub.sql("Length of stage name SN (LEN) exceeds the maximum of 64 characters.", 
                  list(SN = stage.name, len = nchar(stage.name))))
   }
   if (nchar(description.short) > 256) 
   {
      stop(cm.varsub.sql("Length of stage name DS (LEN) exceeds the maximum of 256 characters.", 
                  list(DS = description.short, len = nchar(description.short))))
   }
   if (nchar(description.long) > 10000) 
   {
      warning(cm.varsub.sql("Length of stage name DL (LEN) exceeds the maximum of 10,000 characters. Truncating.", 
                  list(DL = description.long, len = nchar(description.long))))
      description.long <- substr(description.long, 1, 10000)
   }
   
   run.type <- attr(df, "run.type")
   if (!class(df) == "data.frame" || is.null(run.type) || is.na(run.type))
   {
      stop("Expecting a data frame with attribute 'run.type'")      
   }
   if (nchar(run.type) > 64) 
   {
      stop(cm.varsub.sql("Length of run type RT (LEN) exceeds the maximum of 64 characters.", 
                  list(RT = run.type, len = nchar(run.type))))
   }
   
   # Do the work
   id <- nrow(df) + 1
   
   df <- rbind(df, data.frame(run.type = run.type, stage.id = id, stage.name = stage.name,
               description.short = description.short, description.long = description.long, 
               stringsAsFactors = FALSE))
   attr(df, "run.type") <- run.type
   
   return(df)   
}

#-------------------------------------------------------------------------------

stages.register <- function(df, verbose = TRUE)
{
   # Constants
   dsn <- get.config.var("dsn")
   tab.stages <- get.config.var("tab.stages")
   dbtype <- cm.db.get.dbtype(dsn)
   
   # Final checks.
   run.type <- attr(df, "run.type")
   if (!class(df) == "data.frame" || is.null(run.type) || is.na(run.type))
   {
      stop("Expecting a data frame with attribute 'run.type'")      
   }
   df$stage.id <- as.integer(df$stage.id)   
   
   if (nrow(df) == 0) stop("data frame is empty.")
   if (nrow(df) != length(unique(df$stage.name)))
   {
      stop("Stage names must be unique.")
   }
   if (nrow(df) != length(unique(df$stage.id)))
   {
      stop("Stage ids must be unique.")
   }
   lapply(c("run.type", "stage.name", "stage.id", "description.short"), 
      function(field)
      {
         if (any(is.na(df[[field]]) | is.null(df[[field]])))
         {
            stop(paste("Missing values in column", field, "."))
         }            
      })
   
   cm.nzinsert(tab.stages, df, dsn = dsn, gen.stats = (dbtype == "netezza"), verbose = verbose)
}

#-------------------------------------------------------------------------------

# Clear the errors, warnings and/or attempted flags for the specified stage name or id.
stages.clear <- function(run.id, stage.name, clear.attempted = TRUE, 
      clear.error = TRUE, clear.warning = TRUE)
{
   # Constants
   dsn <- get.config.var("dsn")
   tab.run.stages <- get.config.var("tab.run.stages")
   if (!cm.nzexists.bykey(tab.run.stages,
         list("run_id" = as.character(run.id), "stage_name" = stage.name), dsn = dsn))
   {
      stop(paste("Invalid run_id, stage_name combination: ", run.id, ", ", stage.name, sep = ""))
   }
   
   if (clear.attempted || clear.error || clear.warning)
   {
      sql <- paste("update ", tab.run.stages,  " set", sep = "")
      set <- c()
      if (clear.attempted) set <- c(set, "attempted")      
      if (clear.error) set <- c(set, "error")      
      if (clear.warning) set <- c(set, "warning")      
      set <- paste(set, "= 0")
      set <- paste(set, collapse = ", ")
      
      sql <- paste(sql, " ", set, " where run_id = ", run.id, 
            " and stage_name = '", stage.name, "'", sep = "")
      cm.nzquery(sql, dsn = dsn)
   }
}

#-------------------------------------------------------------------------------
