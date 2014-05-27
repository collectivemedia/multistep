#-------------------------------------------------------------------------------
#
# Package multistep 
#
# General description 
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

install.db <- function(verbose = FALSE) 
{
   # Run tables
   ds.multistep.create.table("run_ids", verbose = verbose)          # Tracks each run
   ds.multistep.create.table("run_steps", verbose = verbose)        # Tracks each script
   ds.multistep.create.table("run_stages", verbose = verbose)       # Tracks each stage
   ds.multistep.create.table("stages", verbose = verbose)           # Descriptions of each stage   
}

#-------------------------------------------------------------------------------

# TODO: add indexes for non-netezza dbms.
# TODO: implement and add drop tables for non-netezza dbms
create.table <- function(table.name, verbose = FALSE) 
{  
   dsn <- get.config.var("dsn")
   dbtype <- cm.db.get.dbtype(dsn)
   
   # DBMS-specific types
   int1 <- cm.literal(switch(dbtype
               , netezza = "byteint"
               , mysql = "tinyint"
               , stop(paste("Unsupported dbtype:", dbtype)))
   )
   int4 <- cm.literal(switch(dbtype
               , netezza = "int4"
               , mysql = "integer"
               , stop(paste("Unsupported dbtype:", dbtype)))
   )
   int8 <- cm.literal(switch(dbtype
               , netezza = "bigint"
               , mysql = "bigint"
               , stop(paste("Unsupported dbtype:", dbtype)))
   )
   
   SQL <- switch(table.name,
         
      # Stages table for FIXED stage type.
      "stages" =
         paste("create table ", ds.multistep.get.option("tab.stages"), " (",
               "  run_type             varchar(64)    NOT NULL", # Run type
               ", stage_id             ", int4, "     NOT NULL", # Unique stage id for this run type
               ", stage_name           varchar(64)    NOT NULL", # Stage name
               ", description_short    varchar(256)   NOT NULL", # Short description for reporting
               ", description_long     varchar(10000)         ", # Extended description
               ifelse(dbtype == "netezza", ") distribute on random", ")"),
               sep = "\n"),         
      
      # Stores the unique identifier for every lookalike run  
      "run_ids" =
         paste("create table ", ds.multistep.get.option("tab.run.ids"), " (",
               "  run_id               ", int8, "     NOT NULL", # UID of this run
               ", run_type             varchar(64)    NOT NULL", # Run type
               ", stage_type           varchar(16)    NOT NULL", # Type of the stages ('FIXED' or 'FLEX')
               ", owner_name           varchar(255)   NOT NULL", # Who created this run
               ", system_time          timestamp      NOT NULL", # Timestamp from the originating R system when this ID was created
               ifelse(dbtype == "netezza", ") distribute on random", ")"),
               sep = "\n"),
      
      # Stores the parameters for the run in key-value form.  
      "run_params" =
         paste("create table ", ds.multistep.get.option("tab.run.params"), " (",
               "  run_id               ", int8, "     NOT NULL", # UID of this run
               ", run_type             varchar(64)    NOT NULL", # Run type
               ", param_name           varchar(64)    NOT NULL", # Parameter name
               ", param_value          varchar(255)   NOT NULL", # Parameter value
               ifelse(dbtype == "netezza", ") distribute on random", ")"),
               sep = "\n"),
      
      # Stores data related to stages
      "run_stages" =
         paste("create table ", ds.multistep.get.option("tab.run.stages"), " (",
               "  run_id               ", int8, "     NOT NULL", # UID of this run
               ", stage_id             ", int4, "     NOT NULL", # Unique (for this run) integer identifier for the stage
               ", stage_name           varchar(64)    NOT NULL", # Name of the stage
               ", run                  ", int1, "             ", # 1 = run this stage
               ", attempted            ", int1, "             ", # 1 = this stage was attempted
               ", error                ", int1, "             ", # 1 = this stage errored
               ", warning              ", int1, "             ", # 1 = this stage had a warning
               ", start_time           timestamp              ", # When the stage started
               ", end_time             timestamp              ", # When the stage ended
               ", elapsed              real                   ", # Number of seconds elapsed for stage
               ", message              varchar(255)           ", # Error or warning message, if any
               ifelse(dbtype == "netezza", ") distribute on random", ")"),
               sep = "\n"),
      
      # Stores data related to specific steps. 
      # Having a parent_step_id allows for nesting. The parent step passes its id to the children.
      # One of use cases is parallel processing: when one step actually runs multiple threads and
      # the user wants to log each thread separately. Another use case is just nesting more than 
      # two levels, which should be rare. The step ids can be cm.uid()'s. 
      "run_steps" =
         paste("create table ", ds.multistep.get.option("tab.run.steps"), " (",
               "  run_id               ", int8, "     NOT NULL", # UID of this run
               ", stage_id             ", int4, "     NOT NULL", # Identifier for the stage
               ", stage_name           varchar(64)    NOT NULL", # Stage of the process
               ", step_id              ", int8, "     NOT NULL", # Id of the step - unique for this run
               ", step_name            varchar(255)   NOT NULL", # Step in the process
               ", step_type            varchar(16)    NOT NULL", # Type of step R, sp, sql, etc.
               ", user_name            varchar(255)   NOT NULL", # User who completed this step
               ", parent_step_id       ", int8, "             ", # Id of the parent step (NULL if no parent)
               ", do_run               ", int1, "             ", # 1 = do run this step
               ", attempted            ", int1, "             ", # 1 = this step was attempted
               ", error                ", int1, "             ", # 1 = this step errored
               ", warning              ", int1, "             ", # 1 = this step had a warning
               ", start_time           timestamp              ", # Timestamp when this step was started
               ", end_time             timestamp              ", # When the step completed
               ", elapsed              real                   ", # Number of seconds elapsed to run this step
               ", message              varchar(255)           ", # Error or warning message, if any
               ifelse(dbtype == "netezza", ") distribute on random", ")"),
               sep = "\n")

   )
   if (dbtype == "netezza") cm.nzdrop.tables(table.name, dsn = dsn, verbose = verbose)
   cm.nzquery(SQL, verbose = verbose, dsn = dsn)
}         

#-------------------------------------------------------------------------------
