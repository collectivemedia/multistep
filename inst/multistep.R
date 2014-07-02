#-------------------------------------------------------------------------------
#
# Package cmrutils 
#
# Utilities for running a multi-step process from R 
# 
# Sergei Izrailev, Jeremy Stanley, 2012-2014
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
# TODO: implement dependencies (arg 'depends' = c(stage names))
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

ds.multistep.init <- function(dsn, prefix = NULL,  
   stage.type = c("FIXED", "FLEX"),
   dbtype = c("netezza", "mysql"),
   logs.dir = NULL,              # root directory for logging; default is current directory
   logs.stage.template = NULL,   # when TRUE, splits the output of stages into files
   logs.step.template = NULL,    # when TRUE, splits the output of steps into files 
   logs.overwrite = FALSE,       # when TRUE, overwrites the file, otherwise appends
   logs.user.group = NULL,       # log directories are changed to this group
   logs.grp.permissions = "rx",  # group permissions on the log directories; use "rwx" for group-writable
   temp.dir = NULL               # temp directory for miscellaneous file exchange; default is current dir
)
{
   # Check for non-scalars
   if (length(dsn) > 1) stop("ds.multistep.init: dsn must be a scalar")
   if (length(prefix) > 1) stop("ds.multistep.init: prefix must be a scalar")
   if (length(log.dir) > 1) stop("ds.multistep.init: log.dir must be a scalar")
   if (length(stage.type) > 1) stop("ds.multistep.init: stage.type must be a scalar")
   
   # Check that the dsn is valid.
   cm.db.check.dsn(dsn)
   
   # Check that the prefix is a valid string to prepend to a table name. 32 characters is arbitrary.
   res <- grep("[^A-Za-z0-9_]", prefix)
   if (length(res) > 0) stop(paste("ds.multistep.init: prefix contains invalid characters:", prefix))
   if (nchar(prefix) > 32) 
   {
      stop(paste("ds.multistep.init: prefix is too long:", nchar(prefix), "characters. Max is 32."))
   }
   
   # Check the stage type.
   stage.type <- match.arg(stage.type)
   
   # Check that the directory is valid
   if (!is.null(log.dir) && !isDirectory(log.dir)) stop(paste("Invalid logging directory:", log.dir))

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
   
   
   # Check logs directory
   if (is.null(logs.dir) || is.na(logs.dir))
   {
      logs.dir <- getwd()
   }   
   if (logs.split)
   {
      if (file.exists(logs.dir))
      {
         if (!file.access(logs.dir, mode = 2)) 
         {
            stop(cm.varsub.sql("Logs directory LOGS.DIR is not writable.", list(LOGS.DIR = logs.dir)))
         }
      }
      else
      {
         tryCatch(cm.mkdir(logs.dir, user.group = logs.user.group, permissions = logs.grp.permissions),
            error = function(e)
            {
               stop(paste("Can't create log directory '", logs.dir, "': ", e$message, sep = ""))
            })
      }
   }

   # Check temp directory
   if (is.null(temp.dir) || is.na(temp.dir))
   {
      temp.dir <- getwd()
   }
   # Verify that it either exists and is writable, or we can create it 
   if (file.exists(temp.dir))
   {
      if (!file.access(temp.dir, mode = 2)) 
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
   options(
      DS.MULTISTEP = list(
           dsn = dsn
         , prefix = prefix
         , tab.run.ids = cm.literal(paste(c(prefix, "run_ids"), collapse = "_"))
         , tab.run.steps = cm.literal(paste(c(prefix, "run_steps"), collapse = "_"))
         , tab.run.stages = cm.literal(paste(c(prefix, "run_stages"), collapse = "_"))
         , tab.stages = cm.literal(paste(c(prefix, "stages"), collapse = "_"))
         , tab.run.params = cm.literal(paste(c(prefix, "run_params"), collapse = "_"))
         , stage.type = stage.type
         , logs.dir = logs.dir
         , logs.split = logs.split
         , logs.file.template = logs.file.template
         , logs.overwrite = logs.overwrite
         , logs.user.group = logs.user.group
         , logs.grp.permissions = logs.grp.permissions
         , temp.dir = temp.dir
      ))

   # Initialize transient values
   assign(".multistep.current", list(
        stage.id        = NA
      , step.id         = NA
      , log.file        = NA
      , working.dir     = NA
   ), envir=baseenv())
   
}

#-------------------------------------------------------------------------------

ds.multistep.get.options <- function() 
{
   if (is.null(getOption("DS.MULTISTEP"))) 
   {
      stop("No DS.MULTISTEP option - probably missing initialization.")
   }
   return( getOption("DS.MULTISTEP") )
}

#-------------------------------------------------------------------------------

ds.multistep.set.options <- function(opts) 
{
   if (!is.list(opts)) stop("Expecting a list argument")
   options(DS.MULTISTEP = opts)
}

#-------------------------------------------------------------------------------

ds.multistep.get.option <- function(opt) 
{
   opts <- ds.multistep.get.options()
   if (!is.list(opts)) stop("ds.multistep.get.options() is expected to return a list.")
   if (is.null( opts[[opt]] )) 
   {
      stop(paste("Couldn't find option '", opt, "'.", sep = ""))
   }
   return(opts[[opt]])
}

#-------------------------------------------------------------------------------

ds.multistep.set.option <- function(opt, value) 
{
   opts <- ds.multistep.get.options()
   if (!is.list(opts)) stop("ds.multistep.get.options() is expected to return a list.")
   opts[[opt]] = value 
   ds.multistep.set.options(opts)
}

#-------------------------------------------------------------------------------

ds.multistep.get.state <- function() 
{
   if (!exists(".multistep.current", env = baseenv())) 
   {
      stop("No state - probably missing initialization.")
   }
   return( get(".multistep.current", env = baseenv()) )
}

#-------------------------------------------------------------------------------

ds.multistep.set.state <- function(state) 
{
   if (!is.list(state)) stop("Expecting a list argument")
   assign(".multistep.current", state, env = baseenv())
}

#-------------------------------------------------------------------------------

ds.multistep.get.state.var <- function(var) 
{
   state <- ds.multistep.get.state()
   if (!is.list(state)) stop("ds.multistep.get.state() is expected to return a list.")
   return(state[[var]])
}

#-------------------------------------------------------------------------------

ds.multistep.set.state.var <- function(var, value) 
{
   state <- ds.multistep.get.state()
   if (!is.list(state)) stop("ds.multistep.get.state() is expected to return a list.")
   state[[var]] = value 
   ds.multistep.set.state(state)
}

#-------------------------------------------------------------------------------

ds.multistep.logs.file <- function(logs.file.template)
{
   opts <- ds.multistep.get.options()
   state <- ds.multistep.get.state()
   lst <- c(opts, state)
   varsub <- lapply(lst, function(x) cm.literal(as.character(x)))
   names(varsub) <- toupper(names(lst)) 
   return( cm.varsub.sql(logs.file.template, varsub) )
}

#-------------------------------------------------------------------------------

ds.multistep.install <- function(verbose = FALSE) 
{
   # Run tables
   ds.multistep.create.table("run_ids", verbose = verbose)          # Tracks each run
   ds.multistep.create.table("run_steps", verbose = verbose)        # Tracks each script
   ds.multistep.create.table("run_stages", verbose = verbose)       # Tracks each stage
   ds.multistep.create.table("stages", verbose = verbose)           # Descriptions of each stage   
}

#-------------------------------------------------------------------------------

# TODO: add indexes for non-netezza dbms.
ds.multistep.create.table <- function(table.name, verbose = FALSE) 
{  
   dsn <- ds.multistep.get.option("dsn")
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

# Function that checks to ensure run.id does not already exist, and then inserts it into run_ids
ds.multistep.set.run.id <- function(run.id, run.type, save.args = TRUE) 
{
   # Constants
   dsn <- ds.multistep.get.option("dsn")
   tab.stages <- ds.multistep.get.option("tab.stages")
   tab.run.ids <- ds.multistep.get.option("tab.run.stages")
   tab.run.stages <- ds.multistep.get.option("tab.run.stages")
   stage.type <- ds.multistep.get.option("stage.type")
   
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
   ds.multistep.set.option("run.id", run.id) 
   ds.multistep.set.option("run.type", run.type) 
   
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

# run.id has to be passed on the command line with '--args -runid <run.id>'
# The user must call this function at the start of each individual step running 
# in a separate OS process.
ds.multistep.get.run.id <- function() 
{   
   require("R.utils")
   
   # Constants
   dsn <- ds.multistep.get.option("dsn")
   tab.stages <- ds.multistep.get.option("tab.stages")
   tab.run.ids <- ds.multistep.get.option("tab.run.stages")
   
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
   if (stage.type != cm.get.option("DS.MULTISTEP.STAGE.TYPE"))
   {
      stop(cm.varsub.sql("Stage type TYPE for run_id RUN.ID doesn't match the current stage type CUR",
                  list(TYPE = stage.type, RUN.ID = run.id, CUR = cm.get.option("DS.MULTISTEP.STAGE.TYPE"))))
   }
   
   run.type <- cm.nzquery("select run_type from TAB.IDS where run_id = RUN.ID"
         , list(TAB.IDS = tab.run.ids, RUN.ID  = run.id)
         , convert.names = TRUE
         , dsn = dsn)$run.type
   
   # Set options
   ds.multistep.set.option("run.id", run.id) 
   ds.multistep.set.option("run.type", run.type) 

   # Return if needed locally
   run.id   
}

#-------------------------------------------------------------------------------

ds.multistep.stages.empty.df <- function(run.type)
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

ds.multistep.stages.add <- function(df, stage.name, description.short, description.long = NA)
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

ds.multistep.stages.register <- function(df, verbose = TRUE)
{
   # Constants
   dsn <- cm.get.option("DS.MULTISTEP.DSN")
   tab.stages <- cm.get.option("DS.MULTISTEP.TAB.STAGES")
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
ds.multistep.stages.clear <- function(run.id, stage.name, clear.attempted = TRUE, 
      clear.error = TRUE, clear.warning = TRUE)
{
   # Constants
   dsn <- cm.get.option("DS.MULTISTEP.DSN")
   tab.run.stages <- cm.get.option("DS.MULTISTEP.TAB.RUN.STAGES")
   if (!cm.nzexists.bykey(tab.run.stages,
         list("run_id" = as.character(run.id), "stage_name" = stage.name), dsn = dsn))
   {
      stop(paste("Invalid run_id, stage_name combination: ", run_id, ", ", stage_name, sep = ""))
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
   ds.multistep.init(dsn = "NZS_ERNIE", prefix = "myjobs", 
         log.dir = "/data1/datasci/dslogs", stage.type = "FIXED")
   # Or
   ds.multistep.init(dsn = "mysql_hs", prefix="hs", stage.type = "FIXED")
      
   # Install the tables
   ds.multistep.install(verbose = TRUE)
   
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

#-------------------------------------------------------------------------------

ds.multistep.run.stage <- function(
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
   dsn <- cm.get.option("DS.MULTISTEP.DSN")
   tab.stages <- cm.get.option("DS.MULTISTEP.TAB.STAGES")
   tab.run.ids <- cm.get.option("DS.MULTISTEP.TAB.RUN.IDS")
   tab.run.stages <- cm.get.option("DS.MULTISTEP.TAB.RUN.STAGES")
   stage.type <- cm.get.option("DS.MULTISTEP.STAGE.TYPE")
   run.id <- cm.get.option("DS.MULTISTEP.RUN.ID")
   run.type <- as.character(cm.get.option("DS.MULTISTEP.RUN.TYPE"))
   stage.name <- as.character(stage.name)
   
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
   ds.multistep.set.state.var("stage.id", stage.id)
   
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
   ds.multistep.set.state.var("stage.id", NA)

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

# Runs a step of this process
ds.multistep.run.step <- function(
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
   dsn <- ds.multistep.get.option("dsn")
   tab.stages <- ds.multistep.get.option("tab.stages")
   tab.run.ids <- ds.multistep.get.option("tab.run.stages")
   tab.run.stages <- ds.multistep.get.option("tab.run.stages")
   tab.run.steps <- ds.multistep.get.option("tab.run.steps")
   stage.type <- ds.multistep.get.option("stage.type")
   run.id <- ds.multistep.get.option("run.id")
   run.type <- ds.multistep.get.option("run.type")
   temp.dir <- ds.multistep.get.option("temp.dir")
   
   # Constants for logging
   logs.dir <- ds.multistep.get.option("logs.dir")
   logs.split <- ds.multistep.get.option("logs.split")
   logs.file.template <- ds.multistep.get.option("logs.file.template")
   logs.overwrite <- ds.multistep.get.option("logs.overwrite")
   logs.user.group <- ds.multistep.get.option("logs.user.group")
   logs.grp.permissions <- ds.multistep.get.option("logs.grp.permissions")
      
   do.run <- 1 # should this be a parameter to the function? why?
   
   # Check step type
   step.type <- match.arg(step.type)
   
   # User
   user <- System$getUsername()
      
   # Get the stage and the parent step info, if any
   stage.id <- ds.multistep.get.state.var("stage.id")
   if (is.null(stage.id)) stage.id = NA
   parent.step.id <- ds.multistep.get.state.var("step.id")
   if (is.null(parent.step.id)) parent.step.id = NA
   parent.log.file <- ds.multistep.get.state.var("log.file")
   if (is.null(parent.log.file)) parent.log.file = NA
   parent.working.dir <- ds.multistep.get.state.var("working.dir")
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
   ds.multistep.set.state.var("step.id", step.id)
   
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
   logs.split <- ds.multistep.get.option("logs.split") 
   logs.file.template <- ds.multistep.get.option("logs.file.template") 
   log.file <- ds.multistep.logs.file(logs.file.template)
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
      ds.multistep.set.state.var("log.file", log.file)
   }
   
   # Create a new record
   step.df <- data.frame(
      run.id = as.character(run.id),
      stage.id = stage.id,
      stage.name = stage.name,
      step.id = step.id,
      step.name = step.name,
      step.type = step.type,
      user.name = user.name,
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
   ds.multistep.set.state.var("step.id", parent.step.id)   
   ds.multistep.set.state.var("log.file", parent.log.file)
   ds.multistep.get.state.var("working.dir", parent.working.dir)
   
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

ao.run.status.start <- function(subject) {
   
   # Get data to highlight
   frm.special <- data.frame( 
         cm.nzquery.varsub("select run_id, to_date from input_master where run_id = RUN.ID", var.list = list(RUN.ID = options()$AO.RUN.ID)),
         optimizers.total = cm.nzquery.varsub("select count(*) as cnt from input_optimizers where run_id = RUN.ID", var.list = list(RUN.ID = options()$AO.RUN.ID))$cnt,
         optimizers.noerr = cm.nzquery.varsub("select count(*) as cnt from input_optimizers where run_id = RUN.ID and any_error = 0", var.list = list(RUN.ID = options()$AO.RUN.ID))$cnt,
         optimizers.build = cm.nzquery.varsub("select count(*) as cnt from input_optimizers where run_id = RUN.ID and any_error = 0 and use_prior = 0 and skip_build = 0 and pct_assign_control < 1", var.list = list(RUN.ID = options()$AO.RUN.ID))$cnt)
   
   # Get all master other inputs
   frm.master <- cm.nzquery.varsub("select * from input_master where run_id = RUN.ID", var.list = list(RUN.ID = options()$AO.RUN.ID))
   frm.master <- subset(frm.master, select = -c(run.id, to.date))
   
   ds.status.log(data.date = frm.special$to.date[1], 
         proc.name = paste(toupper(options()$AO.PREFIX), frm.master$mm.label[1], sep="_"), 
         detail = as.character(options()$AO.RUN.ID),
         status = 'started',
         table.name = "datasci..dsproc_status",
         dsn = "NZDS_ERNIE");
   
   # Concatenate together in fixed width form
   status <- 
         paste(ao.format.df(frm.special, header.line = FALSE, transpose = TRUE),
               paste(rep("-", 50), collapse = ""),
               ao.format.df(frm.master, header.line = FALSE, transpose = TRUE),
               sep = "\n\n")
   
   # Print 
   writeLines(status)
   
   # E-mail
   ao.mail(to = options()$AO.EMAIL.LIST, subject = subject, body = status)   
   
}

#-------------------------------------------------------------------------------

ao.run.status.end <- function(subject) {
   
   elapsed.s <- cm.nzquery.varsub("select sum(elapsed) as elapsed from run_stages where run_id = RUN.ID and attempted = 1", var.list =  list(RUN.ID = options()$AO.RUN.ID))$elapsed
   
   # Get data to highlight
   frm.special <- data.frame( 
         cm.nzquery.varsub("select run_id, to_date, mm_label from input_master where run_id = RUN.ID", var.list = list(RUN.ID = options()$AO.RUN.ID)),
         elapsed.minutes = round(elapsed.s / 60, 2),
         elapsed.hours = round(elapsed.s / 60 / 60, 2),
         optimizers.total = cm.nzquery.varsub("select count(*) as cnt from input_optimizers where run_id = RUN.ID", var.list = list(RUN.ID = options()$AO.RUN.ID))$cnt,
         optimizers.noerr = cm.nzquery.varsub("select count(*) as cnt from input_optimizers where run_id = RUN.ID and any_error = 0", var.list = list(RUN.ID = options()$AO.RUN.ID))$cnt,
         optimizers.build = cm.nzquery.varsub("select count(*) as cnt from input_optimizers where run_id = RUN.ID and any_error = 0 and use_prior = 0 and skip_build = 0 and pct_assign_control < 1", var.list = list(RUN.ID = options()$AO.RUN.ID))$cnt,
         models.build = cm.nzquery.varsub("select count(*) as cnt from model_meta where run_id = RUN.ID", var.list = list(RUN.ID = options()$AO.RUN.ID))$cnt)
   
   ds.status.log(data.date = frm.special$to.date[1], 
         proc.name = paste(toupper(options()$AO.PREFIX), frm.special$mm.label[1], sep="_"), 
         detail = as.character(options()$AO.RUN.ID),
         status = 'completed',
         table.name = "datasci..dsproc_status",
         dsn = "NZDS_ERNIE");
   
   # Get stages data
   frm.stages <- cm.nzquery.varsub("select stage_name, run, attempted, elapsed from run_stages where run_id = RUN.ID order by stage_id", var.list = list(RUN.ID = options()$AO.RUN.ID))
   frm.stages <- transform(frm.stages, elapsed.m = round(elapsed / 60, 1))
   frm.stages <- subset(frm.stages, select = -elapsed)
   
   # Concatenate together in fixed width form
   status <- 
         paste(ao.format.df(frm.special, header.line = FALSE, transpose = TRUE),
               paste(rep("-", 50), collapse = ""),
               ao.format.df(frm.stages, header.line = TRUE, transpose = FALSE),
               sep = "\n\n")
   
   # Print 
   writeLines(status)
   
   # E-mail
   ao.mail(to = options()$AO.EMAIL.LIST, subject = subject, body = status)      
   
}

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
