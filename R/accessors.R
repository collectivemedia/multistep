#-------------------------------------------------------------------------------
#
# Package multistep 
#
# MultiStep class constructor
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

# TODO: add tons of error checking on each field of the slots

# Access to slots
setGeneric("config", function(object) { standardGeneric ("config")} )
setGeneric("config<-", function(object, value) { standardGeneric ("config<-")} )
setGeneric("state", function(object) { standardGeneric ("state")} )
setGeneric("state<-", function(object, value) { standardGeneric ("state<-")} )

# Access to slot variables
setGeneric("getConfigVar", function(object, var     ) { standardGeneric ("getConfigVar")} )
setGeneric("setConfigVar", function(object, var, val) { standardGeneric ("setConfigVar")} )
setGeneric("getStateVar",  function(object, var     ) { standardGeneric ("getStateVar") } )
setGeneric("setStateVar",  function(object, var, val) { standardGeneric ("setStateVar") } )

#-------------------------------------------------------------------------------

setMethod("config", "MultiStep", function(object)   
{ 
   if (!(res = valid.config(object@config))) stop(attr(res, "msg"))
   return(object@config)   
})

#-------------------------------------------------------------------------------

setReplaceMethod(f = "config", signature = c("MultiStep", "list"), definition = function(object, value)  
{ 
   if (!valid.config(value)) stop("Invalid config.")
   object@config <- value 
   return(object) 
})

#-------------------------------------------------------------------------------

setMethod("state", "MultiStep", function(object)   
{ 
   if (!(res = valid.config(object@state))) stop(attr(res, "msg"))
   return(object@state)   
})

#-------------------------------------------------------------------------------

setReplaceMethod(f = "state", signature = c("MultiStep", "list"), definition = function(object, value)  
{ 
   if (!valid.state(value)) stop("Invalid config.")
   object@state <- value 
   return(object) 
})

#-------------------------------------------------------------------------------

setMethod("getConfigVar", "MultiStep", function(object, var) 
{ 
   if (!(res = valid.config(object@config))) stop(attr(res, "msg"))
   if (!(var %in% names(object@config))) stop(paste("Invalid config variable '", var, "'", sep = ""))
   return(object@config[[var]]) 
})

#-------------------------------------------------------------------------------

# assign the result back to object
setMethod(f = "setConfigVar", signature = "MultiStep", definition = function(object, var, val) 
{ 
   if (!(res = valid.config(object@config))) stop(attr(res, "msg"))
   if (!(var %in% names(object@config))) stop(paste("Invalid config variable '", var, "'", sep = ""))
   config(object)[[var]] <- val
   return(object)   
})

#-------------------------------------------------------------------------------

setMethod("getStateVar", "MultiStep", function(object, var) 
{ 
   if (!(res = valid.state(object@state))) stop(attr(res, "msg"))
   if (!(var %in% names(object@state))) stop(paste("Invalid state variable '", var, "'", sep = ""))
   return(object@state[[var]]) 
})

#-------------------------------------------------------------------------------

# assign the result back to object
setMethod(f = "setStateVar", signature = "MultiStep", definition = function(object, var, val) 
{ 
   if (!(res = valid.state(object@state))) stop(attr(res, "msg"))
   if (!(var %in% names(object@state))) stop(paste("Invalid state variable '", var, "'", sep = ""))
   state(object)[[var]] <- val
   return(object)   
})

#-------------------------------------------------------------------------------
