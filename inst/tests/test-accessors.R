context("Test accessor methods")

#-------------------------------------------------------------------------------

test_that("config get/set work", 
{
   ms <- MultiStep()
   
   # getter
   config <- config(ms)
   expect_equal(config(ms), ms@config)  
   
   # setter
   config.mod <- config
   config.mod$logs.dir <- "/tmp"
   config(ms) <- config.mod
   expect_equal(ms@config$logs.dir, "/tmp")    
   expect_equal(all.equal(config.mod, ms@config), TRUE)  
   
   expect_error(config(ms) <- NULL)
   expect_error(config(ms) <- NA)
   expect_error(config(ms) <- list())   
   expect_error(config(ms) <- list(a = 1))   
      
})   

#-------------------------------------------------------------------------------

test_that("state get/set work", 
{
   ms <- MultiStep()
   run.id <- 923456789098
   
   # getter
   state <- state(ms)
   expect_equal(state(ms), ms@state)  
   
   # setter
   stat.mod <- state
   state.mod$run.id <- run.id
   state(ms) <- state.mod
   expect_equal(ms@state$run.id, run.id)    
   expect_equal(all.equal(state.mod, ms@state), TRUE)  
   
   expect_error(state(ms) <- NULL)
   expect_error(state(ms) <- NA)
   expect_error(state(ms) <- list())   
   expect_error(state(ms) <- list(a = 1))      
})   

#-------------------------------------------------------------------------------

test_that("config vars get/set work", 
{
   ms <- MultiStep()
   expect_equal(getConfigVar(ms, "stage.type"), ms@config$stage.type)    
   expect_error(setConfigVar(ms, a, 1))
   ms <- setConfigVar(ms, "logs.dir", "/tmp")
   expect_equal(ms@config$logs.dir, "/tmp")    
   expect_error(ms, dsn, "gibbrish_that_couldn't possibly_exist")   
   # add variable-specific tests
})

#-------------------------------------------------------------------------------

test_that("state vars get/set work", 
{
   ms <- MultiStep()
   run.id <- 923456789098
   expect_equal(is.na(getStateVar(ms, "run.id")), TRUE)
   expect_error(getStateVar(ms, a, 1))
   ms <- setStateVar(ms, "run.id", run.id)
   expect_equal(getStateVar(ms, "run.id"), run.id)   
   # add variable-specific tests
   
})

#-------------------------------------------------------------------------------
