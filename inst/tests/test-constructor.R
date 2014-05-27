context("Test MultiStep class construction")

test_that("no default constructor with empty slots", 
{
   expect_error(new("MultiStep", config = list(), current = list()))    
   expect_error(new("MultiStep", current = list()))    
   expect_error(new("MultiStep", config = list()))    
   expect_error(new("MultiStep"))    
   expect_error(MultiStep(config = list(), current = list()))    
   expect_error(MultiStep(current = list()))    
   expect_error(MultiStep(config = list()))    
})

test_that("default constructor functional", 
{
   ms <- MultiStep()
   expect_true(length(ms@config) > 0 && length(ms@current) > 0)    
   expect_error(MultiStep(dsn = "gibbrish_that_couldn't possibly_exist"))    
   expect_error(MultiStep(stage.type = "some other garbage"))
   expect_true(ms@config$stage.type %in% c("FLEX", "FIXED"))
   # Add more error checking
})

