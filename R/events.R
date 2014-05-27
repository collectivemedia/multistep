#-------------------------------------------------------------------------------
#
# Package multistep 
#
# Events and event handlers
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

run.status.start <- function(subject) 
{
   
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

run.status.end <- function(subject) 
{
   
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
