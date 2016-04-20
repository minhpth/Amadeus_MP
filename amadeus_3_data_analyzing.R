## ==============================================================
## Project definition
## ==============================================================

## Amadeus data manipulation challenge, working with real big data
## bookings: Information about bookings, date, time, ports, etc.
## searches: Information about search queries of customers

setwd("D:/Amadeus")

## --------------------------------------------------------------
## Stage 3: Data analyzing
## (1) Import clean data
## (2) Analyze to answer questions
## --------------------------------------------------------------

## ==============================================================
## Data analyzing [bookings.csv]
## ==============================================================

bookings.vars <- readLines(con="bookings_vars.csv")
searches.vars <- readLines(con="searches_vars.csv")

# install.packages("ff")
library(ff)

bookings <- read.table.ffdf(file="bookings_clean2.csv",FUN="read.csv",
                            header=T,sep="^",comment.char="",na.strings="")

searches <- read.table.ffdf(file="searches_clean2.csv",FUN="read.csv",
                            header=T,sep="^",comment.char="",na.strings="")

## ==============================================================
## Last modified on 20 Apr 2016. Minh Phan.
## ==============================================================