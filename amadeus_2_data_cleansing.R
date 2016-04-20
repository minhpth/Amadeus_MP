## ==============================================================
## Project definition
## ==============================================================

## Amadeus data manipulation challenge, working with real big data
## bookings: Information about bookings, date, time, ports, etc.
## searches: Information about search queries of customers

setwd("D:/Amadeus")

## ==============================================================
## Data cleansing [bookings.csv]
## ==============================================================

## --------------------------------------------------------------
## Clean these following errors: [bookings_clean.csv]
## (1) Convert "," to "^"
## (2) Convert "[:space:]^" to "^"
## (3) Remove trailling whitespace
## --------------------------------------------------------------

file.in <- file("bookings.csv","r")
file.out <- file("bookings_clean.csv","a")
x <- readLines(file.in,n=1)
ind <- gsub(",","^",x) # Convert all "," to "^"
ind <- gsub(" +\\^","^",ind) # Convert all "[:space:]^" to "^"
ind <- gsub(" +$","",ind) # Remove trailing whitespace
writeLines(ind,file.out)

block <- 500000 # Size of each data block
count <- 0 # Count how many lines
repeat {
  x <- readLines(file.in,n=block)
  if (length(x)==0) break # End of file
  last.line <- count
  count <- count + length(x)
  print(paste0("# WORKING ON LINE: ",last.line+1," --> ",count))
  
  ind <- gsub(",","^",x) # Convert all "," to "^"
  ind <- gsub(" +\\^","^",ind) # Convert all "[:space:]^" to "^"
  ind <- gsub(" +$","",ind) # Remove trailing whitespace
  writeLines(ind,file.out)
}

close(file.in)
close(file.out)

# 10000010 lines
# 38 vars, 37 separators

## --------------------------------------------------------------
## Read through line by line to detect and clean data error
## --------------------------------------------------------------

# install.packages("stringr")
library(stringr)

error.count = 0
error.line = c()
error.data = c()

file.in <- file("bookings_clean.csv","r")
x <- readLines(file.in,n=1) # Read headers

block <- 500000 # Size of each data block
count <- 0 # Count how many lines
repeat {
  x <- readLines(file.in,n=block)
  if (length(x)==0) break # End of file
  last.line <- count
  count <- count + length(x)
  print(paste0("# WORKING ON LINE: ",last.line+1," --> ",count))
  
  # Look for line with different numbers of separator
  for (i in 1:length(x))
    if (str_count(x[i],"\\^")!=37) {
      print(paste0("Error line: ",last.line+i))
      # print(x[i])
      error.count <- error.count+1
      error.line <- c(error.line,last.line+i)
      error.data <- c(error.data,x[i])
    }
}

close(file.in)

file.out = file("bookings_errors.csv","a")
for (i in 1:error.count)
  writeLines(paste0(error.line[i],"^",error.data[i]),file.out)
close(file.out)

## --------------------------------------------------------------
## Fix error lines
## --------------------------------------------------------------

# install.packages("reader")
library(reader)

# Extract line 5000008 and 5000009
error.lines = n.readLines("bookings_clean2.csv",skip=5000008,n=2,header=F)

## Fix data error
fix.5000008 = c("2013-03-25 00:00:00^1V^JP^a37584d1485cb35991e4ff1a2ba92262^^^2013-03-25 00:00:00^8371^60^NRT^TYO^JP^SIN^SIN^SG^HND^TYO^JP^NRT^TYO^JP^SIN^SIN^SG^NRTSIN^SINTYO^JPSG^1^NRTSIN^XR^Q^Y^2013-04-14 11:05:00^2013-04-14 17:10:56^2^2013^3^NULL")
fix.5000009 = c("2013-03-25 00:00:00^1V^JP^5af045902bd23cab579915611d99e1e0^5073861d8597467c33596bfe16f23c56^a37584d1485cb35991e4ff1a2ba92262^2013-03-25 00:00:00^8371^60^NRT^TYO^JP^SIN^SIN^SG^HND^TYO^JP^SIN^SIN^SG^PEN^PEN^MY^PENSIN^PENSIN^MYSG^1^SINPEN^WS^Y^Y^2013-04-16 15:45:00^2013-04-16 17:15:29^2^2013^3^NULL")

## --------------------------------------------------------------
## Write clean data to file [bookings_clean2.csv]
## --------------------------------------------------------------

file.in <- file("bookings_clean.csv","r")
file.out <- file("bookings_clean2.csv","a")
x <- readLines(file.in,n=1)
writeLines(x,file.out)

block <- 500000 # Size of each data block
count <- 0 # Count how many lines
repeat {
  x <- readLines(file.in,n=block)
  if (length(x)==0) break # End of file
  last.line <- count
  count <- count + length(x)
  print(paste0("# WORKING ON LINE: ",last.line+1," --> ",count))
  
  ## Replace error lines
  if (count==5500000) { # This block contain detected error lines
    print("Fixing...")
    x[8] <- fix.5000008
    x[9] <- fix.5000009
  }
  
  writeLines(x,file.out)
}

close(file.in)
close(file.out)

## --------------------------------------------------------------
## Read through file again to confirm NO error [bOokings_clean2.csv]
## --------------------------------------------------------------

file.in <- file("bookings_clean2.csv","r")
x <- readLines(file.in,n=1) # Read headers

block <- 500000 # Size of each data block
count <- 0 # Count how many lines
repeat {
  x <- read.table(file.in,header=F,nrows=block,sep="^",na.strings="")
  if (length(x)==0) break # End of file
  last.line <- count
  count <- count + nrow(x)
  print(paste0("# WORKING ON LINE: ",last.line+1," --> ",count))
}

close(file.in)

## ==============================================================
## Importing data [bookings.csv]
## ==============================================================

bookings.vars <- readLines(con="bookings_vars.csv")
searches.vars <- readLines(con="searches_vars.csv")

library(ff)
bookings <- read.table.ffdf(file="bookings_clean2.csv",FUN="read.csv",
                            header=T,sep="^",comment.char="",na.strings="")

## ==============================================================
## Data cleansing [searches.csv]
## ==============================================================

## --------------------------------------------------------------
## Clean these following errors: [searches.csv]
## (1) Convert "," to "^"
## (2) Convert "[:space:]^" to "^"
## (3) Remove trailling whitespace
## --------------------------------------------------------------

file.in <- file("searches.csv","r")
file.out <- file("searches_clean.csv","a")
x <- readLines(file.in,n=1)
ind <- gsub(",","^",x) # Convert all "," to "^"
ind <- gsub(" +\\^","^",ind) # Convert all "[:space:]^" to "^"
ind <- gsub(" +$","",ind) # Remove trailing whitespace
writeLines(ind,file.out)

block <- 500000 # Size of each data block
count <- 0 # Count how many lines
repeat {
  x <- readLines(file.in,n=block)
  if (length(x)==0) break # End of file
  last.line <- count
  count <- count + length(x)
  print(paste0("# WORKING ON LINE: ",last.line+1," --> ",count))
  
  ind <- gsub(",","^",x) # Convert all "," to "^"
  ind <- gsub(" +\\^","^",ind) # Convert all "[:space:]^" to "^"
  ind <- gsub(" +$","",ind) # Remove trailing whitespace
  writeLines(ind,file.out)
}

close(file.in)
close(file.out)

# 20390198 lines
# 45 vars, 44 separators

## --------------------------------------------------------------
## Read through line by line to detect and clean data error
## --------------------------------------------------------------

# install.packages("stringr")
library(stringr)

error.count = 0
error.line = c()
error.data = c()

file.in <- file("searches_clean.csv","r")
x <- readLines(file.in,n=1) # Read headers

block <- 500000 # Size of each data block
count <- 0 # Count how many lines
repeat {
  x <- readLines(file.in,n=block)
  if (length(x)==0) break # End of file
  last.line <- count
  count <- count + length(x)
  print(paste0("# WORKING ON LINE: ",last.line+1," --> ",count))
  
  # Look for line with different numbers of separator
  for (i in 1:length(x))
    if (str_count(x[i],"\\^")!=44) {
      print(paste0("Error line: ",last.line+i))
      # print(x[i])
      error.count <- error.count+1
      error.line <- c(error.line,last.line+i)
      error.data <- c(error.data,x[i])
    }
}

close(file.in)

file.out = file("searches_errors.csv","a")
for (i in 1:error.count)
  writeLines(paste0(error.line[i],"^",error.data[i]),file.out)
close(file.out)

## --------------------------------------------------------------
## Fix error lines
## --------------------------------------------------------------

# install.packages("reader")
library(reader)

# Extract line 5000008 and 5000009
error.lines = n.readLines("bookings_clean2.csv",skip=5000008,n=2,header=F)

## Fix data error
fix.5000008 = c("2013-03-25 00:00:00^1V^JP^a37584d1485cb35991e4ff1a2ba92262^^^2013-03-25 00:00:00^8371^60^NRT^TYO^JP^SIN^SIN^SG^HND^TYO^JP^NRT^TYO^JP^SIN^SIN^SG^NRTSIN^SINTYO^JPSG^1^NRTSIN^XR^Q^Y^2013-04-14 11:05:00^2013-04-14 17:10:56^2^2013^3^NULL")
fix.5000009 = c("2013-03-25 00:00:00^1V^JP^5af045902bd23cab579915611d99e1e0^5073861d8597467c33596bfe16f23c56^a37584d1485cb35991e4ff1a2ba92262^2013-03-25 00:00:00^8371^60^NRT^TYO^JP^SIN^SIN^SG^HND^TYO^JP^SIN^SIN^SG^PEN^PEN^MY^PENSIN^PENSIN^MYSG^1^SINPEN^WS^Y^Y^2013-04-16 15:45:00^2013-04-16 17:15:29^2^2013^3^NULL")

## --------------------------------------------------------------
## Write clean data to file [searches_clean2.csv]
## --------------------------------------------------------------

file.in <- file("bookings_clean.csv","r")
file.out <- file("bookings_clean2.csv","a")
x <- readLines(file.in,n=1)
writeLines(x,file.out)

block <- 500000 # Size of each data block
count <- 0 # Count how many lines
while (length(x)) {
  x <- readLines(file.in,n=block)
  if (length(x)==0) break # End of file
  count <- count + length(x)
  print(paste0("# WORKING ON LINE: ",count-block+1," --> ",count))
  
  ## Replace error lines
  if (count==5500000) { # This block contain detected error lines
    print("Fixing...")
    x[8] <- fix.5000008
    x[9] <- fix.5000009
  }
  
  writeLines(x,file.out)
}

close(file.in)
close(file.out)

## --------------------------------------------------------------
## Read through file again to confirm NO error [bOokings_clean2.csv]
## --------------------------------------------------------------

file.in <- file("bookings_clean2.csv","r")
x <- readLines(file.in,n=1) # Read headers

block <- 500000 # Size of each data block
count <- 0 # Count how many lines
while (length(x)) {
  x <- read.table(file.in,header=F,nrows=block,sep="^",na.strings="")
  if (length(x)==0) break # End of file
  count <- count + nrow(x)
  print(paste0("# WORKING ON LINE: ",count-block+1," --> ",count))
}

close(file.in)