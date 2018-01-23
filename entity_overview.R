#salesforce login
source("~/scripts/salesforce_login.R")

#look at all object available in SF
objects<-rforcecom.getObjectList(session)
object_names <- objects$name
object_labels <- objects$label

head(object_names)

#define a try-catch procedure for getting object descriptions
get_object_desc <- function(object_name=""){
  result <- try(rforcecom.getObjectDescription(session,object_name),silent=T)
  if(inherits(result,"try-error")){
    error_message <- paste("Error :",object_name)
    print(error_message)
    result <- list()
  }
  return(result)
}

str(object_columns)

#get object description for each of the objects
object_descriptions <- lapply(object_names,get_object_desc)
#name them accordingly
names(object_descriptions)<-object_names

#get the names and labels for every object, essentially the fields that hold data in this object
object_names_list <- lapply(object_descriptions,"[","name")
object_labels_list <- lapply(object_descriptions,"[","label")

#convert the names to a list of vectors so we can prepare queries
object_names_list <- lapply(object_names_list,function(x) as.character(unlist(x)))
object_names_list

#define a function that prepares a query
prepare_query <- function(o=""){
  object_names_list[]
}

#build a list of queries
query_fields_list_count <- sapply(object_names, function(x) paste("SELECT ",paste(paste("count(",unlist(object_names_list[x]),")",sep=""),collapse=",")," FROM ",x,sep=""))
query_fields_list <- sapply(object_names, function(x) paste("SELECT ",paste(unlist(object_names_list[x]),collapse=",")," FROM ",x,sep=""))
head(query_fields_list)

####################### OBJECT DISTRIBUTIONS ####################################

#define a function that executes them
get_object_stats <- function(q="",limit=10000){
  if(limit>0){
    q = paste(q,"LIMIT",limit)
  }
  result <- try(rforcecom.query(session,q),silent=F)
  if(inherits(result,"try-error")){
    error_message <- paste("Error :",q)
    print(error_message)
    result <- list()
  }
  return(result)
}


object_stats <- lapply(query_fields_list[2],function(x) get_object_stats(x,limit=2000))
#assign the result a name
assign(paste(object_names[2]),object_stats[[1]])
#save it
save(list=as.character(object_names[2]),file=paste("/data/sf/sf_",as.character(object_names[2]),".Rda",sep=""))


#set loop for object
for(i in 1:length(object_names)){
#ask for ith object stats
print(object_names[i])
#get it
object_stats <- lapply(query_fields_list[i],function(x) get_object_stats(x,limit=10000))
#assign the result a name
assign(paste(object_names[i]),object_stats[[1]])
#save it
save(list=as.character(object_names[i]),file=paste("/data/sf/sf_",as.character(object_names[i]),".Rda",sep=""))
#increment i
}

#get all the files you stored
f <- list.files(path="/data/sf",pat="(sf)(.*)(.Rda)")
f
#merge them in a list
l <-lapply(f,function(x) get(load(file=paste("/data/sf/",x,sep=""))))
#get their distributionss
dists<-lapply(l, function(x) apply(x,2,function(y) as.data.frame(prop.table(table(var=y)),responseName="freq")))
names(dists)<-f
#define objects you want to get distributions for
select <- c("sf_OpportunityHistory.Rda","sf_accounts_12_03.Rda","sf_activity_12_03.Rda","sf_activity_history_12_03.Rda","sf_lead_16_03.Rda","sf_contact_17_03.Rda")
#do the selection
dists <- dists[which(names(dists) %in% select)]
#bind distributions together - basically melt their columns
b_dists<-lapply(dists,function(x) do.call("rbind",x))
#add their names as a column
bb_dists <- lapply(b_dists,function(x) as.data.frame(cbind(x,names=rownames(x)),stringsAsFactors=F))
#define a function that gets rid of charaters
tochar <- function(l){
  l[] <- lapply(l, as.character)  
}
#get rid of the characters
bb_dists <- lapply(bb_dists,tochar)
#final unlist accross objects
bb_dists <- lapply(bb_dists,function(x) as.data.frame(x,stringsAsFactors=F))
#final bind
bb_dists<-do.call("rbind",bb_dists)
#naming
bb_dists$object <- rownames(bb_dists)
#write
write.table(bb_dists,"/data/sf/object_distributions.csv",row.names=F,sep=";")

####################### OBJECT COLUMNS ####################################

object_columns <- sapply(object_names, function(x) unlist(object_names_list[x]))
object_columns

#recode the messy parts to NA (or anything you wish)
sub <- sapply(object_columns,function(x) ifelse(length(x)==0,T,F))
object_columns[sub]<-NA

#rbind to a matrix of length of the list and width of its widest element
matrix_x<-as.data.frame(t(sapply(object_columns,'[',1:max(sapply(object_columns,length)))),stringsAsFactors=F) 
colnames(matrix_x)<-paste("var",1:ncol(matrix_x),sep=".")
matrix_x$object <- object_names

write.csv(matrix_x,"/data/sf/object_columns.csv",row.names=F)
saveRDS(object_columns,"/data/sf/object_columns.rds")
