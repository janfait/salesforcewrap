#################
# SETUP
#################
rm(list=ls())

#libraries
source("/home/user/scripts/libs.R")
source("/home/user/scripts/cep/api.R")
source("/home/user/scripts/salesforce/api.R")
source("/home/user/scripts/system/tools.R")
source("/home/user/scripts/system/ftp.R")

#################
# CLASS INSTANCES
#################

#initialize a CEP system
ecm <- cep$new(system="ecircle_marketing",settings=settings,debug=F)
#initialize a SFDC system
sfdc <- salesforce$new(settings=settings)
#tools
tools <- mappMarketingTools$new()
#ftp
f <- ftp$new("ecircle_marketing",settings=settings)
#initialize data map
sfdc$mapping <- read.table("/home/user/settings/sfdc2cepMap.txt",header=T,stringsAsFactors=F,sep=",")

################
# DATA
################

#prepare the data container for all objects to be retrieved during the process
data <- list()
data$temp <- list()
data$contacts <- data.frame()
data$leads <- data.frame()
data$campaigns <- data.frame()
data$campaignMembers <- data.frame()
data$skip <- list()

lookBack <- sfdc$getTimestamp(lookback=1,lookbackUnit="hour")

################
# AUTOMATIC PART OF SCRIPT
################

#log begin
logInfo(paste("Starting the sync with lookback",lookBack),"salesforce_campaign_sync")
#get campaign members
data$campaignMembers <- sfdc$query(
  fields="CampaignId,ContactId,LeadId,CreatedDate,Status",
  object="CampaignMember",
  where=paste("(CreatedDate>",lookBack," OR LastModifiedDate>",lookBack,") AND (Status='Sent' OR Status='Invited' OR Status='Attended') AND CampaignId!='701610000007caoAAA'")
)

if(nrow(data$campaignMembers)>0){
  
  if(length(na.omit(data$campaignMembers$ContactId))!=0){
    #get fields
    queryFields <- paste(c(sfdc$mapping$Contact[sfdc$mapping$Contact!=""],"Do_Not_Email__c","HasOptedOutOfEmail"),collapse=",")
    #get ids
    queryList <- na.omit(data$campaignMembers$ContactId)
    #get the number of contacts
    n <- length(queryList)
    #create chunks of hundred contacts
    queryListSequence <- split(1:n,as.numeric(gl(n,100,n)))
    #query the contacts in batches
    data$temp$contacts <- lapply(queryListSequence,function(q)
      sfdc$queryList(
        data = queryList[q],
        fields = queryFields,
        object = "Contact",
        listField = "Id",
        rename = F
      ) 
    )
    data$contacts <- do.call("rbind",data$temp$contacts)
    data$skip$contacts <- data$contacts[data$contacts$Do_Not_Email__c=="true" || data$contacts$HasOptedOutOfEmail=="true",c("Id","Email")]
    data$contacts <- data$contacts[!(data$contacts$Id %in% data$skip$contacts$Id),]
    #log result
    logInfo(paste("Received",nrow(data$contacts),"contacts"),"salesforce_campaign_sync") 
    #rename columns
    data$contacts <- tools$mapSfdc2Dmc(data$contacts,"Contact","REST2")
  }
  if(length(na.omit(data$campaignMembers$LeadId))!=0){
    #get fields
    queryFields <- paste(c(sfdc$mapping$Lead[sfdc$mapping$Lead!=""],"Do_Not_Email__c","HasOptedOutOfEmail"),collapse=",")
    #get ids
    queryList <- na.omit(data$campaignMembers$LeadId)
    n <- length(queryList)
    queryListSequence <- split(1:n,as.numeric(gl(n,100,n)))
    #query the contacts in batches
    data$temp$leads <- lapply(queryListSequence,function(q)
      sfdc$queryList(
        data = queryList[q],
        fields = queryFields,
        object = "Lead",
        listField = "Id",
        rename = F
      ) 
    )
    data$leads <- do.call("rbind",data$temp$leads)
    data$skip$leads <- data$leads[data$leads$Do_Not_Email__c=="true" || data$leads$HasOptedOutOfEmail=="true",c("Id","Email")]
    data$leads <- data$leads[!(data$leads$Id %in% data$skip$leads$Id), ]
    #log result
    logInfo(paste("Received",nrow(data$leads),"leads"),"salesforce_campaign_sync") 
    #rename columns
    data$leads <- tools$mapSfdc2Dmc(data$leads,"Lead","REST2")
  }
  #get fields
  queryFields<- "Id,Owner.Name,Name,Type,Status,StartDate,Marketing_Campaign_Type__c,Campaign_Area__c"
  #get ids
  queryList <- unique(data$campaignMembers$CampaignId)
  queryList <- queryList[!is.na(queryList)]
  #get records
  data$campaigns <- sfdc$queryList(
    data = queryList,
    fields = queryFields,
    object = "Campaign",
    listField = "Id",
    rename = F
  )
  #define campaign query
  logInfo(paste("Received",nrow(data$campaigns),"campaings"),"salesforce_campaign_sync")
  
  #################
  # CAMPAIGN SYNC
  #################
  
  data$campaigns$groupId <- apply(data$campaigns,1,function(campaign){
    body <- list(
      name = campaign[['Name']],
      email = paste0("campaign_",campaign['Id'],"@news.mapp.com"),
      description = paste0("salesforceId"=campaign['Id']),
      includeTestUsers=F,
      includePreparedMessages=F,
      salesforceId = campaign[['Id']],
      salesforceOwner = campaign[['Owner.Name']],
      salesforceType = campaign[['Type']]
    )
    upsert <- try(ecm$upsert(
      domain = "group",
      params = list(),
      body = body,
      identifier = "salesforceId"
    ))
    if(inherits(upsert,"try-error")){
      logInfo(paste("Error when creating group for",body$name),"salesforce_campaign_sync")
    }else{
      applySettings <- try(ecm$call(
        domain="group",
        method="overrideGroupSettings",
        params=list(groupId=upsert$resourceId,settingsId=ecm$constants$defaultGroupTemplateId))
      )
    }
    
    if(length(upsert$resourceId)>0){
      logInfo(paste("Created group",upsert$resourceId,"for",body$name),"salesforce_campaign_sync")
      return(upsert$resourceId)
    }else{
      return(NA)
    }
  })
  
  #################
  # MEMBER SYNC
  #################
  
  if(nrow(data$campaignMembers)>2000){
    import_dump <- try(rbind(data$leads,data$contacts))
    try(f$dump(import_dump,paste0("import_dump_",as.character(Sys.time()),".csv")))
  }
  
  data$userIds <- apply(data$campaignMembers,1,function(member){
    
    if(is.na(member['LeadId'])){
      dataset <- data$contacts
      identifier <- member['ContactId']
    }else{
      dataset <- data$leads
      identifier <- member['LeadId']
    }
    #collect user object
    user <- subset(dataset,user.Identifier==identifier)
    user[is.na(user)]<-""
    
    if(nrow(user)>0){
      #upsert the user by Email
      upsertUser <- try(ecm$upsert(
        domain="user",
        params = list(email=user$user.Email),
        body=user
      ))
      if(inherits(upsertUser,"try-error")){
        logInfo(paste("upsertUser failed for",identifier),"salesforce_campaign_sync")
        str(user)
      }
      groupId <- data$campaigns[data$campaigns$Id==member['CampaignId'],c("groupId")]
      if(!is.na(groupId) || length(groupId)==0 || inherits(upsertUser,"try-error")){
        #upsert the membership
        upsertMember <- ecm$upsert(
          domain="membership",
          params = list(userId=upsertUser$resourceId,groupId=groupId),
          body=member
        )
      }else{
        logInfo(paste("groupId not provided for",member['CampaignId']),"salesforce_campaign_sync")
      }
    }else{
      upsertUser <- "Skipped"
    }
    return(upsertUser)
    
  })
  logInfo(paste("Created",length(data$userId),"new memberships"),"salesforce_campaign_sync")
}else{
  logInfo("No campaign member data to be synced","salesforce_campaign_sync")
}

