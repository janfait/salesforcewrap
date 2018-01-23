if(!exists("libsLoaded")){
  source("/home/user/scripts/libs.R")
}

# Instead of creating a class all at once:
salesforce <- setRefClass("salesforce", 
  fields = list(
    debug="logical",
    username = "character",
    password = "character",
    token="character",
    auth="character",
    version="character",
    instance="character",
    session="character",
    initializeTime = "POSIXt",
    leadSources = "list",
    mapping = "data.frame",
    memberStates = "data.frame"
  ),
  methods = list(
    debugPrint = function(a){
      if(.self$debug){
        if(is.data.frame(a)){
          str(a)
        }else{
          print(a)
        }
      }
    },
    nan2zero = function(x){
      do.call(cbind, lapply(x, is.nan))
    },
    quotes = quotes <- function(s){
      return(paste0("'",s,"'"))
    },
    or = function(field,conditions){
      field <- paste0(field,"=")
      conditions <- unlist(strsplit(conditions,","))
      conditions <- .self$quotes(conditions)
      or <- paste0(field,conditions,collapse=" OR ")
      or <- paste0("(",or,")")
      return(or)
    },
    and = function(field,conditions){
      field <- paste0(field,"=")
      conditions <- unlist(strsplit(conditions,","))
      conditions <- .self$quotes(conditions)
      and <- paste0(field,conditions,collapse=" AND ")
      and <- paste0("(",and,")")
      return(and)
    },
    login = function(username,password,instance,version){
      return(rforcecom.login(username,password,instance,version))
    },
    create = function(fields,object){
      data <- rforcecom.create(.self$session,object,fields)
      return(data)
    },
    bulkJob = function(data=data.frame(),object,operation=c("insert","update","upsert"),idField=NULL,batchSize=1000){
      #match operation argument
      operation <- match.arg(operation)
      if(operation %in% c("upsert","update") & is.null(idField)){
        stop("Please specify the idField parameter as a update/upsert identifier in your data")
      }
      #initialize job
      j <- rforcecom.createBulkJob(.self$session, operation=operation, object=object,externalIdFieldName = idField)
      .self$debugPrint(j)
      #result
      result <- try(rforcecom.createBulkBatch(.self$session,j$id, data, multiBatch=TRUE, batchSize=batchSize))
      
      if(inherits(result,"try-error")){
        return(FALSE)
      }else{
        rforcecom.closeBulkJob(session,j)
        return(result)
      }
    },
    query = function(fields=NULL,object=NULL,where=NULL,groupby=NULL,rename=F,count=T,limit=NULL,q=NULL,explicit=F){
      if(is.null(fields) & !explicit){
        q <- paste("SELECT * FROM",object)
      }else if(explicit){
        q <- q
      }else{
        fields <- unlist(strsplit(gsub(" ","",fields,fixed=T),","))
        q <- paste("SELECT",paste(fields,collapse=","),"FROM",object)
      }
      if(!is.null(where)){
        q <- paste(q,"WHERE",where)
      }
      if(!is.null(groupby)){
        q <- paste(q,"GROUP BY",groupby)
      }
      if(!is.null(limit)){
        q <- paste(q,"LIMIT",limit)
      }
      if(explicit){
        q <- q
      }
      .self$debugPrint(q)
      data <- rforcecom.query(.self$session,q)
      if(nrow(data)>0){
        if(count){
          data$Count <- 1 
        }
        if('CreatedDate' %in% colnames(data)){
          data$CreatedDate <- as.Date(data$CreatedDate)
        }
        for (field in fields){
          if(!field %in% colnames(data)){
            data[field]<-NA
          }
        }
      }
      if(rename){
        data <- .self$renameColumns(data,object)
      }
      return(data)
    },
    queryList = function(data,fields,object,listField,split,rename=F,count=F,limit=NULL){
      .self$debugPrint(data)
      if(!missing(split)){
        l <- unlist(strsplit(as.character(data),split))
      }else{
        if(length(data)>1){
          l <- unlist(strsplit(data,","))
        }else{
          l <- unlist(data)
        }
      }
      l <- .self$quotes(l)
      l <- paste(l,collapse=",")
      where <- paste0(listField," IN (",l,")")
      .self$debugPrint(where)
      .self$debugPrint(fields)
      data <- .self$query(fields=fields,object=object,where=where,rename=rename,count=count,limit=limit)
      if(length(data)==0){
        data <- data.frame()
      }
      return(data)
    },
    getTimestamp = function(lookback,lookbackUnit=c("hour","day","month")){
      if(missing(lookback)){
        t <- format(rforcecom.getServerTimestamp(.self$session),"%Y-%m-%dT%H:%M:%S.000Z")
      }else{
        u <-switch(EXPR = lookbackUnit,"hour"=hours(lookback),"minute"=minutes(lookback),"day"=days(lookback),"month"=months(lookback))
        t <- format(rforcecom.getServerTimestamp(.self$session)-u,"%Y-%m-%dT%H:%M:%S.000Z")
      }
      return(t)
    },
    getPassword = function(){
      print(paste0(.self$password,.self$token))
    },
    getUsers = function(pattern){
      u <- .self$query(fields = "Id,CreatedDate,Username,Email,Name,FirstName,LastName,Country,Subsidiary__c,IsActive",object = "User")
      if(missing(pattern)){
        return(u)
      }else{
        u <- u[grepl(pattern,tolower(u$Name)),]
        return(u)
      }
    },
    getLeadStatus = function(stage,status){
      if(any(missing(stage),missing(status))){
        stop("Provide a stage and status parameters")
      }else{
        stages <- c("MQL","SAL","SQL","Suspect","Junk")
        states <- c("Open","Closed","Conv")
        stageMatch <- charmatch(stages,stage)
        stateMatch <- charmatch(states,status)
        out <- paste0(states[!is.na(stateMatch)],stages[!is.na(stageMatch)])
        return(out) 
      }
    },
    getLeadSource = function(domain="marketing"){
      return(unlist(strsplit(.self$leadSources[[domain]],",")))
    },
    getMappSystems = function(where=NULL){
      d <- .self$query(
        fields = "Id,Account__c,Name,Account__r.Name,Account__r.Owner.Name,Account__r.BH_Industry__c,Account__r.BillingCountry,Account__r.netsuite_conn__NetSuite_Id__c,System_Id1__c,System_Id2__c",
        object = "System__c",
        count = FALSE
      )
      colnames(d)<-c("Id","Account.Id","System","Account.Name","Account.Owner","Account.Industry","Account.Country","Account.NetsuiteId","Customer.Id","System.Id")
      d$Customer.Id <- as.numeric(d$Customer.Id)
      d$System.Id <- as.numeric(d$System.Id)
      return(d)
    },
    getCampaignNominations = function(campaignId,states=NULL,limit=NULL){
      
      if(is.null(states)){
        states <- c("Sent","Invited")
      }
      #build or condition
      states <- .self$or("Status",states)
      
      #get campaign members
      campaignMembers <- .self$query(
        fields="CampaignId,ContactId,LeadId,CreatedDate,Status,IsDeleted",
        object="CampaignMember",
        where=paste0(states," AND CampaignId='",campaignId,"'"),
        limit=limit,
        count=FALSE
      )
      
      return(campaignMembers)

    },
    getCampaignLeadsAndContacts = function(campaignId,states=NULL,keepUnsubscribers=FALSE,limit=NULL){
      
      #get nominations
      campaignMembers <- .self$getCampaignNominations(campaignId,states=states,limit=limit)
      #prepare default output
      d <- list(leads=data.frame(),contacts=data.frame(),campaignMembers=campaignMembers)
      
      if(nrow(campaignMembers)>0){
        
        temp <- list()

        if(length(na.omit(campaignMembers$ContactId))!=0){
          #get fields
          queryFields <- "Id,Email,FirstName,LastName,Account.Name,Marketing_Role__c,Decision_Making_Level__c,MailingCountry,Phone,Title,Account.Type,Decision_Making_Role_contact__c,Do_Not_Email__c,HasOptedOutOfEmail"
          #get ids
          queryList <- na.omit(campaignMembers$ContactId)
          n <- length(queryList)
          #create chunks of hundred contacts
          queryListSequence <- split(1:n,as.numeric(gl(n,100,n)))
          #query the contacts in batches
          temp$contacts <- lapply(queryListSequence,function(q)
            .self$queryList(
              data = queryList[q],
              fields = queryFields,
              object = "Contact",
              listField = "Id",
              rename = F,
              count = F
            ) 
          )
          contacts <- do.call("rbind",temp$contacts)
          if(!keepUnsubscribers){
            contacts <- contacts[contacts$Do_Not_Email__c!="true" & contacts$HasOptedOutOfEmail!="true", ]
          }
          d[['contacts']] <- contacts
        }
        if(length(na.omit(campaignMembers$LeadId))!=0){
          #get fields
          queryFields <- "Id,Email,FirstName,LastName,Company,Marketing_Role__c,Decision_Making_Level__c,Country,Phone,Title,Status,Lifecycle_Stage__c,LeadSource,Lead_Status_Reason__c,Lead_Type__c,Do_Not_Email__c,HasOptedOutOfEmail"
          #get ids
          queryList <- na.omit(campaignMembers$LeadId)
          n <- length(queryList)
          queryListSequence <- split(1:n,as.numeric(gl(n,100,n)))
          #query the contacts in batches
          temp$leads <- lapply(queryListSequence,function(q)
            .self$queryList(
              data = queryList[q],
              fields = queryFields,
              object = "Lead",
              listField = "Id",
              rename = F,
              count = F
            ) 
          )
          leads <- do.call("rbind",temp$leads)
          if(!keepUnsubscribers){
            leads <- leads[leads$Do_Not_Email__c!="true" & leads$HasOptedOutOfEmail!="true", ]
          }
          d[['leads']] <- leads
        }
      }
      
      return(d)

    },
    getAccountContacts = function(accountId,limit=NULL){
      d<-list()
      d$contacts <- .self$query(
        fields = "Id,AccountId,Email,Title,Decision_Making_Role_contact__c",
        object = "Contact",
        where = paste0("AccountId ='",accountId,"'"),
        limit = limit,
        count = FALSE
      )
      d$n <- nrow(d$contacts)
      return(d)
    },
    getCampaignMemberships = function(Id,respondedOnly=FALSE){
      d<-list()
      d$members <- .self$query(
        fields = "Id,CampaignId,LeadId,ContactId,Status,HasResponded,CreatedDate,Campaign.Type",
        object = "CampaignMember",
        where = paste0("(ContactId ='",Id,"' OR LeadId='",Id,ifelse(respondedOnly,"') AND HasResponded = true","')")),
        count = FALSE
      )
      d$n <- nrow(d$members)
      return(d)
    },
    getAccountTouchpoints = function(accountId,respondedOnly=T,limit=NULL){
      contacts <- .self$getAccountContacts(accountId,limit=limit)
      d <- list()
      d$touchpointList <- lapply(contacts$contacts$Id,function(contactId){
        members <- .self$getCampaignMemberships(contactId,respondedOnly = respondedOnly)
        return(members$members)
      })
      d$AccountId <- accountId
      d$contacts <- contacts$n
      d$touchpoints <- dplyr::bind_rows(d$touchpointList)
      return(d)
    },
    getCampaignEffect = function(campaignId){
      d <- list()
      d$temp <- list()
      d$campaignMembers <- data.frame()

      d$campaignMembers <- .self$query(
        fields="Campaign.Name,ContactId,LeadId,CreatedDate,Status,Lead.Lead_Area__c,Lead.Country,Lead.Company,Lead.Lifecycle_Stage__c,Contact.MailingCountry,Contact.Account.Id,Contact.Account.Type,Contact.Account.Account_Area__c",
        object="CampaignMember",
        where=paste0("CampaignId='",campaignId,"'")
      )
      return(d)
    },
    getGroupedWaterfall = function(from,to=as.character(Sys.Date()),leadSource=NULL,groupby=NULL,includeSuspects=TRUE){
      .self$debugPrint("Running grouped waterfall")
      
      if(missing(from)){
        from <- as.character(Sys.Date()-90)
      }
      if(missing(to)){
        to <- as.character(Sys.Date())
      }
      mseq <- seq.Date(as.Date(from),as.Date(to),"month")
      from <- paste0(from,"T00:00:00Z")
      to <- paste0(to,"T00:00:00Z")

      if(!is.null(groupby)){
        groupby <- unlist(strsplit(groupby,","))
        dots <- lapply(groupby,as.symbol)
      }else{
        groupby <- NULL
        dots <- NULL
      }
      
      #container
      w <- list()
      
      .self$debugPrint("Collecting lead waterfall")
      #leads
      w$lead <- .self$query(
        fields = "COUNT(Id) n,CALENDAR_YEAR(CreatedDate) year,CALENDAR_MONTH(CreatedDate) month,Lead_Area__c,Lifecycle_Stage__c,Status,LeadSource",
        object = "Lead",
        where = paste("Lead_Area__c!= null AND Lead_Type__c='Potential Client' AND CreatedDate>",from,"AND CreatedDate<",to),
        groupby = "CALENDAR_MONTH(CreatedDate),CALENDAR_YEAR(CreatedDate),Lead_Area__c,Lifecycle_Stage__c,Status,LeadSource",
        rename = T,
        count = F
      )
      #rename columns
      w$lead <- w$lead[,c("n.text","month.text","year.text","Stage","Status","Source","Area")]
      colnames(w$lead) <- c("n","Month","Year","Stage","Status","Source","Area")
      
      #subset lead sources
      if(!is.null(leadSource)){
        w$lead <- w$lead[w$lead$Source %in% .self$getLeadSource(leadSource),]
      }
      #convert to numerics
      w$lead$n <- as.numeric(w$lead$n)
      w$lead$Month <- as.numeric(w$lead$Month)
      w$lead$Year <- as.numeric(w$lead$Year)
      #compute new variables
      w$lead$QuarterLabel <- paste(w$lead$Year,1+findInterval(w$lead$Month,c(4,7,10)),sep="-Q")
      w$lead$Month <- as.Date(ISOdate(w$lead$Year,w$lead$Month,1))
      w$lead$Quarter <- update(w$lead$Month,months = ceiling(month(w$lead$Month)/3) * 3 - 2, mdays = 1)
      w$lead$Channel <- "Marketing"
      w$lead$Channel[w$lead$Source %in% .self$getLeadSource("sales")]<- "Sales"
      w$lead$Channel[w$lead$Source %in% .self$getLeadSource("partner")]<- "Partner"
      
      .self$debugPrint("Collecting opp waterfall")
      #opportunities
      w$opp <- .self$query(
        fields = "COUNT(Id) n,CALENDAR_YEAR(CreatedDate) year,CALENDAR_MONTH(CreatedDate) month,SUM(Annual_Contract_V__c) acv,Opportunity_Area__c,Deal_Type__c,StageName,LeadSource",
        object = "Opportunity",
        where = paste("Opportunity_Area__c!= null AND CreatedDate>",from,"AND CreatedDate<",to,"AND Deal_Type__c !='Upsell'"),
        groupby = "CALENDAR_MONTH(CreatedDate),CALENDAR_YEAR(CreatedDate),Opportunity_Area__c,Deal_Type__c,StageName,LeadSource",
        rename = T,
        count = F
      )
      #rename columns
      w$opp <- w$opp[,c("n.text","month.text","year.text","acv.text","StageName","LeadSource","O.Area")]
      colnames(w$opp) <- c("n","Month","Year","ACV","Stage","Source","Area")
      #convert to numerics
      w$opp$n <- as.numeric(w$opp$n)
      w$opp$Month <- as.numeric(w$opp$Month)
      w$opp$Year <- as.numeric(w$opp$Year)
      #compute new variables
      w$opp$QuarterLabel <- paste(w$opp$Year,1+findInterval(w$opp$Month,c(4,7,10)),sep="-Q")
      w$opp$Month <- as.Date(ISOdate(w$opp$Year,w$opp$Month,1))
      w$opp$Quarter <- update(w$opp$Month,months = ceiling(month(w$opp$Month)/3) * 3 - 2, mdays = 1)
      w$opp$Channel <- "Marketing"
      w$opp$Channel[w$opp$Source %in% .self$getLeadSource("sales")]<- "Sales"
      w$opp$Channel[w$opp$Source %in% .self$getLeadSource("partner")]<- "Partner"

      #subset opp sources
      if(!is.null(leadSource)){
        w$opp <- w$opp[w$opp$Source %in% .self$getLeadSource(leadSource),]
      }

      .self$debugPrint("Aggregating lead waterfall")
       w$agg_l <- (
          w$lead
          %>% dplyr::group_by_(.dots=dots)
          %>% dplyr::summarise(
            SQL = sum(n[Stage=='SQL - Sales Qualified Lead']),
            SAL = SQL + sum(n[Stage=='SAL - Sales Accepted Lead']),
            MQL = SAL + sum(n[Stage=='MQL - Marketing Qualified Lead']),
            Suspect = MQL + sum(n[Stage=='Suspect']),
            MQL_2_SAL = round(SAL/MQL,2),
            SAL_2_SQL = round(SQL/SAL,2)
          )
        )
       
       if(!includeSuspects){
         l$agg$Suspect <- NULL
       }
       .self$debugPrint("Aggregating opp waterfall")
       w$agg_o <- (
         w$opp
         %>% dplyr::group_by_(.dots=dots)
         %>% dplyr::summarise(
           SQO = sum(n),
           Won = sum(n[Stage=='Closed Won']),
           ACV_Won = sum(as.numeric(ACV[Stage=='Closed Won'])),
           SQO_2_Won = round(Won/SQO,2)
         )
       )
       .self$debugPrint("Binding waterfall")
       w$agg <- merge(w$agg_l,w$agg_o)

      return(w)
      
    },
    getWaterfall = function(from,to=as.character(Sys.Date()),leadSource=NULL){
      
      .self$debugPrint("Running waterfall")
      
      if(missing(from)){
        from <- as.character(Sys.Date()-90)
      }
      if(missing(to)){
        to <- as.character(Sys.Date())
      }
      if(is.null(leadSource)){
        leadSource = .self$leadSources$marketing
      }
      from <- paste0(from,"T00:00:00Z")
      to <- paste0(to,"T00:00:00Z")
      
      l <- list(data=data.frame(),fields="",conditions="",timeframe="",where="")
      l$fields <- "Id,CreatedDate,IsConverted,LeadSource,Country,Lead_Score_IMP_Plus_EXP__c,Meeting_Date__c,Status,Lead_Area__c,Lead_Type__c,Lifecycle_Stage__c,ConvertedOpportunityId"
      l$conditions <- .self$or("LeadSource",leadSource)
      l$conditions <- paste(w$conditions,"AND","Lead_Type__c='Potential Client'")
      l$timeframe <-paste("CreatedDate<",to,"AND","CreatedDate>",from)
      l$where <- paste(l$conditions,"AND",l$timeframe)
      
      .self$debugPrint("Querying leads")
      l$data <- .self$query(
        fields=l$fields,
        object="Lead",
        where= l$where,
        rename = T
      )
      l$data$LeadScoreCat <- findInterval(as.numeric(l$data$LeadScore),c(50,100))
      l$data$StatusStage <- mapply(.self$getLeadStatus,stage=l$data$Stage,status=l$data$Status)
      .self$debugPrint(str(l$data))
      
      o <- list(data=data.frame(),fields="",conditions="",timeframe="",where="")
      o$fields <- "Id,CreatedDate,Account.Name,StageName,LeadSource,Opportunity_Area__c,CampaignId,Total_Contract_V__c"
      o$conditions <- l$data$ConvertedOpportunityId
      o$conditions <- paste0(o$conditions[!is.na(o$conditions)],collapse=",")
      
      .self$debugPrint("Querying related opportunites")
      o$data <- .self$queryList(data = o$conditions,split=",",listField="Id",fields = o$fields, object="Opportunity",rename=T)
      
      .self$debugPrint(str(o$data))
      if(nrow(o$data)>0){
        o$data$Value <- as.numeric(ifelse(is.na(o$data$Total.Value),0,o$data$Total.Value))
        o$data$Attribution <- "Direct" 
      }else{
        o$data$Value <- c()
        o$data$Attribution <- c()
      }
      i <- list(data=data.frame(),fields="",conditions="",timeframe="",where="")
      i$fields <- "Id,CreatedDate,Account.Name,StageName,LeadSource,Opportunity_Area__c,CampaignId,Total_Contract_V__c"
      i$conditions <- "Deal_Type__c='New'"
      i$timeframe <-paste("CreatedDate<",to,"AND","CreatedDate>",from)
      i$where <- paste(i$conditions,"AND",i$timeframe)
      .self$debugPrint("Querying all marketing opportunities")
      i$data <- .self$query(
        fields=i$fields,
        object="Opportunity",
        where= i$where,
        rename=T
      )
      i$data$Value <- as.numeric(ifelse(is.na(i$data$Total.Value),0,i$data$Total.Value))
      i$data$Attribution <- ifelse(is.na(i$data$CampaignId),"Indirect","Direct")
      .self$debugPrint(nrow(i$data))
      
      if(nrow(o$data)==0){
        o$data <- i$data
      }else{
        o$data <- rbind(o$data,i$data)
      }

      o$data <- o$data[!duplicated(o$data$Id),]
      o$data <- o$data[!grepl("test|Test",o$data$A.Name),]
      o$data$URL <- paste0(.self$instance,o$data$Id)
      o$data <- o$data[o$data$LeadSource %in% unlist(strsplit(.self$leadSources$marketing,",")),]
      .self$debugPrint(str(o$data))
      
      #merge
      d <- merge(l$data,o$data,by.x="ConvertedOpportunityId",by.y="Id",all.x=T,all.y=T)
      #clean
      fill <- is.na(d$Area)
      d$Area[fill]<-d$O.Area[fill]
      d$Source[fill]<-d$LeadSource[fill]
      d$Stage[fill]<-"NA"
      d$LeadScore[fill]<-0
      d$Value[fill]<-0
      #reduce
      d <- d[,c("Stage","LeadScore","Area","Status","Source","StageName","Qualification_Meeting","CampaignId","LeadSource","LeadScoreCat","Value")]
      d <- d %>% dplyr::group_by(Area)
      d <- d %>% dplyr::summarise(
        REVENUE = sum(Value[StageName=='Closed Won'],na.rm=T),
        WINS = sum(StageName=="Closed Won",na.rm=T),
        PIPELINE = sum(Value,na.rm=T),
        SQO = sum(!is.na(StageName)),
        SQL = sum(Stage=="SQL - Sales Qualified Lead",na.rm=T),
        QM = sum(!is.na(Qualification_Meeting)),
        SAL = SQL + sum(Stage=="SAL - Sales Accepted Lead",na.rm=T),
        MQL = SAL + sum(Stage=="MQL - Marketing Qualified Lead",na.rm=T),
        MQL.DIRECT = sum((Stage %in% c("MQL - Marketing Qualified Lead","SAL - Sales Accepted Lead","SQL - Sales Qualified Lead") & (Source=='Website - Demo Request' | Source=='Website - Contact Us Form')),na.rm=T),
        MQL.DISQ = sum(Stage=="MQL - Marketing Qualified Lead" & Status=="Closed",na.rm=T),
        SUSPECT = MQL + sum(Stage=="Suspect",na.rm=T),
        SUSPECT2MQL = round(MQL/SUSPECT,2),
        MQL2DISQ = round(MQL.DISQ/MQL,2),
        MQL2SAL = round(SAL/MQL,2),
        SAL2SQO = round(SQO/SAL,2),
        SQO2WIN = round(WINS/SQO,2)
      )
      d <- d[,c("Area","SUSPECT","SUSPECT2MQL","MQL","MQL.DIRECT","MQL.DISQ","MQL2DISQ","MQL2SAL","SAL","QM","SQL","SAL2SQO","SQO","SQO2WIN","PIPELINE","WINS")]
      d[.self$nan2zero(d)]<-0
      return(list(waterfall=d,opportunities=o$data,leads=l$data))
    },
    splitMultiselect = function(data=NULL,column=NULL){
      if(any(is.null(data),is.null(column))){
        stop("data and column parameters are required")
      }
      if(length(data[column])==0){
        stop("column not found in data")
      }
      a <- strsplit(x1[,column],";")
      a <- lapply(a,function(i){
        i <- gsub(" ,|&","_",i)
        i <- gsub(" ","_",i)
        i <- tolower(i)
        i <- sort(i)
      })
      proto <- unique(unlist(a))
      
      m <- lapply(a,function(i) match(proto,i,nomatch=0))
      m <- do.call("rbind",m)
      colnames(m)<-proto
      
      data <- cbind(data,m)
      return(data)
    },
    getLeadHistory = function(from,fields="Id,CreatedDate,Status,LeadSource,Lead_Area__c,Lifecycle_Stage__c,IsConverted,ConvertedDate",where=NULL,rename=FALSE){
      if(missing(from)){
        from <- paste0(Sys.Date()-30,"T00:00:00Z")
      }else{
        from <- paste0(from,"T00:00:00Z")
      }
      if(is.null(where)){
        where <- paste("CreatedDate>",from)
      }else{
        where <- paste("CreatedDate>",from,"AND",where)
      }
      leads <- .self$query(
        fields= fields,
        object="Lead",
        where=,
        rename = rename
      )
      leadHistory <- .self$query(
        fields="LeadId,CreatedDate,Field,OldValue,NewValue",
        object="LeadHistory",
        where=paste("Field IN ('Lifecycle_Stage__c','Status') AND CreatedDate>",from),
        rename = rename
      )
      leadHistory <- merge(leads,leadHistory,by.x="Id",by.y="LeadId",all.x=T,all.y=F)
      leadHistory$OldValue <- leadHistory$OldValue.text
      leadHistory$NewValue <- leadHistory$NewValue.text
      return(leadHistory)
    },
    getOpportunityStageHistory = function(from,field="StageName",format="wide"){
      
      #format
      if(missing(format)){
        format <- "wide"
      }
      #complete the from input
      if(missing(from)){
        from <- paste0(Sys.Date()-30,"T00:00:00Z")
      }else{
        from <- paste0(from,"T00:00:00Z")
      }
      #query datas
      oppsHistory<-.self$query(
        fields="OpportunityId,CreatedDate,Field,NewValue,OldValue",
        object="OpportunityFieldHistory",
        where = paste0("Field='",field,"' AND CreatedDate>",from)
      )
      #cast 
      if(format=="wide"){
        oppsHistory<-reshape2::dcast(
          data=oppsHistory,
          formula="OpportunityId ~ NewValue.text",
          value.var="CreatedDate",
          fun.aggregate = function(x) paste(x[1])
        )
        colnames(oppsHistory)<- gsub(" ","_",colnames(oppsHistory))
        oppsHistory[oppsHistory=="NA"]<-NA
      }else if(format=="long"){
        oppsHistory <- oppsHistory[,c("OpportunityId","CreatedDate","NewValue.text")]
        colnames(oppsHistory) <- c("OpportunityId","CreatedDate",field)
      }else{
        oppsHistory <- oppsHistory[,c("OpportunityId","CreatedDate","OldValue.text","NewValue.text")]
        colnames(oppsHistory) <- c("OpportunityId","CreatedDate","OldValue","NewValue")
      }

      return(oppsHistory)
    },
    renameColumns = function(data,object){
      fieldSet <- list(
        'Lead' = list(
          'CreatedDate'='Date',
          'Lifecycle_Stage__c'='Stage',
          'Lead_Area__c'='Area',
          'Lead_Type__c'='Type',
          'Lead_Status_Reason__c' = 'Status Reason',
          'Lead_Source_Description__c' = 'Source.Detail',
          'LeadSource' = 'Source',
          'Solution_Interest__c' = 'Solution',
          'Lead_Score_IMP_Plus_EXP__c' = 'LeadScore',
          'Meeting_Date__c'='Qualification_Meeting'
        ),
        'Contact' = list(),
        'Opportunity' = list(
          'StageName' = 'StageName',
          'Total_Contract_Value__c'="Total.Value",
          'Total_Contract_V__c'="Total.Value",
          'Annual_Contract_V__c'="ACV",
          'Annual_Contract_Value__c'="ACV",
          'Total_Software_ACV__c'='ACV.Software',
          'ACV_Services__c'='ACV.Services',
          'Next_Step__c'="Next",
          'Sales_Engineer__c'='PS',
          'ForecastCategoryName'='Forecast',
          'Owner.Name'='Owner',
          'Opportunity_Solution__c'='O.Solution',
          'Opportunity_Area__c'='O.Area',
          'Proposal_Delivered_Date__c'='Proposal.Date',
          'PS_Contract_Value__c'="Service.Value",
          "Account.Name"="A.Name",
          "Account.Website"="A.Website",
          "Account.Type"="A.Type",
          "Account.BH_Industry__c"="A.Industry",
          "Deal_Type__c"="Type"
        ),
        'Account' = list(
          'Number_of_Employees__c' = 'Employees',
          'Annual_Revenue__c' = 'Annual.Revenue',
          'BillingCountry' = 'Country',
          'Account_Area__c' = 'A.Area'
        ),
        'Campaign' = list(
          'Campaign_Area__c' = "C.Area",
          'Owner.Name' = "Owner"
        )
      )
      colnames(data) <- sapply(colnames(data),function(k){
        if(k %in% names(fieldSet[[object]])){
          colnames(data)[which(colnames(data)==k)] <- unlist(fieldSet[[object]][k])
        }else{
          k
        }
      })
      return(data)
    },
    initialize=function(system="production",settings=NULL,debug=F){
      
      #initialize with login values
      .self$debug <- debug
      .self$debugPrint("Initializing the SFDC instance")
      if(.self$debug){
        rforcecom.debug <- TRUE
      }
      
      if(is.null(settings)){
        warning("You have initialized the SFDC instance without any settings argument")
      }  
      if(!require("RForcecom") | !require("dplyr") | !require("reshape2") | !require("jsonlite") | !require("lubridate")){
          stop('Please install the "RForcecom","dplyr","lubridate","reshape2" and "jsonlite" packages before using this class')
      }
      
      #define member states
      .self$memberStates <- data.frame(
        Status=c("Attended","Engaged","Responded","Converted","Opened","Clicked","Invited","Sent"),
        StatusType = c("Event","Event","Web","Web","Email","Email","Event","Email"),
        Nomination = c(TRUE,FALSE,FALSE,FALSE,FALSE,FALSE,TRUE,TRUE)
      )

      .self$username <- settings$sfdc[[system]]$username
      .self$password <- settings$sfdc[[system]]$password
      .self$token <- settings$sfdc[[system]]$token
      .self$instance <- settings$sfdc[[system]]$domain
      .self$version <- "32.0"
      .self$initializeTime <- Sys.time()
      .self$auth <- paste0(.self$password,.self$token)
      .self$session <- login(.self$username,.self$auth,.self$instance,.self$version)
      .self$leadSources <- settings$sfdc[[system]]$leadSources
      
      return(.self)
    }
  )
)

mapSfdc2Dmc <- function(data=data.frame(),object=c("Contact","Lead"),api=c("REST1","REST2")){
  if(missing(api)){
    api <- "REST1"
  }
  if(missing(object)){
    stop("Provide Contact or Lead as object to map")
  }
  if(missing(data)){
    stop("Provide data frame with Salesforce data")
  }
  if(exists("sfdc2dmcMapping")){
    m <- sfdc2dmcMapping
  }else{
    m <- read.table("/data/sf/sfdc2dmcMapping.txt",header=T,stringsAsFactors=F,sep=",")
  }

  sourceCols <- colnames(data)
  targetCols <- m[object]
  sourceCols <- intersect(sourceCols,unlist(targetCols))
  data <- data[,sourceCols]
  
  newColnames <- sapply(sourceCols,function(col) m[which(col==targetCols),api])
  newColnames <- unlist(newColnames)
  colnames(data) <- newColnames
  
  data$user.ISOCountryCode <- unlist(sapply(
    data$user.ISOCountryCode,function(c) mapGeo(c,"/data/utilities/countryMap.json","code")
  ))
  return(data)
  
}


