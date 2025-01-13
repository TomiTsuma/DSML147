library("data.table")
calcCec <- function(inp){
  
  # Function name:
  
  fuNa=c("calcCec")
  
  ## Calculate acid_saturation:
  
  inp$acid_saturation=signif((inp$exchangeable_acidity/(inp$exchangeable_acidity+inp$magnesium/120+inp$calcium/200+inp$potassium/390+inp$sodium/230))*100,2)
  
  ## Calculate CEC:
  
  # Calculate hydrogen percent and other bases:
  
  inp$hydrogen_percent=NA
  inp$other_bases=NA
  for(i in 1:nrow(inp)){
    
    # Hydrogen percentage:
    if(inp$ph[i]>7){
      inp$hydrogen_percent[i]=0
    }
    if(inp$ph[i]>6 & inp$ph[i]<=7){
      inp$hydrogen_percent[i]=(7-inp$ph[i])*15
    }
    if(inp$ph[i]>5 & inp$ph[i]<=6){
      inp$hydrogen_percent[i]=(195-(30*inp$ph[i]))
    }
    if(inp$ph[i]>4 & inp$ph[i]<=5){
      inp$hydrogen_percent[i]=(145-(20*inp$ph[i]))
    }
    if(inp$ph[i]>3 & inp$ph[i]<=4){
      inp$hydrogen_percent[i]=(105-(10*inp$ph[i]))
    }
    if(inp$ph[i]>=2.2 & inp$ph[i]<=3){
      inp$hydrogen_percent[i]=(93-(6*inp$ph[i]))
    }
    if(inp$ph[i]<2.2){
      inp$hydrogen_percent[i]=(155-(25*inp$ph[i]))
    }
    
    # Other bases:
    
    if(inp$ph[i]>6.1){
      inp$other_bases[i]=11.4-inp$ph[i]
    }
    if(inp$ph[i]>3 & inp$ph[i]<=6.1){
      inp$other_bases[i]=17.4-(2*inp$ph[i])
    }
    if(inp$ph[i]<=3){
      inp$other_bases[i]=0
    }
  }
  
  # calculate CEC:
  
  inp$cecCalcul=NA
  inp$cecCalcul=((inp$calcium/200+inp$potassium/390+inp$magnesium/120+inp$sodium/230))/(100-(inp$hydrogen_percent+inp$other_bases))*100
  inp$calciumPercSat=signif(inp$calcium/200/inp$cecCalcul*100,2)
  inp$magnesiumPercSat=signif(inp$magnesium/120/inp$cecCalcul*100,2)
  inp$potassiumPercSat=signif(inp$potassium/390/inp$cecCalcul*100,2)
  inp$sodiumPercSat=signif(inp$sodium/230/inp$cecCalcul*100,2)
  
  # Calculate Ca:Mg ratio:
  
  inp$CaMgRatio=signif(inp$calcium/inp$magnesium,3)
  
  # Calculate CN ratio:
  
  a1=match(c("organic_carbon","total_nitrogen"),colnames(inp))
  if(length(na.omit(a1))==2){
    inp$CNRatio=inp$organic_carbon/inp$total_nitrogen 
  }

  # Create output:
  write.csv(inp, 'D://Cropnuts/DSML147/scoring_output.csv')
  out=inp
  
  ## Return object:
  
  return(out)
  
}

df <- fread("D://Cropnuts/DSML147/scoringresult_latest.csv", header=TRUE)

calcCec(df)


