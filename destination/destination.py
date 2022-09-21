import numpy as np
import pandas as pd
import arcpy
import timestamp
from datetime import datetime
import os

#####################################################

dir="D:/atrin/atrin/clustering"
dirr="D:/atrin/atrin/clustering/destination"

# inputs
brand_name='BrandName'
now = "now"   #time of first run
cluster='{0}/cluster_data_BrandName_20220713_135523.csv'.format(dirr)          #cluster_data_{1}_{2}.csv'.format(dir,brand_name,now) 

##############################################################################

# clusterDF = pd.read_csv (cluster)
# OD= pd.read_csv (r'{0}/OD.csv'.format(dirr))

# clusterDF.insert(1,"TAPercent",clusterDF['TAA_csv_TA']/clusterDF['POP']) 
# clusterDFF = clusterDF.sort_values('TAZ_zone97')
# AllZoneList= clusterDFF['TAZ_zone97']    #Origin zones of TA
# clusterDFF=clusterDFF.reset_index()  
# # print(clusterDFF)
# filt = OD['zones'].isin(AllZoneList) 
# ODD=OD[filt]
# Des=ODD.reset_index()  
# # print(Des)      
# DesTA=Des.iloc[:,2:].multiply(clusterDFF['TAPercent'].squeeze(),axis=0)         #OD of orgin zones of TA
# # print(DesTA)
# DesTAAgg=DesTA.sum(axis=0)       #aggregated destination of all clustered TA    #just for graphs

# #######################################################################################################
# DesTA.insert(0,"TA",clusterDFF['TAA_csv_TA'])
# DesTA.insert(0,"Regions",clusterDFF['Regions'])
# DesTA.insert(0,"zonesO",Des['zones'])
# DesTAGroup=DesTA.groupby(['Regions']).sum()               #Aggregated Destination of all Grouping clustered TA  
# # print (DesTAGroup)
# ##############################################################################################

# temp = np.flip(DesTAGroup.iloc[:,2:].values.argsort(),1)
# DesTAGroupSortLable = pd.DataFrame(DesTAGroup.iloc[:,2:].columns[temp])       #find the column names in descending order for each row
# DesTAGroupSortLable.insert(0,"Regions",DesTAGroup.index)
# # print(DesTAGroupSortLable)
# DesTAGroupSortValue=pd.DataFrame(np.sort(DesTAGroup.iloc[:,2:].values, axis=1)[:,::-1])       # values in descending order for each row
# # print(DesTAGroupSortValue)    
# DesTAGroupSortValueCum=DesTAGroupSortValue.cumsum(axis=1)                # cumulative values which were in descending order for each row
# DesTAGroupSortValueCum.insert(0,"Regions",DesTAGroup.index) 
# # print(DesTAGroupSortValueCum) 
# DesTAGroup=DesTAGroup.reset_index()
# DesTAGroupSortValueCum.insert(0,"TA",DesTAGroup['TA'])
# print(DesTAGroupSortValueCum)
# # #######################################################################################################














# i= (DesTAGroup['0'] + DesTAGroup['2']).to_list()
# print (i)     
# arcpy.env.workspace = "{0}".format(dir)
# Regions='clustering.gdb/RegionTeh'.format(dir)    