import numpy as np
import pandas as pd
import arcpy
from datetime import datetime
import os


########################################################################################################
# inputs
dir="D:/atrin/atrin/clustering"
classes=["A","B","C1"]
TargetAges=['W0','W5','W10','W15','W20','W25','W30','W35','W40','W45','W50','W55','W60','W65','W70']
TargetAgess=['W0','W5','W10','W15','W20','W25','W30','W35','W40','W45','W50','W55','W60','W65','W70','zone97','SocialClass','POP','Regions']
TargetAgesss=['TA','W0','W5','W10','W15','W20','W25','W30','W35','W40','W45','W50','W55','W60','W65','W70','zone97','SocialClass','POP','Regions']
TargetAgessss=['SOURCE_ID','W0','W5','W10','W15','W20','W25','W30','W35','W40','W45','W50','W55','W60','W65','W70','zone97','SocialClass','POP','Regions']
brand_name='BrandName'
#priority
i=0         # please start from 0 , based on TargetAges list
j=1          # index - end of RANGE AGE            
ii=i+13
jj=j+14
##################################################################################################
now = datetime.now()
date_time = now.strftime("%Y%m%d_%H%M%S")
arcpy.env.workspace = dir
df = pd.read_csv (r'{0}/TAZ_BlockData_Aggregatee.csv'.format(dir))
#calculating the number of targetAudiences
df['TA']= df[TargetAges].sum(axis=1)

#socialclass filtering
filt=df['SocialClass'].isin(classes)
df_TargetSocialClass=df[filt]
df2=df_TargetSocialClass[TargetAgesss]
# df2.to_csv('{0}/SC_TA_{1}_{2}.csv'.format(dir,brand_name,date_time))              ******

zones=df['zones']
TargetAudience=pd.merge(zones,df2,left_on='zones',right_on='zone97',how='outer')
TargetAudience['TA'] = TargetAudience['TA'].fillna(0)
TargetAudience.to_csv('{0}/TAA.csv'.format(dir))



TargetAudience=pd.read_csv (r'{0}/TAA.csv'.format(dir))
arcpy.MakeFeatureLayer_management("clustering.gdb/TAZ", "temmp")
arcpy.AddJoin_management("temmp", "zone97", 'TAA.csv', "zones")
arcpy.CopyFeatures_management("temmp", "clustering.gdb/TAZ_join")
TAZ_join='clustering.gdb/TAZ_join'     #filter_final_featureclass


cluster='clustering.gdb/cluster'    #HH-HL-LL-LH
arcpy.stats.ClustersOutliers(TAZ_join, 'TAA_csv_TA', cluster, 'INVERSE_DISTANCE', 'EUCLIDEAN_DISTANCE', 'NONE')


lyr="lyr"
arcpy.MakeFeatureLayer_management(cluster,lyr)
Type=['HH','HL']
sql= "COType ='HH' or COType ='HL'"
arcpy.SelectLayerByAttribute_management(lyr,"NEW_SELECTION",sql)  
# fc_namee=str(fc)
# fc_namee=fc_namee[:-4]  
cluster_HHHL="cluster_HHHL.shp"
arcpy.CopyFeatures_management(lyr,cluster_HHHL) 
arcpy.Delete_management(lyr)

# ###################################################################     adddata_to-firstcluster(Moran)
cluster_xls='{0}/cluster_HHHL.xls'.format(dir)
arcpy.conversion.TableToExcel(cluster_HHHL, cluster_xls)

dfC = pd.read_excel(cluster_xls)
dfTAZ=pd.read_csv('{0}/TAZ.csv'.format(dir))
final=pd.merge(dfC,dfTAZ,left_on='SOURCE_ID',right_on='OBJECTID',how='outer')
finall=final[['SOURCE_ID','TAZ_zone97','COType','TAA_csv_TA']]
# finall.to_csv('{0}/clusterT_{1}_{2}.csv'.format(dir,brand_name,date_time))              ******SS
filt=finall['COType'].isin(['HH','HL'])
finalll=finall[filt]
finalll.to_csv('{0}/cluster_{1}_{2}.csv'.format(dir,brand_name,date_time))

os.remove('{0}/TAA.csv'.format(dir))
os.remove('{0}/cluster_HHHL.xls'.format(dir))

arcpy.env.workspace = "{0}/clustering.gdb".format(dir)
arcpy.Delete_management('TAZ_join',"")
arcpy.Delete_management('cluster',"")


listt=finalll['TAZ_zone97']
filtt=df['zone97'].isin(listt)
dff=df[filtt]
finalll_addcoloum=pd.merge(finalll,dff[TargetAgess],left_on='TAZ_zone97',right_on='zone97',how='outer')
finalll_addcoloum.to_csv('{0}/cluster_data_{1}_{2}.csv'.format(dir,brand_name,date_time))

#data_csv_final
# ################################################################################################################
# arcpy.Delete_management('temmp')
finalll_addcoloumn=finalll_addcoloum[TargetAgessss]

# finalll_addcoloumn.columns = finalll_addcoloumn.columns.str.replace('SOURCE_ID', 'SOURCE_ID')

finalll_addcoloumn.to_csv('{0}/join.csv'.format(dir))

arcpy.MakeFeatureLayer_management("{0}/cluster_HHHL.shp".format(dir), "temmpp")   #11ta field dare
arcpy.AddJoin_management("temmpp", "SOURCE_ID", '{0}/join.csv'.format(dir), "SOURCE_ID")
arcpy.CopyFeatures_management("temmpp", "{0}/cluster_data_{1}_{2}.shp".format(dir,brand_name,date_time))    #cluster_HHHL.shp + cluster_Data.csv (list of columns)
arcpy.Delete_management('temmpp')
os.remove('{0}/join.csv'.format(dir))
arcpy.Delete_management('{0}/cluster_HHHL.shp'.format(dir),"")

# #################################################################################################################Grouping
input_grouping='{0}/cluster_data_{1}_{2}.shp'.format(dir,brand_name,date_time)    
output_grouping='{0}/Grouping_data_{1}_{2}.shp'.format(dir,brand_name,date_time)
Output_Report_File='{0}/Grouping_report_{1}_{2}.pdf'.format(dir,brand_name,date_time)

field_names = []
fields = arcpy.ListFields(input_grouping)
for field in fields:
    field_names.append(field.name)

Analysis_Fields=field_names[ii:jj]

arcpy.stats.GroupingAnalysis(input_grouping, "cluster_HH", output_grouping, "2", Analysis_Fields, 
                             "NO_SPATIAL_CONSTRAINT", "EUCLIDEAN","","","FIND_SEED_LOCATIONS","","",
                             "DO_NOT_EVALUATE") 
#finalCSV
output_grouping_xls_file='{0}/Grouping_data_{1}_{2}.xls'.format(dir,brand_name,date_time)
arcpy.TableToExcel_conversion (output_grouping, output_grouping_xls_file)
df_grouping=pd.read_excel (r'{0}/Grouping_data_{1}_{2}.xls'.format(dir,brand_name,date_time))
df_grouping=df_grouping[['cluster_HH','SS_GROUP']]
final_grouping_clustering_df=pd.merge(df_grouping,finalll,left_on='cluster_HH',right_on='SOURCE_ID',how='outer')
final_grouping_clustering_df.to_csv('{0}/cluster_grouping_{1}_{2}.csv'.format(dir,brand_name,date_time))
#find priority                           
output_grouping_table='sumstats_{0}_{1}'.format(brand_name,date_time)
statistics_fields=Analysis_Fields[0]
arcpy.analysis.Statistics(output_grouping, output_grouping_table,[[statistics_fields, "Mean"]], "SS_GROUP")
out_xls_file='{0}/sumstats_{1}_{2}.xls'.format(dir,brand_name,date_time)
arcpy.TableToExcel_conversion (output_grouping_table, out_xls_file)
df=pd.read_excel (r'{0}/sumstats_{1}_{2}.xls'.format(dir,brand_name,date_time))

if df.iloc[0,3] > df.iloc[1,3]:
    priority=1
else :
      priority=2

os.remove('{0}/sumstats_{1}_{2}.xls'.format(dir,brand_name,date_time))



def dfToJson(df):
    print(df.to_json(orient='index'))

print()

dfToJson(final_grouping_clustering_df)
