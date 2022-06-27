import numpy as np
import pandas as pd
import arcpy
import timestamp
from datetime import datetime
import os
import dbf

dir='D:/atrin/atrin/clustering'
classes=["A","B"]
TargetAges=['WOM_BTW_0_','WOM_BTW_5_','WOM_BTW_10','WOM_BTW_15','WOM_BTW_20','WOM_BTW_25','WOM_BTW_30','WOM_BTW_35','WOM_BTW_40','WOM_BTW_45','WOM_BTW_50','WOM_BTW_55','WOM_BTW_60','WOM_BTW_65','WOM_BTW_70']
TargetAgess=['WOM_BTW_0_','WOM_BTW_5_','WOM_BTW_10','WOM_BTW_15','WOM_BTW_20','WOM_BTW_25','WOM_BTW_30','WOM_BTW_35','WOM_BTW_40','WOM_BTW_45','WOM_BTW_50','WOM_BTW_55','WOM_BTW_60','WOM_BTW_65','WOM_BTW_70','zone97','SocialClass']
TargetAgesss=['TA','WOM_BTW_0_','WOM_BTW_5_','WOM_BTW_10','WOM_BTW_15','WOM_BTW_20','WOM_BTW_25','WOM_BTW_30','WOM_BTW_35','WOM_BTW_40','WOM_BTW_45','WOM_BTW_50','WOM_BTW_55','WOM_BTW_60','WOM_BTW_65','WOM_BTW_70','zone97','SocialClass']
TargetAgessss=['SOURCE_ID','WOM_BTW_0_','WOM_BTW_5_','WOM_BTW_10','WOM_BTW_15','WOM_BTW_20','WOM_BTW_25','WOM_BTW_30','WOM_BTW_35','WOM_BTW_40','WOM_BTW_45','WOM_BTW_50','WOM_BTW_55','WOM_BTW_60','WOM_BTW_65','WOM_BTW_70','zone97','SocialClass']
brand_name='BrandName'



now = datetime.now()
date_time = now.strftime("%Y%m%d_%H%M%S")
arcpy.env.workspace = "{0}".format(dir)

df = pd.read_csv (r'TAZ_BlockData_Aggregate.csv')
#calculating the number of targetAudiences
df['TA']= df[TargetAges].sum(axis=1)

#socialclass filtering
filt=df['SocialClass'].isin(classes)
df_TargetSocialClass=df[filt]
df2=df_TargetSocialClass[TargetAgesss]
df2.to_csv('{0}/SC_TA_{1}_{2}.csv'.format(dir,brand_name,date_time))

zones=df['zones']
TargetAudience=pd.merge(zones,df2,left_on='zones',right_on='zone97',how='outer')
TargetAudience['TA'] = TargetAudience['TA'].fillna(0)
TargetAudience.to_csv('TargetAudience.csv')



TargetAudience=pd.read_csv (r'TargetAudience.csv')
arcpy.MakeFeatureLayer_management("clustering.gdb/TAZ", "temmp")
arcpy.AddJoin_management("temmp", "zone97", 'TargetAudience.csv', "zones")
arcpy.CopyFeatures_management("temmp", "clustering.gdb/TAZ_join")
TAZ_join='clustering.gdb/TAZ_join'     #filter_final_featureclass


cluster='clustering.gdb/cluster'    #HH-HL-LL-LH
arcpy.stats.ClustersOutliers(TAZ_join, 'TargetAudience_csv_TA', cluster, 'INVERSE_DISTANCE', 'EUCLIDEAN_DISTANCE', 'NONE')


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

###################################################################     adddata_to-firstcluster(Moran)
cluster_xls='cluster_HHHL.xls'
arcpy.conversion.TableToExcel(cluster_HHHL, cluster_xls)

dfC = pd.read_excel('{0}/cluster_HHHL.xls'.format(dir))
dfTAZ=pd.read_csv('{0}/TAZ.csv'.format(dir))
final=pd.merge(dfC,dfTAZ,left_on='SOURCE_ID',right_on='OBJECTID',how='outer')
finall=final[['SOURCE_ID','TAZ_zone97','COType','TargetAudi']]
finall.to_csv('{0}/clusterT_{1}_{2}.csv'.format(dir,brand_name,date_time))
filt=finall['COType'].isin(['HH','HL'])
finalll=finall[filt]
finalll.to_csv('{0}/cluster_{1}_{2}.csv'.format(dir,brand_name,date_time))

os.remove('TargetAudience.csv')
os.remove('cluster_HHHL.xls')
arcpy.env.workspace = "{0}/clustering.gdb".format(dir)
arcpy.Delete_management('TAZ_join',"")
arcpy.Delete_management('cluster',"")


listt=finalll['TAZ_zone97']
filtt=df['zone97'].isin(listt)
dff=df[filtt]
finalll_addcoloum=pd.merge(finalll,dff[TargetAgess],left_on='TAZ_zone97',right_on='zone97',how='outer')
finalll_addcoloum.to_csv('{0}/cluster_data_{1}_{2}.csv'.format(dir,brand_name,date_time))

#data_csv_final
################################################################################################################
# arcpy.Delete_management('temmp')
finalll_addcoloumn=finalll_addcoloum[TargetAgessss]
print (finalll_addcoloumn)
finalll_addcoloumn.columns = finalll_addcoloumn.columns.str.replace('SOURCE_ID', 'SOURCEIDD')

finalll_addcoloumn.to_csv('{0}/join.csv'.format(dir,brand_name,date_time))

arcpy.MakeFeatureLayer_management("{0}/cluster_HHHL.shp".format(dir), "temmpp")   #11ta field dare
arcpy.AddJoin_management("temmpp", "SOURCE_ID", '{0}/join.csv'.format(dir,brand_name,date_time), "SOURCEIDD")
arcpy.CopyFeatures_management("temmpp", "{0}/cluster_data_{1}_{2}.shp".format(dir,brand_name,date_time))
arcpy.Delete_management('temmpp')
os.remove('{0}/join.csv'.format(dir,brand_name,date_time))


#################################################################################################################
#spatial join with 172nahie
input_grouping='{0}/cluster_data_{1}_{2}.shp'.format(dir,brand_name,date_time)
# region=spatial join
output_grouping='{0}/Grouping_data_{1}_{2}.shp'.format(dir,brand_name,date_time)
Output_Report_File='{0}/Grouping_report_{1}_{2}.pdf'.format(dir,brand_name,date_time)
fields = arcpy.ListFields(input_grouping)
Analysis_Fields=fields[-9:-3]
print(Analysis_Fields)
arcpy.stats.GroupingAnalysis(input_grouping, "cluster_HH", output_grouping, "2", Analysis_Fields, 
                             "NO_SPATIAL_CONSTRAINT", "EUCLIDEAN","","","FIND_SEED_LOCATIONS","",Output_Report_File,
                             "DO_NOT_EVALUATE")                            







# inputtt="C:/Users/arefeh.alaee/Documents/ArcGIS/Default.gdb/pop_hhhl_GroupingAnalysis33"
# outputtt="C:/Users/arefeh.alaee/Documents/ArcGIS/Default.gdb/pop_hhhl_GroupingAnalysis33_grouping"
# arcpy.gapro.GroupByProximity(inputtt, outputtt, "NEAR_PLANAR")




# TargetAudience.csv
# temmp
# clustering.gdb/TAZ_join
# clustering.gdb/cluster
# cluster.xls
##############################################################################
# from esda.moran import Moran
# from libpysal.weights import Queen, KNN
# import seaborn 
# import pandas
# import geopandas 
# import numpy
# import matplotlib.pyplot as plt

# import matplotlib.pyplot as plt  # Graphics
# from matplotlib import colors
# import seaborn                   # Graphics
# # import geopandas                 # Spatial data manipulation
# import pandas                    # Tabular data manipulation
# import rioxarray                 # Surface data manipulation
# import xarray                    # Surface data manipulation
# from pysal.explore import esda   # Exploratory Spatial analytics
# from pysal.lib import weights    # Spatial weights
# import contextily                # Background tiles



# # lisa = esda.moran.Moran_Local(db['Pct_Leave'], w)
# print ('hello')

# import seaborn as sns
# import pandas as pd
# import esda
# # from pysal.lib import weights
# # from splot.esda import moran_scatterplot, lisa_cluster, plot_local_autocorrelation
# # import geopandas as gpd 
# import numpy as np
# # import contextily as ctx
# import matplotlib.pyplot as plt



