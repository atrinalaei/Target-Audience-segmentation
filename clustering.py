import numpy as np
import pandas as pd
import arcpy
import timestamp
from datetime import datetime
import os

dir='D:/atrin/atrin/clustering'
classes=["A","B"]
TargetAges=['MAN_BTW_20','MAN_BTW_30']
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
df2=df_TargetSocialClass[['TA','zone97','SocialClass']]
df2.to_csv('{0}/SC_TA_{1}_{2}.csv'.format(dir,brand_name,date_time))

zones=df['zones']
TargetAudience=pd.merge(zones,df2,left_on='zones',right_on='zone97',how='outer')
TargetAudience['TA'] = TargetAudience['TA'].fillna(0)
TargetAudience.to_csv('TargetAudience.csv')


arcpy.env.workspace = "{0}".format(dir)
TargetAudience=pd.read_csv (r'TargetAudience.csv')
arcpy.MakeFeatureLayer_management("clustering.gdb/TAZ", "temmp")
arcpy.AddJoin_management("temmp", "zone97", 'TargetAudience.csv', "zones")
arcpy.CopyFeatures_management("temmp", "clustering.gdb/TAZ_join")
TAZ_join='clustering.gdb/TAZ_join'     #filter_final_featureclass


outputt='clustering.gdb/cluster'
arcpy.stats.ClustersOutliers(TAZ_join, 'TargetAudience_csv_TA', outputt, 'INVERSE_DISTANCE', 'EUCLIDEAN_DISTANCE', 'NONE')
outputt_xls='cluster.xls'
arcpy.conversion.TableToExcel(outputt, outputt_xls)

dfC = pd.read_excel('{0}/cluster.xls'.format(dir))
dfTAZ=pd.read_csv('{0}/TAZ.csv'.format(dir))
final=pd.merge(dfC,dfTAZ,left_on='SOURCE_ID',right_on='OBJECTID',how='outer')
finall=final[['TAZ_zone97','COType','TargetAudience_csv_TA']]
filt=finall['COType'].isin(['HH','HL'])
finalll=finall[filt]
finalll.to_csv('{0}/cluster_{1}_{2}.csv'.format(dir,brand_name,date_time))

os.remove('TargetAudience.csv')
os.remove('cluster.xls')
arcpy.env.workspace = "{0}/clustering.gdb".format(dir)
arcpy.Delete_management('TAZ_join',"")
arcpy.Delete_management('cluster',"")





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



