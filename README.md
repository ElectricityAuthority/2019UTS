### Electricity Authority 2019 UTS analysis (Market Monitoring)

Market Monitoring analysis of the 2019 UTS.

All data analysis can be found in the python notebook: UTS2019.ipynb

https://github.com/ElectricityAuthority/2019UTS/blob/master/UTS2019.ipynb

Spill data from Contact and Meridian, as well as reconciled generation data for LSI hydro generators is found in the data directory.

vSPD simulations results are saved as python dictionaries of pandas dataframes in the compressed parquet file format.  These reside in vSPD_data dir.

An additional directory has been added with excel format vSPD output.  Due to the size of vSPD result files, we provide only the resulting nodal spot price files for the different vSPD runs, along with the basecase vSPD run.  ALso provided is a basecase load file which provides nodal load data for the simulation perod and a daily system load cost which is produced by vSPD.

Contents of vSPD_data/EXCEL_FILES/
  
  - vSPD_UTS_daily_system_load_cost_reults.xlsx (daily System Load Cost reported by vSPD)
  - vSPD_UTS_basecase_load.xlsx (basecase load)
  - vSPD_UTS_basecase_spotprice.xlsx (basecase Nodal spot price)
  - vSPD_UTS_LSI_offers_001MWh_spotprice.xlsx (Nodal spot price at all nodes when LSI generation offerred at $0.01/MWh)
  - vSPD_UTS_LSI_offers_6pt35_spotprice.xlsx (Nodal spot price at all nodes when LSI generation offerred at $6.35/MWh)
  - vSPD_UTS_LSI_offers_10MWh_spotprice.xlsx (Nodal spot price at all nodes when LSI generation offerred at $10/MWh)
  - vSPD_UTS_LSI_offers_20MWh_spotprice.xlsx (Nodal spot price at all nodes when LSI generation offerred at $20/MWh)
  - vSPD_UTS_LSI_offers_30MWh_spotprice.xlsx (Nodal spot price at all nodes when LSI generation offerred at $30/MWh)

