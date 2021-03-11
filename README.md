### Electricity Authority 2019 UTS analysis (Market Monitoring)

Market Monitoring analysis of the 2019 UTS.

All data analysis can be found in the python notebook: UTS2019.ipynb

https://github.com/ElectricityAuthority/2019UTS/blob/master/UTS2019.ipynb

Spill data from Contact and Meridian, as well as reconciled generation data for LSI hydro generators is found in the data directory.

vSPD simulations results are saved as python dictionaries of pandas dataframes in the compressed parquet file format.  These reside in vSPD_data dir.

An additional directory has been added with excel format vSPD output.  Due to the size of vSPD result files, we provide only the resulting nodal spot price files for the different vSPD runs, along with the basecase vSPD run.  Also provided is a basecase load data file with nodal load data for the simulation period, along with the daily system load cost reported by vSPD for each of the simulation runs.

Contents of https://github.com/ElectricityAuthority/2019UTS/tree/master/vSPD_data/EXCEL_FILES

  
  - vSPD_UTS_daily_system_load_cost_reults.xlsx (daily System Load Cost reported by vSPD)
  - vSPD_UTS_basecase_load.xlsx (basecase load)
  - vSPD_UTS_basecase_spotprice.xlsx (basecase Nodal spot price)
  - vSPD_UTS_LSI_offers_001MWh_spotprice.xlsx (Nodal spot price at all nodes when LSI generation offerred at $0.01/MWh)
  - vSPD_UTS_LSI_offers_6pt35_spotprice.xlsx (Nodal spot price at all nodes when LSI generation offerred at $6.35/MWh)
  - vSPD_UTS_LSI_offers_10MWh_spotprice.xlsx (Nodal spot price at all nodes when LSI generation offerred at $10/MWh)
  - vSPD_UTS_LSI_offers_20MWh_spotprice.xlsx (Nodal spot price at all nodes when LSI generation offerred at $20/MWh)
  - vSPD_UTS_LSI_offers_30MWh_spotprice.xlsx (Nodal spot price at all nodes when LSI generation offerred at $30/MWh)

#### UPDATE (2020/7/16)

We have added an additional vSPD analysis for November 2019 where we have adjusted offers for periods when Roxburgh and Clyde hydro stations were both spilling. 
We have not tested if the outcome of this simulation was possible hydrologically.  

The November analysis can be found here:

https://github.com/ElectricityAuthority/2019UTS/blob/master/UTS2019_NOV_ROX_CYD_SPILL.ipynb

#### UPDATE Actions to Correct (2021/3/11)

We have added data and analysis used in the consulation paper for the proposed Actions to Correct the UTS - here:

https://github.com/ElectricityAuthority/2019UTS/blob/24b0105df433462ab67cbdbde5724afa698f2d64/ATC/ATC.ipynb

