## Project overview


<p align='center'>
     <img src="figs/end-to-end-pipeline.jpg"
     alt='Figure 1: Completed data pipeline'
     width=1000px/>
     <br>
     <em>Figure 1: A representation of the pipeline architecture to implement within this project.</em>
</p>

Data plays an extremely important role in the study of publicly listed companies and stock market analyses. As part of a small team tasked with migrating an on-premise application to the AWS cloud. This application shows valuable information about the share prices of thousands of companies to a group of professional stock market traders. The data is further used by data scientists to gain insights into the performance of various companies and predict share price changes that are fed back to the stock market traders.

As part of this migration, raw data, given in the form of company `csv` snapshot batches gathered by the on-premise application, needs to be moved to the AWS cloud. As an initial effort in this movement of data, the stock traders have homed in on the trading data of 1000 companies over the past ten years, and will only require a subset of the raw data to be migrated to the cloud in a batch-wise manner.  

With the above requirements, the aim here is to create a robust data pipeline that can extract, transform and load the `csv` data from the source data system to a SQL-based database within AWS. The final pipeline (represented in Figure 1) is built in [Airflow](https://airflow.apache.org/) and should utilise AWS services as its functional components. 

