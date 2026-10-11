from src.features.Sales_Performance.jobs.sales_bronze_ingestion_job import SalesBronzeIngestionJob

job = SalesBronzeIngestionJob()
results = job.run(mode='full')
print(results)
