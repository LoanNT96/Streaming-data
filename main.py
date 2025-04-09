import uuid
import pandas as pd
import pyarrow as pa
import random
from datetime import datetime, date, timedelta
import pyarrow.parquet as pq
from pyspark.sql import SparkSession

# Define desired row group size (e.g., 10000 rows)
row_group_size = 10000
BATCH_SIZE = 10
MAX_RANGE = 10

schema = pa.schema([
    pa.field("our_reference", pa.string()),
    pa.field("amt_lcy", pa.int32()),
    pa.field("booking_date", pa.date32()),
    pa.field("transaction_code", pa.string()),
    pa.field("company_code", pa.string()),
    pa.field("stmt_entry_id", pa.string()),
    pa.field("currency", pa.string()),
    pa.field("system_id", pa.int32())
])

df1 = pq.read_table('parquet_data', schema=schema).to_pandas()

def random_date():
    start_date = date(2000, 1, 1)
    end_date = date(2024, 4, 20)
    time_between_days = end_date - start_date
    days_until_random_date = random.randrange(time_between_days.days + 1)
    return start_date + timedelta(days=days_until_random_date)


def generate_parquet_table():
    trans_code = ['420', '434', '750', '751', '752', '753', '408']
    data = []

    for i in range(BATCH_SIZE):
        order = random.randrange(1, MAX_RANGE)

        data.append({
            "our_reference": "LD_{}".format(order),
            "amt_lcy": random.randrange(1, MAX_RANGE),
            "booking_date": random_date(),
            "transaction_code": random.choice(trans_code),
            "company_code": "COMPANY_CODE_{}".format(order),
            "stmt_entry_id": datetime.now().strftime('%Y%m-%d%H-%M%S-') + str(uuid.uuid4()),
            "currency": "VND",
            "system_id": random.randrange(1, 1000)
        })

    table = pa.Table.from_pylist(data, schema=schema)
    
    writer = pa.parquet.ParquetWriter('parquet_data_onS3', schema)
    writer.write_table(table)
    writer.close()



def read_pyarrow():
    print(df1.where(df1['our_reference'] == 'LD_420').count())


if __name__ == '__main__':
    generate_parquet_table()