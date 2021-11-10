import pandas as pd
from pandasql import sqldf


def load_gsc_file(file):
    return pd.read_csv(file)


def query(audience_dataset, transaction_dataset, query_name, query_sql):
    audience = load_gsc_file(audience_dataset)
    tranx = load_gsc_file(transaction_dataset)
    print("############################################################")
    print("---------- transaction dataset: ")
    print(tranx)
    print("---------- audience dataset: ")
    print(audience)
    print("************************************************************")
    print("{} results: ".format(query_name))
    print(sqldf(query_sql, locals()))


def main():
    audience_dataset = "gs://mart-test/audience.csv"
    transaction_dataset = "gs://mart-test/transaction.csv"
    query_name = "Get customers who payed for productId=20003341 total amount >= 30.0 during 2018-11-08 to 2018-11-10"
    query_sql = '''
    select target.customerId as customerId, au.ppid as ppid, target.productId as productId, target.total_amount as amount
    from
    (select customerId, productId, SUM(amount) as total_amount from tranx 
    where  productId = 20003341 and
    tranxDate >= '2018-11-08' and 
    tranxDate <= '2018-11-10' 
    group by customerId) target
    inner join audience au
    on au.customerId = target.customerId
    where target.total_amount > 30.0;
    '''
    query(audience_dataset, transaction_dataset, query_name, query_sql)


if __name__ == "__main__":
    main()
