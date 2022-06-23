def create_table_dim(schema):
  query = f"""
  CREATE TABLE IF NOT EXISTS {schema}.dim_date (
    date text primary key,
    day text,
    month text,
    year text);
  CREATE TABLE IF NOT EXISTS {schema}.dim_product (
      product_id int primary key,
      product_name text,
      product_category text);
  CREATE TABLE IF NOT EXISTS {schema}.dim_status (
      id SERIAL primary key,
      status_name text);
  """

  return query


def create_table_fact(schema):
  query = f"""
  CREATE TABLE IF NOT EXISTS {schema}.fact_product_complete_monthly (
    id SERIAL,
    product_id int,
    status_id int,
    month text,
    total_item bigint,
    total_sale_price bigint);
  """

  return query