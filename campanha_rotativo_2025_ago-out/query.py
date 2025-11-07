# Pacotes
from google.cloud import bigquery
import pandas as pd
import csv

# Projeto
client = bigquery.Client(project='credit-business-9fd5bf2eed53')

# Query
query = """
WITH
pn AS (
    SELECT 
        customerdocument,
        IF(LOWER(grupo) = 'rotativo - teste', 1, 0) AS tratamento,
        IF(LOWER(grupo) = 'rotativo - teste', taxa_teste, tx) AS interestrate,
        rating,
        CASE
            WHEN tpv_monthly_reference <  15000  THEN '0-15k'
            WHEN tpv_monthly_reference <  100000 THEN '15-100k'
            WHEN tpv_monthly_reference >= 100000 THEN '100k+'
            END AS tpvreference,
        customerdocumenttype AS doctype,
        referencedate
    FROM `dataplatform-prd.credit_negocios.2025_08_01_teste_rotativo_base_produtiva`
    CROSS JOIN UNNEST(
        GENERATE_DATE_ARRAY(DATE '2025-08-04', DATE '2025-10-31', INTERVAL 1 DAY)
    ) AS referencedate
)
SELECT
    pn.customerdocument,
    pn.referencedate,
    pn.tratamento,
    pn.rating,
    pn.tpvreference,
    pn.doctype,
    IF(b.customerdocument IS NOT NULL, 1, 0) AS survivor,
    pn.interestrate,
    IF(a.withdrawaldate IS NULL OR a.customerdocument IS NULL, 0, 1) AS saque,
    COALESCE(a.amount, 0) AS amount
FROM pn
LEFT JOIN `credit-business-9fd5bf2eed53.rascunhos.vw_withdrawal_revolving` a
ON pn.customerdocument = a.customerdocument
    AND pn.referencedate = a.withdrawaldate
LEFT JOIN `credit-business-9fd5bf2eed53.rascunhos.20251105_rotativo` b
ON pn.customerdocument = b.customerdocument
"""

df = client.query(query).to_dataframe()

# Save
with open('painel.csv','w',newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerows(df)
