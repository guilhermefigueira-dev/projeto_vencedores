create or replace table `dataplatform-prd.credit_negocios.20251016_testelasticidade_pool` as 

with BASE AS (
SELECT 
  CustomerDocument, 
  Rating, 
  CustomerDocumentType, 
  MaturidadeAcumuladaCliente,
  (MaturidadeAcumuladaCliente + 
    DATE_DIFF(CURRENT_DATE(), DATE(DataSubidaOferta),DAY)
    ) as Maturidade_real,
  IsRAVAuto,
  CreditLimit,
  InterestRate

FROM `dataplatform-prd.credit_business_intelligence.tbl_business_offer_tracker`
WHERE 1=1 
--AND (MaturidadeAcumuladaCliente + (CURRENT_DATE() - DATE(DataSubidaOferta))) 
AND Status = 'Available'
AND OfferType = 'Automatica'),

porte as (
  SELECT 
    DISTINCT 
      document, 
      tpv_monthly_reference,
      CASE 
        WHEN tpv_monthly_reference < 10000 then 'P7'
        WHEN tpv_monthly_reference < 15000 then 'P15'
        WHEN tpv_monthly_reference < 30000 then 'P30'
        WHEN tpv_monthly_reference < 50000 then 'P50'        
        WHEN tpv_monthly_reference < 100000 then 'P100'
        WHEN tpv_monthly_reference < 200000 then 'P200'
        WHEN tpv_monthly_reference < 500000 then 'P500'
        WHEN tpv_monthly_reference < 2000000 then 'P2000'
        ELSE 'Large'
      end as Porte_tbl_pricing                                                
  FROM `dataplatform-prd.credit_negocios.Credit_Policy_Output_Daily_Update`
    WHERE 1 = 1
    QUALIFY ROW_NUMBER() OVER(PARTITION BY document ORDER BY created_At DESC) =1),

teste_henrique as (
SELECT
  Documento as document
FROM
  `dataplatform-prd.credit_business_intelligence.vw_clientes_rosa_ventos`
WHERE GrupoKGiro IN ('Grupo D', 'Grupo C') 
QUALIFY 
  ROW_NUMBER() OVER(PARTITION BY Documento ORDER BY ReferenceDate DESC) = 1
),

prop as (
  SELECT *
  FROM `dataplatform-prd.credit_business_intelligence.tbl_cgssci_output_docs`
  QUALIFY 
    ROW_NUMBER() OVER(PARTITION BY CustomerDocument ORDER BY ProcessingDate DESC) = 1
),

carteira as (
SELECT 
  gere.LoanId,
  SaldoProdutosConsiderado as Saldo_Kgiro_Stone,
  CustomerDocument   
FROM 
  `dataplatform-prd.credit_business_intelligence.tbl_replica_carteira_gerencial` gere
INNER JOIN `dataplatform-prd.credit_business_intelligence.tbl_business_offer_tracker` bot ON gere.LoanID = bot.LoanID
QUALIFY 
  ROW_NUMBER() OVER(PARTITION BY LoanId ORDER BY EffectiveDate DESC) = 1
)


SELECT 
  Base.CustomerDocument,
  BASE.Maturidade_real,
  ExpectedRate,
  CreditLimit,
  BASE.CustomerDocumentType
FROM BASE
INNER JOIN porte ON BASE.CustomerDOcument = porte.document
INNER JOIN PROP ON BASE.CustomerDOcument = PROP.CustomerDocument
INNER JOIN teste_henrique on BASE.CustomerDocument = teste_henrique.document
LEFT JOIN carteira ON BASE.CustomerDOcument = carteira.CustomerDocument

WHERE 1=1
AND carteira.CustomerDocument is null
and PROP.expectedrate > 0.01
AND Maturidade_real > 120
and rating >= 5
and creditLimit >= 20000
AND CustomerDocumentType = 'CNPJ'