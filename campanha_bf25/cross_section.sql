with
trat as (
	select 
		a.customerdocument,
		1 as tratamento,
		b.offerid,
		if(b.detailedcustomerdocumenttype = 'MEI',1,0) as mei,
		b.clustermaturidade,
		case 
			when lower(b.perfilsocietario) = 'socio unico' then 0
			when lower(b.perfilsocietario) = 'multisocio'  then 1
			end as multisocio,
		cast(b.isacquireractive               as int) as acquirer,
		cast(b.isbankingactive                as int) as banking,
		cast(b.isstoneactive                  as int) as stone,
		cast(b.isravauto                      as int) as ravauto,
		cast(b.clientejaaceitouofertaanterior as int) as preaceite,
		b.rating,
		d.tpv_total,
		c.expectedrate,
		b.interestrate,
		b.creditlimit,
		b.creditamount,
		coalesce(b.dataaceitacaooferta, '2030-01-01') as dataaceitacaooferta,
		b.datasubidaoferta,
    case
      when b.dataaceitacaooferta    is not null then b.dataaceitacaooferta
      when b.datacancelamentooferta is not null then b.datacancelamentooferta
      when b.dataexpiracaooferta    is not null then b.dataexpiracaooferta
      end as datafimoferta,
		e.policy,
	from `dataplatform-prd.credit_negocios.20251016_testelasticidade_rct` a
	left join `dataplatform-prd.credit_business_intelligence.tbl_business_offer_tracker` b
	on a.customerdocument = b.customerdocument
	left join (
		select * from `dataplatform-prd.credit_business_intelligence.tbl_retreino_output_docs`
		qualify row_number() over(partition by customerdocument order by processingdate desc) = 1
	) c
	on b.customerdocument = c.customerdocument
	left join (
		select document, tpv_total
		from `dataplatform-prd.economic_research.sbca_monthly_metrics`
		qualify row_number() over(partition by document order by reference_month desc) = 1
	) d
	on b.customerdocument = d.document
	left join (
		select v1.id as offerid, v2.pricingpolicyid as policy
		from `dataplatform-prd.credit_negotiations.view_offer` v1
		inner join `dataplatform-prd.credit_commercial.view_commercial_analysis` v2
		on v1.referenceid = v2.id
	) e
	on b.offerid = e.offerid
	where b.datasubidaoferta >= '2025-10-20'
		and b.datasubidaoferta <  '2025-10-28'
		and a.arm = 'T'
),
ctrl as (
	select 
		a.customerdocument,
		0 as tratamento,
		b.offerid,
		if(b.detailedcustomerdocumenttype = 'MEI',1,0) as mei,
		b.clustermaturidade,
		case 
			when lower(b.perfilsocietario) = 'socio unico' then 0
			when lower(b.perfilsocietario) = 'multisocio'  then 1
			end as multisocio,
		cast(b.isacquireractive               as int) as acquirer,
		cast(b.isbankingactive                as int) as banking,
		cast(b.isstoneactive                  as int) as stone,
		cast(b.isravauto                      as int) as ravauto,
		cast(b.clientejaaceitouofertaanterior as int) as preaceite,
		b.rating,
		d.tpv_total,
		c.expectedrate,
		b.interestrate,
		b.creditlimit,
		b.creditamount,
		coalesce(b.dataaceitacaooferta, '2030-01-01') as dataaceitacaooferta,
		b.datasubidaoferta,
    case
      when b.dataaceitacaooferta    is not null then b.dataaceitacaooferta
      when b.datacancelamentooferta is not null then b.datacancelamentooferta
      when b.dataexpiracaooferta    is not null then b.dataexpiracaooferta
      end as datafimoferta,
		e.policy
	from `dataplatform-prd.credit_negocios.20251016_testelasticidade_rct` a
	left join `dataplatform-prd.credit_business_intelligence.tbl_business_offer_tracker` b
	on a.customerdocument = b.customerdocument
	left join (
		select * from `dataplatform-prd.credit_business_intelligence.tbl_retreino_output_docs`
		qualify row_number() over(partition by customerdocument order by processingdate desc) = 1
	) c
	on b.customerdocument = c.customerdocument
	left join (
		select document, tpv_total
		from `dataplatform-prd.economic_research.sbca_monthly_metrics`
		qualify row_number() over(partition by document order by reference_month desc) = 1
	) d
	on b.customerdocument = d.document
	left join (
		select v1.id as offerid, v2.pricingpolicyid as policy
		from `dataplatform-prd.credit_negotiations.view_offer` v1
		inner join `dataplatform-prd.credit_commercial.view_commercial_analysis` v2
		on v1.referenceid = v2.id
	) e
	on b.offerid = e.offerid
	where b.datasubidaoferta >= '2025-10-15'
		and b.datasubidaoferta <  '2025-10-28'
		and a.arm = 'C'
),
main as (
  select * from trat
  union all
  select * from ctrl
),
contagio as (
  select customerdocument
  from main
  group by 1
  having max(if((tratamento = 0 and policy = 'cg_v107') or (tratamento = 1 and policy != 'cg_v107'),1,0)) = 0
),
rosaventos as (
  select group_id, compass_rose_group_working_capital as grupo_rosaventos, compass_rose_reference_date
  from `dataplatform-prd.credit_business_intelligence.entity_pricing_models`
  where compass_rose_group_working_capital in ('Grupo C','Grupo D')
  qualify row_number() over(partition by group_id order by compass_rose_reference_date desc) = 1
)

select
  main.*,
  if(date_diff(dataaceitacaooferta,datasubidaoferta,day) <= 7,1,0) as aceite,
  grupo_rosaventos,
  if(contagio.customerdocument is null, 1, 0) as defier
from main
left join contagio
on main.customerdocument = contagio.customerdocument
left join rosaventos
on if(length(main.customerdocument)=11,main.customerdocument,substr(main.customerdocument,1,8)) = rosaventos.group_id
qualify row_number() over(partition by main.customerdocument order by aceite desc, dataaceitacaooferta asc, datasubidaoferta desc) = 1
