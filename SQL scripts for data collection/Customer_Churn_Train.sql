-------------------- CUSTOMER CHURN TRAINING SCRIPT----------------------------

/*Get 6 months(Nov 2022 - April 2023) USSD transactions count*/
drop table customer_churn_ussd_6months_transactions

select account_number, 'USSD' Channel,  11 mnth, count(*) total_mnth_txn, max(cast(trandate as date)) last_tran_date
into customer_churn_ussd_6months_transactions
from ussd_transactions_202211
where response_code = 00 and transaction_amount > 0
group by account_number
union
select account_number, 'USSD' Channel,  3 mnth, count(*) total_mnth_txn, max(cast(trandate as date)) last_tran_date
from ussd_transactions_202303
where response_code = 00 and transaction_amount > 0
group by account_number
union
select account_number, 'USSD' Channel,  1 mnth, count(*) total_mnth_txn, max(cast(trandate as date)) last_tran_date
from ussd_transactions_202301
where response_code = 00 and transaction_amount > 0
group by account_number
union
select account_number, 'USSD' Channel,  2 mnth, count(*) total_mnth_txn, max(cast(trandate as date)) last_tran_date
from ussd_transactions_202302
where response_code = 00 and transaction_amount > 0
group by account_number
union
select account_number, 'USSD' Channel,  3 mnth, count(*) total_mnth_txn, max(cast(trandate as date)) last_tran_date
from ussd_transactions_202303
where response_code = 00 and transaction_amount > 0
group by account_number
union
select account_number, 'USSD' Channel,  4 mnth, count(*) total_mnth_txn, max(cast(trandate as date)) last_tran_date
from ussd_transactions_202304
where response_code = 00 and transaction_amount > 0
group by account_number

create index ind_account_number on customer_churn_ussd_6months_transactions (account_number)

/*Get each months transaction count for USSD*/
drop table customer_churn_ussd_6months_frequency_count

select distinct account_number, channel,
	max(case when mnth = 11 then total_mnth_txn else 0 end) over (partition by account_number) cnt_nov,
	max(case when mnth = 12 then total_mnth_txn else 0 end) over (partition by account_number) cnt_dec,
	max(case when mnth = 1 then total_mnth_txn else 0 end) over (partition by account_number) cnt_jan,
	max(case when mnth = 2 then total_mnth_txn else 0 end) over (partition by account_number) cnt_feb,
	max(case when mnth = 3 then total_mnth_txn else 0 end) over (partition by account_number) cnt_mar,
	max(case when mnth = 4 then total_mnth_txn else 0 end) over (partition by account_number) cnt_apr
	into customer_churn_ussd_6months_frequency_count
from customer_churn_ussd_6months_transactions


/*Get 6 months(Nov 2022 - April 2023) Retail Internet Banking transactions count*/
drop table customer_churn_rib_6months_transactions

select account_number, 'RIB' Channel,  11 mnth, count(*) total_mnth_txn, max(cast(datecreated as date)) last_tran_date
into customer_churn_rib_6months_transactions
from internet_banking_transactions_202211
where response_code = 00 and transaction_amount > 0
group by account_number
union
select account_number, 'RIB' Channel,  12 mnth, count(*) total_mnth_txn, max(cast(datecreated as date)) last_tran_date
from internet_banking_transactions_202212
where response_code = 00 and transaction_amount > 0
group by account_number
union
select account_number, 'RIB' Channel,  1 mnth, count(*) total_mnth_txn, max(cast(datecreated as date)) last_tran_date
from internet_banking_transactions_202301
where response_code = 00 and transaction_amount > 0
group by account_number
union
select account_number, 'RIB' Channel,  2 mnth, count(*) total_mnth_txn, max(cast(datecreated as date)) last_tran_date
from internet_banking_transactions_202302
where response_code = 00 and transaction_amount > 0
group by account_number
union
select account_number, 'RIB' Channel,  3 mnth, count(*) total_mnth_txn, max(cast(datecreated as date)) last_tran_date
from internet_banking_transactions_202303
where response_code = 00 and transaction_amount > 0
group by account_number
union
select account_number, 'RIB' Channel,  4 mnth, count(*) total_mnth_txn, max(cast(datecreated as date)) last_tran_date
from internet_banking_transactions_202304
where response_code = 00 and transaction_amount > 0
group by account_number

create index ind_account_number on customer_churn_rib_6months_transactions (account_number)

/*Get each months transaction count for RIB*/
drop table customer_churn_rib_6months_frequency_count

select distinct account_number, channel,
	max(case when mnth = 11 then total_mnth_txn else 0 end) over (partition by account_number) cnt_nov,
	max(case when mnth = 12 then total_mnth_txn else 0 end) over (partition by account_number) cnt_dec,
	max(case when mnth = 1 then total_mnth_txn else 0 end) over (partition by account_number) cnt_jan,
	max(case when mnth = 2 then total_mnth_txn else 0 end) over (partition by account_number) cnt_feb,
	max(case when mnth = 3 then total_mnth_txn else 0 end) over (partition by account_number) cnt_mar,
	max(case when mnth = 4 then total_mnth_txn else 0 end) over (partition by account_number) cnt_apr
	into customer_churn_rib_6months_frequency_count
from customer_churn_rib_6months_transactions


/*Get 6 months(Nov 2022 - April 2023) NEWMOBILE transactions count*/
drop table customer_churn_mobile_6months_transactions

select account_number, 'NEWMOBILE' Channel, 11 mnth, count(*) total_mnth_txn, max(cast(datecreated as date)) last_tran_date
into customer_churn_mobile_6months_transactions
from new_mobile_transactions_202211
where response_code = 00 and transaction_amount > 0
group by account_number
union
select account_number, 'NEWMOBILE' Channel, 12 mnth, count(*) total_mnth_txn, max(cast(datecreated as date)) last_tran_date
from new_mobile_transactions_202212
where response_code = 00 and transaction_amount > 0
group by account_number
union
select account_number, 'NEWMOBILE' Channel, 1 mnth, count(*) total_mnth_txn, max(cast(datecreated as date)) last_tran_date
from new_mobile_transactions_202301
where response_code = 00 and transaction_amount > 0
group by account_number
union
select account_number, 'NEWMOBILE' Channel, 2 mnth, count(*) total_mnth_txn, max(cast(datecreated as date)) last_tran_date
from new_mobile_transactions_202302
where response_code = 00 and transaction_amount > 0
group by account_number
union
select account_number, 'NEWMOBILE' Channel, 3 mnth, count(*) total_mnth_txn, max(cast(datecreated as date)) last_tran_date
from new_mobile_transactions_202303
where response_code = 00 and transaction_amount > 0
group by account_number
union
select account_number, 'NEWMOBILE' Channel, 4 mnth, count(*) total_mnth_txn, max(cast(datecreated as date)) last_tran_date
from new_mobile_transactions_202304
where response_code = 00 and transaction_amount > 0
group by account_number

create index ind_account_number on customer_churn_mobile_6months_transactions (account_number)

/*Get each months transaction count for NEWMOBILE*/
drop table customer_churn_mobile_6months_frequency_count

select distinct account_number, channel,
	max(case when mnth = 11 then total_mnth_txn else 0 end) over (partition by account_number) cnt_nov,
	max(case when mnth = 12 then total_mnth_txn else 0 end) over (partition by account_number) cnt_dec,
	max(case when mnth = 1 then total_mnth_txn else 0 end) over (partition by account_number) cnt_jan,
	max(case when mnth = 2 then total_mnth_txn else 0 end) over (partition by account_number) cnt_feb,
	max(case when mnth = 3 then total_mnth_txn else 0 end) over (partition by account_number) cnt_mar,
	max(case when mnth = 4 then total_mnth_txn else 0 end) over (partition by account_number) cnt_apr
	into customer_churn_mobile_6months_frequency_count
from customer_churn_mobile_6months_transactions

/*Get 6 months(May 2022 - April 2023) CARD transactions count*/
drop table customer_churn_card_6months_transactions

select from_account_id account_number, 'CARD' Channel, 11 mnth, count(*) total_mnth_txn, max(cast(datetime_req as date)) last_tran_date
into customer_churn_card_6months_transactions
from atm_transactions_202211
where len(from_account_id) = 10 and response_code = '00' and tran_amount_req > 0
group by from_account_id
union
select from_account_id account_number, 'CARD' Channel, 12 mnth, count(*) total_mnth_txn, max(cast(datetime_req as date)) last_tran_date
from atm_transactions_202212
where len(from_account_id) = 10 and response_code = '00' and tran_amount_req > 0
group by from_account_id
union
select from_account_id account_number, 'CARD' Channel, 1 mnth, count(*) total_mnth_txn, max(cast(datetime_req as date)) last_tran_date
from atm_transactions_202301
where len(from_account_id) = 10 and response_code = '00' and tran_amount_req > 0
group by from_account_id
union
select from_account_id account_number, 'CARD' Channel, 2 mnth, count(*) total_mnth_txn, max(cast(datetime_req as date)) last_tran_date
from atm_transactions_202302
where len(from_account_id) = 10 and response_code = '00' and tran_amount_req > 0
group by from_account_id
union
select from_account_id account_number, 'CARD' Channel, 3 mnth, count(*) total_mnth_txn, max(cast(datetime_req as date)) last_tran_date
from atm_transactions_202303
where len(from_account_id) = 10 and response_code = '00' and tran_amount_req > 0
group by from_account_id
union
select from_account_id account_number, 'CARD' Channel, 4 mnth, count(*) total_mnth_txn, max(cast(datetime_req as date)) last_tran_date 
from atm_transactions_202304
where len(from_account_id) = 10 and response_code = '00' and tran_amount_req > 0
group by from_account_id

create index ind_account_number on customer_churn_card_6months_transactions (account_number)

/*Get each months transaction count for CARD*/
drop table customer_churn_card_6months_frequency_count

select distinct account_number, channel,
	max(case when mnth = 11 then total_mnth_txn else 0 end) over (partition by account_number) cnt_nov,
	max(case when mnth = 12 then total_mnth_txn else 0 end) over (partition by account_number) cnt_dec,
	max(case when mnth = 1 then total_mnth_txn else 0 end) over (partition by account_number) cnt_jan,
	max(case when mnth = 2 then total_mnth_txn else 0 end) over (partition by account_number) cnt_feb,
	max(case when mnth = 3 then total_mnth_txn else 0 end) over (partition by account_number) cnt_mar,
	max(case when mnth = 4 then total_mnth_txn else 0 end) over (partition by account_number) cnt_apr
	into customer_churn_card_6months_frequency_count
from customer_churn_card_6months_transactions


/*Combine 6months transaction for the 4 channels*/
drop table customer_churn_channels_6months_frequency_count

select * into customer_churn_channels_6months_frequency_count
from customer_churn_ussd_6months_frequency_count
union 
select * from customer_churn_rib_6months_frequency_count 
union
select * from customer_churn_mobile_6months_frequency_count
union 
select * from customer_churn_card_6months_frequency_count

/*Get customer_id and customer demographic data*/
drop table customer_churn_channels_and_demography_6months_data

select b.customer_id, a.*, region_name, datediff(year, date_of_birth, getdate()) age, customer_status, sex, customer_segment, fn_get_generation(year(date_of_birth)) Generation,
case when customer_occupation in ('CIVIL SERVANT','PUBLIC SERVANTS','DEFENCE','POLICEMEN','MEMBER OF LEGISLATIVE ASSEMBLY','MEMBER OF PARLIAMENT','COUNCILLOR') THEN 'GOVERNMENT'
WHEN customer_occupation IN ('ENTREPRENEUR','BUSINESS','CONTRACTOR') THEN 'BUSINESS'
WHEN customer_occupation IN ('ARTIST','ENTERTAINER','MEDIA','MUSICIAN','ACTOR/ACTRESS') THEN 'ENTERTAINER'
WHEN customer_occupation IN ('NURSE','DOCTOR','MEDICAL REPRESENTATIVE') THEN 'MEDICAL'
WHEN customer_occupation IN ('BROADCASTING','JOURNALIST','MEDIA','WRITER') THEN 'MEDIA'
WHEN customer_occupation IN ('VEGETABLE VENDOR','AGRICULTURIST','FARMING') THEN 'FARMING'
WHEN customer_occupation IN ('TEACHER','LECTURER') THEN 'EDUCATION'
WHEN customer_occupation IN ('LAWYERS','LEGAL PRACTICE','JUDGE') THEN 'LEGAL'
WHEN customer_occupation IN ('BANKING','FINANCIAL SERVICES') THEN 'FINANCE'
WHEN customer_occupation IN ('UNKNOWN') THEN 'UNKNOWN'
WHEN customer_occupation IN ('STUDENT') THEN 'STUDENT'
WHEN customer_occupation IN ('TRADER') THEN 'TRADER'
WHEN customer_occupation IN ('SERVICES') THEN 'SERVICES'
WHEN customer_occupation IN ('ARTISANS') THEN 'ARTISANS'
WHEN customer_occupation IN ('ENGINEER') THEN 'ENGINEER'
WHEN customer_occupation IN ('DRIVER') THEN 'DRIVER'
WHEN customer_occupation IN ('CLERGY') THEN 'CLERGY'
WHEN customer_occupation IN ('PRIVATE SECTOR EMPLOYEE','OTHER PSU EMPLOYEE') THEN 'PRIVATE SECTOR EMPLOYEE'
WHEN customer_occupation IN ('ARTISANS') THEN 'ARTISANS'
ELSE 'OTHERS' END occupation
into customer_churn_channels_and_demography_6months_data
from customer_churn_channels_6months_frequency_count a
join customer_accounts_table b
on b.account_number = a.account_number
join customer_segmentation_table c
on b.customer_id = c.customer_id


/*Make data by customer 6months transaction for the 4 channels*/
drop table customer_churn_by_customer

with Channeltxns1 as (
select *,
max(case when channel = 'CARD' then 1 else 0 end) over(partition by customer_id) card , 
max(case when channel = 'ussd' then 1 else 0 end) over(partition by customer_id) ussd,
max(case when channel = 'rib' then 1 else 0 end) over(partition by customer_id) rib,
max(case when channel = 'newmobile' then 1 else 0 end) over(partition by customer_id) newmobile
from customer_churn_channels_and_demography_6months_data
)

select distinct customer_id, sum(cnt_nov)  over (partition by customer_id) cnt_nov, 
sum(cnt_dec)  over (partition by customer_id) cnt_dec, 
sum(cnt_jan)  over (partition by customer_id) cnt_jan, 
sum(cnt_feb)  over (partition by customer_id) cnt_feb, 
sum(cnt_mar)  over (partition by customer_id) cnt_mar, 
sum(cnt_apr)  over (partition by customer_id) cnt_apr, 
region_name, age, customer_status, sex, customer_segment, generation, occupation, card, ussd, rib, newmobile
into customer_churn_by_customer
from Channeltxns1