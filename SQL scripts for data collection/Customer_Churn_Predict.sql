-------------------- CUSTOMER CHURN PREDICTION SCRIPT----------------------------
--CURRENT MONTH : MAY     PREDICTING FOR: JUNE

/*Get recent transaction count for USSD and join with previous 5 months transaction data*/
drop table if exists customer_churn_ussd_frequency_may

;With CurrentMonthUssd as (
select account_number, 'USSD' Channel, count(*) cnt_recent_mnth, max(cast(trandate as date)) last_tran_date
from ussd_transactions_current_month
where response_code = 00 and transaction_amount > 0
group by account_number)

select isnull(a.account_number, b.account_number) account_number, 'USSD' channel, isnull(cnt_dec,0) mnth1,
isnull(cnt_jan, 0) mnth2, isnull(cnt_feb, 0) mnth3, isnull(cnt_mar, 0) mnth4, isnull(cnt_apr, 0) mnth5, isnull(b.cnt_recent_mnth, 0) mnth6
into customer_churn_ussd_frequency_may
from customer_churn_ussd_6months_frequency_count a --this is from training script nov-april data
full outer join CurrentMonthUssd b
on a.account_number = b.account_number


/*Get recent transaction count for CARD and join with previous 5 months transaction data*/
drop table if exists customer_churn_card_frequency_may

;With CurrentMonthCard as (
select from_account_id account_number, 'CARD' Channel, count(*) cnt_recent_mnth, max(cast(datetime_req as date)) last_tran_date
from atm_transactions_current_month
where len(from_account_id) = 10 and response_code = '00' and tran_amount_req > 0
group by from_account_id)

select isnull(a.account_number, b.account_number) account_number, 'CARD' channel, isnull(cnt_dec,0) mnth1,
isnull(cnt_jan, 0) mnth2, isnull(cnt_feb, 0) mnth3, isnull(cnt_mar, 0) mnth4, isnull(cnt_apr, 0) mnth5, isnull(b.cnt_recent_mnth, 0) mnth6
into customer_churn_card_frequency_may
from customer_churn_card_6months_frequency_count a --this is from training script nov-april data
full outer join CurrentMonthCard b
on a.account_number = b.account_number


/*Get recent transaction count for NEWMOBILE and join with previous 5 months transaction data*/
drop table if exists customer_churn_mobile_frequency_may

;With CurrentMonthMobile as (
select account_number, 'NEWMOBILE' Channel, count(*) cnt_recent_mnth, max(cast(datecreated as date)) last_tran_date
from new_mobile_transactions_current_month
where response_code = 00 and transaction_amount > 0
group by account_number)

select isnull(a.account_number, b.account_number) account_number, 'NEWMOBILE' channel, isnull(cnt_dec,0) mnth1,
isnull(cnt_jan, 0) mnth2, isnull(cnt_feb, 0) mnth3, isnull(cnt_mar, 0) mnth4, isnull(cnt_apr, 0) mnth5, isnull(b.cnt_recent_mnth, 0) mnth6
into customer_churn_mobile_frequency_may
from customer_churn_mobile_6months_frequency_count a --this is from training script nov-april data
full outer join CurrentMonthMobile b
on a.account_number = b.account_number


/*Get recent transaction count for RIB and join with previous 5 months transaction data*/
drop table if exists customer_churn_rib_frequency_may

;With CurrentMonthRib as (
select account_number, 'RIB' Channel, count(*) cnt_recent_mnth, max(cast(datecreated as date)) last_tran_date
from internet_banking_transactions_current_month
where response_code = 00 and transaction_amount > 0
group by account_number)

select isnull(a.account_number, b.account_number) account_number, 'RIB' channel, isnull(cnt_dec,0) mnth1,
isnull(cnt_jan, 0) mnth2, isnull(cnt_feb, 0) mnth3, isnull(cnt_mar, 0) mnth4, isnull(cnt_apr, 0) mnth5, isnull(b.cnt_recent_mnth, 0) mnth6
into customer_churn_rib_frequency_may
from customer_churn_rib_6months_frequency_count a --this is from training script nov-april data
full outer join CurrentMonthRib b
on a.account_number = b.account_number


/*Merge 6 months data for each channel*/
drop table if exists customer_churn_combined_channels_frequency_may

select *
into customer_churn_combined_channels_frequency_may
from customer_churn_ussd_frequency_may
union all
select *
from customer_churn_card_frequency_may
union all
select *
from customer_churn_mobile_frequency_may
union all
select *
from customer_churn_rib_frequency_may


/*Drop rows with 0 counts in all 6 months*/
drop table if exists customer_churn_6months_transacting_customers_pred

select account_number, channel, mnth1, mnth2, mnth3, mnth4, mnth5, mnth6
into customer_churn_6months_transacting_customers_pred
from (
	select *, case when mnth1 = 0 and mnth2 = 0 and mnth3 = 0 and mnth4 = 0 and mnth5 = 0 and mnth6 = 0 then 1 else 0 end as Has_transacted
	from customer_churn_combined_channels_frequency_may) b
where b.has_transacted !=1


/*Add Customer Demographics*/
drop table if exists customer_churn_6months_channels_data_pred

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
into customer_churn_6months_channels_data_pred
from customer_churn_6months_transacting_customers_pred a
join customer_accounts_table b
on b.account_number = a.account_number
join customer_segmentation_table c
on b.customer_id = c.customer_ID


/*Make data by customer 6months transaction for the 4 channels*/
drop table if exists customer_churn_by_customer_pred

;with Channeltxns1 as (
select *,
max(case when channel = 'CARD' then 1 else 0 end) over(partition by customer_id) card , 
max(case when channel = 'ussd' then 1 else 0 end) over(partition by customer_id) ussd,
max(case when channel = 'rib' then 1 else 0 end) over(partition by customer_id) rib,
max(case when channel = 'newmobile' then 1 else 0 end) over(partition by customer_id) newmobile
from customer_churn_6months_channels_data_pred
)

select distinct customer_id, sum(mnth1)  over (partition by customer_id) mnth1, 
sum(mnth2)  over (partition by customer_id) mnth2, 
sum(mnth3)  over (partition by customer_id) mnth3, 
sum(mnth4)  over (partition by customer_id) mnth4, 
sum(mnth5)  over (partition by customer_id) mnth5, 
sum(mnth6)  over (partition by customer_id) mnth6, 
region_name, age, customer_status, sex, customer_segment, generation, occupation, card, ussd, rib, newmobile
into customer_churn_by_customer_pred
from Channeltxns1