
--- changing datatype of amount to bignint
alter table  dbo.credit_card_transaction
alter column amount bigint



---Exploring dataset
---num of rows
select count(*) from dbo.credit_card_transaction

---num of cities
select distinct city from dbo.credit_card_transaction
select count(distinct city) from dbo.credit_card_transaction

---num of card type
select distinct card_type from dbo.credit_card_transaction

---min and max transaction date
select min(transaction_date),max(transaction_date) from dbo.credit_card_transaction

---total amnt and num of transactions by exp type
select exp_type, count(exp_type) as num_of_transactions,sum(amount) as amnt
from dbo.credit_card_transaction
group by exp_type
order by num_of_transactions desc

---num of transactions by gender
select gender, count(gender) as no_of_transaction
from dbo.credit_card_transaction
group by gender
 
 ---avg of trancsaction
 select exp_type,avg(amount)as mean
 from dbo.credit_card_transaction 
 group by exp_type

select * from dbo.credit_card_transaction

--top 5 cities with highest spends and their percentage contribution of total credit card spends 
select top 5 city, sum(amount) as amnt, 
round(sum(cast(amount as float))/(select sum(cast(amount as float)) from dbo.credit_card_transaction),2) as spend_percent
from dbo.credit_card_transaction
group by city
order by amnt desc

--print highest spend month and amount spent in that month for each card type
with monthly_spends as
(select card_type,year(transaction_date) as yr,datename(MONTH,transaction_date) as [month], sum(amount) as total_amnt,
DENSE_RANK() over(partition by card_type order by sum(amount) desc) as rnk
from credit_card_transaction
group by card_type, year(transaction_date),datename(MONTH,transaction_date))

select card_type, yr,[month] ,total_amnt
from monthly_spends
where rnk =1

-- print the transaction details(all columns from the table) for each card type when
--it reaches a cumulative of 1000000 total spends(We should have 4 rows in the o/p one for each card type)

with cumulative_table as   --cte1
(select *, 
sum(amount) over(partition by card_type order by transaction_date, transaction_id) as cum_amnt
from dbo.credit_card_transaction),
pick_1000000th as          --cte2
(select *,
dense_rank() over(partition by card_type order by amount) as rnk
from
cumulative_table
where cum_amnt > 999999)

select * from pick_1000000th
where rnk = 1

-- city which had lowest percentage spend for gold card type
select city,sum(amount),
cast(sum(amount) as float)/(select sum(amount) from dbo.credit_card_transaction ) as sales
from dbo.credit_card_transaction
where card_type = 'Gold'
group by city
order by sales 

-- city, highest_expense_type , lowest_expense_type (example format : Delhi , bills, Fuel)

with cte as(
select city,
FIRST_VALUE(exp_type) over(partition by city order by sum(amount) desc rows between unbounded preceding and unbounded following) as highest_expense_type,
LAST_VALUE(exp_type) over(partition by city order by sum(amount) desc rows between unbounded preceding and unbounded following) as lowest_expense_type
from dbo.credit_card_transaction
group by city,exp_type)

select * from cte
group by city,highest_expense_type,lowest_expense_type;


-- percentage contribution of spends by females for each expense type

with female_spends_exp_type as 
(select exp_type, sum(amount) as total_exp_type_spend
from dbo.credit_card_transaction
where gender ='F'
group by exp_type)

select exp_type, total_exp_type_spend,round(cast(total_exp_type_spend as float)/sum(total_exp_type_spend),2) as spend_percent
from female_spends_exp_type
group by exp_type,total_exp_type_spend;

-- card and expense type combination saw highest month over month growth in Jan-2014

with month_sales as (
select card_type, exp_type, 
sum(case when month(transaction_date) = 12 and year(transaction_date) = 2013  then amount end)  as dec_2013_sales,
sum(case when month(transaction_date) = 1 and year(transaction_date) = 2014  then amount end)  as jan_2014_sales
from dbo.credit_card_transaction
group by card_type, exp_type)

select top 1 *, round((cast(jan_2014_sales as float)- dec_2013_sales)/dec_2013_sales,2) as per
from month_sales
order by per desc

-- during weekends which city has highest total spend to total no of transcations ratio 

with weekday_agg as(
select transaction_id,city, datename(WEEKDAY,transaction_date) as weekday ,sum(amount) as total
from dbo.credit_card_transaction
group by transaction_id,city,datename(WEEKDAY,transaction_date)),
weekend_spends as(
select city , sum(total) as total_weekend_spend, count(transaction_id) as tid_count
from weekday_agg 
where weekday in ('Saturday', 'Sunday')
group by city)

select top 1 *, round(cast(total_weekend_spend as float)/tid_count,2) as ratio from weekend_spends
order by ratio desc


select * from dbo.credit_card_transaction
where city = 'Bengaluru' and datename(WEEKDAY,transaction_date) in ('Saturday', 'Sunday');


--which city took least number of days to reach its 500th transaction after the first transaction in that city
with cte as(
select *,
min(transaction_date) over(partition by city order by transaction_date) as first_trans_date,
row_number() over(partition by city order by transaction_date) as transaction_number
from dbo.credit_card_transaction)

select top 1 city,first_trans_date,transaction_date, DATEDIFF(day,first_trans_date,transaction_date) as days_taken
from cte 
where transaction_number = 500
order by days_taken


