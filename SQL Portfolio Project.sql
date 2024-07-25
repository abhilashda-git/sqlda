/* Portfolio Project on Creditcard Transactions

https://www.kaggle.com/datasets/thedevastator/analyzing-credit-card-spending-habits-in-india */

/*select gender,YEAR(transaction_date) as yr ,SUM(amount) as amount_spent from cc_transactions
group by gender, YEAR(transaction_date)
order by YEAR(transaction_date) desc, amount_spent desc;

select  exp_type, YEAR(transaction_date) as yr, SUM(amount) as amount_spent from cc_transactions
group by exp_type,YEAR(transaction_date)
order by yr desc, SUM(amount) desc;

select min(transaction_date), max(transaction_date)
from cc_transactions

select * from 
(
select card_type,city, SUM(amount) as amount_spent, ROW_NUMBER() OVER(partition by card_type order by sum(amount) DESC) rn from cc_transactions
group by city, card_type
--order by amount_spent desc
)A
where A.rn = 1
order by amount_spent desc;

select sum(amount) from cc_transactions; */

--Below is the query to print top 5 cities with highest spends and their percentage contribution of total credit card spends
select top 5 cct.city, 
SUM(cct.amount) as spend,
tot_spend.total_spend,
cast(SUM(cct.amount)*100.0/tot_spend.total_spend as decimal(4,2)) as percentage_contribution
from cc_transactions cct
inner join (select sum(cast (amount as bigint)) as total_spend from cc_transactions) tot_spend on 1 = 1
group by city, tot_spend.total_spend
order by spend DESC;

--Below is the query to print highest spend month and amount spent in that month for each card type

select * from 
(
select card_type, YEAR(transaction_date) as yr,
MONTH(transaction_date) as mnth,
SUM(amount) as spend,
rank() OVER(partition by card_type order by sum(amount) desc) rn
from cc_transactions
group by card_type, YEAR(transaction_date),
MONTH(transaction_date)
)a 
where a.rn = 1;

--Below is the query to print the transaction details(all columns from the table) for each card type when
--it reaches a cumulative of 1000000 total spends(We should have 4 rows in the o/p one for each card type)
with cum_sum AS
(
select 
transaction_id,
city,
transaction_date,
exp_type, 
gender,
card_type,
amount,
sum(amount) OVER(partition by card_type order by transaction_date, transaction_id) as cumm_sum
from cc_transactions
),
 reahces_million as
(
select *, RANK() OVER(partition by card_type order by cumm_sum) rn from cum_sum
where cumm_sum >= 1000000
)
select 
transaction_id,
city,
transaction_date,
exp_type, 
gender,
card_type,
amount
from reahces_million
where rn = 1;


--Below is the query to find city which had lowest percentage spend for gold card type

--Analysis 1 - dividing gold amount for that city to the total spend amount of that city -  sum(gold_amount in hyd)/sum(goldamount in hyd+sliveramount in hyd+platinum_amount in hyd)

with cte as 
(
select city, card_type, SUM(amount) total_spend,
SUM(case when card_type = 'Gold' then amount else 0 end) as gold_amount
from cc_transactions
--where city = 'Shikaripur'
group by city, card_type
)
select top 1 city,SUM(gold_amount) as city_gold_amount,SUM(total_spend) as city_total_spend ,SUM(gold_amount)*1.0/SUM(total_spend) as perc
from cte
group by city
having SUM(gold_amount) >0
order by perc;

--Analysis 2 - dividing gold amount of a city to the total_spend across all the cities with gold amount transaction (if the gold card transaction is not present that state then dont consider that city in the denominator)
--sum(gold amount in hyd)/sum(goldamount in all cities+sliveramount in all cities+platinum_amount in all cities) --denominator considers cities which have gold_type card transaction

with spend as
(
   select city, SUM(amount) as spend
   from cc_transactions
   where card_type = 'Gold'
   group by city 
)
,total_spend as 
(
    select SUM(total_spend) as total_spend from
    (
    select city,count(distinct card_type) as cnt ,sum(cast(amount as bigint)) total_spend
    --ROW_NUMBER() over(partition by city order by card_type)
    from cc_transactions
    group by city
    having count(distinct card_type) = 4
    )tot
)

select city,spend, total_spend, percentage_spend
from (
select spend.city, spend,
total_spend.total_spend,
cast((spend*1.0/total_spend) as decimal(8,7)) as percentage_spend,
DENSE_RANK()OVER(order by cast((spend*100.0/total_spend) as decimal(6,4))) rk
from spend
inner join total_spend on 1=1
--where cast((spend*100.0/total_spend) as decimal(6,4))>0
)A
where A.rk=1;

--Below is the query to print 3 columns:  city, highest_expense_type , lowest_expense_type (example format : Delhi , bills, Fuel)

--Analysis 1:

with cte as 
(
select city, exp_type, SUM(amount) as spend,
DENSE_RANK() OVER(partition by city order by SUM(amount)) lowest_expense,
DENSE_RANK() OVER(partition by city order by sum(amount) desc) highest_expense
from cc_transactions
group by city, exp_type
)
select city, 
min(case when lowest_expense = 1 then exp_type end) as low_exp_type,
min(case when highest_expense = 1 then exp_type end) as high_exp_type
from cte
group by city;

--Analysis 2

with cte as 
(
select city, exp_type, SUM(amount) as spend,
DENSE_RANK() OVER(partition by city order by SUM(amount)) lowest_expense,
DENSE_RANK() OVER(partition by city order by sum(amount) desc) highest_expense
from cc_transactions
group by city, exp_type
)
select city, 
max(case when lowest_expense = 1 then exp_type end) as low_exp_type,
max(case when highest_expense = 1 then exp_type end) as high_exp_type
from cte
group by city;



--Below is the query to find percentage contribution of spends by females for each expense type

--Solution 1:
select exp_type,
ROUND(sum(case when gender='F' then amount else 0 end)*100.0/sum(amount),2) as percentage_female_contribution
from cc_transactions
group by exp_type
order by percentage_female_contribution desc;

--Solution 2:
with female_spend as
(
select exp_type, SUM(amount) as spend
from cc_transactions
where gender = 'F'
group by exp_type
)
,total_spend as
(
    select exp_type, SUM(amount) as total_spend from cc_transactions
    --where gender = 'F'
    group by exp_type
)
select female_spend.exp_type, spend, total_spend, cast(spend*100.0/total_spend as decimal(4,2)) as percentage_contribution_female
from female_spend
inner join total_spend on female_spend.exp_type=total_spend.exp_type
order by percentage_contribution_female desc;

--Below is the query to fetch card and expense type combination saw highest month over month growth in Jan-2014
--Analysis 1: based on the mom change

with cte as
(
select card_type, exp_type, YEAR(transaction_date) as yr, month(transaction_date) as mnth, SUM(amount) as spend,
ROW_NUMBER() OVER(partition by card_type, exp_type order by YEAR(transaction_date), month(transaction_date)) rn,
lag(SUM(amount), 1) OVER(partition by card_type, exp_type order by YEAR(transaction_date), month(transaction_date)) as prev_mnth_spend,
SUM(amount) - lag(SUM(amount), 1) OVER(partition by card_type, exp_type order by YEAR(transaction_date), month(transaction_date)) as mom_diff,
100.0*(SUM(amount) - lag(SUM(amount), 1) OVER(partition by card_type, exp_type order by YEAR(transaction_date), month(transaction_date)))
/lag(SUM(amount), 1) OVER(partition by card_type, exp_type order by YEAR(transaction_date), month(transaction_date)) as mom_change
from cc_transactions
GROUP BY card_type, exp_type, YEAR(transaction_date), month(transaction_date)
)
select card_type, exp_type, yr, mnth,spend, prev_mnth_spend,mom_diff, mom_change
FROM
(
select card_type, exp_type, yr, mnth,spend, prev_mnth_spend,mom_diff, mom_change ,DENSE_RANK()OVER(order by mom_change desc) rk
from cte
where yr = 2014 and mnth = 1
)A 
where A.rk = 1; 

--Analysis 2: based on mom diff

with cte as 
(
select card_type, exp_type, YEAR(transaction_date) as yr, month(transaction_date) as mnth, SUM(amount) as spend,
ROW_NUMBER() OVER(partition by card_type, exp_type order by YEAR(transaction_date), month(transaction_date)) rn,
lag(SUM(amount), 1) OVER(partition by card_type, exp_type order by YEAR(transaction_date), month(transaction_date)) as prev_mnth_spend,
SUM(amount) - lag(SUM(amount), 1) OVER(partition by card_type, exp_type order by YEAR(transaction_date), month(transaction_date)) as mom_diff,
100.0*(SUM(amount) - lag(SUM(amount), 1) OVER(partition by card_type, exp_type order by YEAR(transaction_date), month(transaction_date)))
/lag(SUM(amount), 1) OVER(partition by card_type, exp_type order by YEAR(transaction_date), month(transaction_date)) as mom_change
from cc_transactions
GROUP BY card_type, exp_type, YEAR(transaction_date), month(transaction_date)
)

select top 1 * from
(
select card_type, exp_type, yr, mnth, spend,prev_mnth_spend, mom_diff, mom_change
from cte
where yr = 2014 and mnth = 1
)a
order by mom_diff desc;

--8. during weekends which city has highest total spend to total no of transcations ratio 

select top 1 city, COUNT(transaction_id) as transactions, sum(amount) as spend, 1.0*SUM(amount)/COUNT(transaction_id) as transaction_ratio
from cc_transactions
where DATENAME(WEEKDAY, transaction_date) in ('Saturday', 'Sunday')
group by city
order by transaction_ratio desc;

--in the above query I've used Saturday and sunday in where filter which are strings. so using strings in comparisons or where filter on in join conditions is little slow when compared to using integers.
--so it is optimal to use integers in comparisons or in where filters or in join conditions whenever possible. So instead of saturday and sunday we can use datepart(weekday, transaction_date) in (1,7)

--9. which city took least number of days to reach its 500th transaction after the first transaction in that city

--Analysis 1:

with cte as
(
select city,transaction_id, transaction_date, 
ROW_NUMBER() OVER (partition by city order by  transaction_date, transaction_id) as rn
from cc_transactions
)
,cte2 as
(
select city, 
min(case when rn = 1 then transaction_date end) as frst_trans_date,
min(case when rn = 500 then transaction_date end) as five00_trans_date
from cte
where rn in (1,500)
group by city
)
select top 1 city, frst_trans_date, five00_trans_date, DATEDIFF(DAY,frst_trans_date ,five00_trans_date) as no_of_days
from cte2
where frst_trans_date is not null 
and five00_trans_date is not null 
order by no_of_days;

--Analysis 2:

with cte as (
select *
,row_number() over(partition by city order by transaction_date,transaction_id) as rn
from cc_transactions)

select top 1 city,datediff(day,min(transaction_date),max(transaction_date)) as no_of_days
from cte
where rn=1 or rn=500
group by city
having count(1)=2
order by no_of_days;