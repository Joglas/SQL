-------------------   Load data from S3 to a new Redshift cluster -----------------------------

drop table if exists action;
create table action (
 user_id         varchar(48)         ,
 action_type     char(1)             ,
 action_ts       timestamp   DISTKEY ,
 item_id         varchar(48)         ,
 device          varchar(2)          ,
 b2c             BOOLEAN
);

--Copying data from S3 (automatic compression)
copy         action
from        's3://xxxxxx'
credentials 'xxxxxxxxxxx'
region      'xxxxxxxxxxx'
gzip
compupdate on;

--- Making sure the table was populated
select count(*) from action;
select * from action order by action_ts desc limit 100 ;

--- Checking Distribution
select    slice,
          col,
          num_values,
          minvalue,
          maxvalue
from      svv_diskusage
where     name='action' and col=2
order by  slice,col;


-------------------  Segment customer base for below 5 lifecycle stages -----------------------------


/* Creating table for segment each user
    Rules for segmentation:
       Repeat: 1 or more actions in last 4 weeks (28 days) / At least 8 weeks (56 days) since first post or reply
       Lost: 0 posts / replies in last 12 weeks (84 days)
       Dormant: 0 posts / replies in last 4 weeks (28 days)
       Novice: At least 1 weeksince first post or reply
       Trial: Posted new listing for the first time / Contacted seller for the first time
Action = Post or Reply (P or R in the action_type column)
*/

drop table if exists user_segment;
create table user_segment as
    -- Applying cases to obey the rules described above
    select    b.user_id,
              case  when number_days_last_interaction >= 84 then 'Lost'
                    when (number_days_last_interaction <= 28 and max_number_of_interactions > 1)
                         and number_days_since_first_interaction >= 56 then 'Repeat'
                    when number_days_last_interaction >= 28 then 'Dormant'
                    when number_days_since_first_interaction >= 7 then 'Novice'
                    else 'Trial' end as segment
    from(
        -- Determining the number of the days of the last interaction for each user
        -- Determining number of days since first interaction for each user
        -- Determining the maximum number of interactions for each user
        select    a.user_id,
                  min(a.number_days) over (partition by a.user_id) as number_days_last_interaction,
                  max(a.number_days) over (partition by a.user_id) as number_days_since_first_interaction,
                  max(a.number_of_interactions) over (partition by a.user_id) as max_number_of_interactions

         from (
                -- Calculating the number of days for each post/replies considering base date 2016-02-01
                -- Ranking partition by user to separate each one of the interactions (post/replies)
                select    user_id,
                          datediff(DAY, date(action_ts), '2016-02-01') AS number_days,
                          action_ts,
                          rank () over (partition by user_id order by action_ts) as number_of_interactions
                from      action
                where     action_type IN ('R', 'P')
              ) a
        ) b
    group by user_id, segment order by segment
;

-- Relative size of users by segment
select    a.segment,
          cast(a.count_by_segment as float) / (select count(*) from user_segment) as relative_size
from(
     select    segment,
               count(*) as count_by_segment
     from      user_segment
     group by segment
     ) a
;


-------------------  Calculate liquidity -----------------------------

--- Creating dimensions
drop table if exists dates;
create table dates distkey(dates_datekey) as
  select distinct
         a.action_ts as dates_datekey,
         trunc (a.action_ts) as dates_date,
         to_char(a.action_ts, 'HH24:MI:SS') as dates_time,
         extract(day from a.action_ts) as dates_day,
         extract(month from a.action_ts) as dates_month_num,
         extract(year from a.action_ts) as dates_year,
         to_char(a.action_ts, 'Month') as dates_month
  from action a;

drop table if exists items;
create table items distkey(item_id) sortkey(post_date) as
  select distinct
         item_id,
         action_ts as post_date
  from action
  where action_type = 'P';

/* Creating fact table (fact_item_liquidity)
   Measures:
      # replies received within 1 day
      # replies received within 7 days
*/

drop table if exists fact_item_liquidity;
create table fact_item_liquidity diststyle key distkey (item_id) sortkey (post_date) as
  select b.item_id,
         b.post_date,
         sum(b.num_repl_wth_one_day) as num_repl_wth_one_day,
         sum(b.num_repl_wth_seven_days) as num_repl_wth_seven_days
  from(
        -- cases to mark items (1 or 0) that are according to the measures we need to calculate and we can sum up im the query above
        select a.item_id,
               a.post_date,
               a.number_days_reply,
               case when a.number_days_reply <= 1 then 1 else 0 end as num_repl_wth_one_day,
               case when a.number_days_reply <= 7 then 1 else 0 end as num_repl_wth_seven_days
        from(
              -- Getting all the posted items and join with replies, keeping posts that doesn't have replies associated
              with post as (select item_id, post_date from items)
              select p.item_id,
                     r.action_ts as reply_date,
                     d.dates_date as post_date,
                     datediff(day, p.post_date, reply_date) as number_days_reply
              from (
              -- Select to consider only the replies actions to be join with the post actions
                    select
                        item_id,
                        action_type,
                        action_ts
                    from action
                    where action_type = 'R'
                  ) r
              right outer join post p on p.item_id = r.item_id
              inner join dates d on d.dates_datekey = p.post_date
            ) a
      ) b
  group by b.item_id, b.post_date
;

/* Creating fact table (fact_liquidity)
   Measures:
      # items posted on date
      Liquid items 1 reply within 1 day
      Liquid items 3 replies within 7 days
      Liquid items 5 replies within 7 days
      % liquidity 1 reply within 1 day
      % liquidity 3 replies within 7 days
      % liquidity 5 replies within 7 days
*/

drop table if exists fact_liquidity;
create table fact_liquidity diststyle key distkey (post_date) as
  -- total number of posts for a given date
  with all_posts_on_date as (
      select
           post_date as date,
           count(*)  as num_itens_posted_date
      from fact_item_liquidity
      group by date
  )
  -- final query and calculating the % measures, the all_posts_on_date above is used to bring the total number of posts for a given date
  select     b.post_date,
             c.num_itens_posted_date,
             b.liquid_items_one_reply_wth_one_day,
             b.liquid_items_three_replies_wth_seven_days,
             b.liquid_items_five_replies_wth_seven_days,
             cast(b.liquid_items_one_reply_wth_one_day as float) / c.num_itens_posted_date as percent_liquid_items_one_reply_wth_one_day,
             cast(b.liquid_items_three_replies_wth_seven_days as float) / c.num_itens_posted_date as percent_liquid_items_three_replies_wth_seven_days,
             cast(b.liquid_items_five_replies_wth_seven_days as float) / c.num_itens_posted_date as percent_liquid_items_five_replies_wth_seven_days
  from(
            select    a.post_date,
                      sum(a.replies_one_day) as liquid_items_one_reply_wth_one_day,
                      sum(a.three_replies_seven_days) as liquid_items_three_replies_wth_seven_days,
                      sum(a.five_replies_seven_days) as liquid_items_five_replies_wth_seven_days
            from(
                      -- cases to mark items (1 or 0) that are according to the measures we need to calculate and we can sum up im the query above
                      select    post_date,
                                case when num_repl_wth_one_day >= 1 then 1 else 0 end replies_one_day,
                                case when num_repl_wth_seven_days >=3 then 1 else 0 end three_replies_seven_days,
                                case when num_repl_wth_seven_days >= 5 then 1 else 0 end five_replies_seven_days
                      from      fact_item_liquidity
                )a
            group by post_date
  ) b
  inner join all_posts_on_date c on c.date = post_date
;