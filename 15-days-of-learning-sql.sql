with temp as (select submission_date,count(hacker_id) c1 from 
(
    select distinct hacker_id,submission_date,submission_date-TO_DATE('01-03-2016','DD-MM-YYYY')+1 d,
            dense_rank() over ( partition by hacker_id
             order by submission_date) as r
             from submissions 
            )
              where r=d
              group by submission_date
),
temp2 as (
        select * from (
            select  submission_date,hacker_id, count(submission_id) c2,
            rank() over ( partition by submission_date
            order by count(submission_id) desc,hacker_id asc ) as r2
            from submissions  
            group by submission_date,hacker_id  
        ) t
        where t.r2=1
)
    select temp.submission_date,temp.c1,temp2.hacker_id ,hackers.name
    from temp 
    left join temp2 on temp.submission_date = temp2.submission_date
    left join hackers on temp2.hacker_id = hackers.hacker_id;
