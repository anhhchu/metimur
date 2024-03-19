--Q10--
select * from yearly_mv 
where YYYYMM IN (to_date('2019-01-01', 'yyyy-MM-dd')) 
  AND EMPL_CNT >= 90
  AND CLNT_CNT >= 10;


--Q10--
select * from yearly_mv 
where YYYYMM IN (to_date('2020-01-01', 'yyyy-MM-dd')) 
  AND EMPL_CNT >= 10
  AND CLNT_CNT >= 5;


--Q10--
select * from yearly_mv 
where YYYYMM IN (to_date('2021-01-01', 'yyyy-MM-dd')) 
  AND EMPL_CNT >= 100
  AND CLNT_CNT >= 5;


--Q10--
select * from yearly_mv 
where YYYYMM IN (to_date('2022-01-01', 'yyyy-MM-dd')) 
  AND EMPL_CNT >= 100
  AND CLNT_CNT >= 5;