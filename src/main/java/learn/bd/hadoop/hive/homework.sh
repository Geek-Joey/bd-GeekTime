#!/bin/bash

# hql test

# t_user 观众表共6000+条数据 ,字段为：UserID, Sex, Age, Occupation, Zipcode (用户id，性别，年龄，职业，邮编)
# t_movie 电影表共3000+条数据,字段为：MovieID, MovieName, MovieType (电影ID，电影名，电影类型)
# t_rating 影评表100万+条数据,字段为：UserID, MovieID, Rate, Times (用户ID，电影ID，评分，评分时间)


# 题目1：展示电影ID为2116这部电影各年龄段的平均影评分
def test_01(){
  sql=":
  select
   t3.Age ,
   avg(Rate) as avgrate
  from t_movie t1
  join t_rating t2 on t1.MovieID = t2.MovieID
  join t_user t3 on t2.UserID = t3.UserID
  where t1.MovieID = 2116
  group by t3.Age
  "
  hive -e $sql
}

#找出男性评分最高且评分次数超过50次的10部电影，展示电影名，平均影评分和评分次数
def test_02(){
  sql="
  select
   t1.MovieID,MovieName,
   count(1) as total,
   avg(Rate) as avgrate
  from t_movie t1
  join t_rating t2 on t1.MovieID = t2.MovieID
  join t_user t3 on t2.UserID = t3.UserID
  where t3.Sex = 'M'
  group by MovieID,MovieName
  having count(1) > 50
  order by avgrate desc
  limit 10;
  "
  hive -e $sql
}

#找出影评次数最多的女士所给出最高分的10部电影 的平均影评分，展示电影名和平均影评分（可使用多行SQL）



#
def test_02(){
  sql="
  select
    MovieName,
    count(1) as total,
    avg(Rate) as avgrate
   from t_movie t1
   join t_rating t2 on t1.MovieID = t2.MovieID
   join t_user t3 on t2.UserID = t3.UserID
   where t3.Sex = 'F'
   group by t1.MovieID,MovieName
   order by avgrate, total desc
   limit 10 ;
  "
  hive -e $sql
}

