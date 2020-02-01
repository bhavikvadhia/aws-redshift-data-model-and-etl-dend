import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "drop table events"
staging_songs_table_drop = "drop table songs_raw"
songplay_table_drop = "drop table songplays"
user_table_drop = "drop table users"
song_table_drop = "drop table songs"
artist_table_drop = "drop table artists"
time_table_drop = "drop table time"

# CREATE TABLES

staging_events_table_create= ("""
create table events(
                   artist varchar
                   ,auth varchar
                   ,firstName varchar
                   ,gender char
                   ,itemInSession int
                   ,lastName varchar
                   ,length float
                   ,level varchar
                   ,location varchar
                   ,method varchar
                   ,page varchar
                   ,registration varchar
                   ,sessionId int
                   ,song varchar
                   ,status int
                   ,ts timestamp
                   ,userAgent varchar
                   ,userId int
                   );
""")

staging_songs_table_create = ("""
create table songs_raw(
                       song_id varchar(18)
                       ,num_songs int
                       ,title varchar(400)
                       ,artist_name varchar(400)
                       ,artist_latitude float
                       ,year int
                       ,duration float
                       ,artist_id varchar(18)
                       ,artist_longitude float
                       ,artist_location varchar(400)
                      )
""")

songplay_table_create = ("""
create table songplays(
                       songplay_id int IDENTITY(0,1) primary key not null
                       ,start_time timestamp not null distkey sortkey
                       ,user_id int not null
                       ,level varchar(10) not null
                       ,song_id varchar(18)
                       ,artist_id varchar(18)
                       ,session_id int not null
                       ,location varchar not null
                       ,user_agent varchar not null
                      )
""")

user_table_create = ("""
create table users(
                   user_id int primary key not null
                   ,first_name varchar not null
                   ,last_name varchar not null
                   ,gender char(1) not null
                   ,level varchar(10) not null
                  )
""")

song_table_create = ("""
create table songs(
                   song_id varchar(18) primary key not null distkey
                   ,title varchar not null
                   ,artist_id varchar(18) not null
                   ,year int
                   ,duration float not null
                  )
""")

artist_table_create = ("""
create table artists(
                     artist_id varchar(18) primary key not null distkey
                     ,name varchar not null
                     ,location varchar(400)
                     ,latitude float
                     ,longitude float
                    )
""")

time_table_create = ("""
create table time(
                  start_time timestamp primary key not null distkey sortkey
                  ,hour int not null
                  ,day int not null
                  ,week int not null
                  ,month int not null
                  ,year int not null
                  ,weekday int not null
                 )
""")

# STAGING TABLES

staging_events_copy = ("""copy events from {}
        iam_role '{}'
        timeformat 'epochmillisecs'
        maxerror as 10
        format as json {}
""").format(config['S3']['LOG_DATA'],config['IAM_ROLE']['ARN'],config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""copy songs_raw from {}
    iam_role '{}'
    maxerror as 10
    format as json 'auto'
""").format(config['S3']['SONG_DATA'],config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
insert into songplays(start_time,user_id,level,song_id,artist_id,session_id,location,user_agent)
select  e.ts
        ,e.userid
        ,e.level
        ,s.song_id
        ,a.artist_id
        ,e.sessionid
        ,e.location
        ,e.useragent
  from events e
  left join songs s
    on e.song = s.title
   and e.length = s.duration
  left join artists a
    on s.artist_id = a.artist_id
   and e.artist = a.name
 where e.page = 'NextSong';
""")

user_table_insert = ("""
insert into users
select userid
       ,firstname
       ,lastname
       ,gender
       ,level
  from
       (
        select userid
               ,firstname
               ,lastname
               ,gender
               ,level
               ,rank() over(partition by userid order by ts) as rnk
          from events
         where page = 'NextSong'
        ) events_rnk
 where rnk = 1;
""")


song_table_insert = ("""
insert into songs
select distinct song_id
       ,title
       ,artist_id
       ,case when year= 0 then null else year end as year
       ,duration
 from songs_raw;
""")

artist_table_insert = ("""
insert into artists
select distinct artist_id
       ,artist_name
       ,artist_location
       ,artist_latitude
       ,artist_longitude
 from songs_raw;
""")


time_table_insert = ("""
insert into time
select distinct ts
       ,extract(hour from ts)
       ,extract(day from ts)
       ,extract(week from ts)
       ,extract(month from ts)
       ,extract(year from ts)
       ,extract(dow from ts)
 from events
where page = 'NextSong';
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [song_table_insert, artist_table_insert, time_table_insert, user_table_insert, songplay_table_insert]
