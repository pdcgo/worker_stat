package stock

// with lday as (
// 	select
// 		max(d.day) as day,
// 		d.team_id
// 	from daily_team_restock d
// 	group by team_id
// ),
// lastdata as (
// 	select
// 		*
// 	from lday l
// 	join daily_team_restock d on l.day = d.day and l.team_id = d.team_id
// )

// select
