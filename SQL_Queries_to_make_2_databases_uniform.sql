# Select p.playerID, nameFirst, nameLast, yearID, teamID, lgID, G, AB, R, H, 2B, 3B, HR, RBI, SB, BB, SO, IBB, HBP, SH, SF, salary

use csci721_project;

CREATE VIEW ChadDataset AS
Select p.playerID, concat(p.nameFirst, ' ' ,p.nameLast) as playerName, b.yearID, b.teamID, b.lgID, b.G, b.AB, b.R, b.H, 2B, 3B, HR, RBI, SB, BB, SO, IBB, HBP, SH, SF, s.salary from
people p
join batting b
on p.playerID = b.playerID
join salaries s
on s.playerID = p.playerID;

create table ChadDatasetTable AS
(Select distinct playerID, `PlayerName` as playerName, yearID as Season, teamID, lgID as League, G, AB, R, H, 2B, 3B, HR, RBI, SB, BB, SO, IBB, HBP, SH, SF, salary
from ChadDataset
);

#--------------------------------------------------Chad Dataset----------------------------------------------------#
Select playerID, playerName, Season, teamID, League, G, AB, R, H, 2B, 3B, HR, RBI, SB, BB, SO, IBB, HBP, SH, SF, salary from ChadDatasetTable
GROUP BY playerName, Season
having
R != 0
and G > 10
and SF != 0
and season >= 2000
order by season desc;
#---------------------------------------------MLB Dataset-------------------------------------------------#
Select playerID, `Player Name` as playerName,  Season, teamID, League, G, AB, R, H, 2B, 3B, HR, RBI, SB, BB, SO, IBB, HBP, SH, SF, salary from mlb_batting
GROUP BY playerName, Season
having
R != 0
and G > 10
and SF != 0
and season >= 2000 
order by season desc;