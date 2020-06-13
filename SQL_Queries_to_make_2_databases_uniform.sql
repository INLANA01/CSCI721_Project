# Select p.playerID, nameFirst, nameLast, yearID, teamID, lgID, G, AB, R, H, 2B, 3B, HR, RBI, SB, BB, SO, IBB, HBP, SH, SF, salary
CREATE VIEW ChadDataset AS
Select p.playerID, concat(p.nameFirst, ' ' ,p.nameLast) as playerName, b.yearID, b.teamID, b.lgID, b.G, b.AB, b.R, b.H, 2B, 3B, HR, RBI, SB, BB, SO, IBB, HBP, SH, SF, s.salary from
people p
join batting b
on p.playerID = b.playerID
join salaries s
on s.playerID = p.playerID;


Select playerID, 'Player Name' as playerName,  Season, teamID, League, G, AB, R, H, 2B, 3B, HR, RBI, SB, BB, SO, IBB, HBP, SH, SF, salary from mlb_batting
limit 100;
Select playerID, 'Player Name' as playerName, yearID as Season, teamID, lgID as League, G, AB, R, H, 2B, 3B, HR, RBI, SB, BB, SO, IBB, HBP, SH, SF, salary from ChadDataset
limit 100;
