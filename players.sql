--task 1
ALTER TABLE players4 
ADD COLUMN AgeCategory VARCHAR(10),
ADD COLUMN GoalsPerClubGame REAL;

UPDATE players4
SET agecategory = 
    CASE 
        WHEN age <= 23 THEN 'Young'
        WHEN age >= 24 AND age <= 32 THEN 'MidAge'
        WHEN age >= 33 THEN 'Old'
    END,
	goalsperclubgame = 
        CASE 
            WHEN appearances_current_club > 0 THEN CAST(goals_current_club AS FLOAT) / appearances_current_club
            ELSE NULL
        END;

--task2
SELECT 
    current_club, 
    AVG(age) AS avg_age, 
    AVG(appearances_current_club) AS avg_apps, 
    COUNT(1) AS total_number_players 
FROM 
    players4 
WHERE 
    current_club IS NOT NULL 
GROUP BY 
    current_club;

--task3
SELECT
    p1.name AS player_name,
    COALESCE(COUNT(p2.playerid), 0) AS num_similar_players 
FROM players4 p1
LEFT JOIN players4 p2 
                ON p1.position = p2.position
                AND p1.appearances_current_club < p2.appearances_current_club
                AND p1.date_of_birth < p2.date_of_birth
WHERE p1.current_club = 'Real Madrid'
GROUP BY p1.name;



