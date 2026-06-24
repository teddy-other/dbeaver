DROP TABLE IF EXISTS Employee;
DROP TABLE IF EXISTS Manages;

CREATE VERTEX TABLE Employee (
    id   INT,
    name STRING,
    roles STRING,
    lvl  INT
);

CREATE EDGE TABLE Manages (
    id          INT NOT NULL AUTO_INCREMENT,
    sourceId      INT,
    destinationId INT
);

-- ── Level 0: Root ─────────────────────────────────────────────
INSERT VERTEX INTO Employee VALUES (1,  'CEO',           'Chief Executive Officer',    0);

-- ── Level 1: VPs ──────────────────────────────────────────────
INSERT VERTEX INTO Employee VALUES (2,  'VP_Eng',        'VP of Engineering',          1);
INSERT VERTEX INTO Employee VALUES (3,  'VP_Mkt',        'VP of Marketing',            1);
INSERT VERTEX INTO Employee VALUES (4,  'VP_HR',         'VP of HR',                   1);

-- ── Level 2: Leads ────────────────────────────────────────────
INSERT VERTEX INTO Employee VALUES (5,  'Backend_Lead',  'Backend Lead',               2);
INSERT VERTEX INTO Employee VALUES (6,  'Frontend_Lead', 'Frontend Lead',              2);
INSERT VERTEX INTO Employee VALUES (7,  'DevOps_Lead',   'DevOps Lead',                2);
INSERT VERTEX INTO Employee VALUES (8,  'Digital_Lead',  'Digital Marketing Lead',     2);
INSERT VERTEX INTO Employee VALUES (9,  'Brand_Lead',    'Brand Lead',                 2);
INSERT VERTEX INTO Employee VALUES (10, 'Recruit_Lead',  'Recruitment Lead',           2);

-- ── Level 3: Members ──────────────────────────────────────────
INSERT VERTEX INTO Employee VALUES (11, 'Alice',         'Backend Engineer',           3);
INSERT VERTEX INTO Employee VALUES (12, 'Bob',           'Backend Engineer',           3);
INSERT VERTEX INTO Employee VALUES (13, 'Carol',         'Frontend Engineer',          3);
INSERT VERTEX INTO Employee VALUES (14, 'Dave',          'Frontend Engineer',          3);
INSERT VERTEX INTO Employee VALUES (15, 'Eve',           'DevOps Engineer',            3);
INSERT VERTEX INTO Employee VALUES (16, 'Frank',         'Digital Marketer',           3);
INSERT VERTEX INTO Employee VALUES (17, 'Grace',         'Digital Marketer',           3);
INSERT VERTEX INTO Employee VALUES (18, 'Hank',          'Brand Designer',             3);
INSERT VERTEX INTO Employee VALUES (19, 'Iris',          'HR Recruiter',               3);
INSERT VERTEX INTO Employee VALUES (20, 'Jack',          'HR Recruiter',               3);

-- ── CEO → VPs ─────────────────────────────────────────────────
INSERT EDGE FROM (SELECT Employee FROM Employee WHERE id = 1)
              TO (SELECT Employee FROM Employee WHERE id = 2)
            INTO Manages VALUES (1, 1, 2);
INSERT EDGE FROM (SELECT Employee FROM Employee WHERE id = 1)
              TO (SELECT Employee FROM Employee WHERE id = 3)
            INTO Manages VALUES (2, 1, 3);
INSERT EDGE FROM (SELECT Employee FROM Employee WHERE id = 1)
              TO (SELECT Employee FROM Employee WHERE id = 4)
            INTO Manages VALUES (3, 1, 4);

-- ── VP_Eng → Leads ────────────────────────────────────────────
INSERT EDGE FROM (SELECT Employee FROM Employee WHERE id = 2)
              TO (SELECT Employee FROM Employee WHERE id = 5)
            INTO Manages VALUES (4, 2, 5);
INSERT EDGE FROM (SELECT Employee FROM Employee WHERE id = 2)
              TO (SELECT Employee FROM Employee WHERE id = 6)
            INTO Manages VALUES (5, 2, 6);
INSERT EDGE FROM (SELECT Employee FROM Employee WHERE id = 2)
              TO (SELECT Employee FROM Employee WHERE id = 7)
            INTO Manages VALUES (6, 2, 7);

-- ── VP_Mkt → Leads ────────────────────────────────────────────
INSERT EDGE FROM (SELECT Employee FROM Employee WHERE id = 3)
              TO (SELECT Employee FROM Employee WHERE id = 8)
            INTO Manages VALUES (7, 3, 8);
INSERT EDGE FROM (SELECT Employee FROM Employee WHERE id = 3)
              TO (SELECT Employee FROM Employee WHERE id = 9)
            INTO Manages VALUES (8, 3, 9);

-- ── VP_HR → Lead ──────────────────────────────────────────────
INSERT EDGE FROM (SELECT Employee FROM Employee WHERE id = 4)
              TO (SELECT Employee FROM Employee WHERE id = 10)
            INTO Manages VALUES (9, 4, 10);

-- ── Backend_Lead → Engineers ──────────────────────────────────
INSERT EDGE FROM (SELECT Employee FROM Employee WHERE id = 5)
              TO (SELECT Employee FROM Employee WHERE id = 11)
            INTO Manages VALUES (10, 5, 11);
INSERT EDGE FROM (SELECT Employee FROM Employee WHERE id = 5)
              TO (SELECT Employee FROM Employee WHERE id = 12)
            INTO Manages VALUES (11, 5, 12);

-- ── Frontend_Lead → Engineers ─────────────────────────────────
INSERT EDGE FROM (SELECT Employee FROM Employee WHERE id = 6)
              TO (SELECT Employee FROM Employee WHERE id = 13)
            INTO Manages VALUES (12, 6, 13);
INSERT EDGE FROM (SELECT Employee FROM Employee WHERE id = 6)
              TO (SELECT Employee FROM Employee WHERE id = 14)
            INTO Manages VALUES (13, 6, 14);

-- ── DevOps_Lead → Engineer ────────────────────────────────────
INSERT EDGE FROM (SELECT Employee FROM Employee WHERE id = 7)
              TO (SELECT Employee FROM Employee WHERE id = 15)
            INTO Manages VALUES (14, 7, 15);

-- ── Digital_Lead → Marketers ──────────────────────────────────
INSERT EDGE FROM (SELECT Employee FROM Employee WHERE id = 8)
              TO (SELECT Employee FROM Employee WHERE id = 16)
            INTO Manages VALUES (15, 8, 16);
INSERT EDGE FROM (SELECT Employee FROM Employee WHERE id = 8)
              TO (SELECT Employee FROM Employee WHERE id = 17)
            INTO Manages VALUES (16, 8, 17);

-- ── Brand_Lead → Designer ─────────────────────────────────────
INSERT EDGE FROM (SELECT Employee FROM Employee WHERE id = 9)
              TO (SELECT Employee FROM Employee WHERE id = 18)
            INTO Manages VALUES (17, 9, 18);

-- ── Recruit_Lead → Recruiters ─────────────────────────────────
INSERT EDGE FROM (SELECT Employee FROM Employee WHERE id = 10)
              TO (SELECT Employee FROM Employee WHERE id = 19)
            INTO Manages VALUES (18, 10, 19);
INSERT EDGE FROM (SELECT Employee FROM Employee WHERE id = 10)
              TO (SELECT Employee FROM Employee WHERE id = 20)
            INTO Manages VALUES (19, 10, 20);

-- =============================================================
-- Graph SELECT queries
-- =============================================================

-- Full tree (all 4 levels at once)
-- SELECT json_graph(n1,e,n2) FROM MATCH (n1:Employee)-[e:Manages]->(n2:Employee);
-- SELECT json_info(n1),json_info(e),json_info(n2) FROM MATCH (n1:Employee)-[e:Manages]->(n2:Employee);

-- CEO + direct reports only (2 levels)
-- SELECT json_graph(n1,e,n2) FROM MATCH (n1:Employee)-[e:Manages]->(n2:Employee) WHERE n1.id = '1'
-- SELECT json_info(n1),json_info(e),json_info(n2) FROM MATCH (n1:Employee)-[e:Manages]->(n2:Employee);

-- Engineering subtree only (not excute)
-- SELECT JSON_GRAPH(n1,e,n2) FROM MATCH pth=(n1:Employee)-[e:Manages {1,2}]->(n2:Employee)
-- SELECT JSON_GRAPH(n1,e,n2) FROM MATCH (n1:Employee)-[e:Manages {1,2}]->(n2:Employee)


-- Engineering subtree only (not graph)
-- SELECT property_of(first(nodes(pth)), name, Employee) AS first_node, property_of(last(nodes(pth)), name, Employee) AS last_node
-- FROM MATCH pth=(n1:Employee)-[e:Manages {1,2}]->(n2:Employee)
