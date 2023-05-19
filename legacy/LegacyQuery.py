from sqlalchemy import text

pieQuery = text("""SELECT a.assessment_type, a.weight, si.code_module,count(si.id_student) as count,
si.region,si.gender,si.final_result, si.age_band,si.code_presentation,si.highest_education,
        CASE 
           WHEN si.code_presentation LIKE '%J' THEN Concat('Feb ',left(si.code_presentation,4))
           WHEN si.code_presentation LIKE '%B' THEN Concat('Oct ',left(si.code_presentation,4))
        END code_presentation_description,
        CASE a.assessment_type
           WHEN 'TMA' THEN 'Tutor Marked Assessment (TMA)'
           WHEN 'CMA' THEN 'Computer Marked Assessment (CMA)'
        END assessment_type_description
        FROM studentassessment sa join studentvle sv
        on sa.id_student = sv.id_student
	    join studentinfo si
	    on si.id_student = sa.id_student
	    join assessments a
		on sa.id_assessment = a.id_assessment
        where (sa.id_student, sa.date_submitted)
        in (select id_student, max(date_submitted)
        from studentassessment group by id_student)
        and sa.date_submitted = sv.date
		group by si.code_module, si.code_presentation,a.assessment_type, a.weight,si.region,
		si.age_band,si.highest_education,si.final_result,si.gender""")

regionLineQuery = text("""SELECT si.code_module,si.code_presentation, si.region,count(*) as count,
		CASE 
           WHEN si.code_presentation LIKE '%J' THEN Concat('Feb ',left(si.code_presentation,4))
           WHEN si.code_presentation LIKE '%B' THEN Concat('Oct ',left(si.code_presentation,4))
        END code_presentation_description
    FROM studentassessment sa join studentvle sv
    on sa.id_student = sv.id_student
	join studentinfo si
	on si.id_student = sa.id_student
	join assessments a
	on sa.id_assessment = a.id_assessment
    where (sa.id_student, sa.date_submitted) 
    in (select id_student, max(date_submitted)
    from studentassessment group by id_student)
    and sa.date_submitted = sv.date
	group by si.region,si.code_module,si.code_presentation""")

ageBandLineQuery = text("""SELECT si.code_presentation,si.code_module,count(*) as count,si.age_band,
		CASE 
           WHEN si.code_presentation LIKE '%J' THEN Concat('Feb ',left(si.code_presentation,4))
           WHEN si.code_presentation LIKE '%B' THEN Concat('Oct ',left(si.code_presentation,4))
        END code_presentation_description
    FROM studentassessment sa join studentvle sv
    on sa.id_student = sv.id_student
	join studentinfo si
	on si.id_student = sa.id_student
	join assessments a
	on sa.id_assessment = a.id_assessment
    where (sa.id_student, sa.date_submitted) 
    in (select id_student, max(date_submitted)
    from studentassessment group by id_student)
    and sa.date_submitted = sv.date
	group by si.age_band,si.code_module, si.code_presentation""")

HighestEducationLineQuery = text("""SELECT si.code_presentation,si.highest_education,count(*) as count,si.code_module, 
		CASE 
           WHEN si.code_presentation LIKE '%J' THEN Concat('Feb ',left(si.code_presentation,4))
           WHEN si.code_presentation LIKE '%B' THEN Concat('Oct ',left(si.code_presentation,4))
        END code_presentation_description
FROM studentassessment sa join studentvle sv
on sa.id_student = sv.id_student
join studentinfo si
on si.id_student = sa.id_student
join assessments a
on sa.id_assessment = a.id_assessment
where (sa.id_student, sa.date_submitted) 
in (select id_student, max(date_submitted)
from studentassessment group by id_student)
and sa.date_submitted = sv.date
group by si.highest_education,si.code_module,si.code_presentation""")

finalResultLineQuery = text("""SELECT si.code_presentation,si.final_result,count(*) as count,si.code_module, 
		CASE 
           WHEN si.code_presentation LIKE '%J' THEN Concat('Feb ',left(si.code_presentation,4))
           WHEN si.code_presentation LIKE '%B' THEN Concat('Oct ',left(si.code_presentation,4))
        END code_presentation_description
FROM studentassessment sa join studentvle sv
on sa.id_student = sv.id_student
join studentinfo si
on si.id_student = sa.id_student
join assessments a
on sa.id_assessment = a.id_assessment
where (sa.id_student, sa.date_submitted) 
in (select id_student, max(date_submitted)
from studentassessment group by id_student)
and sa.date_submitted = sv.date
group by si.final_result,si.code_module, si.code_presentation""")

genderLineQuery = text("""SELECT si.code_presentation,si.gender,count(*) as count,si.code_module, 
		CASE 
           WHEN si.code_presentation LIKE '%J' THEN Concat('Feb ',left(si.code_presentation,4))
           WHEN si.code_presentation LIKE '%B' THEN Concat('Oct ',left(si.code_presentation,4))
        END code_presentation_description
FROM studentassessment sa join studentvle sv
on sa.id_student = sv.id_student
join studentinfo si
on si.id_student = sa.id_student
join assessments a
on sa.id_assessment = a.id_assessment
where (sa.id_student, sa.date_submitted) 
in (select id_student, max(date_submitted)
from studentassessment group by id_student)
and sa.date_submitted = sv.date
group by si.gender,si.code_module, si.code_presentation""")

GenderBarQuery = text("""SELECT si.code_presentation, count(v.activity_type) as login_count, v.activity_type, si.gender,
	CASE 
        WHEN si.code_presentation LIKE '%J' THEN Concat('Feb ',left(si.code_presentation,4))
        WHEN si.code_presentation LIKE '%B' THEN Concat('Oct ',left(si.code_presentation,4))
    END code_presentation_description
    FROM studentassessment sa 
	join studentvle sv
    on sa.id_student = sv.id_student
	join vle v
	on v.id_site = sv.id_site
	join studentinfo si
	on si.id_student = sa.id_student
	join assessments a
	on sa.id_assessment = a.id_assessment
    where (sa.id_student, sa.date_submitted) 
    in (select id_student, max(date_submitted)
    from studentassessment group by id_student)
    and sa.date_submitted = sv.date
	group by si.code_presentation,v.activity_type,si.gender""")

RegionBarQuery = text("""SELECT si.code_presentation, count(v.activity_type) as login_count, v.activity_type, si.region,
	CASE 
        WHEN si.code_presentation LIKE '%J' THEN Concat('Feb ',left(si.code_presentation,4))
        WHEN si.code_presentation LIKE '%B' THEN Concat('Oct ',left(si.code_presentation,4))
    END code_presentation_description
    FROM studentassessment sa 
	join studentvle sv
    on sa.id_student = sv.id_student
	join vle v
	on v.id_site = sv.id_site
	join studentinfo si
	on si.id_student = sa.id_student
	join assessments a
	on sa.id_assessment = a.id_assessment
    where (sa.id_student, sa.date_submitted) 
    in (select id_student, max(date_submitted)
    from studentassessment group by id_student)
    and sa.date_submitted = sv.date
	group by si.code_presentation,v.activity_type,si.region""")

AgeBandBarQuery = text("""SELECT si.code_presentation,count(v.activity_type) as login_count, v.activity_type, si.age_band,
	CASE 
        WHEN si.code_presentation LIKE '%J' THEN Concat('Feb ',left(si.code_presentation,4))
        WHEN si.code_presentation LIKE '%B' THEN Concat('Oct ',left(si.code_presentation,4))
    END code_presentation_description
    FROM studentassessment sa 
	join studentvle sv
    on sa.id_student = sv.id_student
	join vle v
	on v.id_site = sv.id_site
	join studentinfo si
	on si.id_student = sa.id_student
	join assessments a
	on sa.id_assessment = a.id_assessment
    where (sa.id_student, sa.date_submitted) 
    in (select id_student, max(date_submitted)
    from studentassessment group by id_student)
    and sa.date_submitted = sv.date
	group by si.code_presentation,v.activity_type,si.age_band""")

FinalResultBarQuery = text("""SELECT si.code_presentation,count(v.activity_type) as login_count, v.activity_type, si.final_result,
	CASE 
        WHEN si.code_presentation LIKE '%J' THEN Concat('Feb ',left(si.code_presentation,4))
        WHEN si.code_presentation LIKE '%B' THEN Concat('Oct ',left(si.code_presentation,4))
    END code_presentation_description
    FROM studentassessment sa 
	join studentvle sv
    on sa.id_student = sv.id_student
	join vle v
	on v.id_site = sv.id_site
	join studentinfo si
	on si.id_student = sa.id_student
	join assessments a
	on sa.id_assessment = a.id_assessment
    where (sa.id_student, sa.date_submitted) 
    in (select id_student, max(date_submitted)
    from studentassessment group by id_student)
    and sa.date_submitted = sv.date
	group by si.code_presentation,v.activity_type,si.final_result""")

HighestEduBarQuery = text("""SELECT si.code_presentation,count(v.activity_type) as login_count, v.activity_type, si.highest_education,
	CASE 
        WHEN si.code_presentation LIKE '%J' THEN Concat('Feb ',left(si.code_presentation,4))
        WHEN si.code_presentation LIKE '%B' THEN Concat('Oct ',left(si.code_presentation,4))
    END code_presentation_description
    FROM studentassessment sa 
	join studentvle sv
    on sa.id_student = sv.id_student
	join vle v
	on v.id_site = sv.id_site
	join assessments a
	on sa.id_assessment = a.id_assessment
	join studentinfo si
	on si.id_student = sa.id_student
    where (sa.id_student, sa.date_submitted) 
    in (select id_student, max(date_submitted)
    from studentassessment group by id_student)
    and sa.date_submitted = sv.date
	group by si.code_presentation,v.activity_type,si.highest_education""")

GenderScatterQuery = text("""SELECT si.code_presentation, count(*), sa.score,sum(sv.sum_click) as totalClick,si.gender,
	CASE 
        WHEN si.code_presentation LIKE '%J' THEN Concat('Feb ',left(si.code_presentation,4))
        WHEN si.code_presentation LIKE '%B' THEN Concat('Oct ',left(si.code_presentation,4))
    END code_presentation_description
    FROM studentassessment sa 
	join studentvle sv
    on sa.id_student = sv.id_student
    join assessments a
	on sa.id_assessment = a.id_assessment
	join studentinfo si
	on si.id_student = sa.id_student
    where (sa.id_student, sa.date_submitted) 
    in (select id_student, max(date_submitted)
    from studentassessment group by id_student)
    and sa.date_submitted = sv.date
	group by si.code_presentation, sa.score,si.gender""")

RegionScatterQuery = text("""SELECT si.code_presentation, count(*), sa.score,sum(sv.sum_click) as totalClick,si.region,
	CASE 
        WHEN si.code_presentation LIKE '%J' THEN Concat('Feb ',left(si.code_presentation,4))
        WHEN si.code_presentation LIKE '%B' THEN Concat('Oct ',left(si.code_presentation,4))
    END code_presentation_description
    FROM studentassessment sa 
	join studentvle sv
    on sa.id_student = sv.id_student
    join assessments a
	on sa.id_assessment = a.id_assessment
	join studentinfo si
	on si.id_student = sa.id_student
    where (sa.id_student, sa.date_submitted) 
    in (select id_student, max(date_submitted)
    from studentassessment group by id_student)
    and sa.date_submitted = sv.date
	group by si.code_presentation, sa.score,si.region""")

AgeBandScatterQuery = text("""SELECT si.code_presentation, count(*), sa.score,sum(sv.sum_click) as totalClick,si.age_band,
	CASE 
        WHEN si.code_presentation LIKE '%J' THEN Concat('Feb ',left(si.code_presentation,4))
        WHEN si.code_presentation LIKE '%B' THEN Concat('Oct ',left(si.code_presentation,4))
    END code_presentation_description
    FROM studentassessment sa 
	join studentvle sv
    on sa.id_student = sv.id_student
    join assessments a
	on sa.id_assessment = a.id_assessment
	join studentinfo si
	on si.id_student = sa.id_student
    where (sa.id_student, sa.date_submitted) 
    in (select id_student, max(date_submitted)
    from studentassessment group by id_student)
    and sa.date_submitted = sv.date
	group by si.code_presentation, sa.score,si.age_band""")

FinalResultScatterQuery = text("""SELECT si.code_presentation, count(*), sa.score,sum(sv.sum_click) as totalClick,si.final_result,
	CASE 
        WHEN si.code_presentation LIKE '%J' THEN Concat('Feb ',left(si.code_presentation,4))
        WHEN si.code_presentation LIKE '%B' THEN Concat('Oct ',left(si.code_presentation,4))
    END code_presentation_description
    FROM studentassessment sa 
	join studentvle sv
    on sa.id_student = sv.id_student
    join assessments a
	on sa.id_assessment = a.id_assessment
	join studentinfo si
	on si.id_student = sa.id_student
    where (sa.id_student, sa.date_submitted) 
    in (select id_student, max(date_submitted)
    from studentassessment group by id_student)
    and sa.date_submitted = sv.date
	group by si.code_presentation, sa.score,si.final_result""")

HighestEduScatterQuery = text("""SELECT si.code_presentation, count(*), sa.score,sum(sv.sum_click) as totalClick,si.highest_education,
	CASE 
        WHEN si.code_presentation LIKE '%J' THEN Concat('Feb ',left(si.code_presentation,4))
        WHEN si.code_presentation LIKE '%B' THEN Concat('Oct ',left(si.code_presentation,4))
    END code_presentation_description
    FROM studentassessment sa 
	join studentvle sv
    on sa.id_student = sv.id_student
	join studentinfo si
	on si.id_student = sa.id_student
	join assessments a
	on sa.id_assessment = a.id_assessment
    where (sa.id_student, sa.date_submitted) 
    in (select id_student, max(date_submitted)
    from studentassessment group by id_student)
    and sa.date_submitted = sv.date
	group by si.code_presentation, sa.score,si.highest_education""")