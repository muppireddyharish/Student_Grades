CREATE OR REPLACE VIEW STUDENTS_SEMANTIC.STUDENTS.VW_STUDENTS(
	STUDENT_ID,
	NAME,
	MATH,
	SCIENCE,
	HISTORY,
	ENGLISH,
	MISSED_DAYS
) AS
SELECT * FROM STUDENTS_STAGING.STUDENTS_STG.STUDENTS_MERGED_DB WHERE MATH > 90;