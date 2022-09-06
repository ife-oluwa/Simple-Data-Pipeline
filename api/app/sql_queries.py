get_users_by_department_company = """
SELECT u.id, u.firstname, u.lastname, u.email, u.Phone1, u.Phone2,
u.zip_code, u.Address, u.City, u.state, d.name as department, c.name as company
FROM users u
INNER JOIN companies c
on c.id = u.company_id
INNER JOIN departments d
ON d.id = u.department_id
WHERE u.company_id = {company} AND u.department_id = {department}
LIMIT {limit};
"""
