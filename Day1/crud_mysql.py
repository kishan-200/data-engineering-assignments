import mysql.connector

# Connect to MySQL
db = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Reddy@2004",
    database="crud"
)

cursor = db.cursor()

# CREATE - Insert data
def create_user(name, age):
    sql = "INSERT INTO users (name, age) VALUES (%s, %s)"
    values = (name, age)

    cursor.execute(sql, values)
    db.commit()

    print("User inserted successfully")


# READ - Fetch data
def get_users():
    cursor.execute("SELECT * FROM users")

    users = cursor.fetchall()

    for user in users:
        print(user)


# UPDATE - Update data
def update_user(user_id, name, age):
    sql = "UPDATE users SET name=%s, age=%s WHERE id=%s"
    values = (name, age, user_id)

    cursor.execute(sql, values)
    db.commit()

    print("User updated successfully")


# DELETE - Delete data
def delete_user(user_id):
    sql = "DELETE FROM users WHERE id=%s"
    values = (user_id,)

    cursor.execute(sql, values)
    db.commit()

    print("User deleted successfully")


# Example usage
create_user("Annem Venkata Kishan Kumar Reddy", 24)

print("Users:")
get_users()

update_user(1, "kishan", 22)

delete_user(1)

# Close connection
cursor.close()
db.close()