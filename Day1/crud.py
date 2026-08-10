from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import mysql.connector

app = FastAPI()


# -------------------------
# MySQL Connection
# -------------------------

db = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Reddy@2004",
    database="crud"
)

cursor = db.cursor()


# -------------------------
# Pydantic Model
# -------------------------

class User(BaseModel):
    name: str
    age: int


# -------------------------
# CREATE - POST
# -------------------------

@app.post("/users")
def create_user(user: User):

    sql = "INSERT INTO users (name, age) VALUES (%s, %s)"
    values = (user.name, user.age)

    cursor.execute(sql, values)
    db.commit()

    return {
        "message": "User created successfully"
    }


# -------------------------
# READ - GET
# -------------------------

@app.get("/users")
def get_users():

    cursor.execute("SELECT * FROM users")

    users = cursor.fetchall()

    result = []

    for user in users:
        result.append({
            "id": user[0],
            "name": user[1],
            "age": user[2]
        })

    return result


# -------------------------
# READ ONE USER
# -------------------------

@app.get("/users/{user_id}")
def get_user(user_id: int):

    sql = "SELECT * FROM users WHERE id=%s"

    cursor.execute(sql, (user_id,))

    user = cursor.fetchone()

    if user is None:
        raise HTTPException(
            status_code=404,
            detail="User not found"
        )

    return {
        "id": user[0],
        "name": user[1],
        "age": user[2]
    }


# -------------------------
# UPDATE - PUT
# -------------------------

@app.put("/users/{user_id}")
def update_user(user_id: int, user: User):

    sql = """
    UPDATE users
    SET name=%s, age=%s
    WHERE id=%s
    """

    values = (
        user.name,
        user.age,
        user_id
    )

    cursor.execute(sql, values)

    if cursor.rowcount == 0:
        raise HTTPException(
            status_code=404,
            detail="User not found"
        )

    db.commit()

    return {
        "message": "User updated successfully"
    }


# -------------------------
# DELETE - DELETE
# -------------------------

@app.delete("/users/{user_id}")
def delete_user(user_id: int):

    sql = "DELETE FROM users WHERE id=%s"

    cursor.execute(sql, (user_id,))

    if cursor.rowcount == 0:
        raise HTTPException(
            status_code=404,
            detail="User not found"
        )

    db.commit()

    return {
        "message": "User deleted successfully"
    }