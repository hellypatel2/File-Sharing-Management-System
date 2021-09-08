import mysql.connector
from flask import (
    Flask, render_template, request, session, send_file, make_response,
    current_app, Response, redirect
)
from io import BytesIO
import json
from forms import UploadFileForm
import boto3
from uuid import uuid4
import sns
import db_secret


application = Flask(__name__)
application.secret_key="cloud"
BUCKET_NAME="group14-cloud-project-bucket"


def create_db_schema(conn):
    cursor = conn.cursor()
    cursor.execute("CREATE DATABASE IF NOT EXISTS cloud")
    cursor.fetchall()
    conn.commit()

    cursor.execute("use cloud")
    cursor.fetchall()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS user_details(
        id INT NOT NULL AUTO_INCREMENT,
        name VARCHAR(45) NULL,
        email VARCHAR(45) NOT NULL,
        password VARCHAR(45) NULL,
        sns_topic_arn VARCHAR(100) NULL,
        PRIMARY KEY (id, email)
    )""")

    cursor.execute("""CREATE TABLE IF NOT EXISTS file_uploads (
        id INT NOT NULL AUTO_INCREMENT,
        user_id INT NULL,
        file_name VARCHAR(255) NULL,
        orig_file_name VARCHAR(255) NULL,
        mimetype VARCHAR(255) NULL,
        size INT NULL,
        PRIMARY KEY (id),
        FOREIGN KEY(user_id) REFERENCES user_details(id)
    )""")

    cursor.execute("""CREATE TABLE IF NOT EXISTS shared_files (
        id INT NOT NULL AUTO_INCREMENT,
        file_id INT NOT NULL,
        shared_by INT NOT NULL,
        shared_with INT NOT NULL,
        PRIMARY KEY(id),
        FOREIGN KEY (file_id) REFERENCES file_uploads(id),
        FOREIGN KEY (shared_by) REFERENCES user_details(id),
        FOREIGN KEY (shared_with) REFERENCES user_details(id)
    )""")
    cursor.fetchall()
    conn.commit()
    cursor.close()


def get_db_connection():
    conn_credentials = json.loads(db_secret.get_secret())

    conn = mysql.connector.connect(
        host=conn_credentials["host"],
        port=conn_credentials["port"],
        username=conn_credentials["username"],
        password=conn_credentials["password"],
    )
    create_db_schema(conn)
    return conn


def get_user_by_email(user_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * from user_details where email=%s",(user_id,))
    rows = cursor.fetchall()
    if not rows:
        return None
    user = rows[0]
    cursor.close()
    conn.close()
    return user


@application.route("/")
def register():
    return render_template('register.html')


@application.route("/addUser", methods=["POST"])
def add_user():
    entered_name = request.form.get("name")
    entered_email = request.form.get("email")
    entered_password = request.form.get("password")
    conn = get_db_connection()
    cursor = conn.cursor()
    sql1 = "SELECT * FROM user_details where Email=%s"
    val1 = (entered_email,)
    cursor.execute(sql1, val1)
    exist = cursor.fetchall()
    if len(exist) >= 1:
        message = "User already exists"
        cursor.close()
        conn.close()
        return render_template("register.html", success_message=message)
    else:
        topic_arn = sns.create_and_subscribe_topic(entered_email)
        sql = """INSERT INTO user_details (name, email, password, sns_topic_arn)
                 VALUES (%s, %s,%s, %s)"""
        params = (entered_name, entered_email, entered_password, topic_arn)
        cursor.execute(sql, params)
        conn.commit()
        message = "User added successfully"
        cursor.close()
        conn.close()
        return render_template("register.html", success_message=message)


@application.route("/login", methods=["GET"])
def login():
    return render_template('login.html')


@application.route("/validateUser", methods=["POST"])
def validateUser():
    entered_email = request.form.get("email")
    entered_password = request.form.get("password")
    sql = "SELECT id FROM user_details where email=%s"
    check_password="SELECT password FROM user_details where email=%s"
    val = (entered_email,)
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(check_password, val)
    password_val=cursor.fetchone()
    cursor.execute(sql, val)
    user = cursor.fetchall()
    cursor.close()
    conn.close()
    if user==0:
        return render_template('login.html', msg="User does not exists")
    elif password_val[0]!=entered_password:
        return render_template('login.html', msg="Password incorrect")
    else:
        session["user"] = entered_email
        return redirect("/dashboard")


@application.route("/dashboard")
def dashboard():
    if "user" in session:
        return render_template('dashboard.html')
    else:
        return render_template('login.html')


@application.route("/logout")
def logout():
    session.pop("user",None)
    return redirect('/login')


@application.route("/upload", methods=["GET"])
def upload_file_form():
    return render_template("upload.html")


@application.route("/upload", methods=["POST"])
def upload_file():
    user_email = session["user"]
    form = UploadFileForm(request.files)
    s3 = boto3.client("s3")
    if not form.validate():
        print(form.errors)
        return "Error in form", 400

    uploaded_file = form.file.data
    file_body = uploaded_file.read()
    file_name = "%s.%s"%(uploaded_file.filename, str(uuid4()))
    s3.put_object(
        Bucket=BUCKET_NAME,
        Body=file_body,
        Key=file_name,
        ContentLength=len(file_body),
        ContentType=uploaded_file.mimetype,
    )
    conn = get_db_connection()
    cursor = conn.cursor()
    query = """Insert into file_uploads(user_id, file_name, orig_file_name, mimetype, size)
    values(%s, %s, %s, %s, %s);
    """
    user = get_user_by_email(user_email)
    query_params = (user[0], file_name, uploaded_file.filename, uploaded_file.mimetype, len(file_body))
    cursor.execute(query, query_params)
    cursor.close()
    conn.commit()
    conn.close()
    sns.publish_to_topic(user[4], "File uploaded", f"Your file {uploaded_file.filename} is ready")
    return redirect("/show-uploads")


@application.route("/show-uploads", methods=["GET"])
def show_uploads():
    user_email = session["user"]
    current_user = get_user_by_email(user_email)
    query = "select * from file_uploads where user_id=%s"
    params = (current_user[0],)
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(query, params)
    my_files = cursor.fetchall()

    # id | user_id | file_name | orig_file_name | mimetype | size | id | file_id | shared_by | shared_with |
    cursor.execute(
        "select * from file_uploads fu join shared_files sf on sf.file_id=fu.id and sf.shared_by=%s",
        (current_user[0],)
    )
    shared_by_me = cursor.fetchall()

    cursor.execute(
        "select * from file_uploads fu join shared_files sf on sf.file_id=fu.id and sf.shared_with=%s",
        (current_user[0],)
    )
    shared_with_me = cursor.fetchall()
    user_list = {0,-1}
    user_list = user_list.union([u[8] for u in shared_by_me])
    user_list = user_list.union([u[9] for u in shared_by_me])
    user_list = user_list.union(u[8] for u in shared_with_me)
    user_list = user_list.union(u[9] for u in shared_with_me)

    cursor.execute("select * from user_details where id in %s" % ((str(tuple(user_list)))))
    users = cursor.fetchall()
    user_map = {user[0]: user for user in users}
    cursor.close()
    conn.close()

    return render_template(
        "uploads.html",
        my_files=my_files,
        shared_by_me=shared_by_me,
        shared_with_me=shared_with_me,
        user_map=user_map,
    )


@application.route("/file/<id>")
def serve_file(id):
    user_email = session["user"]

    user = get_user_by_email(user_email)
    query = "select * from file_uploads where id=%s"
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(query, (id,))
    file = cursor.fetchall()[0]

    cursor.execute("select shared_with from shared_files where shared_with=%s", (user[0],))
    shared_users = {u[0] for u in cursor.fetchall()}
    cursor.close()
    conn.close()

    if file[1] != user[0] and user[0] not in shared_users:
        return "Access denied", 400
    s3 = boto3.client("s3")
    resp = s3.get_object(
        Bucket=BUCKET_NAME,
        Key=f"{file[2]}.gz",
    )

    compressed_file = BytesIO(resp["Body"].read())
    resp = Response(compressed_file, headers={
        "Content-Type": file[4],
        "Content-Encoding": "gzip",
        "Content-Disposition": f"attachment; filename={file[3]}"
    })

    return resp


@application.route("/share", methods=["POST"])
def share_file_handler():
    share_file_id = request.json["file_id"]
    share_with_email = request.json["share_with_email"]

    # get users from db
    current_user = get_user_by_email(session["user"])
    share_with_user = get_user_by_email(share_with_email)
    if not share_with_user:
        return "Unknown user", 404
    if share_with_user[0] == current_user[0]:
        return "Cannot share with self", 404

    conn = get_db_connection()
    cursor = conn.cursor()
    exists_query = cursor.execute(
        "select * from shared_files where shared_by=%s and shared_with=%s and file_id=%s",
        (current_user[0], share_with_user[0], share_file_id)
    )
    rows = cursor.fetchall()
    if not rows:
        sns.publish_to_topic(share_with_user[4],
            "File Shared",
            f"A new file has been shared by {current_user[2]}",
        )
        insert_query = """INSERT into shared_files(file_id, shared_by, shared_with)
                    values(%s, %s, %s)"""
        insert_params = (share_file_id, current_user[0], share_with_user[0])
        cursor.execute(insert_query, insert_params)
        conn.commit()

    cursor.close()
    conn.close()
    return "File shared", 200


@application.route("/share/<share_id>", methods=["DELETE"])
def remove_access(share_id):
    current_user = get_user_by_email(session["user"])

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("delete from shared_files where id=%s", (share_id,))
    conn.commit()
    cursor.close()
    conn.close()
    return "access removed", 200


if __name__ == '__main__':
    application.run()
