# 从flask模块中导入Flask类和render_template函数
from flask import Flask, render_template, jsonify
import pymysql

# 创建一个Flask实例
app = Flask(__name__, static_folder="static",
            template_folder="templates")


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/a1")
def a1():
    conn = pymysql.connect(host="localhost", user="root", password="baidu123", database="test")
    cursor = conn.cursor()
    cursor.execute("select * from exam")
    data = cursor.fetchall()
    exam_map = {"year": [], "num": []}
    for row in data:
        exam_map.get("year").append(row[0])
        exam_map.get("num").append(row[1])
    return jsonify(exam_map)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
