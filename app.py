import os
from flask import Flask, render_template_string, request, redirect, url_for
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.sql import text

# Khởi tạo ứng dụng Flask
app = Flask(__name__)

# Cấu hình kết nối Database
# Trên Render, chúng ta sẽ đặt biến môi trường DATABASE_URL
# Định dạng: mysql+pymysql://user:password@host:port/dbname?ssl_ca=/etc/ssl/certs/ca-certificates.crt
app.config['SQLALCHEMY_DATABASE_URI'] = os.environ.get('DATABASE_URL', 'sqlite:///local.db')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)

# Định nghĩa bảng dữ liệu (Model)
class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False)
    email = db.Column(db.String(120), unique=True, nullable=False)

    def __repr__(self):
        return f'<User {self.username}>'

# Tạo bảng tự động nếu chưa có (chạy khi app khởi động)
with app.app_context():
    try:
        db.create_all()
        print("Đã kết nối DB và tạo bảng thành công!")
    except Exception as e:
        print(f"Lỗi kết nối DB: {e}")

# HTML Template đơn giản (nhúng trực tiếp để tiện demo 1 file)
HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="vi">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Python + Render + TiDB</title>
    <style>
        body { font-family: sans-serif; max-width: 800px; margin: 40px auto; padding: 20px; line-height: 1.6; }
        h1 { color: #333; }
        form { background: #f4f4f4; padding: 20px; border-radius: 8px; margin-bottom: 20px; }
        input { padding: 8px; margin-right: 10px; border: 1px solid #ddd; border-radius: 4px; }
        button { padding: 8px 15px; background: #28a745; color: white; border: none; border-radius: 4px; cursor: pointer; }
        button:hover { background: #218838; }
        ul { list-style: none; padding: 0; }
        li { background: #fff; border-bottom: 1px solid #eee; padding: 10px; display: flex; justify-content: space-between; }
        .tag { background: #007bff; color: white; padding: 2px 6px; border-radius: 4px; font-size: 0.8em; }
    </style>
</head>
<body>
    <h1>Demo TiDB Cloud + Render</h1>
    
    <form action="/add" method="POST">
        <h3>Thêm người dùng mới</h3>
        <input type="text" name="username" placeholder="Tên đăng nhập" required>
        <input type="email" name="email" placeholder="Email" required>
        <button type="submit">Thêm vào Database</button>
    </form>

    <h3>Danh sách người dùng (Lấy từ TiDB)</h3>
    <ul>
        {% for user in users %}
        <li>
            <span><strong>{{ user.username }}</strong> - {{ user.email }}</span>
            <span class="tag">ID: {{ user.id }}</span>
        </li>
        {% else %}
        <li>Chưa có dữ liệu nào trong database.</li>
        {% endfor %}
    </ul>
</body>
</html>
"""

@app.route('/')
def index():
    try:
        users = User.query.order_by(User.id.desc()).all()
        return render_template_string(HTML_TEMPLATE, users=users)
    except Exception as e:
        return f"Lỗi truy vấn dữ liệu: {str(e)}", 500

@app.route('/add', methods=['POST'])
def add_user():
    username = request.form.get('username')
    email = request.form.get('email')
    if username and email:
        try:
            new_user = User(username=username, email=email)
            db.session.add(new_user)
            db.session.commit()
        except Exception as e:
            return f"Lỗi khi thêm dữ liệu (có thể trùng email/username): {str(e)}"
    return redirect(url_for('index'))

if __name__ == '__main__':
    # Chạy cục bộ
    app.run(debug=True)