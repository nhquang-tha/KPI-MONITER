import os
from datetime import datetime
from flask import Flask, render_template_string, request, redirect, url_for, flash
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
from werkzeug.security import generate_password_hash, check_password_hash

# --- CẤU HÌNH APP ---
app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'bi_mat_khong_the_bat_mi') # Cần cho session login

# Cấu hình DB
app.config['SQLALCHEMY_DATABASE_URI'] = os.environ.get('DATABASE_URL', 'sqlite:///local.db')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'

# --- MODELS (BẢNG DỮ LIỆU) ---

class User(UserMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(50), unique=True, nullable=False)
    password_hash = db.Column(db.String(255), nullable=False)
    role = db.Column(db.String(20), default='user') # 'admin' hoặc 'user'
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    def set_password(self, password):
        self.password_hash = generate_password_hash(password)

    def check_password(self, password):
        return check_password_hash(self.password_hash, password)

# --- LOGIN MANAGER ---
@login_manager.user_loader
def load_user(user_id):
    return db.session.get(User, int(user_id))

# --- HTML TEMPLATES (Giao diện Material Design) ---

# CSS & Layout chung
BASE_LAYOUT = """
<!DOCTYPE html>
<html lang="vi">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>KPI Monitor System</title>
    <!-- Bootstrap 5 CSS -->
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    <!-- FontAwesome Icons -->
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
    <style>
        body { background-color: #f0f2f5; font-family: 'Roboto', sans-serif; overflow-x: hidden; }
        
        /* Sidebar Styles */
        .sidebar {
            height: 100vh;
            width: 250px;
            position: fixed;
            top: 0;
            left: 0;
            background-color: #ffffff;
            box-shadow: 2px 0 5px rgba(0,0,0,0.05);
            transition: all 0.3s;
            z-index: 1000;
        }
        .sidebar-header {
            padding: 20px;
            background: #0d6efd;
            color: white;
            text-align: center;
        }
        .sidebar-menu { padding: 10px 0; list-style: none; margin: 0; }
        .sidebar-menu li { padding: 0; }
        .sidebar-menu a {
            display: block;
            padding: 12px 20px;
            color: #333;
            text-decoration: none;
            transition: 0.3s;
            border-left: 4px solid transparent;
        }
        .sidebar-menu a:hover, .sidebar-menu a.active {
            background-color: #e9ecef;
            border-left-color: #0d6efd;
            color: #0d6efd;
        }
        .sidebar-menu i { margin-right: 10px; width: 20px; text-align: center; }

        /* Main Content */
        .main-content {
            margin-left: 250px;
            padding: 20px;
            transition: all 0.3s;
        }
        
        /* Material Cards */
        .card {
            border: none;
            border-radius: 8px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.05);
            background: white;
            margin-bottom: 20px;
        }
        .card-header {
            background-color: white;
            border-bottom: 1px solid #f0f0f0;
            padding: 15px 20px;
            font-weight: bold;
            color: #444;
        }

        /* Responsive */
        @media (max-width: 768px) {
            .sidebar { margin-left: -250px; }
            .sidebar.active { margin-left: 0; }
            .main-content { margin-left: 0; }
        }
        
        /* Utility */
        .btn-material { border-radius: 4px; text-transform: uppercase; font-weight: 500; font-size: 0.85rem; letter-spacing: 0.5px; }
        .alert-float { position: fixed; top: 20px; right: 20px; z-index: 9999; }
    </style>
</head>
<body>

    <!-- Sidebar -->
    <div class="sidebar" id="sidebar">
        <div class="sidebar-header">
            <h4><i class="fa-solid fa-network-wired"></i> KPI Monitor</h4>
        </div>
        <ul class="sidebar-menu">
            <li><a href="/" class="{{ 'active' if active_page == 'dashboard' else '' }}"><i class="fa-solid fa-gauge"></i> Dashboard</a></li>
            <li><a href="/kpi" class="{{ 'active' if active_page == 'kpi' else '' }}"><i class="fa-solid fa-chart-line"></i> KPI</a></li>
            <li><a href="/rf" class="{{ 'active' if active_page == 'rf' else '' }}"><i class="fa-solid fa-tower-broadcast"></i> RF</a></li>
            <li><a href="/poi" class="{{ 'active' if active_page == 'poi' else '' }}"><i class="fa-solid fa-map-location-dot"></i> POI</a></li>
            <li><a href="/worst-cell" class="{{ 'active' if active_page == 'worst_cell' else '' }}"><i class="fa-solid fa-triangle-exclamation"></i> Worst Cell</a></li>
            <li><a href="/conges-3g" class="{{ 'active' if active_page == 'conges_3g' else '' }}"><i class="fa-solid fa-users-slash"></i> Conges 3G</a></li>
            <li><a href="/traffic-down" class="{{ 'active' if active_page == 'traffic_down' else '' }}"><i class="fa-solid fa-arrow-trend-down"></i> Traffic Down</a></li>
            <li><a href="/script" class="{{ 'active' if active_page == 'script' else '' }}"><i class="fa-solid fa-code"></i> Script</a></li>
            <li><a href="/import" class="{{ 'active' if active_page == 'import' else '' }}"><i class="fa-solid fa-file-import"></i> Import</a></li>
            
            <li class="mt-3 text-muted ps-3"><small>HỆ THỐNG</small></li>
            
            {% if current_user.role == 'admin' %}
            <li><a href="/users" class="{{ 'active' if active_page == 'users' else '' }}"><i class="fa-solid fa-users-gear"></i> Quản lý User</a></li>
            {% endif %}
            
            <li><a href="/profile" class="{{ 'active' if active_page == 'profile' else '' }}"><i class="fa-solid fa-user-shield"></i> Tài khoản ({{ current_user.username }})</a></li>
            <li><a href="/logout"><i class="fa-solid fa-right-from-bracket"></i> Đăng xuất</a></li>
        </ul>
    </div>

    <!-- Main Content -->
    <div class="main-content">
        <!-- Toggle btn for mobile -->
        <button class="btn btn-primary d-md-none mb-3" onclick="document.getElementById('sidebar').classList.toggle('active')">
            <i class="fa-solid fa-bars"></i> Menu
        </button>

        <!-- Flash Messages -->
        <div class="container-fluid">
            {% with messages = get_flashed_messages(with_categories=true) %}
                {% if messages %}
                    {% for category, message in messages %}
                        <div class="alert alert-{{ category }} alert-dismissible fade show" role="alert">
                            {{ message }}
                            <button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>
                        </div>
                    {% endfor %}
                {% endif %}
            {% endwith %}
            
            <!-- Page Content Injection -->
            {% block content %}{% endblock %}
        </div>
    </div>

    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/js/bootstrap.bundle.min.js"></script>
</body>
</html>
"""

# Trang Login (Không dùng layout chung)
LOGIN_PAGE = """
<!DOCTYPE html>
<html lang="vi">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Đăng nhập - KPI Monitor</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    <style>
        body { background: #e0e5ec; height: 100vh; display: flex; align-items: center; justify-content: center; }
        .login-card {
            width: 100%; max-width: 400px;
            background: white; padding: 40px; border-radius: 10px;
            box-shadow: 0 10px 25px rgba(0,0,0,0.05);
        }
        .login-header { text-align: center; margin-bottom: 30px; color: #0d6efd; }
    </style>
</head>
<body>
    <div class="login-card">
        <h3 class="login-header"><i class="fa-solid fa-network-wired"></i> Đăng nhập</h3>
        {% with messages = get_flashed_messages(with_categories=true) %}
            {% if messages %}
                {% for category, message in messages %}
                    <div class="alert alert-{{ category }}">{{ message }}</div>
                {% endfor %}
            {% endif %}
        {% endwith %}
        <form method="POST">
            <div class="mb-3">
                <label class="form-label">Tên đăng nhập</label>
                <input type="text" name="username" class="form-control" required autofocus>
            </div>
            <div class="mb-3">
                <label class="form-label">Mật khẩu</label>
                <input type="password" name="password" class="form-control" required>
            </div>
            <button type="submit" class="btn btn-primary w-100 py-2">Đăng nhập</button>
        </form>
    </div>
</body>
</html>
"""

# Trang Dashboard & Nội dung các menu
CONTENT_TEMPLATE = """
{% extends "base" %}

{% block content %}
<div class="row">
    <div class="col-12">
        <div class="card">
            <div class="card-header d-flex justify-content-between align-items-center">
                <span>{{ title }}</span>
                <span class="badge bg-primary">{{ current_user.role }}</span>
            </div>
            <div class="card-body">
                {% if active_page == 'dashboard' %}
                    <div class="row g-4">
                        <div class="col-md-3">
                            <div class="p-3 bg-light rounded text-center border">
                                <h3 class="text-primary">98.5%</h3>
                                <p class="mb-0 text-muted">KPI Tuần</p>
                            </div>
                        </div>
                        <div class="col-md-3">
                            <div class="p-3 bg-light rounded text-center border">
                                <h3 class="text-danger">12</h3>
                                <p class="mb-0 text-muted">Worst Cells</p>
                            </div>
                        </div>
                        <div class="col-md-3">
                            <div class="p-3 bg-light rounded text-center border">
                                <h3 class="text-warning">5</h3>
                                <p class="mb-0 text-muted">Congestion 3G</p>
                            </div>
                        </div>
                        <div class="col-md-3">
                            <div class="p-3 bg-light rounded text-center border">
                                <h3 class="text-success">OK</h3>
                                <p class="mb-0 text-muted">System Status</p>
                            </div>
                        </div>
                    </div>
                    <hr>
                    <p>Chào mừng <strong>{{ current_user.username }}</strong> quay trở lại hệ thống giám sát.</p>
                {% else %}
                    <div class="text-center py-5 text-muted">
                        <i class="fa-solid fa-person-digging fa-3x mb-3"></i>
                        <h5>Chức năng {{ title }} đang được xây dựng</h5>
                        <p>Dữ liệu sẽ sớm được cập nhật tại đây.</p>
                    </div>
                {% endif %}
            </div>
        </div>
    </div>
</div>
{% endblock %}
"""

# Trang Quản lý User (Admin Only)
USER_MANAGEMENT_TEMPLATE = """
{% extends "base" %}

{% block content %}
<div class="row">
    <!-- Form Thêm User -->
    <div class="col-md-4">
        <div class="card">
            <div class="card-header">Thêm User Mới</div>
            <div class="card-body">
                <form action="/users/add" method="POST">
                    <div class="mb-3">
                        <label class="form-label">Username</label>
                        <input type="text" name="username" class="form-control" required>
                    </div>
                    <div class="mb-3">
                        <label class="form-label">Password</label>
                        <input type="password" name="password" class="form-control" required>
                    </div>
                    <div class="mb-3">
                        <label class="form-label">Quyền hạn</label>
                        <select name="role" class="form-select">
                            <option value="user">User (Xem báo cáo)</option>
                            <option value="admin">Admin (Toàn quyền)</option>
                        </select>
                    </div>
                    <button type="submit" class="btn btn-success w-100">Tạo User</button>
                </form>
            </div>
        </div>
    </div>

    <!-- Danh sách User -->
    <div class="col-md-8">
        <div class="card">
            <div class="card-header">Danh sách người dùng</div>
            <div class="card-body p-0">
                <table class="table table-hover mb-0">
                    <thead class="table-light">
                        <tr>
                            <th>ID</th>
                            <th>Username</th>
                            <th>Role</th>
                            <th>Ngày tạo</th>
                            <th>Thao tác</th>
                        </tr>
                    </thead>
                    <tbody>
                        {% for u in users %}
                        <tr>
                            <td>{{ u.id }}</td>
                            <td>{{ u.username }}</td>
                            <td>
                                <span class="badge bg-{{ 'danger' if u.role == 'admin' else 'info' }}">{{ u.role }}</span>
                            </td>
                            <td>{{ u.created_at.strftime('%Y-%m-%d') }}</td>
                            <td>
                                {% if u.username != 'admin' %}
                                <a href="/users/delete/{{ u.id }}" class="btn btn-sm btn-outline-danger" onclick="return confirm('Xóa user này?')">Xóa</a>
                                <button class="btn btn-sm btn-outline-warning" onclick="promptReset({{ u.id }}, '{{ u.username }}')">Đổi Pass</button>
                                {% else %}
                                <span class="text-muted small">System Admin</span>
                                {% endif %}
                            </td>
                        </tr>
                        {% endfor %}
                    </tbody>
                </table>
            </div>
        </div>
    </div>
</div>

<script>
function promptReset(uid, uname) {
    let newPass = prompt("Nhập mật khẩu mới cho user " + uname + ":");
    if (newPass) {
        window.location.href = "/users/reset-pass/" + uid + "?new_pass=" + encodeURIComponent(newPass);
    }
}
</script>
{% endblock %}
"""

# Trang Đổi mật khẩu cá nhân
PROFILE_TEMPLATE = """
{% extends "base" %}
{% block content %}
<div class="row justify-content-center">
    <div class="col-md-6">
        <div class="card">
            <div class="card-header">Thông tin tài khoản</div>
            <div class="card-body">
                <p><strong>Username:</strong> {{ current_user.username }}</p>
                <p><strong>Role:</strong> {{ current_user.role }}</p>
                <hr>
                <h5>Đổi mật khẩu</h5>
                <form action="/change-password" method="POST">
                    <div class="mb-3">
                        <label>Mật khẩu hiện tại</label>
                        <input type="password" name="current_password" class="form-control" required>
                    </div>
                    <div class="mb-3">
                        <label>Mật khẩu mới</label>
                        <input type="password" name="new_password" class="form-control" required>
                    </div>
                    <button type="submit" class="btn btn-primary">Cập nhật mật khẩu</button>
                </form>
            </div>
        </div>
    </div>
</div>
{% endblock %}
"""

# Kết hợp template inheritance giả lập
def render_page(template_content, **kwargs):
    full_template = template_content.replace('{% extends "base" %}', BASE_LAYOUT)
    return render_template_string(full_template, **kwargs)

# --- ROUTES ---

@app.route('/login', methods=['GET', 'POST'])
def login():
    if current_user.is_authenticated:
        return redirect(url_for('index'))
        
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        user = User.query.filter_by(username=username).first()
        
        if user and user.check_password(password):
            login_user(user)
            return redirect(url_for('index'))
        else:
            flash('Sai tên đăng nhập hoặc mật khẩu!', 'danger')
            
    return render_template_string(LOGIN_PAGE)

@app.route('/logout')
@login_required
def logout():
    logout_user()
    flash('Đã đăng xuất thành công.', 'info')
    return redirect(url_for('login'))

@app.route('/')
@login_required
def index():
    return render_page(CONTENT_TEMPLATE, title="Dashboard Tổng quan", active_page='dashboard')

# --- MENU ROUTES (KPI, RF, POI...) ---
@app.route('/kpi')
@login_required
def kpi():
    return render_page(CONTENT_TEMPLATE, title="Báo cáo KPI", active_page='kpi')

@app.route('/rf')
@login_required
def rf():
    return render_page(CONTENT_TEMPLATE, title="Thông tin RF", active_page='rf')

@app.route('/poi')
@login_required
def poi():
    return render_page(CONTENT_TEMPLATE, title="Quản lý POI", active_page='poi')

@app.route('/worst-cell')
@login_required
def worst_cell():
    return render_page(CONTENT_TEMPLATE, title="Worst Cell Monitor", active_page='worst_cell')

@app.route('/conges-3g')
@login_required
def conges_3g():
    return render_page(CONTENT_TEMPLATE, title="Nghẽn mạng 3G (Congestion)", active_page='conges_3g')

@app.route('/traffic-down')
@login_required
def traffic_down():
    return render_page(CONTENT_TEMPLATE, title="Cảnh báo Traffic Down", active_page='traffic_down')

@app.route('/script')
@login_required
def script():
    return render_page(CONTENT_TEMPLATE, title="Kho Script", active_page='script')

@app.route('/import')
@login_required
def import_data():
    return render_page(CONTENT_TEMPLATE, title="Import Dữ liệu", active_page='import')

# --- USER MANAGEMENT ROUTES ---

@app.route('/users')
@login_required
def manage_users():
    if current_user.role != 'admin':
        flash('Bạn không có quyền truy cập trang này!', 'danger')
        return redirect(url_for('index'))
    
    users = User.query.all()
    return render_page(USER_MANAGEMENT_TEMPLATE, users=users, active_page='users')

@app.route('/users/add', methods=['POST'])
@login_required
def add_user():
    if current_user.role != 'admin':
        return redirect(url_for('index'))
    
    username = request.form['username']
    password = request.form['password']
    role = request.form['role']
    
    if User.query.filter_by(username=username).first():
        flash('Username đã tồn tại!', 'warning')
    else:
        new_user = User(username=username, role=role)
        new_user.set_password(password)
        db.session.add(new_user)
        db.session.commit()
        flash(f'Đã tạo user {username} thành công!', 'success')
        
    return redirect(url_for('manage_users'))

@app.route('/users/delete/<int:user_id>')
@login_required
def delete_user(user_id):
    if current_user.role != 'admin':
        return redirect(url_for('index'))
        
    user = db.session.get(User, user_id)
    if user:
        if user.username == 'admin':
            flash('Không thể xóa tài khoản Admin gốc!', 'danger')
        else:
            db.session.delete(user)
            db.session.commit()
            flash('Đã xóa user thành công.', 'success')
    return redirect(url_for('manage_users'))

@app.route('/users/reset-pass/<int:user_id>')
@login_required
def reset_pass_admin(user_id):
    if current_user.role != 'admin':
        return redirect(url_for('index'))
        
    new_pass = request.args.get('new_pass')
    user = db.session.get(User, user_id)
    if user and new_pass:
        user.set_password(new_pass)
        db.session.commit()
        flash(f'Đã đổi mật khẩu cho {user.username}', 'success')
    return redirect(url_for('manage_users'))

@app.route('/profile')
@login_required
def profile():
    return render_page(PROFILE_TEMPLATE, active_page='profile')

@app.route('/change-password', methods=['POST'])
@login_required
def change_password():
    current_pass = request.form['current_password']
    new_pass = request.form['new_password']
    
    if current_user.check_password(current_pass):
        current_user.set_password(new_pass)
        db.session.commit()
        flash('Đổi mật khẩu thành công!', 'success')
    else:
        flash('Mật khẩu hiện tại không đúng.', 'danger')
    return redirect(url_for('profile'))

# --- INIT ---
def create_default_admin():
    # Tạo bảng nếu chưa có
    db.create_all()
    # Kiểm tra xem có admin chưa
    if not User.query.filter_by(username='admin').first():
        admin = User(username='admin', role='admin')
        admin.set_password('admin123') # Mật khẩu mặc định
        db.session.add(admin)
        db.session.commit()
        print(">>> Đã tạo tài khoản mặc định: admin / admin123")

if __name__ == '__main__':
    with app.app_context():
        create_default_admin()
    app.run(debug=True)
