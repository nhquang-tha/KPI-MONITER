import os
import jinja2
import pandas as pd # Thêm thư viện xử lý dữ liệu
from datetime import datetime
from flask import Flask, render_template_string, request, redirect, url_for, flash
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.utils import secure_filename
from sqlalchemy import text 

# --- CẤU HÌNH APP ---
app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'bi_mat_khong_the_bat_mi')

# Cấu hình DB
app.config['SQLALCHEMY_DATABASE_URI'] = os.environ.get('DATABASE_URL', 'sqlite:///local.db')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
# Tự động ping DB để tránh lỗi "Gone away"
app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {
    'pool_recycle': 280,
    'pool_pre_ping': True
}

db = SQLAlchemy(app)
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'

# --- MODELS (USER) ---
class User(UserMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(50), unique=True, nullable=False)
    password_hash = db.Column(db.String(255), nullable=False)
    role = db.Column(db.String(20), default='user')
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    def set_password(self, password):
        self.password_hash = generate_password_hash(password)

    def check_password(self, password):
        return check_password_hash(self.password_hash, password)

# --- MODELS (RF DATA) ---
# Định nghĩa các bảng RF giống file Excel

class RF3G(db.Model):
    __tablename__ = 'rf_3g'
    id = db.Column(db.Integer, primary_key=True)
    csht_code = db.Column(db.String(50))
    cell_name = db.Column(db.String(100))
    cell_code = db.Column(db.String(50))
    site_code = db.Column(db.String(50))
    latitude = db.Column(db.Float)
    longitude = db.Column(db.Float)
    equipment = db.Column(db.String(50))
    frequency = db.Column(db.String(50)) # Excel: Frenquency
    psc = db.Column(db.String(50))
    dl_uarfcn = db.Column(db.String(50))
    bsc_lac = db.Column(db.String(50))
    ci = db.Column(db.String(50))
    anten_height = db.Column(db.Float)
    azimuth = db.Column(db.Integer)
    m_t = db.Column(db.Float)
    e_t = db.Column(db.Float)
    total_tilt = db.Column(db.Float)
    hang_sx = db.Column(db.String(50)) # Excel: Hãng_SX
    antena = db.Column(db.String(100))
    swap = db.Column(db.String(50))
    start_day = db.Column(db.String(50))
    ghi_chu = db.Column(db.String(255)) # Excel: Ghi_chú

class RF4G(db.Model):
    __tablename__ = 'rf_4g'
    id = db.Column(db.Integer, primary_key=True)
    csht_code = db.Column(db.String(50))
    cell_name = db.Column(db.String(100))
    cell_code = db.Column(db.String(50))
    site_code = db.Column(db.String(50))
    latitude = db.Column(db.Float)
    longitude = db.Column(db.Float)
    equipment = db.Column(db.String(50))
    frequency = db.Column(db.String(50))
    dl_uarfcn = db.Column(db.String(50))
    pci = db.Column(db.String(50))
    tac = db.Column(db.String(50))
    enodeb_id = db.Column(db.String(50)) # Excel: ENodeBID
    lcrid = db.Column(db.String(50))
    anten_height = db.Column(db.Float)
    azimuth = db.Column(db.Integer)
    m_t = db.Column(db.Float)
    e_t = db.Column(db.Float)
    total_tilt = db.Column(db.Float)
    mimo = db.Column(db.String(50))
    hang_sx = db.Column(db.String(50))
    antena = db.Column(db.String(100))
    swap = db.Column(db.String(50))
    start_day = db.Column(db.String(50))
    ghi_chu = db.Column(db.String(255))

class RF5G(db.Model):
    __tablename__ = 'rf_5g'
    id = db.Column(db.Integer, primary_key=True)
    csht_code = db.Column(db.String(50))
    site_name = db.Column(db.String(100)) # Excel: SITE_NAME
    cell_code = db.Column(db.String(50))
    site_code = db.Column(db.String(50))
    latitude = db.Column(db.Float)
    longitude = db.Column(db.Float)
    equipment = db.Column(db.String(50))
    frequency = db.Column(db.String(50))
    nrarfcn = db.Column(db.String(50))
    pci = db.Column(db.String(50))
    tac = db.Column(db.String(50))
    gnodeb_id = db.Column(db.String(50)) # Excel: gNodeB ID
    lcrid = db.Column(db.String(50))
    anten_height = db.Column(db.Float)
    azimuth = db.Column(db.Integer)
    m_t = db.Column(db.Float)
    e_t = db.Column(db.Float)
    total_tilt = db.Column(db.Float)
    mimo = db.Column(db.String(50))
    hang_sx = db.Column(db.String(50))
    antena = db.Column(db.String(100))
    dong_bo = db.Column(db.String(50)) # Excel: Đồng_bộ
    start_day = db.Column(db.String(50))
    ghi_chu = db.Column(db.String(255))

@login_manager.user_loader
def load_user(user_id):
    return db.session.get(User, int(user_id))

# --- KHỞI TẠO DB (LOGIC MỚI: PROBE & RESET) ---
def init_database():
    with app.app_context():
        try:
            # 1. Tạo bảng nếu chưa có (lệnh này sẽ bỏ qua nếu bảng đã tồn tại)
            db.create_all()
            
            # 2. KIỂM TRA MẠNH: Thử select cột password_hash
            # Nếu bảng cũ không có cột này, lệnh sẽ sinh lỗi Exception ngay
            try:
                db.session.execute(text("SELECT password_hash FROM user LIMIT 1"))
                # Kiểm tra thêm bảng RF (nếu cần)
                print(">>> Cấu trúc Database hợp lệ.")
            except Exception:
                print(">>> CẢNH BÁO: Database cũ hoặc thiếu cột. Đang Reset Database...")
                db.session.rollback() # Rollback transaction bị lỗi
                # Lưu ý: drop_all sẽ xóa cả dữ liệu cũ. Cẩn thận khi dùng trên production thật.
                db.drop_all()         
                db.create_all()       
                print(">>> Đã tạo lại Database mới thành công!")

            # 3. Tạo Admin nếu chưa có (sau khi đã đảm bảo bảng đúng chuẩn)
            if not User.query.filter_by(username='admin').first():
                admin = User(username='admin', role='admin')
                admin.set_password('admin123')
                db.session.add(admin)
                db.session.commit()
                print(">>> Đã tạo Admin mặc định: admin / admin123")
                
        except Exception as e:
            print(f"LỖI KHỞI TẠO DB: {e}")

# Gọi hàm này ngay khi file được import
init_database()

# --- TEMPLATES (GIỮ NGUYÊN GIAO DIỆN CŨ) ---

BASE_LAYOUT = """
<!DOCTYPE html>
<html lang="vi">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>KPI Monitor System</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
    <style>
        body { background-color: #f0f2f5; font-family: 'Roboto', sans-serif; overflow-x: hidden; }
        .sidebar { height: 100vh; width: 250px; position: fixed; top: 0; left: 0; background-color: #ffffff; box-shadow: 2px 0 5px rgba(0,0,0,0.05); z-index: 1000; transition: 0.3s; }
        .sidebar-header { padding: 20px; background: #0d6efd; color: white; text-align: center; }
        .sidebar-menu { padding: 10px 0; list-style: none; margin: 0; }
        .sidebar-menu a { display: block; padding: 12px 20px; color: #333; text-decoration: none; border-left: 4px solid transparent; transition: 0.3s; }
        .sidebar-menu a:hover, .sidebar-menu a.active { background-color: #e9ecef; border-left-color: #0d6efd; color: #0d6efd; }
        .sidebar-menu i { margin-right: 10px; width: 20px; text-align: center; }
        .main-content { margin-left: 250px; padding: 20px; transition: 0.3s; }
        .card { border: none; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.05); background: white; margin-bottom: 20px; }
        .card-header { background-color: white; border-bottom: 1px solid #f0f0f0; padding: 15px 20px; font-weight: bold; color: #444; }
        @media (max-width: 768px) { .sidebar { margin-left: -250px; } .sidebar.active { margin-left: 0; } .main-content { margin-left: 0; } }
    </style>
</head>
<body>
    <div class="sidebar" id="sidebar">
        <div class="sidebar-header"><h4><i class="fa-solid fa-network-wired"></i> KPI Monitor</h4></div>
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
            <li><a href="/profile" class="{{ 'active' if active_page == 'profile' else '' }}"><i class="fa-solid fa-user-shield"></i> Tài khoản</a></li>
            <li><a href="/logout"><i class="fa-solid fa-right-from-bracket"></i> Đăng xuất</a></li>
        </ul>
    </div>
    <div class="main-content">
        <button class="btn btn-primary d-md-none mb-3" onclick="document.getElementById('sidebar').classList.toggle('active')"><i class="fa-solid fa-bars"></i> Menu</button>
        <div class="container-fluid">
            {% with messages = get_flashed_messages(with_categories=true) %}
                {% if messages %}
                    {% for category, message in messages %}
                        <div class="alert alert-{{ category }} alert-dismissible fade show">{{ message }}<button type="button" class="btn-close" data-bs-dismiss="alert"></button></div>
                    {% endfor %}
                {% endif %}
            {% endwith %}
            {% block content %}{% endblock %}
        </div>
    </div>
    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/js/bootstrap.bundle.min.js"></script>
</body>
</html>
"""

LOGIN_PAGE = """
<!DOCTYPE html>
<html lang="vi">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Đăng nhập</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    <style>body { background: #e0e5ec; height: 100vh; display: flex; align-items: center; justify-content: center; } .login-card { width: 100%; max-width: 400px; background: white; padding: 40px; border-radius: 10px; box-shadow: 0 10px 25px rgba(0,0,0,0.05); }</style>
</head>
<body>
    <div class="login-card">
        <h3 class="text-center mb-4 text-primary">Đăng nhập</h3>
        {% with messages = get_flashed_messages(with_categories=true) %}{% if messages %}{% for category, message in messages %}<div class="alert alert-{{ category }}">{{ message }}</div>{% endfor %}{% endif %}{% endwith %}
        <form method="POST">
            <div class="mb-3"><label>Username</label><input type="text" name="username" class="form-control" required></div>
            <div class="mb-3"><label>Password</label><input type="password" name="password" class="form-control" required></div>
            <button type="submit" class="btn btn-primary w-100">Đăng nhập</button>
        </form>
    </div>
</body>
</html>
"""

CONTENT_TEMPLATE = """
{% extends "base" %}
{% block content %}
<div class="card">
    <div class="card-header">{{ title }} <span class="badge bg-primary float-end">{{ current_user.role }}</span></div>
    <div class="card-body">
        {% if active_page == 'dashboard' %}
            <div class="row g-4 text-center">
                <div class="col-md-3"><div class="p-3 border rounded bg-light"><h3 class="text-primary">98.5%</h3><p class="text-muted mb-0">KPI Tuần</p></div></div>
                <div class="col-md-3"><div class="p-3 border rounded bg-light"><h3 class="text-danger">12</h3><p class="text-muted mb-0">Worst Cells</p></div></div>
                <div class="col-md-3"><div class="p-3 border rounded bg-light"><h3 class="text-warning">5</h3><p class="text-muted mb-0">Congestion</p></div></div>
                <div class="col-md-3"><div class="p-3 border rounded bg-light"><h3 class="text-success">OK</h3><p class="text-muted mb-0">System</p></div></div>
            </div>
            <hr><p>Chào mừng <strong>{{ current_user.username }}</strong>!</p>
        
        {% elif active_page == 'import' %}
            <ul class="nav nav-tabs" id="importTabs" role="tablist">
                <li class="nav-item" role="presentation">
                    <button class="nav-link active" id="rf3g-tab" data-bs-toggle="tab" data-bs-target="#rf3g" type="button" role="tab">Import RF 3G</button>
                </li>
                <li class="nav-item" role="presentation">
                    <button class="nav-link" id="rf4g-tab" data-bs-toggle="tab" data-bs-target="#rf4g" type="button" role="tab">Import RF 4G</button>
                </li>
                <li class="nav-item" role="presentation">
                    <button class="nav-link" id="rf5g-tab" data-bs-toggle="tab" data-bs-target="#rf5g" type="button" role="tab">Import RF 5G</button>
                </li>
            </ul>
            <div class="tab-content p-4 border border-top-0 rounded-bottom" id="importTabsContent">
                <!-- Tab 3G -->
                <div class="tab-pane fade show active" id="rf3g" role="tabpanel">
                    <h5>Upload dữ liệu RF 3G</h5>
                    <form action="/import?type=3g" method="POST" enctype="multipart/form-data">
                        <div class="mb-3">
                            <label class="form-label">Chọn file Excel (.xlsx) hoặc CSV</label>
                            <input type="file" name="file" class="form-control" accept=".xlsx, .xls, .csv" required>
                        </div>
                        <button type="submit" class="btn btn-primary"><i class="fa-solid fa-cloud-arrow-up"></i> Tải lên RF 3G</button>
                    </form>
                </div>
                <!-- Tab 4G -->
                <div class="tab-pane fade" id="rf4g" role="tabpanel">
                    <h5>Upload dữ liệu RF 4G</h5>
                    <form action="/import?type=4g" method="POST" enctype="multipart/form-data">
                        <div class="mb-3">
                            <label class="form-label">Chọn file Excel (.xlsx) hoặc CSV</label>
                            <input type="file" name="file" class="form-control" accept=".xlsx, .xls, .csv" required>
                        </div>
                        <button type="submit" class="btn btn-primary"><i class="fa-solid fa-cloud-arrow-up"></i> Tải lên RF 4G</button>
                    </form>
                </div>
                <!-- Tab 5G -->
                <div class="tab-pane fade" id="rf5g" role="tabpanel">
                    <h5>Upload dữ liệu RF 5G</h5>
                    <form action="/import?type=5g" method="POST" enctype="multipart/form-data">
                        <div class="mb-3">
                            <label class="form-label">Chọn file Excel (.xlsx) hoặc CSV</label>
                            <input type="file" name="file" class="form-control" accept=".xlsx, .xls, .csv" required>
                        </div>
                        <button type="submit" class="btn btn-primary"><i class="fa-solid fa-cloud-arrow-up"></i> Tải lên RF 5G</button>
                    </form>
                </div>
            </div>

        {% else %}
            <div class="text-center py-5 text-muted"><h5>Chức năng {{ title }} đang xây dựng</h5></div>
        {% endif %}
    </div>
</div>
{% endblock %}
"""

USER_MANAGEMENT_TEMPLATE = """
{% extends "base" %}
{% block content %}
<div class="row">
    <div class="col-md-4">
        <div class="card"><div class="card-header">Thêm User</div><div class="card-body">
            <form action="/users/add" method="POST">
                <div class="mb-2"><label>Username</label><input type="text" name="username" class="form-control" required></div>
                <div class="mb-2"><label>Password</label><input type="password" name="password" class="form-control" required></div>
                <div class="mb-3"><label>Role</label><select name="role" class="form-select"><option value="user">User</option><option value="admin">Admin</option></select></div>
                <button class="btn btn-success w-100">Tạo</button>
            </form>
        </div></div>
    </div>
    <div class="col-md-8">
        <div class="card"><div class="card-header">Danh sách User</div><div class="card-body p-0">
            <table class="table table-hover mb-0">
                <thead class="table-light"><tr><th>ID</th><th>User</th><th>Role</th><th>Thao tác</th></tr></thead>
                <tbody>
                    {% for u in users %}
                    <tr><td>{{ u.id }}</td><td>{{ u.username }}</td><td><span class="badge bg-{{ 'danger' if u.role=='admin' else 'info' }}">{{ u.role }}</span></td>
                    <td>{% if u.username != 'admin' %}<a href="/users/delete/{{ u.id }}" class="btn btn-sm btn-outline-danger" onclick="return confirm('Xóa?')">Xóa</a> <button class="btn btn-sm btn-outline-warning" onclick="promptReset({{ u.id }}, '{{ u.username }}')">Đổi Pass</button>{% endif %}</td></tr>
                    {% endfor %}
                </tbody>
            </table>
        </div></div>
    </div>
</div>
<script>function promptReset(id, name) { let p = prompt("Pass mới cho " + name); if(p) location.href="/users/reset-pass/"+id+"?new_pass="+encodeURIComponent(p); }</script>
{% endblock %}
"""

PROFILE_TEMPLATE = """
{% extends "base" %}
{% block content %}
<div class="row justify-content-center"><div class="col-md-6"><div class="card"><div class="card-header">Đổi mật khẩu</div><div class="card-body">
    <p>User: <strong>{{ current_user.username }}</strong></p><hr>
    <form action="/change-password" method="POST">
        <div class="mb-3"><label>Mật khẩu cũ</label><input type="password" name="current_password" class="form-control" required></div>
        <div class="mb-3"><label>Mật khẩu mới</label><input type="password" name="new_password" class="form-control" required></div>
        <button class="btn btn-primary">Lưu thay đổi</button>
    </form>
</div></div></div></div>
{% endblock %}
"""

# --- CẤU HÌNH LOADER TEMPLATE ẢO (KHẮC PHỤC LỖI BLOCK DEFINED TWICE) ---
# Tạo một bộ nhớ chứa template, cho phép Jinja2 hiểu kế thừa 'base' từ biến BASE_LAYOUT
app.jinja_loader = jinja2.DictLoader({'base': BASE_LAYOUT})

def render_page(tpl, **kwargs):
    # Không cần thay thế chuỗi thủ công nữa, Jinja2 sẽ tự resolve {% extends "base" %}
    return render_template_string(tpl, **kwargs)

# --- ROUTES ---
@app.route('/login', methods=['GET', 'POST'])
def login():
    if current_user.is_authenticated: return redirect(url_for('index'))
    if request.method == 'POST':
        try:
            user = User.query.filter_by(username=request.form['username']).first()
            if user and user.check_password(request.form['password']):
                login_user(user)
                return redirect(url_for('index'))
            flash('Sai thông tin đăng nhập', 'danger')
        except Exception as e: flash(f"Lỗi DB: {e}", 'danger')
    return render_template_string(LOGIN_PAGE)

@app.route('/logout')
@login_required
def logout(): logout_user(); return redirect(url_for('login'))

@app.route('/')
@login_required
def index(): return render_page(CONTENT_TEMPLATE, title="Dashboard", active_page='dashboard')

# Các route menu khác
@app.route('/kpi')
@login_required
def kpi(): return render_page(CONTENT_TEMPLATE, title="Báo cáo KPI", active_page='kpi')
@app.route('/rf')
@login_required
def rf(): return render_page(CONTENT_TEMPLATE, title="RF", active_page='rf')
@app.route('/poi')
@login_required
def poi(): return render_page(CONTENT_TEMPLATE, title="POI", active_page='poi')
@app.route('/worst-cell')
@login_required
def worst_cell(): return render_page(CONTENT_TEMPLATE, title="Worst Cell", active_page='worst_cell')
@app.route('/conges-3g')
@login_required
def conges_3g(): return render_page(CONTENT_TEMPLATE, title="Congestion 3G", active_page='conges_3g')
@app.route('/traffic-down')
@login_required
def traffic_down(): return render_page(CONTENT_TEMPLATE, title="Traffic Down", active_page='traffic_down')
@app.route('/script')
@login_required
def script(): return render_page(CONTENT_TEMPLATE, title="Script", active_page='script')

@app.route('/import', methods=['GET', 'POST'])
@login_required
def import_data():
    if request.method == 'POST':
        file = request.files.get('file')
        import_type = request.args.get('type') # 3g, 4g, hoặc 5g
        
        if not file or not file.filename:
            flash('Chưa chọn file!', 'warning')
            return redirect(url_for('import_data'))

        try:
            # Đọc file (hỗ trợ cả csv và excel)
            filename = file.filename
            if filename.endswith('.csv'):
                df = pd.read_csv(file)
            elif filename.endswith(('.xls', '.xlsx')):
                df = pd.read_excel(file)
            else:
                flash('Chỉ hỗ trợ file .csv hoặc .xlsx', 'danger')
                return redirect(url_for('import_data'))

            # Chuẩn hóa tên cột để mapping với database
            # Xóa khoảng trắng, chuyển về chữ thường
            # Mapping cột đặc biệt
            column_map = {
                'Frenquency': 'frequency',
                'Hãng_SX': 'hang_sx',
                'Ghi_chú': 'ghi_chu',
                'Ghi_chu': 'ghi_chu',
                'Hãng SX': 'hang_sx',
                'ENodeBID': 'enodeb_id',
                'gNodeB ID': 'gnodeb_id',
                'SITE_NAME': 'site_name',
                'Đồng_bộ': 'dong_bo',
                'Dong_bo': 'dong_bo'
            }
            
            # Hàm làm sạch tên cột
            def clean_col(col_name):
                col_name = str(col_name).strip()
                if col_name in column_map:
                    return column_map[col_name]
                return col_name.lower().replace(' ', '_')

            df.columns = [clean_col(c) for c in df.columns]

            # Chọn model tương ứng
            model_class = None
            if import_type == '3g':
                model_class = RF3G
            elif import_type == '4g':
                model_class = RF4G
            elif import_type == '5g':
                model_class = RF5G
            
            if model_class:
                # Xóa dữ liệu cũ nếu muốn (tùy chọn - ở đây tôi để nạp thêm vào)
                # db.session.query(model_class).delete() 
                
                # Convert DataFrame to list of dicts
                records = df.to_dict(orient='records')
                
                # Lọc chỉ lấy các cột có trong model database để tránh lỗi
                valid_columns = [c.key for c in model_class.__table__.columns if c.key != 'id']
                
                objects_to_add = []
                for row in records:
                    # Chỉ giữ lại các trường khớp với database
                    filtered_row = {k: v for k, v in row.items() if k in valid_columns}
                    # Xử lý NaN thành None (NULL trong DB)
                    for k, v in filtered_row.items():
                        if pd.isna(v):
                            filtered_row[k] = None
                    objects_to_add.append(model_class(**filtered_row))

                db.session.add_all(objects_to_add)
                db.session.commit()
                flash(f'Đã import thành công {len(objects_to_add)} bản ghi vào RF {import_type.upper()}!', 'success')
            else:
                flash('Loại dữ liệu không hợp lệ', 'danger')

        except Exception as e:
            db.session.rollback()
            flash(f'Lỗi khi import: {str(e)}', 'danger')
            print(f"IMPORT ERROR: {e}")

        return redirect(url_for('import_data'))

    return render_page(CONTENT_TEMPLATE, title="Import Dữ liệu", active_page='import')

@app.route('/profile')
@login_required
def profile(): return render_page(PROFILE_TEMPLATE, active_page='profile')

# Quản lý User
@app.route('/users')
@login_required
def manage_users():
    if current_user.role != 'admin': return redirect(url_for('index'))
    return render_page(USER_MANAGEMENT_TEMPLATE, users=User.query.all(), active_page='users')

@app.route('/users/add', methods=['POST'])
@login_required
def add_user():
    if current_user.role != 'admin': return redirect(url_for('index'))
    try:
        if User.query.filter_by(username=request.form['username']).first(): flash('User tồn tại', 'warning')
        else:
            u = User(username=request.form['username'], role=request.form['role'])
            u.set_password(request.form['password'])
            db.session.add(u); db.session.commit(); flash('Đã tạo user', 'success')
    except Exception as e: flash(f"Lỗi: {e}", 'danger')
    return redirect(url_for('manage_users'))

@app.route('/users/delete/<int:id>')
@login_required
def delete_user(id):
    if current_user.role != 'admin': return redirect(url_for('index'))
    u = db.session.get(User, id)
    if u and u.username != 'admin': db.session.delete(u); db.session.commit()
    return redirect(url_for('manage_users'))

@app.route('/users/reset-pass/<int:id>')
@login_required
def reset_pass(id):
    if current_user.role != 'admin': return redirect(url_for('index'))
    u = db.session.get(User, id)
    if u: u.set_password(request.args.get('new_pass')); db.session.commit(); flash('Đã đổi pass', 'success')
    return redirect(url_for('manage_users'))

@app.route('/change-password', methods=['POST'])
@login_required
def change_password():
    if current_user.check_password(request.form['current_password']):
        current_user.set_password(request.form['new_password']); db.session.commit(); flash('Đã đổi mật khẩu', 'success')
    else: flash('Sai mật khẩu cũ', 'danger')
    return redirect(url_for('profile'))

if __name__ == '__main__':
    app.run(debug=True)
