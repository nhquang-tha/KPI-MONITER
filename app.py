import os
import jinja2
import pandas as pd
import json
import gc # Thư viện quản lý bộ nhớ
import re # Thư viện xử lý chuỗi Regular Expression
import zipfile # Thư viện xử lý nén file
from io import BytesIO, StringIO
from datetime import datetime
from flask import Flask, render_template_string, request, redirect, url_for, flash, send_file, Response, stream_with_context
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.utils import secure_filename
from sqlalchemy import text, func
from itertools import zip_longest
from collections import defaultdict # Thêm để xử lý logic đếm ngày

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
    __tablename__ = 'user'
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
    frequency = db.Column(db.String(50))
    psc = db.Column(db.String(50))
    dl_uarfcn = db.Column(db.String(50))
    bsc_lac = db.Column(db.String(50))
    ci = db.Column(db.String(50))
    anten_height = db.Column(db.Float)
    azimuth = db.Column(db.Integer)
    m_t = db.Column(db.Float)
    e_t = db.Column(db.Float)
    total_tilt = db.Column(db.Float)
    hang_sx = db.Column(db.String(50))
    antena = db.Column(db.String(100))
    swap = db.Column(db.String(50))
    start_day = db.Column(db.String(50))
    ghi_chu = db.Column(db.String(255))

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
    enodeb_id = db.Column(db.String(50))
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
    site_name = db.Column(db.String(100))
    cell_code = db.Column(db.String(50))
    site_code = db.Column(db.String(50))
    latitude = db.Column(db.Float)
    longitude = db.Column(db.Float)
    equipment = db.Column(db.String(50))
    frequency = db.Column(db.String(50))
    nrarfcn = db.Column(db.String(50))
    pci = db.Column(db.String(50))
    tac = db.Column(db.String(50))
    gnodeb_id = db.Column(db.String(50))
    lcrid = db.Column(db.String(50))
    anten_height = db.Column(db.Float)
    azimuth = db.Column(db.Integer)
    m_t = db.Column(db.Float)
    e_t = db.Column(db.Float)
    total_tilt = db.Column(db.Float)
    mimo = db.Column(db.String(50))
    hang_sx = db.Column(db.String(50))
    antena = db.Column(db.String(100))
    dong_bo = db.Column(db.String(50))
    start_day = db.Column(db.String(50))
    ghi_chu = db.Column(db.String(255))

# --- MODELS (KPI DATA) ---
class KPI3G(db.Model):
    __tablename__ = 'kpi_3g'
    id = db.Column(db.Integer, primary_key=True)
    stt = db.Column(db.String(50))
    nha_cung_cap = db.Column(db.String(50))
    tinh = db.Column(db.String(50))
    ten_rnc = db.Column(db.String(100))
    ten_cell = db.Column(db.String(100))
    ma_vnp = db.Column(db.String(50))
    loai_ne = db.Column(db.String(50))
    lac = db.Column(db.String(50))
    ci = db.Column(db.String(50))
    thoi_gian = db.Column(db.String(50))
    # Metrics
    traffic = db.Column(db.Float)
    pstraffic = db.Column(db.Float) # New for Chart
    cssr = db.Column(db.Float)
    dcr = db.Column(db.Float)
    ps_cssr = db.Column(db.Float)
    ps_dcr = db.Column(db.Float)
    hsdpa_throughput = db.Column(db.Float)
    hsupa_throughput = db.Column(db.Float)
    cs_so_att = db.Column(db.Float)
    ps_so_att = db.Column(db.Float)
    csconges = db.Column(db.Float)
    psconges = db.Column(db.Float)

class KPI4G(db.Model):
    __tablename__ = 'kpi_4g'
    id = db.Column(db.Integer, primary_key=True)
    stt = db.Column(db.String(50))
    nha_cung_cap = db.Column(db.String(50))
    tinh = db.Column(db.String(50))
    ten_rnc = db.Column(db.String(100))
    ten_cell = db.Column(db.String(100))
    ma_vnp = db.Column(db.String(50))
    loai_ne = db.Column(db.String(50))
    enodeb_id = db.Column(db.String(50))
    cell_id = db.Column(db.String(50))
    thoi_gian = db.Column(db.String(50))
    # Metrics
    traffic = db.Column(db.Float) # New for Chart (Total Data Traffic)
    traffic_vol_dl = db.Column(db.Float)
    traffic_vol_ul = db.Column(db.Float)
    cell_dl_avg_thputs = db.Column(db.Float)
    cell_ul_avg_thput = db.Column(db.Float)
    user_dl_avg_thput = db.Column(db.Float)
    user_ul_avg_thput = db.Column(db.Float)
    erab_ssrate_all = db.Column(db.Float)
    service_drop_all = db.Column(db.Float)
    unvailable = db.Column(db.Float)
    res_blk_dl = db.Column(db.Float) # New for Chart
    cqi_4g = db.Column(db.Float)     # New for Chart

class KPI5G(db.Model):
    __tablename__ = 'kpi_5g'
    id = db.Column(db.Integer, primary_key=True)
    nha_cung_cap = db.Column(db.String(50))
    tinh = db.Column(db.String(50))
    ten_gnodeb = db.Column(db.String(100))
    ten_cell = db.Column(db.String(100))
    ma_vnp = db.Column(db.String(50))
    loai_ne = db.Column(db.String(50))
    gnodeb_id = db.Column(db.String(50))
    cell_id = db.Column(db.String(50))
    thoi_gian = db.Column(db.String(50))
    # Metrics
    traffic = db.Column(db.Float) # New for Chart
    dl_traffic_volume_gb = db.Column(db.Float)
    ul_traffic_volume_gb = db.Column(db.Float)
    cell_downlink_average_throughput = db.Column(db.Float)
    cell_uplink_average_throughput = db.Column(db.Float)
    user_dl_avg_throughput = db.Column(db.Float) # New for Chart
    cqi_5g = db.Column(db.Float) # New for Chart
    cell_avaibility_rate = db.Column(db.Float)
    sgnb_addition_success_rate = db.Column(db.Float)
    sgnb_abnormal_release_rate = db.Column(db.Float)

@login_manager.user_loader
def load_user(user_id):
    return db.session.get(User, int(user_id))

# --- KHỞI TẠO DB ---
def init_database():
    with app.app_context():
        try:
            db.create_all()
            # Kiểm tra xem các bảng KPI đã có các cột mới chưa
            try:
                db.session.execute(text("SELECT pstraffic FROM kpi_3g LIMIT 1"))
                db.session.execute(text("SELECT cqi_4g FROM kpi_4g LIMIT 1"))
                db.session.execute(text("SELECT cqi_5g FROM kpi_5g LIMIT 1"))
            except Exception:
                print(">>> Cập nhật cấu trúc Database (Thêm cột cho KPI để vẽ biểu đồ)...")
                db.session.rollback()
                db.drop_all()         
                db.create_all()       
                print(">>> Đã Reset Database thành công!")

            if not User.query.filter_by(username='admin').first():
                admin = User(username='admin', role='admin')
                admin.set_password('admin123')
                db.session.add(admin)
                db.session.commit()
        except Exception as e:
            print(f"LỖI KHỞI TẠO DB: {e}")

init_database()

# --- TEMPLATES ---
BASE_LAYOUT = """
<!DOCTYPE html>
<html lang="vi">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>KPI Monitor System</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
    <!-- Chart.js -->
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
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
        .btn-action { padding: 0.25rem 0.5rem; font-size: 0.75rem; }
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
            <li><a href="/backup-restore" class="{{ 'active' if active_page == 'backup_restore' else '' }}"><i class="fa-solid fa-database"></i> Backup / Restore</a></li>
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
        
        {% elif active_page == 'kpi' %}
            <div class="row mb-4">
                <div class="col-md-12">
                    <form method="GET" action="/kpi" class="row g-3 align-items-center">
                        <div class="col-auto">
                            <label class="col-form-label fw-bold">Công nghệ:</label>
                        </div>
                        <div class="col-auto">
                            <select name="tech" class="form-select">
                                <option value="3g" {% if selected_tech == '3g' %}selected{% endif %}>3G</option>
                                <option value="4g" {% if selected_tech == '4g' %}selected{% endif %}>4G</option>
                                <option value="5g" {% if selected_tech == '5g' %}selected{% endif %}>5G</option>
                            </select>
                        </div>
                        <div class="col-auto">
                            <label class="col-form-label fw-bold">Cell/Site:</label>
                        </div>
                        <div class="col-md-6">
                            <input type="text" name="cell_name" class="form-control" 
                                   placeholder="Nhập Site Code (để vẽ toàn trạm) hoặc danh sách Cell Code (cách nhau bởi dấu phẩy/dấu cách)..." 
                                   value="{{ cell_name_input }}">
                        </div>
                        <div class="col-auto">
                            <button type="submit" class="btn btn-primary"><i class="fa-solid fa-chart-line"></i> Vẽ biểu đồ</button>
                        </div>
                    </form>
                </div>
            </div>

            {% if charts %}
                {% for chart_id, chart_config in charts.items() %}
                <div class="card mb-4 border shadow-sm">
                    <div class="card-body">
                        <div class="chart-container" style="position: relative; height:40vh; width:100%">
                            <canvas id="{{ chart_id }}"></canvas>
                        </div>
                    </div>
                </div>
                {% endfor %}
                
                <script>
                    // Render tất cả các biểu đồ
                    {% for chart_id, chart_data in charts.items() %}
                    (function() {
                        const ctx = document.getElementById('{{ chart_id }}').getContext('2d');
                        new Chart(ctx, {
                            type: 'line',
                            data: {{ chart_data | safe }},
                            options: {
                                responsive: true,
                                maintainAspectRatio: false,
                                interaction: {
                                    mode: 'index',
                                    intersect: false,
                                },
                                plugins: {
                                    title: {
                                        display: true,
                                        text: '{{ chart_data.title }}',
                                        font: { size: 16 }
                                    },
                                    legend: {
                                        position: 'bottom'
                                    }
                                },
                                scales: {
                                    y: {
                                        beginAtZero: true,
                                        title: { display: true, text: 'Giá trị' }
                                    }
                                }
                            }
                        });
                    })();
                    {% endfor %}
                </script>
            {% elif cell_name_input %}
                <div class="alert alert-warning">
                    Không tìm thấy dữ liệu cho <strong>{{ cell_name_input }}</strong>. 
                    <br>Vui lòng kiểm tra:
                    <ul>
                        <li>Tên Cell/Site đã đúng chưa?</li>
                        <li>Đã chọn đúng công nghệ (3G/4G/5G) chưa?</li>
                        <li>Đã import dữ liệu KPI cho các cell này chưa?</li>
                    </ul>
                </div>
            {% else %}
                <div class="text-center text-muted py-5">
                    <i class="fa-solid fa-chart-area fa-3x mb-3"></i>
                    <p>Nhập tên Site hoặc danh sách Cell để xem biểu đồ KPI so sánh.</p>
                </div>
            {% endif %}

        {% elif active_page == 'conges_3g' %}
            <div class="alert alert-info">
                <strong><i class="fa-solid fa-filter"></i> Điều kiện lọc:</strong> 
                (CS_CONG > 2% & CS_ATT > 100) HOẶC (PS_CONG > 2% & PS_ATT > 500) <br>
                <strong><i class="fa-solid fa-clock"></i> Thời gian:</strong> Xảy ra liên tiếp trong 3 ngày dữ liệu gần nhất 
                ({% for d in dates %}{{ d }}{{ ", " if not loop.last else "" }}{% endfor %})
            </div>
            
            <div class="table-responsive">
                <table class="table table-bordered table-hover small">
                    <thead class="table-light">
                        <tr>
                            <th>Cell Name</th>
                            <th>Site Code</th>
                            <th>CSHT</th>
                            <th>Antenna</th>
                            <th>Tilt</th>
                            <th class="text-center">Hành động</th>
                        </tr>
                    </thead>
                    <tbody>
                        {% for row in conges_data %}
                        <tr>
                            <td class="fw-bold text-primary">{{ row.cell_name }}</td>
                            <td>{{ row.site_code }}</td>
                            <td>{{ row.csht }}</td>
                            <td>{{ row.antena }}</td>
                            <td>{{ row.tilt }}</td>
                            <td class="text-center">
                                <a href="/rf/detail/3g/{{ row.rf_id }}" class="btn btn-sm btn-info text-white" title="Chi tiết RF"><i class="fa-solid fa-eye"></i></a>
                            </td>
                        </tr>
                        {% else %}
                        <tr><td colspan="6" class="text-center py-4 text-muted">Tuyệt vời! Không có cell nào bị nghẽn liên tiếp 3 ngày.</td></tr>
                        {% endfor %}
                    </tbody>
                </table>
            </div>

        {% elif active_page == 'rf' %}
            <div class="mb-3 d-flex justify-content-between">
                <div class="btn-group">
                    <a href="/rf?tech=3g" class="btn btn-{{ 'primary' if current_tech == '3g' else 'outline-primary' }}">3G</a>
                    <a href="/rf?tech=4g" class="btn btn-{{ 'primary' if current_tech == '4g' else 'outline-primary' }}">4G</a>
                    <a href="/rf?tech=5g" class="btn btn-{{ 'primary' if current_tech == '5g' else 'outline-primary' }}">5G</a>
                </div>
                <div>
                    <a href="/rf/add?tech={{ current_tech }}" class="btn btn-primary me-2"><i class="fa-solid fa-plus"></i> Thêm mới</a>
                    <a href="/rf?tech={{ current_tech }}&action=export" class="btn btn-success"><i class="fa-solid fa-file-csv"></i> Xuất Excel (CSV)</a>
                </div>
            </div>

            <div class="table-responsive" style="max-height: 70vh; overflow-y: auto;">
                <table class="table table-sm table-bordered table-hover small">
                    <thead class="table-light position-sticky top-0 shadow-sm">
                        <tr>
                            <th class="text-center" style="width: 100px;">Hành động</th>
                            <th>CSHT Code</th>
                            <th>{{ 'Site Name' if current_tech == '5g' else 'Cell Name' }}</th>
                            <th>Cell Code</th>
                            <th>Site Code</th>
                            <th>Long</th>
                            <th>Lat</th>
                            <th>Freq</th>
                            <th>Azimuth</th>
                            <th>Tilt</th>
                            <th>Antenna</th>
                            <th>Ghi chú</th>
                        </tr>
                    </thead>
                    <tbody>
                        {% for row in rf_data %}
                        <tr>
                            <td class="text-center">
                                <a href="/rf/detail/{{ current_tech }}/{{ row.id }}" class="btn btn-info btn-action text-white" title="Chi tiết"><i class="fa-solid fa-eye"></i></a>
                                <a href="/rf/edit/{{ current_tech }}/{{ row.id }}" class="btn btn-warning btn-action text-white" title="Sửa"><i class="fa-solid fa-pen-to-square"></i></a>
                                <a href="/rf/delete/{{ current_tech }}/{{ row.id }}" class="btn btn-danger btn-action" title="Xóa" onclick="return confirm('Bạn có chắc muốn xóa bản ghi này?')"><i class="fa-solid fa-trash"></i></a>
                            </td>
                            <td>{{ row.csht_code }}</td>
                            <td>{{ row.site_name if current_tech == '5g' else row.cell_name }}</td>
                            <td>{{ row.cell_code }}</td>
                            <td>{{ row.site_code }}</td>
                            <td>{{ row.longitude }}</td>
                            <td>{{ row.latitude }}</td>
                            <td>{{ row.frequency }}</td>
                            <td>{{ row.azimuth }}</td>
                            <td>{{ row.total_tilt }}</td>
                            <td>{{ row.antena }}</td>
                            <td>{{ row.ghi_chu }}</td>
                        </tr>
                        {% else %}
                        <tr><td colspan="12" class="text-center py-3">Không có dữ liệu. Vui lòng vào menu Import để tải file lên hoặc nhấn Thêm mới.</td></tr>
                        {% endfor %}
                    </tbody>
                </table>
                <div class="text-muted small mt-2 fst-italic">Hiển thị tối đa 500 bản ghi trên web. Để xem đầy đủ, vui lòng chọn "Xuất Excel".</div>
            </div>

        {% elif active_page == 'import' %}
            <div class="row">
                <div class="col-md-8">
                    <ul class="nav nav-tabs" id="importTabs" role="tablist">
                        <!-- RF Tabs -->
                        <li class="nav-item"><button class="nav-link active" data-bs-toggle="tab" data-bs-target="#rf3g">Import RF 3G</button></li>
                        <li class="nav-item"><button class="nav-link" data-bs-toggle="tab" data-bs-target="#rf4g">Import RF 4G</button></li>
                        <li class="nav-item"><button class="nav-link" data-bs-toggle="tab" data-bs-target="#rf5g">Import RF 5G</button></li>
                        <!-- KPI Tabs -->
                        <li class="nav-item"><button class="nav-link text-success" data-bs-toggle="tab" data-bs-target="#kpi3g">KPI 3G</button></li>
                        <li class="nav-item"><button class="nav-link text-success" data-bs-toggle="tab" data-bs-target="#kpi4g">KPI 4G</button></li>
                        <li class="nav-item"><button class="nav-link text-success" data-bs-toggle="tab" data-bs-target="#kpi5g">KPI 5G</button></li>
                    </ul>
                    <div class="tab-content p-4 border border-top-0 rounded-bottom">
                        <!-- RF Forms -->
                        <div class="tab-pane fade show active" id="rf3g">
                            <form action="/import?type=3g" method="POST" enctype="multipart/form-data">
                                <div class="mb-3"><label class="form-label">Chọn file Excel/CSV RF 3G</label><input type="file" name="file" class="form-control" accept=".xlsx, .xls, .csv" required></div>
                                <div class="d-flex justify-content-between">
                                    <button type="submit" class="btn btn-primary"><i class="fa-solid fa-cloud-arrow-up"></i> Tải lên RF 3G</button>
                                    <a href="/rf/reset?type=3g" class="btn btn-danger" onclick="return confirm('CẢNH BÁO: Hành động này sẽ XÓA SẠCH dữ liệu RF 3G. Bạn có chắc chắn không?')"><i class="fa-solid fa-trash-can"></i> Xóa toàn bộ RF 3G</a>
                                </div>
                            </form>
                        </div>
                        <div class="tab-pane fade" id="rf4g">
                            <form action="/import?type=4g" method="POST" enctype="multipart/form-data">
                                <div class="mb-3"><label class="form-label">Chọn file Excel/CSV RF 4G</label><input type="file" name="file" class="form-control" accept=".xlsx, .xls, .csv" required></div>
                                <div class="d-flex justify-content-between">
                                    <button type="submit" class="btn btn-primary"><i class="fa-solid fa-cloud-arrow-up"></i> Tải lên RF 4G</button>
                                    <a href="/rf/reset?type=4g" class="btn btn-danger" onclick="return confirm('CẢNH BÁO: Hành động này sẽ XÓA SẠCH dữ liệu RF 4G. Bạn có chắc chắn không?')"><i class="fa-solid fa-trash-can"></i> Xóa toàn bộ RF 4G</a>
                                </div>
                            </form>
                        </div>
                        <div class="tab-pane fade" id="rf5g">
                            <form action="/import?type=5g" method="POST" enctype="multipart/form-data">
                                <div class="mb-3"><label class="form-label">Chọn file Excel/CSV RF 5G</label><input type="file" name="file" class="form-control" accept=".xlsx, .xls, .csv" required></div>
                                <div class="d-flex justify-content-between">
                                    <button type="submit" class="btn btn-primary"><i class="fa-solid fa-cloud-arrow-up"></i> Tải lên RF 5G</button>
                                    <a href="/rf/reset?type=5g" class="btn btn-danger" onclick="return confirm('CẢNH BÁO: Hành động này sẽ XÓA SẠCH dữ liệu RF 5G. Bạn có chắc chắn không?')"><i class="fa-solid fa-trash-can"></i> Xóa toàn bộ RF 5G</a>
                                </div>
                            </form>
                        </div>
                        
                        <!-- KPI Forms -->
                        <div class="tab-pane fade" id="kpi3g">
                            <h5 class="text-success">Import KPI 3G Hàng Ngày</h5>
                            <form action="/import?type=kpi3g" method="POST" enctype="multipart/form-data">
                                <div class="mb-3">
                                    <label class="form-label">Chọn các file KPI 3G (.csv)</label>
                                    <input type="file" name="file" class="form-control" accept=".csv" multiple required>
                                    <small class="text-muted">Có thể chọn nhiều file cùng lúc để import.</small>
                                </div>
                                <button type="submit" class="btn btn-success"><i class="fa-solid fa-chart-line"></i> Tải lên KPI 3G</button>
                            </form>
                        </div>
                        <div class="tab-pane fade" id="kpi4g">
                            <h5 class="text-success">Import KPI 4G Hàng Ngày</h5>
                            <form action="/import?type=kpi4g" method="POST" enctype="multipart/form-data">
                                <div class="mb-3">
                                    <label class="form-label">Chọn các file KPI 4G (.csv)</label>
                                    <input type="file" name="file" class="form-control" accept=".csv" multiple required>
                                </div>
                                <button type="submit" class="btn btn-success"><i class="fa-solid fa-chart-line"></i> Tải lên KPI 4G</button>
                            </form>
                        </div>
                        <div class="tab-pane fade" id="kpi5g">
                            <h5 class="text-success">Import KPI 5G Hàng Ngày</h5>
                            <form action="/import?type=kpi5g" method="POST" enctype="multipart/form-data">
                                <div class="mb-3">
                                    <label class="form-label">Chọn các file KPI 5G (.csv)</label>
                                    <input type="file" name="file" class="form-control" accept=".csv" multiple required>
                                </div>
                                <button type="submit" class="btn btn-success"><i class="fa-solid fa-chart-line"></i> Tải lên KPI 5G</button>
                            </form>
                        </div>
                    </div>
                </div>
                <div class="col-md-4">
                    <div class="card h-100">
                        <div class="card-header bg-info text-white"><i class="fa-solid fa-calendar-check"></i> Dữ liệu KPI đã có</div>
                        <div class="card-body p-0" style="max-height: 400px; overflow-y: auto;">
                            <table class="table table-bordered table-striped small mb-0 text-center">
                                <thead class="table-light">
                                    <tr>
                                        <th>KPI 3G</th>
                                        <th>KPI 4G</th>
                                        <th>KPI 5G</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    {% for r3, r4, r5 in kpi_rows %}
                                    <tr>
                                        <td class="{{ 'text-success fw-bold' if r3 else 'text-muted' }}">{{ r3 if r3 else '-' }}</td>
                                        <td class="{{ 'text-success fw-bold' if r4 else 'text-muted' }}">{{ r4 if r4 else '-' }}</td>
                                        <td class="{{ 'text-success fw-bold' if r5 else 'text-muted' }}">{{ r5 if r5 else '-' }}</td>
                                    </tr>
                                    {% else %}
                                    <tr><td colspan="3" class="text-center text-muted">Chưa có dữ liệu</td></tr>
                                    {% endfor %}
                                </tbody>
                            </table>
                        </div>
                    </div>
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

BACKUP_RESTORE_TEMPLATE = """
{% extends "base" %}
{% block content %}
<div class="container py-4">
    <div class="row g-4">
        <!-- Backup Section -->
        <div class="col-md-6">
            <div class="card h-100 shadow-sm">
                <div class="card-header bg-primary text-white">
                    <h5 class="mb-0"><i class="fa-solid fa-download me-2"></i>Sao lưu Dữ liệu (Backup)</h5>
                </div>
                <div class="card-body d-flex flex-column justify-content-center align-items-center p-5">
                    <p class="text-center text-muted mb-4">
                        Tải xuống toàn bộ dữ liệu hiện tại (User, RF, KPI) dưới dạng file nén (.zip).<br>
                        File này có thể được sử dụng để khôi phục dữ liệu sau này.
                    </p>
                    <form action="/backup" method="POST">
                        <button type="submit" class="btn btn-primary btn-lg px-5">
                            <i class="fa-solid fa-file-zipper me-2"></i> Tải xuống bản sao lưu
                        </button>
                    </form>
                </div>
            </div>
        </div>

        <!-- Restore Section -->
        <div class="col-md-6">
            <div class="card h-100 shadow-sm border-warning">
                <div class="card-header bg-warning text-dark">
                    <h5 class="mb-0"><i class="fa-solid fa-upload me-2"></i>Khôi phục Dữ liệu (Restore)</h5>
                </div>
                <div class="card-body p-4">
                    <div class="alert alert-danger" role="alert">
                        <i class="fa-solid fa-triangle-exclamation me-2"></i>
                        <strong>CẢNH BÁO QUAN TRỌNG:</strong><br>
                        Hành động này sẽ <strong>XÓA TOÀN BỘ</strong> dữ liệu hiện tại trong các bảng tương ứng có trong file backup và thay thế bằng dữ liệu mới.
                    </div>
                    <form action="/restore" method="POST" enctype="multipart/form-data">
                        <div class="mb-4">
                            <label for="backupFile" class="form-label fw-bold">Chọn file Backup (.zip)</label>
                            <input class="form-control form-control-lg" type="file" id="backupFile" name="file" accept=".zip" required>
                        </div>
                        <div class="d-grid">
                            <button type="submit" class="btn btn-warning btn-lg" onclick="return confirm('Bạn có chắc chắn muốn khôi phục dữ liệu? Mọi dữ liệu hiện tại sẽ bị ghi đè!')">
                                <i class="fa-solid fa-rotate-left me-2"></i> Tiến hành Khôi phục
                            </button>
                        </div>
                    </form>
                </div>
            </div>
        </div>
    </div>
</div>
{% endblock %}
"""

RF_FORM_TEMPLATE = """
{% extends "base" %}
{% block content %}
<div class="card">
    <div class="card-header">
        <span class="h5">{{ title }}</span>
        <a href="/rf?tech={{ tech }}" class="btn btn-secondary btn-sm float-end">Quay lại</a>
    </div>
    <div class="card-body">
        <form method="POST">
            <div class="row g-3">
                {% for col in columns %}
                <div class="col-md-4">
                    <label class="form-label fw-bold small text-uppercase text-muted">{{ col }}</label>
                    <input type="text" name="{{ col }}" class="form-control" 
                           value="{{ obj[col] if obj and obj[col] is not none else '' }}">
                </div>
                {% endfor %}
            </div>
            <hr>
            <div class="d-flex justify-content-end">
                <button type="submit" class="btn btn-primary px-4"><i class="fa-solid fa-save"></i> Lưu lại</button>
            </div>
        </form>
    </div>
</div>
{% endblock %}
"""

RF_DETAIL_TEMPLATE = """
{% extends "base" %}
{% block content %}
<div class="card">
    <div class="card-header">
        <span class="h5">Chi tiết bản ghi RF {{ tech.upper() }} #{{ obj.id }}</span>
        <div class="float-end">
            <a href="/rf/edit/{{ tech }}/{{ obj.id }}" class="btn btn-warning btn-sm text-white"><i class="fa-solid fa-pen"></i> Sửa</a>
            <a href="/rf?tech={{ tech }}" class="btn btn-secondary btn-sm">Quay lại</a>
        </div>
    </div>
    <div class="card-body">
        <div class="table-responsive">
            <table class="table table-bordered table-striped">
                <tbody>
                    {% for col, val in obj.items() %}
                        {% if col != '_sa_instance_state' %}
                        <tr>
                            <th class="bg-light" style="width: 30%">{{ col.upper() }}</th>
                            <td>{{ val if val is not none else '' }}</td>
                        </tr>
                        {% endif %}
                    {% endfor %}
                </tbody>
            </table>
        </div>
    </div>
</div>
{% endblock %}
"""

# --- CẤU HÌNH LOADER TEMPLATE ẢO ---
app.jinja_loader = jinja2.DictLoader({
    'base': BASE_LAYOUT,
    'backup_restore': BACKUP_RESTORE_TEMPLATE
})

def render_page(tpl, **kwargs):
    # Nếu là template tên 'backup_restore', load từ DictLoader
    if tpl == BACKUP_RESTORE_TEMPLATE:
        return render_template_string(tpl, **kwargs)
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
def kpi():
    selected_tech = request.args.get('tech', '3g')
    cell_name_input = request.args.get('cell_name', '').strip()
    charts = {} # Dictionary to store multiple chart data objects

    # Danh sách màu sắc để phân biệt các Cell trên biểu đồ
    colors = [
        '#007bff', '#28a745', '#dc3545', '#ffc107', '#17a2b8', 
        '#6610f2', '#e83e8c', '#fd7e14', '#20c997', '#6c757d'
    ]

    if cell_name_input:
        KPI_Model = {'3g': KPI3G, '4g': KPI4G, '5g': KPI5G}.get(selected_tech)
        RF_Model = {'3g': RF3G, '4g': RF4G, '5g': RF5G}.get(selected_tech)
        
        target_cells = []

        if KPI_Model and RF_Model:
            # 1. Kiểm tra xem input có phải là Site Code không
            site_cells = RF_Model.query.filter(RF_Model.site_code == cell_name_input).all()
            
            if site_cells:
                # Nếu là Site Code, lấy toàn bộ Cell Code thuộc trạm đó
                target_cells = [cell.cell_code for cell in site_cells]
            else:
                # Nếu không phải Site Code, tách chuỗi input thành danh sách Cell
                # Hỗ trợ dấu phẩy, dấu cách, dấu chấm phẩy
                target_cells = [c.strip() for c in re.split(r'[,\s;]+', cell_name_input) if c.strip()]

            if target_cells:
                # 2. Query dữ liệu KPI cho tất cả các cell tìm được
                data = KPI_Model.query.filter(KPI_Model.ten_cell.in_(target_cells)).all()
                
                # Sắp xếp dữ liệu theo thời gian để vẽ biểu đồ đúng
                try:
                    data.sort(key=lambda x: datetime.strptime(x.thoi_gian, '%d/%m/%Y'))
                except ValueError:
                    pass 

                if data:
                    # Lấy danh sách tất cả các ngày (trục X)
                    all_labels = sorted(list(set([x.thoi_gian for x in data])), key=lambda d: datetime.strptime(d, '%d/%m/%Y'))
                    
                    # Gom nhóm dữ liệu theo Cell
                    data_by_cell = defaultdict(list)
                    for x in data:
                        data_by_cell[x.ten_cell].append(x)

                    # Cấu hình các chỉ số cần vẽ (Metrics)
                    metrics_config = {
                        '3g': [
                            {'key': 'pstraffic', 'label': 'PSTRAFFIC (GB)'},
                            {'key': 'traffic', 'label': 'TRAFFIC (Erl)'},
                            {'key': 'psconges', 'label': 'PS CONGESTION (%)'},
                            {'key': 'csconges', 'label': 'CS CONGESTION (%)'}
                        ],
                        '4g': [
                            {'key': 'traffic', 'label': 'TOTAL TRAFFIC (GB)'},
                            {'key': 'user_dl_avg_thput', 'label': 'USER DL AVG THPUT (Mbps)'},
                            {'key': 'res_blk_dl', 'label': 'RES BLOCK DL (%)'},
                            {'key': 'cqi_4g', 'label': 'CQI 4G'}
                        ],
                        '5g': [
                            {'key': 'traffic', 'label': 'TOTAL TRAFFIC (GB)'},
                            {'key': 'user_dl_avg_throughput', 'label': 'USER DL AVG THPUT (Mbps)'},
                            {'key': 'cqi_5g', 'label': 'CQI 5G'}
                        ]
                    }
                    
                    current_metrics = metrics_config.get(selected_tech, [])

                    # 3. Tạo dữ liệu cho từng biểu đồ riêng biệt
                    for metric in current_metrics:
                        metric_key = metric['key']
                        metric_label = metric['label']
                        
                        datasets = []
                        # Tạo dataset cho từng cell
                        for i, cell_code in enumerate(target_cells):
                            # Tìm dữ liệu của cell này
                            cell_data = data_by_cell.get(cell_code, [])
                            
                            # Map dữ liệu vào trục thời gian chung (all_labels)
                            # Nếu ngày nào thiếu dữ liệu thì điền None (đứt nét) hoặc 0
                            data_map = {item.thoi_gian: getattr(item, metric_key, 0) or 0 for item in cell_data}
                            aligned_data = [data_map.get(label, None) for label in all_labels]
                            
                            # Chọn màu (xoay vòng nếu nhiều hơn số màu có sẵn)
                            color = colors[i % len(colors)]
                            
                            datasets.append({
                                'label': cell_code,
                                'data': aligned_data,
                                'borderColor': color,
                                'backgroundColor': color,
                                'tension': 0.1,
                                'fill': False,
                                'spanGaps': True # Nối liền nếu thiếu dữ liệu giữa chừng
                            })
                        
                        # Lưu cấu hình biểu đồ
                        chart_id = f"chart_{metric_key}"
                        charts[chart_id] = json.dumps({
                            'title': metric_label,
                            'labels': all_labels,
                            'datasets': datasets
                        })

    return render_page(CONTENT_TEMPLATE, title="Báo cáo KPI", active_page='kpi', 
                       selected_tech=selected_tech, cell_name_input=cell_name_input, charts=charts)

@app.route('/rf')
@login_required
def rf():
    tech = request.args.get('tech', '3g')
    action = request.args.get('action')
    
    model_map = {'3g': RF3G, '4g': RF4G, '5g': RF5G}
    CurrentModel = model_map.get(tech, RF3G)
    
    if action == 'export':
        def generate():
            yield '\ufeff'.encode('utf-8')
            header = [c.key for c in CurrentModel.__table__.columns]
            yield (','.join(header) + '\n').encode('utf-8')
            query = db.select(CurrentModel).execution_options(yield_per=100)
            result = db.session.execute(query)
            for row in result.scalars():
                row_data = []
                for col in header:
                    val = getattr(row, col)
                    if val is None: val = ''
                    val = str(val).replace(',', ';').replace('\n', ' ')
                    row_data.append(val)
                yield (','.join(row_data) + '\n').encode('utf-8')

        return Response(stream_with_context(generate()), mimetype='text/csv', 
                       headers={"Content-Disposition": f"attachment; filename=RF_{tech.upper()}.csv"})

    data = CurrentModel.query.limit(500).all()
    return render_page(CONTENT_TEMPLATE, title="Dữ liệu RF", active_page='rf', rf_data=data, current_tech=tech)

@app.route('/rf/add', methods=['GET', 'POST'])
@login_required
def rf_add():
    tech = request.args.get('tech', '3g')
    model_class = {'3g': RF3G, '4g': RF4G, '5g': RF5G}.get(tech)
    
    if not model_class:
        flash('Công nghệ không hợp lệ', 'danger')
        return redirect(url_for('rf'))
    
    if request.method == 'POST':
        try:
            data = {}
            for col in model_class.__table__.columns:
                if col.key == 'id': continue
                val = request.form.get(col.key)
                if val == '': val = None
                data[col.key] = val
            
            new_obj = model_class(**data)
            db.session.add(new_obj)
            db.session.commit()
            flash('Thêm mới thành công!', 'success')
            return redirect(url_for('rf', tech=tech))
        except Exception as e:
            db.session.rollback()
            flash(f'Lỗi thêm mới: {str(e)}', 'danger')

    columns = [c.key for c in model_class.__table__.columns if c.key != 'id']
    return render_page(RF_FORM_TEMPLATE, title=f"Thêm mới RF {tech.upper()}", columns=columns, tech=tech, obj=None)

@app.route('/rf/edit/<tech>/<int:id>', methods=['GET', 'POST'])
@login_required
def rf_edit(tech, id):
    model_class = {'3g': RF3G, '4g': RF4G, '5g': RF5G}.get(tech)
    if not model_class: return redirect(url_for('rf'))

    obj = db.session.get(model_class, id)
    if not obj:
        flash('Không tìm thấy bản ghi', 'danger')
        return redirect(url_for('rf', tech=tech))

    if request.method == 'POST':
        try:
            for col in model_class.__table__.columns:
                if col.key == 'id': continue
                val = request.form.get(col.key)
                if val == '': val = None
                setattr(obj, col.key, val)
            
            db.session.commit()
            flash('Cập nhật thành công!', 'success')
            return redirect(url_for('rf', tech=tech))
        except Exception as e:
            db.session.rollback()
            flash(f'Lỗi cập nhật: {str(e)}', 'danger')

    obj_dict = obj.__dict__
    columns = [c.key for c in model_class.__table__.columns if c.key != 'id']
    return render_page(RF_FORM_TEMPLATE, title=f"Sửa RF {tech.upper()}", columns=columns, tech=tech, obj=obj_dict)

@app.route('/rf/delete/<tech>/<int:id>')
@login_required
def rf_delete(tech, id):
    model_class = {'3g': RF3G, '4g': RF4G, '5g': RF5G}.get(tech)
    if model_class:
        obj = db.session.get(model_class, id)
        if obj:
            db.session.delete(obj)
            db.session.commit()
            flash('Đã xóa bản ghi thành công', 'success')
        else:
            flash('Không tìm thấy bản ghi', 'warning')
    return redirect(url_for('rf', tech=tech))

@app.route('/rf/reset')
@login_required
def rf_reset():
    tech = request.args.get('type') # 3g, 4g, 5g
    if current_user.role != 'admin':
        flash('Chỉ Admin mới có quyền xóa toàn bộ dữ liệu!', 'danger')
        return redirect(url_for('import_data'))

    model_class = {'3g': RF3G, '4g': RF4G, '5g': RF5G}.get(tech)
    
    if model_class:
        try:
            db.session.query(model_class).delete()
            db.session.commit()
            flash(f'Đã xóa toàn bộ dữ liệu RF {tech.upper()} thành công!', 'success')
        except Exception as e:
            db.session.rollback()
            flash(f'Lỗi khi xóa dữ liệu: {str(e)}', 'danger')
    else:
        flash('Loại dữ liệu không hợp lệ', 'danger')
        
    return redirect(url_for('import_data'))

@app.route('/rf/detail/<tech>/<int:id>')
@login_required
def rf_detail(tech, id):
    model_class = {'3g': RF3G, '4g': RF4G, '5g': RF5G}.get(tech)
    if not model_class: return redirect(url_for('rf'))
    
    obj = db.session.get(model_class, id)
    if not obj:
        flash('Không tìm thấy bản ghi', 'danger')
        return redirect(url_for('rf', tech=tech))
        
    return render_page(RF_DETAIL_TEMPLATE, obj=obj.__dict__, tech=tech)

# --- ROUTES CHO TÍNH NĂNG CONGESTION 3G ---
@app.route('/conges-3g')
@login_required
def conges_3g():
    dates_query = db.session.query(KPI3G.thoi_gian).distinct().order_by(KPI3G.thoi_gian.desc()).limit(3).all()
    if len(dates_query) < 3:
        found_dates = [d[0] for d in dates_query]
        flash(f'Chưa đủ 3 ngày dữ liệu KPI 3G để phân tích xu hướng. Hiện có: {", ".join(found_dates)}', 'warning')
        return render_page(CONTENT_TEMPLATE, title="Cảnh báo Nghẽn 3G", active_page='conges_3g', conges_data=[], dates=found_dates)
    
    target_dates = [d[0] for d in dates_query]
    
    subquery = db.session.query(KPI3G.ten_cell, KPI3G.thoi_gian).filter(
        KPI3G.thoi_gian.in_(target_dates),
        (
            ((KPI3G.csconges > 2) & (KPI3G.cs_so_att > 100)) | 
            ((KPI3G.psconges > 2) & (KPI3G.ps_so_att > 500))
        )
    ).all()
    
    cell_counts = defaultdict(set)
    for cell_name, date in subquery:
        cell_counts[cell_name].add(date)
    
    congested_cells = [cell for cell, dates in cell_counts.items() if len(dates) == 3]
    
    results = []
    if congested_cells:
        rf_info = RF3G.query.filter(RF3G.cell_code.in_(congested_cells)).all()
        rf_map = {r.cell_code: r for r in rf_info}
        
        for cell in congested_cells:
            rf = rf_map.get(cell)
            results.append({
                'cell_name': cell,
                'rf_id': rf.id if rf else '#',
                'site_code': rf.site_code if rf else 'N/A',
                'csht': rf.csht_code if rf else 'N/A',
                'antena': rf.antena if rf else 'N/A',
                'tilt': rf.total_tilt if rf else 'N/A'
            })
            
    return render_page(CONTENT_TEMPLATE, title="Cảnh báo Nghẽn 3G (3 ngày liên tiếp)", active_page='conges_3g', conges_data=results, dates=target_dates)


@app.route('/poi')
@login_required
def poi(): return render_page(CONTENT_TEMPLATE, title="POI", active_page='poi')
@app.route('/worst-cell')
@login_required
def worst_cell(): return render_page(CONTENT_TEMPLATE, title="Worst Cell", active_page='worst_cell')
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
        files = request.files.getlist('file')
        import_type = request.args.get('type') # 3g, 4g, 5g, kpi3g, kpi4g, kpi5g
        
        if not files or files[0].filename == '':
            flash('Chưa chọn file!', 'warning'); return redirect(url_for('import_data'))

        type_config = {
            '3g': {'model': RF3G, 'required': ['antena', 'azimuth']}, # RF 3G
            '4g': {'model': RF4G, 'required': ['enodeb_id', 'pci']},    # RF 4G
            '5g': {'model': RF5G, 'required': ['gnodeb_id', 'pci']},   # RF 5G
            'kpi3g': {'model': KPI3G, 'required': ['traffic', 'cssr']}, # KPI 3G (Traffic/CSSR)
            'kpi4g': {'model': KPI4G, 'required': ['traffic_vol_dl', 'erab_ssrate_all']}, # KPI 4G
            'kpi5g': {'model': KPI5G, 'required': ['dl_traffic_volume_gb']} # KPI 5G
        }
        
        config = type_config.get(import_type)
        if not config:
            flash('Loại import không hợp lệ', 'danger'); return redirect(url_for('import_data'))

        target_model = config['model']
        required_cols = config['required']
        total_rows_imported = 0
        
        try:
            for file in files:
                filename = file.filename
                
                def clean_col(col_name):
                    col_name = str(col_name).strip()
                    map_kpi = {
                        'UL Traffic Volume (GB)': 'ul_traffic_volume_gb',
                        'DL Traffic Volume (GB)': 'dl_traffic_volume_gb',
                        'Total Data Traffic Volume (GB)': 'traffic', 
                        'Cell Uplink Average Throughput': 'cell_uplink_average_throughput',
                        'Cell Downlink Average Throughput': 'cell_downlink_average_throughput',
                        'A User Downlink Average Throughput': 'user_dl_avg_throughput', 
                        'CQI_5G': 'cqi_5g', 
                        'Cell avaibility rate': 'cell_avaibility_rate',
                        'SgNB Addition Success Rate': 'sgnb_addition_success_rate',
                        'SgNB Abnormal Release Rate': 'sgnb_abnormal_release_rate',
                        'Frenquency': 'frequency', 'Hãng_SX': 'hang_sx', 'Hãng SX': 'hang_sx', 
                        'ENodeBID': 'enodeb_id', 'gNodeB ID': 'gnodeb_id', 'GNODEB_ID': 'gnodeb_id',
                        'CELL_ID': 'cell_id', 'SITE_NAME': 'site_name', 'Đồng_bộ': 'dong_bo',
                        'Thời gian': 'thoi_gian', 'Nhà cung cấp': 'nha_cung_cap', 'Tỉnh': 'tinh',
                        'Tên RNC': 'ten_rnc', 'Tên CELL': 'ten_cell', 'Mã VNP': 'ma_vnp', 'Loại NE': 'loai_ne',
                        'Tên GNODEB': 'ten_gnodeb', 'TRAFFIC': 'traffic', 'CSSR': 'cssr', 'DCR': 'dcr',
                        'TRAFFIC_VOL_DL': 'traffic_vol_dl', 'TRAFFIC_VOL_UL': 'traffic_vol_ul',
                        'CELL_DL_AVG_THPUTS': 'cell_dl_avg_thputs', 'UNVAILABLE': 'unvailable',
                        'Antena': 'antena', 'Anten_height': 'anten_height', 'Azimuth': 'azimuth',
                        'PCI': 'pci',
                        'CS_SO_ATT': 'cs_so_att', 'PS_SO_ATT': 'ps_so_att', 'CSCONGES': 'csconges', 'PSCONGES': 'psconges',
                        'PSTRAFFIC': 'pstraffic', 'RES_BLK_DL': 'res_blk_dl', 'CQI_4G': 'cqi_4g'
                    }
                    if col_name in map_kpi: return map_kpi[col_name]
                    clean = col_name.lower()
                    clean = re.sub(r'[^a-z0-9_]', '_', clean)
                    return clean

                valid_db_columns = [c.key for c in target_model.__table__.columns if c.key != 'id']

                if filename.lower().endswith('.csv'):
                    df_header = pd.read_csv(file, nrows=0)
                elif filename.lower().endswith(('.xls', '.xlsx')):
                    df_header = pd.read_excel(file, nrows=0)
                else:
                    flash(f'Bỏ qua {filename}: Định dạng file không hỗ trợ', 'warning')
                    continue

                file_cols = [clean_col(c) for c in df_header.columns]
                
                missing_cols = [req for req in required_cols if req not in file_cols]
                if missing_cols:
                    flash(f"LỖI FILE {filename}: Không đúng định dạng {import_type.upper()}. Thiếu cột: {', '.join(missing_cols)}", 'danger')
                    continue

                file.stream.seek(0)

                if filename.lower().endswith('.csv'):
                    for chunk in pd.read_csv(file, chunksize=2000):
                        chunk.columns = [clean_col(c) for c in chunk.columns]
                        bulk_data = []
                        for row in chunk.to_dict(orient='records'):
                            filtered_row = {k: v for k, v in row.items() if k in valid_db_columns}
                            for k, v in filtered_row.items():
                                if pd.isna(v): filtered_row[k] = None
                            
                            if import_type == 'kpi4g' and 'traffic' not in filtered_row:
                                if 'traffic_vol_dl' in filtered_row:
                                    filtered_row['traffic'] = filtered_row['traffic_vol_dl']

                            if filtered_row: bulk_data.append(filtered_row)
                        
                        if bulk_data:
                            db.session.bulk_insert_mappings(target_model, bulk_data)
                            db.session.commit()
                            total_rows_imported += len(bulk_data)
                        del chunk; gc.collect()

                elif filename.lower().endswith(('.xls', '.xlsx')):
                    df = pd.read_excel(file)
                    df.columns = [clean_col(c) for c in df.columns]
                    records = df.to_dict(orient='records')
                    del df; gc.collect()
                    
                    batch_size = 2000
                    for i in range(0, len(records), batch_size):
                        batch = records[i:i + batch_size]
                        bulk_data = []
                        for row in batch:
                            filtered_row = {k: v for k, v in row.items() if k in valid_db_columns}
                            for k, v in filtered_row.items():
                                if pd.isna(v): filtered_row[k] = None
                            
                            if import_type == 'kpi4g' and 'traffic' not in filtered_row:
                                if 'traffic_vol_dl' in filtered_row:
                                    filtered_row['traffic'] = filtered_row['traffic_vol_dl']

                            if filtered_row: bulk_data.append(filtered_row)
                        if bulk_data:
                            db.session.bulk_insert_mappings(target_model, bulk_data)
                            db.session.commit()
                            total_rows_imported += len(bulk_data)
                        gc.collect()

            if total_rows_imported > 0:
                flash(f'Đã import thành công {total_rows_imported} bản ghi!', 'success')

        except Exception as e:
            db.session.rollback()
            flash(f'Lỗi khi import: {str(e)}', 'danger')
            print(f"IMPORT ERROR: {e}")

        return redirect(url_for('import_data'))

    dates_3g = []
    dates_4g = []
    dates_5g = []
    try:
        kpi3g_dates = db.session.query(KPI3G.thoi_gian).distinct().order_by(KPI3G.thoi_gian.desc()).all()
        dates_3g = [d[0] for d in kpi3g_dates]
        kpi4g_dates = db.session.query(KPI4G.thoi_gian).distinct().order_by(KPI4G.thoi_gian.desc()).all()
        dates_4g = [d[0] for d in kpi4g_dates]
        kpi5g_dates = db.session.query(KPI5G.thoi_gian).distinct().order_by(KPI5G.thoi_gian.desc()).all()
        dates_5g = [d[0] for d in kpi5g_dates]
    except Exception as e:
        print(f"Error fetching dates: {e}")

    kpi_rows = list(zip_longest(dates_3g, dates_4g, dates_5g, fillvalue=None))

    return render_page(CONTENT_TEMPLATE, title="Import Dữ liệu", active_page='import', kpi_rows=kpi_rows)

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

# --- BACKUP / RESTORE ROUTES ---
@app.route('/backup-restore')
@login_required
def backup_restore():
    if current_user.role != 'admin': return redirect(url_for('index'))
    return render_page(BACKUP_RESTORE_TEMPLATE, title="Backup & Restore", active_page='backup_restore')

@app.route('/backup', methods=['POST'])
@login_required
def backup_db():
    if current_user.role != 'admin': return redirect(url_for('index'))
    
    models = [User, RF3G, RF4G, RF5G, KPI3G, KPI4G, KPI5G]
    buffer = BytesIO()
    
    with zipfile.ZipFile(buffer, 'w') as zf:
        for model in models:
            # Query all records
            records = model.query.all()
            if records:
                data = []
                for r in records:
                    row = r.__dict__.copy()
                    row.pop('_sa_instance_state', None)
                    data.append(row)
                df = pd.DataFrame(data)
                csv_data = df.to_csv(index=False)
                zf.writestr(f"{model.__tablename__}.csv", csv_data)
            else:
                # Create empty csv with headers
                cols = [c.key for c in model.__table__.columns]
                df = pd.DataFrame(columns=cols)
                csv_data = df.to_csv(index=False)
                zf.writestr(f"{model.__tablename__}.csv", csv_data)
                
    buffer.seek(0)
    filename = f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip"
    return send_file(buffer, download_name=filename, as_attachment=True)

@app.route('/restore', methods=['POST'])
@login_required
def restore_db():
    if current_user.role != 'admin': return redirect(url_for('index'))
    
    file = request.files.get('file')
    if not file or not file.filename.endswith('.zip'):
        flash('Vui lòng chọn file .zip', 'danger')
        return redirect(url_for('backup_restore'))
        
    try:
        with zipfile.ZipFile(file) as zf:
            table_map = {
                'user.csv': User,
                'rf_3g.csv': RF3G,
                'rf_4g.csv': RF4G,
                'rf_5g.csv': RF5G,
                'kpi_3g.csv': KPI3G,
                'kpi_4g.csv': KPI4G,
                'kpi_5g.csv': KPI5G
            }
            
            for filename in zf.namelist():
                if filename in table_map:
                    model = table_map[filename]
                    with zf.open(filename) as f:
                        df = pd.read_csv(f)
                        
                        # Clear existing data
                        db.session.query(model).delete()
                        
                        # Insert new data
                        records = df.to_dict(orient='records')
                        
                        # Handle NaN values
                        for row in records:
                            for k, v in row.items():
                                if pd.isna(v): row[k] = None
                                
                        if records:
                            db.session.bulk_insert_mappings(model, records)
                            
            db.session.commit()
            flash('Khôi phục dữ liệu thành công!', 'success')
            
    except Exception as e:
        db.session.rollback()
        flash(f'Lỗi khôi phục: {str(e)}', 'danger')
        
    return redirect(url_for('backup_restore'))

if __name__ == '__main__':
    app.run(debug=True)
